package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

// BlobStore stores content-addressed payload blobs for cloud snapshots.
// The hash IS the storage key: uploads of identical content are
// idempotent and any pod can fetch any blob.
type BlobStore interface {
	PutBlob(ctx context.Context, hash string, r io.Reader, size int64) error
	GetBlob(ctx context.Context, hash string) (io.ReadCloser, error)
}

// ManifestFile is one file's entry inside a snapshot manifest. Field
// names are kept short — manifests list every file of every turn.
type ManifestFile struct {
	Hash    string `json:"h"` // sha256 hex; the blob key
	Size    int64  `json:"s"`
	ModTime int64  `json:"m"` // unix nanos of the source file; 0 = unknown (never fast-pathed)
}

// ManifestRecord is one turn-boundary snapshot: the tree state as a
// path→file map plus the fork coordinate ("turn msgs=<n>"). Stored as a
// JSON document (object store) or a TEXT column (SQL store).
type ManifestRecord struct {
	ID      string                  `json:"id"`
	Message string                  `json:"message"`
	Msgs    int                     `json:"msgs"`
	Time    int64                   `json:"time"` // unix nanos
	Files   map[string]ManifestFile `json:"files"`
}

// ManifestStore persists snapshot manifests for cloud history scopes.
// Implementations must be concurrency-safe and append-only friendly:
// Save never updates an existing manifest, List returns newest first.
type ManifestStore interface {
	Save(ctx context.Context, scope HistScope, m *ManifestRecord) error
	List(ctx context.Context, scope HistScope) ([]ManifestRecord, error)
}

// CloudHistory is the object-store HistoryBackend for multi-pod
// deployments: content-addressed blobs hold file payloads (in the object
// store — never in SQL, matching the store layer's binary-content rule),
// per-turn manifests hold the path→blob mapping.
//
// Snapshot semantics match the git backend exactly:
//   - whole-tree snapshot at the turn boundary (captures file tools,
//     exec flushes and uploads at one consistency point);
//   - unchanged trees are skipped (the manifest diff plays git's
//     `status --porcelain` role);
//   - messages carry the "turn msgs=<n>" fork coordinate;
//   - restore is an overlay (extra destination files stay).
//
// Storage backend roles are injected: blobs always come from the object
// store (ObjectHistoryStore), manifests from the object store (D1) or
// the SQL store (D2) depending on deployment shape — see
// gateway/history_backend.go for the selection logic.
type CloudHistory struct {
	blobs     BlobStore
	manifests ManifestStore
	// locks serializes commits per scope, same rationale as the git
	// backend (two turns racing one manifest chain). Cross-POD races are
	// inherently safe: blobs are content-addressed (idempotent writes)
	// and manifests are append-only.
	locks sync.Map // flat scope key -> *sync.Mutex
}

// Compile-time proof CloudHistory satisfies the backend contract.
var _ HistoryBackend = (*CloudHistory)(nil)

// NewCloudHistory assembles the cloud backend from its storage roles.
func NewCloudHistory(blobs BlobStore, manifests ManifestStore) *CloudHistory {
	return &CloudHistory{blobs: blobs, manifests: manifests}
}

func (h *CloudHistory) lockFor(key string) *sync.Mutex {
	lock, _ := h.locks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// Commit snapshots the source tree. Changed files are streamed once
// through sha256 into a spool file, then uploaded under their hash; files
// whose size+modtime match the latest manifest reuse the recorded hash
// without a re-read. A tree identical to the latest manifest produces no
// snapshot (clean-tree skip).
func (h *CloudHistory) Commit(ctx context.Context, scope HistScope, src TreeSource, message string) error {
	if scope.Session == "" || src == nil {
		return fmt.Errorf("workspace history: scope and source required")
	}
	key := scope.Key()
	lock := h.lockFor(key)
	lock.Lock()
	defer lock.Unlock()

	files, err := src.Files(ctx)
	if err != nil {
		return fmt.Errorf("workspace history: list tree: %w", err)
	}
	if len(files) == 0 {
		return nil // nothing to snapshot
	}

	// Latest manifest = the diff base. A missing/unreadable base is not
	// fatal: we just re-read and re-upload everything (blobs are
	// content-addressed, so the bucket converges regardless).
	var latest *ManifestRecord
	latest, err = h.latestManifest(ctx, scope)
	if err != nil {
		slog.Warn("workspace history: previous manifest unavailable, snapshotting full tree",
			"scope", key, "error", err)
		latest = nil
	}
	var prev map[string]ManifestFile
	if latest != nil {
		prev = latest.Files
	}

	entries := make(map[string]ManifestFile, len(files))
	spool := func(f TreeFile) (ManifestFile, error) {
		rc, err := src.Open(ctx, f.Path)
		if err != nil {
			return ManifestFile{}, fmt.Errorf("open %s: %w", f.Path, err)
		}
		defer rc.Close()
		mf, err := spoolFile(ctx, h.blobs, rc)
		if err != nil {
			return ManifestFile{}, fmt.Errorf("upload %s: %w", f.Path, err)
		}
		mf.Size = f.Size
		mf.ModTime = f.ModTime.UnixNano()
		return mf, nil
	}
	for _, f := range files {
		if pf, ok := prev[f.Path]; ok && fileUnchanged(pf, f) {
			entries[f.Path] = pf
			continue
		}
		mf, err := spool(f)
		if err != nil {
			return err
		}
		entries[f.Path] = mf
	}

	// Clean-tree skip: identical file set with identical hashes.
	if latest != nil && sameFiles(prev, entries) {
		return nil
	}

	m := ManifestRecord{
		ID:      newSnapshotID(),
		Message: message,
		Msgs:    entryMsgs(message),
		Time:    time.Now().UnixNano(),
		Files:   entries,
	}
	if err := h.manifests.Save(ctx, scope, &m); err != nil {
		return fmt.Errorf("workspace history: save manifest: %w", err)
	}
	slog.Info("workspace history committed", "scope", key, "snapshot", m.ID, "files", len(entries))
	return nil
}

// List maps the scope's manifests to API entries, newest first.
func (h *CloudHistory) List(ctx context.Context, scope HistScope) ([]Entry, error) {
	ms, err := h.manifests.List(ctx, scope)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(ms))
	for _, m := range ms {
		entries = append(entries, Entry{
			Hash:    m.ID,
			Message: m.Message,
			Time:    time.Unix(0, m.Time).UTC().Unix(),
			Msgs:    m.Msgs,
		})
	}
	return entries, nil
}

// Restore materializes the snapshot into dst (overlay semantics). Blobs
// are streamed straight from the object store into the sink — no host
// staging, so any pod can restore any scope.
func (h *CloudHistory) Restore(ctx context.Context, scope HistScope, id string, dst TreeSink) error {
	if dst == nil {
		return fmt.Errorf("workspace history: restore sink required")
	}
	ms, err := h.manifests.List(ctx, scope)
	if err != nil {
		return err
	}
	var m *ManifestRecord
	for i := range ms {
		if ms[i].ID == id {
			m = &ms[i]
			break
		}
	}
	if m == nil {
		return fmt.Errorf("workspace history: no snapshot %q for scope %q", id, scope.Key())
	}
	// Deterministic order for reproducible restores and readable logs.
	paths := make([]string, 0, len(m.Files))
	for p := range m.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		blob, err := h.blobs.GetBlob(ctx, m.Files[p].Hash)
		if err != nil {
			return fmt.Errorf("workspace history: fetch blob for %s: %w", p, err)
		}
		err = dst.Put(ctx, p, blob, m.Files[p].Size)
		blob.Close()
		if err != nil {
			return fmt.Errorf("workspace history: write %s: %w", p, err)
		}
	}
	slog.Info("workspace history restored", "scope", scope.Key(), "snapshot", id, "files", len(paths))
	return nil
}

// latestManifest returns the newest manifest of the scope, or nil when
// the scope has no history yet.
func (h *CloudHistory) latestManifest(ctx context.Context, scope HistScope) (*ManifestRecord, error) {
	ms, err := h.manifests.List(ctx, scope)
	if err != nil {
		return nil, err
	}
	if len(ms) == 0 {
		return nil, nil
	}
	return &ms[0], nil
}

// fileUnchanged reports whether the file can keep its previous blob:
// same size and same mtime. A zero recorded mtime (unknown) never
// matches — those files are always re-read, never wrongly skipped.
func fileUnchanged(pf ManifestFile, f TreeFile) bool {
	return pf.ModTime != 0 && pf.ModTime == f.ModTime.UnixNano() && pf.Size == f.Size
}

// sameFiles compares the previous and new file maps by path→hash.
func sameFiles(prev, cur map[string]ManifestFile) bool {
	if len(prev) != len(cur) {
		return false
	}
	for p, pf := range prev {
		cf, ok := cur[p]
		if !ok || pf.Hash != cf.Hash {
			return false
		}
	}
	return true
}

// spoolFile streams r once through sha256 into a temp file, uploads the
// temp file as a content-addressed blob and removes it. The hash must be
// known before the upload (the blob key IS the hash), so the single-pass
// reader goes to disk rather than memory — binary-heavy workspaces must
// not OOM the pod.
func spoolFile(ctx context.Context, blobs BlobStore, r io.Reader) (ManifestFile, error) {
	tmp, err := os.CreateTemp("", "fastclaw-hist-*")
	if err != nil {
		return ManifestFile{}, fmt.Errorf("spool temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, hasher), r)
	closeErr := tmp.Close()
	if err != nil {
		return ManifestFile{}, fmt.Errorf("spool: %w", err)
	}
	if closeErr != nil {
		return ManifestFile{}, fmt.Errorf("spool close: %w", closeErr)
	}
	hash := hex.EncodeToString(hasher.Sum(nil))

	f, err := os.Open(tmpPath)
	if err != nil {
		return ManifestFile{}, err
	}
	defer f.Close()
	if err := blobs.PutBlob(ctx, hash, f, size); err != nil {
		return ManifestFile{}, fmt.Errorf("put blob: %w", err)
	}
	return ManifestFile{Hash: hash, Size: size}, nil
}
