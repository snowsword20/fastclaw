package workspace

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// HistoryBackend is the per-scope workspace version history behind the
// agent loop's turn-boundary snapshots, the fork/variant restore path and
// the REST history endpoints. Two implementations exist:
//
//   - History (history.go): git bare repos on the host disk. The
//     single-host default — zero extra infrastructure, content dedup via
//     git's object store.
//   - CloudHistory (history_cloud.go): content-addressed blobs in the
//     object store plus per-turn manifests in the object store (D1) or
//     the SQL store (D2). Required for multi-pod deployments, where no
//     host directory is shared between pods.
//
// Everything is best-effort by contract: implementations return errors,
// callers log them and move on — a failed snapshot must never fail a turn.

type HistoryBackend interface {
	// Commit snapshots the tree at src into the scope's history. message
	// carries the turn coordinate ("turn msgs=<n>") so a fork can pick
	// the snapshot AT its fork point. Unchanged trees are skipped without
	// error (no empty snapshots).
	Commit(ctx context.Context, scope HistScope, src TreeSource, message string) error

	// List returns the scope's snapshots, newest first. No history yet
	// yields an empty slice (not an error).
	List(ctx context.Context, scope HistScope) ([]Entry, error)

	// Restore materializes the snapshot identified by id into dst with
	// overlay semantics: files present in dst but absent from the
	// snapshot are left untouched — matching the git backend's
	// `git checkout <hash> -- .` behavior.
	Restore(ctx context.Context, scope HistScope, id string, dst TreeSink) error
}

// HistScope identifies one workspace-history scope. Key() produces the
// flat scope string the git backend and the REST API have always used
// (chatID, or "<projectID>-<chatID>" for project chats); the cloud
// backends keep the fields separate so storage keys stay unambiguous.
type HistScope struct {
	AgentID   string
	ProjectID string
	Session   string // workspace scope: resolved session key (web) or raw chatID (IM)
}

// Key returns the flat scope string. Project chats prefix the project id;
// session keys may themselves contain dashes, so the result is never
// parsed back apart — consumers that need the triple use HistScope
// directly (that is exactly why the cloud backends take the struct).
func (s HistScope) Key() string {
	if s.ProjectID == "" {
		return s.Session
	}
	return s.ProjectID + "-" + s.Session
}

// TreeSource reads the current state of one workspace tree. Directory
// layouts differ between LocalFS and object stores, so consumers (loop,
// fork, REST handlers) pick the matching implementation at the call site.
type TreeSource interface {
	// Files lists every file in the tree, recursive, slash-separated
	// paths relative to the tree root. Directories are not listed.
	Files(ctx context.Context) ([]TreeFile, error)

	// Open returns a reader for one path previously returned by Files.
	Open(ctx context.Context, p string) (io.ReadCloser, error)
}

// TreeFile describes one file in a workspace tree. Size/ModTime feed the
// cloud backend's changed-file fast path; ModTime may be zero when the
// backend doesn't track it (then the file is always re-read).
type TreeFile struct {
	Path    string
	Size    int64
	ModTime time.Time
}

// TreeSink receives the files of a restored snapshot.
type TreeSink interface {
	Put(ctx context.Context, p string, r io.Reader, size int64) error
}

// DirSource reads the tree from a host directory (LocalFS-backed
// workspaces; also used by tests).
type DirSource struct {
	Root string
}

// NewDirSource returns a TreeSource over the directory at root.
func NewDirSource(root string) *DirSource {
	return &DirSource{Root: root}
}

// Files walks the directory tree. A missing root is not an error — it
// yields an empty tree (the caller decides whether that means "nothing
// to snapshot").
func (d *DirSource) Files(ctx context.Context) ([]TreeFile, error) {
	if d.Root == "" {
		return nil, nil
	}
	if _, err := os.Stat(d.Root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []TreeFile
	err := filepath.WalkDir(d.Root, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(d.Root, p)
		if err != nil {
			return err
		}
		out = append(out, TreeFile{
			Path:    filepath.ToSlash(rel),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Open opens one file inside the root. Paths from Files are trusted, but
// the join is still guarded so a crafted path can't escape the root.
func (d *DirSource) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	full, err := d.resolve(p)
	if err != nil {
		return nil, err
	}
	return os.Open(full)
}

func (d *DirSource) resolve(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("workspace history: empty path")
	}
	clean := path.Clean("/" + p)[1:] // strips any leading .. escape
	if clean == "" || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("workspace history: path escapes tree root: %q", p)
	}
	return filepath.Join(d.Root, filepath.FromSlash(clean)), nil
}

// DirSink writes restored files into a host directory, creating parent
// directories as needed.
type DirSink struct {
	Root string
}

// NewDirSink returns a TreeSink writing into the directory at root.
func NewDirSink(root string) *DirSink {
	return &DirSink{Root: root}
}

func (d *DirSink) Put(ctx context.Context, p string, r io.Reader, size int64) error {
	root := DirSource{Root: d.Root}
	full, err := root.resolve(p)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	f, err := os.Create(full)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

// StoreSource reads the tree through the workspace Store — the cloud
// snapshot path. Snapshot == the store's durable state at turn end: any
// pod can snapshot any scope, and the result matches what a fresh pod
// would hydrate into a sandbox.
type StoreSource struct {
	store     Store
	agentID   string
	projectID string
	sessionID string
}

// NewStoreSource returns a TreeSource over one (agent, project, session)
// scope of the workspace Store.
func NewStoreSource(st Store, agentID, projectID, sessionID string) *StoreSource {
	return &StoreSource{store: st, agentID: agentID, projectID: projectID, sessionID: sessionID}
}

func (s *StoreSource) Files(ctx context.Context) ([]TreeFile, error) {
	objs, err := s.store.List(ctx, s.agentID, s.projectID, s.sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]TreeFile, 0, len(objs))
	for _, o := range objs {
		out = append(out, TreeFile{Path: o.Path, Size: o.Size, ModTime: o.ModTime})
	}
	return out, nil
}

func (s *StoreSource) Open(ctx context.Context, p string) (io.ReadCloser, error) {
	return s.store.Get(ctx, s.agentID, s.projectID, s.sessionID, p)
}

// StoreSink writes restored files back into a workspace Store scope —
// the cloud restore path (fork materialization, REST restore).
type StoreSink struct {
	store     Store
	agentID   string
	projectID string
	sessionID string
}

// NewStoreSink returns a TreeSink writing into one workspace Store scope.
func NewStoreSink(st Store, agentID, projectID, sessionID string) *StoreSink {
	return &StoreSink{store: st, agentID: agentID, projectID: projectID, sessionID: sessionID}
}

func (s *StoreSink) Put(ctx context.Context, p string, r io.Reader, size int64) error {
	// contentType "": backends fall back to the extension-derived mime
	// (see S3.Put), same as ordinary file-tool writes.
	return s.store.Put(ctx, s.agentID, s.projectID, s.sessionID, p, r, size, "")
}
