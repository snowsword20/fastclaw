package workspace

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// memBlobs is an in-memory BlobStore that counts uploads — the probe for
// the content-addressing assertions (dedup, idempotent rewrite).
type memBlobs struct {
	mu      sync.Mutex
	data    map[string][]byte
	uploads int
}

func newMemBlobs() *memBlobs {
	return &memBlobs{data: map[string][]byte{}}
}

func (m *memBlobs) PutBlob(_ context.Context, hash string, r io.Reader, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads++
	m.data[hash] = b
	return nil
}

func (m *memBlobs) GetBlob(_ context.Context, hash string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[hash]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(strings.NewReader(string(b))), nil
}

func (m *memBlobs) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.uploads
}

// memManifests is an in-memory ManifestStore keyed by flat scope.
type memManifests struct {
	mu    sync.Mutex
	rows  map[string][]ManifestRecord
	saves int
}

func newMemManifests() *memManifests {
	return &memManifests{rows: map[string][]ManifestRecord{}}
}

func (m *memManifests) Save(_ context.Context, scope HistScope, rec *ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	key := scope.Key()
	// prepend so List's newest-first ordering holds without sorting
	m.rows[key] = append([]ManifestRecord{*rec}, m.rows[key]...)
	return nil
}

func (m *memManifests) List(_ context.Context, scope HistScope) ([]ManifestRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := m.rows[scope.Key()]
	out := make([]ManifestRecord, len(rows))
	copy(out, rows)
	return out, nil
}

func (m *memManifests) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saves
}

func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for p, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func newCloudTestHistory() (*CloudHistory, *memBlobs, *memManifests) {
	blobs := newMemBlobs()
	manifests := newMemManifests()
	return NewCloudHistory(blobs, manifests), blobs, manifests
}

func TestCloudHistoryCommitAndRestore(t *testing.T) {
	h, blobs, _ := newCloudTestHistory()
	ctx := context.Background()
	srcDir := t.TempDir()
	writeTree(t, srcDir, map[string]string{
		"a.txt":        "v1",
		"sub/b.txt":    "hello",
		"sub/deep.txt": "deep",
	})
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=2"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	entries, err := h.List(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Msgs != 2 || entries[0].Message != "turn msgs=2" {
		t.Fatalf("entry should carry the fork coordinate: %+v", entries[0])
	}

	// restore into a different dir — overlay into empty target
	dstDir := t.TempDir()
	if err := h.Restore(ctx, scope, entries[0].Hash, NewDirSink(dstDir)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	for p, want := range map[string]string{
		"a.txt":        "v1",
		"sub/b.txt":    "hello",
		"sub/deep.txt": "deep",
	} {
		data, err := os.ReadFile(filepath.Join(dstDir, filepath.FromSlash(p)))
		if err != nil || string(data) != want {
			t.Fatalf("restored %s = %q (%v), want %q", p, data, err, want)
		}
	}

	// blobs are content-addressed: 3 files → 3 distinct blobs
	if len(blobs.data) != 3 {
		t.Fatalf("expected 3 blobs, got %d", len(blobs.data))
	}

	// restore of an unknown id must fail cleanly
	if err := h.Restore(ctx, scope, "snap-does-not-exist", NewDirSink(dstDir)); err == nil {
		t.Fatal("unknown snapshot id must error")
	}
}

func TestCloudHistoryRestoreOverlays(t *testing.T) {
	h, _, _ := newCloudTestHistory()
	ctx := context.Background()
	srcDir := t.TempDir()
	writeTree(t, srcDir, map[string]string{"a.txt": "v1"})
	scope := HistScope{AgentID: "ag", Session: "chat1"}
	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=1"); err != nil {
		t.Fatal(err)
	}
	entries, _ := h.List(ctx, scope)

	// git-checkout semantics: files present in dst but not in the
	// snapshot are left untouched.
	dstDir := t.TempDir()
	writeTree(t, dstDir, map[string]string{"extra.txt": "keep me"})
	if err := h.Restore(ctx, scope, entries[0].Hash, NewDirSink(dstDir)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dstDir, "extra.txt"))
	if err != nil || string(data) != "keep me" {
		t.Fatalf("overlay restore must keep extra files, got %q (%v)", data, err)
	}
}

func TestCloudHistorySkipsCleanTree(t *testing.T) {
	h, _, manifests := newCloudTestHistory()
	ctx := context.Background()
	srcDir := t.TempDir()
	writeTree(t, srcDir, map[string]string{"a.txt": "v1"})
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=1"); err != nil {
		t.Fatal(err)
	}
	// Same content, untouched mtime → clean tree → no second snapshot.
	time.Sleep(10 * time.Millisecond)
	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=2"); err != nil {
		t.Fatal(err)
	}
	if got := manifests.count(); got != 1 {
		t.Fatalf("clean tree must not snapshot twice, got %d manifests", got)
	}
	entries, _ := h.List(ctx, scope)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after clean re-commit, got %d", len(entries))
	}
}

func TestCloudHistoryDedupsUnchangedFiles(t *testing.T) {
	h, blobs, _ := newCloudTestHistory()
	ctx := context.Background()
	srcDir := t.TempDir()
	writeTree(t, srcDir, map[string]string{"a.txt": "stable", "b.txt": "v1"})
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("v2"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Touch a.txt's mtime WITHOUT changing content: the fast path keys on
	// (size, modtime), so a rewritten-but-identical file re-uploads — but
	// the blob store dedups by hash, so uploads converge to content.
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("stable"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, scope, NewDirSource(srcDir), "turn msgs=2"); err != nil {
		t.Fatal(err)
	}

	// b.txt v1 and a.txt were uploaded in turn 1 (2 uploads); turn 2
	// re-uploads a.txt (fast-path miss) + b.txt v2 → 4 total uploads but
	// only 3 DISTINCT blobs.
	if got := blobs.count(); got != 4 {
		t.Fatalf("expected 4 uploads (2+2), got %d", got)
	}
	if len(blobs.data) != 3 {
		t.Fatalf("expected 3 distinct blobs (content-addressed), got %d", len(blobs.data))
	}

	entries, _ := h.List(ctx, scope)
	if len(entries) != 2 || entries[0].Msgs != 2 || entries[1].Msgs != 1 {
		t.Fatalf("newest first with coordinates: %+v", entries)
	}
}

func TestCloudHistoryEmptyTreeDoesNotSnapshot(t *testing.T) {
	h, _, manifests := newCloudTestHistory()
	ctx := context.Background()
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	// Missing dir → empty tree → no snapshot, no error (matches git
	// backend's missing-worktree no-op).
	if err := h.Commit(ctx, scope, NewDirSource(filepath.Join(t.TempDir(), "nope")), "turn msgs=1"); err != nil {
		t.Fatalf("missing tree must no-op, got %v", err)
	}
	if got := manifests.count(); got != 0 {
		t.Fatalf("empty tree must not snapshot, got %d manifests", got)
	}
}

func TestCloudHistoryConcurrentCommitsAllLand(t *testing.T) {
	h, _, manifests := newCloudTestHistory()
	ctx := context.Background()
	srcDir := t.TempDir()
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	const n = 8
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			name := fmt.Sprintf("f%d.txt", i)
			if err := os.WriteFile(filepath.Join(srcDir, name), []byte(fmt.Sprintf("v%d", i)), 0o644); err != nil {
				done <- err
				return
			}
			done <- h.Commit(ctx, scope, NewDirSource(srcDir), "turn "+name)
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent commit must not fail: %v", err)
		}
	}
	if got := manifests.count(); got == 0 {
		t.Fatal("at least one snapshot must land")
	}
	entries, _ := h.List(ctx, scope)
	if len(entries) != manifests.count() {
		t.Fatalf("list must show every snapshot: %d vs %d", len(entries), manifests.count())
	}
}

func TestCloudHistoryRestoreFromStoreSink(t *testing.T) {
	// The cloud path end-to-end through the StoreSource/StoreSink pair —
	// the exact shape a multi-pod fork restore takes (no host dirs).
	h, _, _ := newCloudTestHistory()
	ctx := context.Background()
	store := NewLocalFS(t.TempDir())
	scope := HistScope{AgentID: "ag", ProjectID: "proj", Session: "chat1"}

	// Seed the workspace store as file tools would.
	for p, content := range map[string]string{"notes.md": "v1", "img/out.png": "PNG"} {
		if err := store.Put(ctx, scope.AgentID, scope.ProjectID, scope.Session, p, strings.NewReader(content), int64(len(content)), ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Commit(ctx, scope, NewStoreSource(store, scope.AgentID, scope.ProjectID, scope.Session), "turn msgs=1"); err != nil {
		t.Fatalf("commit from store source: %v", err)
	}

	// Update + re-commit, then restore the fork-point state into a NEW
	// session's store scope (what forkWorkspaceFiles does).
	if err := store.Put(ctx, scope.AgentID, scope.ProjectID, scope.Session, "notes.md", strings.NewReader("v2"), 2, ""); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, scope, NewStoreSource(store, scope.AgentID, scope.ProjectID, scope.Session), "turn msgs=2"); err != nil {
		t.Fatal(err)
	}
	entries, _ := h.List(ctx, scope)

	dstScope := HistScope{AgentID: scope.AgentID, ProjectID: scope.ProjectID, Session: "chat1-fork"}
	if err := h.Restore(ctx, scope, entries[1].Hash, NewStoreSink(store, dstScope.AgentID, dstScope.ProjectID, dstScope.Session)); err != nil {
		t.Fatalf("fork-point restore into store sink: %v", err)
	}
	rc, err := store.Get(ctx, dstScope.AgentID, dstScope.ProjectID, dstScope.Session, "notes.md")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "v1" {
		t.Fatalf("fork branch should carry fork-point content v1, got %q", data)
	}
}

func TestSnapshotIDOrdering(t *testing.T) {
	// The zero-padded nanos is load-bearing: object-store manifests sort
	// lexicographically, so the timestamp field must be fixed-width for
	// lexicographic order to equal time order. Snapshot ids minted
	// within the same clock tick share their timestamp and carry no
	// relative order — that's fine, they are simultaneous snapshots and
	// fork matching keys on msgs, not on intra-tick order.
	mk := func(nanos int64, suffix string) string {
		return fmt.Sprintf("snap-%020d-%s", nanos, suffix)
	}
	ids := []string{mk(200, "aaaa"), mk(300, "aaaa"), mk(100, "aaaa")}
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	want := []string{mk(300, "aaaa"), mk(200, "aaaa"), mk(100, "aaaa")}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("desc lexicographic must equal desc time order: %v", ids)
		}
	}
	// The real minter pads to 20 digits.
	ts := strings.Split(newSnapshotID(), "-")[1]
	if len(ts) != 20 {
		t.Fatalf("nanos field must be zero-padded 20 digits, got %q", ts)
	}
}
