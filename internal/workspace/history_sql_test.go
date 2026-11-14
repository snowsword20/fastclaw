package workspace

import (
	"context"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/store"
)

// newSQLManifestStore opens the shared in-memory sqlite store used by the
// store package's own tests, migrates it (creating
// workspace_history_snapshots) and returns the D2 manifest backend.
func newSQLManifestStore(t *testing.T) (*SQLManifestStore, *store.DBStore) {
	t.Helper()
	dbs, err := store.NewDBStore("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := dbs.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewSQLManifestStore(dbs.DB(), dbs.Dialect()), dbs
}

func TestSQLManifestStoreRoundTrip(t *testing.T) {
	s, _ := newSQLManifestStore(t)
	ctx := context.Background()
	scope := HistScope{AgentID: "ag", ProjectID: "proj", Session: "chat1"}

	mk := func(id string, msgs int, files map[string]ManifestFile) *ManifestRecord {
		return &ManifestRecord{
			ID:      id,
			Message: "turn msgs=" + string(rune('0'+msgs)),
			Msgs:    msgs,
			Time:    time.Now().UnixNano(),
			Files:   files,
		}
	}
	first := mk("snap-0001", 1, map[string]ManifestFile{"a.txt": {Hash: "aa", Size: 2, ModTime: 111}})
	second := mk("snap-0002", 2, map[string]ManifestFile{
		"a.txt": {Hash: "aa", Size: 2, ModTime: 111},
		"b.txt": {Hash: "bb", Size: 2, ModTime: 222},
	})
	if err := s.Save(ctx, scope, first); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.Save(ctx, scope, second); err != nil {
		t.Fatalf("save: %v", err)
	}

	rows, err := s.List(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// newest first (creation order in this test)
	if rows[0].ID != "snap-0002" || rows[1].ID != "snap-0001" {
		t.Fatalf("newest first: %v, %v", rows[0].ID, rows[1].ID)
	}
	// the fork coordinate survives the round trip
	if rows[1].Msgs != 1 || rows[1].Message != "turn msgs=1" {
		t.Fatalf("msgs/message must round-trip: %+v", rows[1])
	}
	if len(rows[0].Files) != 2 || rows[0].Files["b.txt"].Hash != "bb" {
		t.Fatalf("files must round-trip: %+v", rows[0].Files)
	}
	if rows[0].Time == 0 {
		t.Fatal("time must survive the round trip")
	}

	// scopes don't bleed into each other
	other, err := s.List(ctx, HistScope{AgentID: "ag", ProjectID: "proj", Session: "other"})
	if err != nil || len(other) != 0 {
		t.Fatalf("other scope must list empty: %v %v", other, err)
	}
	if looser, err := s.List(ctx, HistScope{AgentID: "ag", Session: "chat1"}); err != nil || len(looser) != 0 {
		t.Fatalf("project and loose scopes are distinct: %v %v", looser, err)
	}
}

func TestSQLManifestStoreSkipsCorruptRow(t *testing.T) {
	s, dbs := newSQLManifestStore(t)
	ctx := context.Background()
	scope := HistScope{AgentID: "ag", Session: "chat1"}

	if err := s.Save(ctx, scope, &ManifestRecord{ID: "snap-good", Msgs: 3, Files: map[string]ManifestFile{}}); err != nil {
		t.Fatal(err)
	}
	// Plant a corrupt document directly.
	if _, err := dbs.DB().ExecContext(ctx,
		`INSERT INTO workspace_history_snapshots (snap_id, agent_id, project_id, session_key, msgs, message, manifest)
		 VALUES ('snap-bad', 'ag', '', 'chat1', 9, 'turn msgs=9', '{not json')`); err != nil {
		t.Fatal(err)
	}

	rows, err := s.List(ctx, scope)
	if err != nil {
		t.Fatalf("one corrupt row must not break listing: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "snap-good" {
		t.Fatalf("corrupt row must be skipped: %+v", rows)
	}
}
