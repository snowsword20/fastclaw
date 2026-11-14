package store

import (
	"context"
	"testing"
)

// The workspace-history manifest table (D2) is created by Migrate; both
// dialects must accept repeated runs (idempotency) — the gateway calls
// Migrate on every boot, including against an existing database.
func TestMigrateWorkspaceHistoryIdempotentSQLite(t *testing.T) {
	dbs, err := NewDBStore("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	if err := dbs.Migrate(ctx); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := dbs.Migrate(ctx); err != nil {
		t.Fatalf("second migrate must be idempotent: %v", err)
	}
	if !dbs.tableHasColumnQuick(t, WorkspaceHistoryTable, "manifest") {
		t.Fatalf("table %s must exist with a manifest column", WorkspaceHistoryTable)
	}
}

func TestMigrateWorkspaceHistoryIdempotentPostgres(t *testing.T) {
	dbs, cleanup := openTestPG(t)
	defer cleanup()
	ctx := context.Background()
	if err := dbs.Migrate(ctx); err != nil {
		t.Fatalf("second migrate must be idempotent: %v", err)
	}
	if !dbs.tableHasColumnQuick(t, WorkspaceHistoryTable, "manifest") {
		t.Fatalf("table %s must exist with a manifest column", WorkspaceHistoryTable)
	}
}

// tableHasColumnQuick probes column existence through the existing
// helper without writing dialect-specific catalogs in this test.
func (d *DBStore) tableHasColumnQuick(t *testing.T, table, col string) bool {
	t.Helper()
	has, err := d.tableHasColumn(context.Background(), table, col)
	if err != nil {
		t.Fatalf("probe %s.%s: %v", table, col, err)
	}
	return has
}
