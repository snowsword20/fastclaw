package store

import "context"

// workspace_history_snapshots backs the cloud workspace-history manifest
// store (internal/workspace/history_sql.go — D2). One row per turn-
// boundary snapshot: metadata columns for listing/ordering, the manifest
// itself as a JSON TEXT document (path → {hash, size, modtime}), same
// JSON-in-TEXT convention as sessions.messages / usage_events.detail.
//
// The blob payloads are NOT here — they live in the object store under
// history/blobs/<sha256> (content-addressed, shared across pods). This
// table only holds the tiny per-turn manifests, which are what the
// history list / fork-point selection actually query.
//
// Tenancy mirrors the workspace scoping (docs/integration-model.md): the
// (agent_id, project_id, session_key) triple — the same coordinate the
// git backend flattens into its "<projectID>-<chatID>" scope string.
// Ownership is enforced upstream (agents belong to owners; the REST
// layer checks caller access), not here.
const WorkspaceHistoryTable = "workspace_history_snapshots"

// migrateWorkspaceHistory creates the snapshot manifest table. Idempotent.
func (d *DBStore) migrateWorkspaceHistory(ctx context.Context) error {
	idCol := "id INTEGER PRIMARY KEY AUTOINCREMENT"
	if d.dialect == "postgres" {
		idCol = "id BIGSERIAL PRIMARY KEY"
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + WorkspaceHistoryTable + ` (
			` + idCol + `,
			snap_id TEXT NOT NULL,
			agent_id TEXT NOT NULL DEFAULT '',
			project_id TEXT NOT NULL DEFAULT '',
			session_key TEXT NOT NULL DEFAULT '',
			msgs INTEGER NOT NULL DEFAULT 0,
			message TEXT NOT NULL DEFAULT '',
			manifest TEXT NOT NULL DEFAULT '{}',
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_ws_history_snap_id ON ` + WorkspaceHistoryTable + ` (snap_id)`,
		`CREATE INDEX IF NOT EXISTS idx_ws_history_scope ON ` + WorkspaceHistoryTable + ` (agent_id, project_id, session_key, created_at)`,
	}
	for _, s := range stmts {
		if _, err := d.db.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
