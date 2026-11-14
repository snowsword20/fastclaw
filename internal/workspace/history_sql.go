package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// SQLManifestStore is the D2 manifest backend: snapshot manifests in the
// SQL store (sqlite or postgres), blob payloads still in the object
// store. It follows the usage.NewSQLMeter pattern — own queries against
// the shared *sql.DB pool, dialect-branched placeholders — and pairs
// with the workspace_history_snapshots table created by the store
// package's migration (internal/store/workspace_history.go; the table
// name is duplicated here deliberately, mirroring how usage metering
// owns its queries while the store owns the DDL).
type SQLManifestStore struct {
	db      *sql.DB
	dialect string // "sqlite" | "postgres"
}

// Compile-time proof SQLManifestStore satisfies the manifest contract.
var _ ManifestStore = (*SQLManifestStore)(nil)

// NewSQLManifestStore builds the D2 manifest backend on the gateway's
// shared DB pool.
func NewSQLManifestStore(db *sql.DB, dialect string) *SQLManifestStore {
	return &SQLManifestStore{db: db, dialect: dialect}
}

// ph renders a numbered placeholder for the dialect ("?" vs "$n").
func (s *SQLManifestStore) ph(n int) string {
	if s.dialect == "postgres" {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// Save inserts one manifest row. Save is append-only: snap ids are
// time-ordered and unique, so a retry after an unknown network failure
// hits the unique index rather than duplicating.
func (s *SQLManifestStore) Save(ctx context.Context, scope HistScope, m *ManifestRecord) error {
	if m.ID == "" {
		return fmt.Errorf("workspace history: manifest id required")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("workspace history: marshal manifest: %w", err)
	}
	q := fmt.Sprintf(`INSERT INTO workspace_history_snapshots
		(snap_id, agent_id, project_id, session_key, msgs, message, manifest, created_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)`,
		s.ph(1), s.ph(2), s.ph(3), s.ph(4), s.ph(5), s.ph(6), s.ph(7))
	_, err = s.db.ExecContext(ctx, q,
		m.ID, scope.AgentID, scope.ProjectID, scope.Session, m.Msgs, m.Message, string(data))
	return err
}

// List reads the scope's manifests, newest first. Ordering is
// created_at DESC with snap_id DESC as the tie-break — snap ids embed a
// zero-padded nanosecond timestamp, so same-second rows still order by
// true creation time. No manifests yields an empty slice (not an error).
func (s *SQLManifestStore) List(ctx context.Context, scope HistScope) ([]ManifestRecord, error) {
	q := fmt.Sprintf(`SELECT snap_id, msgs, message, manifest, created_at
		FROM workspace_history_snapshots
		WHERE agent_id = %s AND project_id = %s AND session_key = %s
		ORDER BY created_at DESC, snap_id DESC`,
		s.ph(1), s.ph(2), s.ph(3))
	rows, err := s.db.QueryContext(ctx, q, scope.AgentID, scope.ProjectID, scope.Session)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ManifestRecord{}
	for rows.Next() {
		var (
			colID   string
			colMsgs int
			colMsg  string
			data    string
			created time.Time
		)
		if err := rows.Scan(&colID, &colMsgs, &colMsg, &data, &created); err != nil {
			return nil, err
		}
		var m ManifestRecord
		if err := json.Unmarshal([]byte(data), &m); err != nil {
			// One corrupt manifest must not hide the rest of the
			// history — skip it loudly and keep listing.
			slog.Warn("workspace history: corrupt manifest row", "snap", colID, "error", err)
			continue
		}
		if m.ID == "" {
			m.ID = colID
		}
		if m.Msgs == 0 {
			m.Msgs = colMsgs
		}
		if m.Message == "" {
			m.Message = colMsg
		}
		// The document carries the authoritative nanos timestamp; the row's
		// created_at is the backstop for rows whose document lost it.
		if m.Time == 0 {
			m.Time = created.UTC().UnixNano()
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
