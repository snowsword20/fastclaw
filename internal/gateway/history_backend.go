package gateway

import (
	"context"
	"log/slog"
	"path/filepath"

	"github.com/fastclaw-ai/fastclaw/internal/config"
	"github.com/fastclaw-ai/fastclaw/internal/scope"
	"github.com/fastclaw-ai/fastclaw/internal/store"
	"github.com/fastclaw-ai/fastclaw/internal/workspace"
)

// Workspace history backend assembly. The implementation follows the
// deployment shape, decided once at boot and injected into every agent
// manager and the setup server:
//
//   - LocalFS object store → git bare repos under FASTCLAW_HOME
//     (single-host default; behavior unchanged since the feature
//     existed).
//   - Object store (S3/MinIO/R2/...) → CloudHistory: content-addressed
//     blobs in the object store plus manifests in the SQL store (D2,
//     the default — sqlite and postgres both work) or in the object
//     store (D1, workspaceHistory.manifestStore="object"; also the
//     fallback when no SQL DBStore is available).
//
// Blobs always live in the object store — the SQL store never holds
// binary payloads (see internal/store package docs).
//
// readSystemWorkspaceHistory mirrors readObjectStoreCfg: the
// "workspaceHistory" setting namespace, env layered on top.
func readSystemWorkspaceHistory(st store.Store) config.WorkspaceHistoryCfg {
	cfg := &config.Config{}
	if st != nil {
		_ = scope.SettingInto(context.Background(), st, NSWorkspaceHistory, "", "", &cfg.WorkspaceHistory)
	}
	config.LoadEnv().ApplyToConfig(cfg)
	return cfg.WorkspaceHistory
}

// buildHistoryBackend picks and constructs the HistoryBackend. Returns
// nil only when the cloud assembly fails (misconfigured object store) —
// callers log and agents then run without history, matching the
// feature's best-effort contract.
func buildHistoryBackend(st store.Store, osCfg config.ObjectStoreCfg, homeDir string, whCfg config.WorkspaceHistoryCfg) workspace.HistoryBackend {
	isCloud := osCfg.Type != "" && osCfg.Type != "local"
	if !isCloud {
		return workspace.NewHistory(filepath.Join(homeDir, "workspace-history"))
	}

	blobs, err := workspace.NewObjectHistoryStore(workspace.S3Config{
		Endpoint:  osCfg.S3.Endpoint,
		Region:    osCfg.S3.Region,
		Bucket:    osCfg.S3.Bucket,
		Prefix:    osCfg.S3.Prefix,
		AccessKey: osCfg.S3.AccessKey,
		SecretKey: osCfg.S3.SecretKey,
		UseSSL:    osCfg.S3.UseSSL,
	})
	if err != nil {
		slog.Error("workspace history: object store unavailable, history disabled", "error", err)
		return nil
	}

	var manifests workspace.ManifestStore
	switch mode := defaultStr(whCfg.ManifestStore, "auto"); mode {
	case "object": // D1: manifests in the object store
		manifests = blobs
	case "sql", "auto": // D2: manifests in the SQL store
		dbs, ok := st.(*store.DBStore)
		if !ok {
			if mode == "sql" {
				slog.Warn("workspace history: manifestStore=sql but store is not a SQL DB; falling back to object manifests")
			}
			manifests = blobs
			break
		}
		manifests = workspace.NewSQLManifestStore(dbs.DB(), dbs.Dialect())
		slog.Info("workspace history", "backend", "cloud", "manifests", "sql", "dialect", dbs.Dialect())
	default:
		slog.Warn("workspace history: unknown manifestStore value, using auto", "value", mode)
		return buildHistoryBackend(st, osCfg, homeDir, config.WorkspaceHistoryCfg{ManifestStore: "auto"})
	}
	if manifests == blobs {
		slog.Info("workspace history", "backend", "cloud", "manifests", "object")
	}
	return workspace.NewCloudHistory(blobs, manifests)
}
