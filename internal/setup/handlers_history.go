package setup

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/fastclaw-ai/fastclaw/internal/config"
	"github.com/fastclaw-ai/fastclaw/internal/workspace"
)

// Workspace version history handlers. The feature is opt-in at the WRITE
// side (workspaceHistory.enabled gates the turn-boundary snapshot in the
// agent loop); reading and restoring existing history needs no toggle —
// rollback is a deliberate user action.
//
// All four endpoints serve from the injected HistoryBackend
// (SetHistoryBackend) — the same instance the agents snapshot through,
// so a snapshot taken on pod A lists and restores from pod B. Standalone
// servers without an injected backend fall back to the git backend
// under FASTCLAW_HOME; on object stores that fallback 503s as before.

// historyBackendFor resolves the backend to serve with: the injected one,
// or the git-backend fallback for standalone LocalFS servers.
func (s *Server) historyBackendFor(w http.ResponseWriter) workspace.HistoryBackend {
	if s.historyBackend != nil {
		return s.historyBackend
	}
	if _, isLocal := s.workspaceStore.(workspace.LocalScoper); !isLocal {
		jsonResponse(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace history requires a workspace history backend"})
		return nil
	}
	root, err := s.workspaceHistoryRoot()
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return nil
	}
	return workspace.NewHistory(root)
}

// workspaceHistoryScope resolves the URL sessionId to the history scope.
// Authorization mirrors the file list (the caller's own session, or for
// the agent's owner any session of the agent), but the scope keeps the
// RAW session id: snapshots are committed under the resolved session key
// so a fork branch owns its own history, while workspaceFolderScope's
// chatID merge maps a fork branch back to its origin chat — the file
// browser's coordinate, not the history's. For web chats the URL
// sessionId IS the session key; for IM chats it is the chatID — both
// match what the agent loop committed under.
func (s *Server) workspaceHistoryScope(w http.ResponseWriter, r *http.Request, agentID, urlToken string) (scope workspace.HistScope, ok bool) {
	if s.historyBackend == nil {
		// Standalone fallback still requires a local workspace store —
		// object stores have no history there, and the error must not
		// look like a 404 "no history yet".
		if _, isLocal := s.workspaceStore.(workspace.LocalScoper); !isLocal {
			jsonResponse(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace history requires a workspace history backend"})
			return workspace.HistScope{}, false
		}
	}
	projectID, chatID, found := s.workspaceFolderScope(r, agentID, urlToken, "")
	if !found || chatID == "" {
		jsonResponse(w, http.StatusNotFound, map[string]any{"error": "session not found"})
		return workspace.HistScope{}, false
	}
	return workspace.HistScope{AgentID: agentID, ProjectID: projectID, Session: strings.TrimSpace(urlToken)}, true
}

// historySink picks where a restored snapshot lands: the chat's on-disk
// scope dir on LocalFS stores, the store scope itself on object stores.
func (s *Server) historySink(scope workspace.HistScope) workspace.TreeSink {
	if ls, isLocal := s.workspaceStore.(workspace.LocalScoper); isLocal {
		if dir, _ := ls.LocalScopeDir(scope.AgentID, scope.ProjectID, scope.Session); dir != "" {
			return workspace.NewDirSink(dir)
		}
	}
	if s.workspaceStore != nil {
		return workspace.NewStoreSink(s.workspaceStore, scope.AgentID, scope.ProjectID, scope.Session)
	}
	return nil
}

// parseChatFolder reads a chat folder given as an agent-relative path —
// "sessions/<chat>" or "projects/<pid>/<chat>" — as the agent-wide file
// view knows it. Each id must be one plain path segment.
func parseChatFolder(dir string) (projectID, chatID string, ok bool) {
	parts := strings.Split(strings.Trim(dir, "/"), "/")
	plain := func(seg string) bool {
		return seg != "" && seg != "." && seg != ".." && !strings.ContainsAny(seg, "\\\x00")
	}
	switch {
	case len(parts) == 2 && parts[0] == "sessions" && plain(parts[1]):
		return "", parts[1], true
	case len(parts) == 3 && parts[0] == "projects" && plain(parts[1]) && plain(parts[2]):
		return parts[1], parts[2], true
	}
	return "", "", false
}

// workspaceFolderHistoryScope resolves ?dir= (or the restore body's dir)
// for the agent-wide file view, which only the agent's owner may use.
func (s *Server) workspaceFolderHistoryScope(w http.ResponseWriter, r *http.Request, agentID, dir string) (scope workspace.HistScope, ok bool) {
	if s.historyBackend == nil {
		if _, isLocal := s.workspaceStore.(workspace.LocalScoper); !isLocal {
			jsonResponse(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace history requires a workspace history backend"})
			return workspace.HistScope{}, false
		}
	}
	if !s.callerOwnsAgent(r, agentID) {
		jsonResponse(w, http.StatusForbidden, map[string]any{"error": "only the agent's owner can browse its whole workspace"})
		return workspace.HistScope{}, false
	}
	projectID, chatID, valid := parseChatFolder(dir)
	if !valid {
		jsonResponse(w, http.StatusBadRequest, map[string]any{"error": "dir must be sessions/<chat> or projects/<project>/<chat>"})
		return workspace.HistScope{}, false
	}
	return workspace.HistScope{AgentID: agentID, ProjectID: projectID, Session: chatID}, true
}

// handleAgentFolderHistory lists one chat folder's snapshots by path
// (?dir=sessions/<chat>), for the owner's agent-wide file view, where a
// previewed file is known by its folder rather than a session id.
func (s *Server) handleAgentFolderHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireAgentReadable(w, r, id) {
		return
	}
	scope, ok := s.workspaceFolderHistoryScope(w, r, id, r.URL.Query().Get("dir"))
	if !ok {
		return
	}
	s.writeHistoryList(w, r, scope)
}

// handleAgentFolderHistoryRestore is the restore counterpart:
// body {"dir": "sessions/<chat>", "commit": "<hash>"}.
func (s *Server) handleAgentFolderHistoryRestore(w http.ResponseWriter, r *http.Request) {
	if !s.requireWritable(w, r) {
		return
	}
	id := r.PathValue("id")
	if !s.requireAgentReadable(w, r, id) {
		return
	}
	var body struct {
		Dir    string `json:"dir"`
		Commit string `json:"commit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]any{"error": "invalid json: " + err.Error()})
		return
	}
	scope, ok := s.workspaceFolderHistoryScope(w, r, id, body.Dir)
	if !ok {
		return
	}
	s.restoreHistory(w, r, scope, body.Commit)
}

func (s *Server) workspaceHistoryRoot() (string, error) {
	home, err := config.HomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "workspace-history"), nil
}

// handleAgentSessionHistory lists the snapshots of one chat's workspace,
// newest first: {"history": [{hash, message, time, msgs}]}.
func (s *Server) handleAgentSessionHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireAgentReadable(w, r, id) {
		return
	}
	scope, ok := s.workspaceHistoryScope(w, r, id, r.PathValue("sessionId"))
	if !ok {
		return
	}
	s.writeHistoryList(w, r, scope)
}

func (s *Server) writeHistoryList(w http.ResponseWriter, r *http.Request, scope workspace.HistScope) {
	hb := s.historyBackendFor(w)
	if hb == nil {
		return
	}
	entries, err := hb.List(r.Context(), scope)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if entries == nil {
		entries = []workspace.Entry{}
	}
	jsonResponse(w, http.StatusOK, map[string]any{"history": entries})
}

// handleAgentSessionHistoryRestore checks the chat's workspace out to one
// snapshot. The snapshot id is validated by the backend.
func (s *Server) handleAgentSessionHistoryRestore(w http.ResponseWriter, r *http.Request) {
	if !s.requireWritable(w, r) {
		return
	}
	id := r.PathValue("id")
	if !s.requireAgentReadable(w, r, id) {
		return
	}
	scope, ok := s.workspaceHistoryScope(w, r, id, r.PathValue("sessionId"))
	if !ok {
		return
	}
	var body struct {
		Commit string `json:"commit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]any{"error": "invalid json: " + err.Error()})
		return
	}
	s.restoreHistory(w, r, scope, body.Commit)
}

func (s *Server) restoreHistory(w http.ResponseWriter, r *http.Request, scope workspace.HistScope, commit string) {
	hb := s.historyBackendFor(w)
	if hb == nil {
		return
	}
	sink := s.historySink(scope)
	if sink == nil {
		jsonResponse(w, http.StatusServiceUnavailable, map[string]any{"error": "workspace history restore requires a workspace store"})
		return
	}
	if err := hb.Restore(r.Context(), scope, commit, sink); err != nil {
		jsonResponse(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	jsonResponse(w, http.StatusOK, map[string]any{"ok": true})
}
