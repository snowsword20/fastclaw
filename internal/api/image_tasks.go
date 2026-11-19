package api

// GET /v1/media-tasks/{id} — direct status lookup for background media
// tasks (image_gen / video_gen / tts / music_gen in background mode).
// Lets an API caller poll its task without round-tripping through the
// chat: the response carries the status and, when done, 7-day download
// links for the result files.
//
// Tasks live in the owning agent's memory (ids do not survive a
// restart; a restarted process re-claims the queue tasks under fresh
// ids). The handler sweeps the caller's loaded agents and returns the
// first match — task ids are process-global, so the sweep is only
// about namespace isolation, not lookup speed. The id prefix names the
// media category: imgtask_/videotask_/ttstask_/mustask_.

import (
	"net/http"

	"github.com/fastclaw-ai/fastclaw/internal/auth"
)

// HandleGetMediaTask serves GET /v1/media-tasks/{id}.
func (s *Server) HandleGetMediaTask(w http.ResponseWriter, r *http.Request) {
	ident, ok := auth.FromContext(r.Context())
	if !ok {
		writeUnauth(w, "missing or invalid credentials")
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeAPIError(w, http.StatusBadRequest, errTypeInvalidRequest, codeInvalidRequest, "task id is required")
		return
	}
	space, err := s.resolver.UserSpaceFor(ident.EffectiveUserID())
	if err != nil || space == nil || space.Agents == nil {
		writeAPIError(w, http.StatusNotFound, errTypeNotFound, codeAgentNotFound, "no such media task")
		return
	}
	for _, ag := range space.Agents.All() {
		snap, ok := ag.MediaTaskLookup(id)
		if !ok {
			continue
		}
		writeJSON(w, http.StatusOK, snap)
		return
	}
	writeAPIError(w, http.StatusNotFound, errTypeNotFound, codeAgentNotFound,
		"no such media task (ids are process-local; a restart re-claims queue tasks under fresh ids)")
}
