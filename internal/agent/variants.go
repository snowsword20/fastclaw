package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/provider"
	"github.com/fastclaw-ai/fastclaw/internal/workspace"
)

// Answer variants ("重新生成"多版本) live entirely on message metadata —
// no schema change. One Q&A turn may own several answer segments; every
// message in a segment carries:
//
//	variantGroup  string — id shared by all segments of the same question
//	variantIndex  int    — 0 = the original answer, 1..n = retries
//	variantActive bool   — exactly one segment per group is active
//
// Invariants:
//   - The sessions.messages working set (= LLM context via GetMessages)
//     contains ONLY the active segment: retry trims the old segment off
//     before re-sending, switch splices the chosen archived segment in.
//   - The session_messages archive keeps every segment (AppendMessage is
//     append-only), so history can render all variants and switch can
//     pull an inactive one back.
//   - Segment scope = everything after the question's user message:
//     assistant + tool messages of the whole ReAct loop, final answer
//     included. Retrying mid-conversation turns is out of scope (the web
//     panel only retries the last turn; mid-history edits go through
//     fork).
const (
	metaVariantGroup  = "variantGroup"
	metaVariantIndex  = "variantIndex"
	metaVariantActive = "variantActive"
	// ParamKeyVariant carries the variant stamp through bus.InboundMessage
	// Params into HandleMessage; the append sites tag every message the
	// turn produces (see tagVariantFromParams).
	ParamKeyVariant = "__fastclawVariant"
	// ParamKeySession pins the EXPLICIT web session key: when set, the
	// turn binds that exact session instead of resolving the channel
	// triple (whose activeOverride a previous fork may have flipped to a
	// branch). Injected by HandleWebChatStream whenever the caller's
	// sessionId already exists.
	ParamKeySession = "__fastclawSessionKey"
)

// variantMetaFromParams extracts the variant stamp chatRequest.Params
// (set by PrepareWebRetry) as a metadata map, or nil when absent.
func variantMetaFromParams(params map[string]any) map[string]any {
	raw, _ := params[ParamKeyVariant].(map[string]any)
	if raw == nil {
		return nil
	}
	group, _ := raw["group"].(string)
	if group == "" {
		return nil
	}
	index := 0
	switch v := raw["index"].(type) {
	case float64:
		index = int(v)
	case int:
		index = v
	}
	return map[string]any{
		metaVariantGroup:  group,
		metaVariantIndex:  index,
		metaVariantActive: true,
	}
}

// tagVariantFromParams stamps the variant metadata onto a message the
// current retry turn is about to append. No-op outside a retry turn.
func tagVariantFromParams(m *provider.Message, params map[string]any) {
	vm := variantMetaFromParams(params)
	if vm == nil {
		return
	}
	m.Metadata = mergeMetadata(vm, m.Metadata)
}

// variantOf reports the (group, index, active) stamp on a message; ok is
// false for messages outside any variant group (plain turns).
func variantOf(m provider.Message) (group string, index int, active bool, ok bool) {
	return variantFromMeta(m.Metadata)
}

// variantFromMeta is variantOf over a raw metadata map — shared by
// provider.Message rows and store.SessionMessage archive rows.
func variantFromMeta(meta map[string]any) (group string, index int, active bool, ok bool) {
	group, _ = meta[metaVariantGroup].(string)
	if group == "" {
		return "", 0, false, false
	}
	switch v := meta[metaVariantIndex].(type) {
	case float64:
		index = int(v)
	case int:
		index = int(v)
	case int64:
		index = int(v)
	}
	active, _ = meta[metaVariantActive].(bool)
	return group, index, active, true
}

func randToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b)
}

// archiveUserID resolves the user_id that owns the session rows. The
// store adapter normally handles this resolution on its own calls; when
// the agent hits the store directly (variant re-tagging) it must do the
// same dance: prefer the row's real owner, fall back to the trusted
// owner id.
func (a *Agent) archiveUserID(ctx context.Context, sessionKey string) string {
	if a.dataStore == nil {
		return ""
	}
	if owner, err := a.dataStore.LookupSessionOwner(ctx, a.name, sessionKey); err == nil && owner != "" {
		return owner
	}
	return a.trustOwnerID()
}

// lastUserTurn splits the working set at the final user message — the
// only turn the web panel retries. Returns the prefix (everything up to
// and including that user message) and the answer segment after it.
func lastUserTurn(msgs []provider.Message) (prefix []provider.Message, segment []provider.Message, ok bool) {
	last := -1
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" && (msgs[i].TextContent() != "" || len(msgs[i].ContentParts) > 0) {
			last = i
			break
		}
	}
	if last < 0 {
		return nil, nil, false
	}
	return msgs[:last+1], msgs[last+1:], true
}

// userImageURLs extracts image_url content parts for re-sending a turn.
func userImageURLs(m provider.Message) []string {
	var urls []string
	for _, p := range m.ContentParts {
		if p.Type == "image_url" && p.ImageURL != nil && p.ImageURL.URL != "" {
			urls = append(urls, p.ImageURL.URL)
		}
	}
	return urls
}

// WebRetryPlan is what PrepareWebRetry hands back for the SSE handler to
// re-run the trimmed turn through HandleWebChatStream.
type WebRetryPlan struct {
	Text      string
	ImageURLs []string
	// Params carries ParamKeyVariant so every message this turn appends
	// lands in the new variant segment.
	Params map[string]any
}

// PrepareWebRetry trims the session working set for a retry of the last
// turn and archives the old answer as an inactive variant. The caller
// re-sends plan.Text (with plan.Params) through the normal stream path.
//
// Mirrors slashRetry's snapshot + trim (slash.go), plus variant tagging:
//   - the old segment keeps its archive rows but flips variantActive to
//     false (metadata re-tag via UpdateSessionMessagesMeta);
//   - the working set drops it entirely (ReplaceMessages) so the LLM
//     context never sees two answers to the same question.
func (a *Agent) PrepareWebRetry(ctx context.Context, sessionId string) (*WebRetryPlan, error) {
	resolved := a.sessions.ResolveSessionKey(sessionId)
	sess := a.sessions.GetByKey(resolved)
	if sess == nil {
		return nil, fmt.Errorf("session %q not found", sessionId)
	}
	msgs := sess.GetMessages()
	prefix, _, ok := lastUserTurn(msgs)
	if !ok {
		return nil, errors.New("no previous message to retry")
	}
	question := prefix[len(prefix)-1]

	// Reuse the segment's group when the old answer was itself a retry;
	// otherwise open a fresh group at index 0 for the original answer.
	// Group membership is read from the ARCHIVE (last turn's rows), not
	// just the working set: the working set only holds the ACTIVE
	// segment, and after a switch-back the archive still owns higher
	// indices that a fresh retry must not reuse. Scoped to the last turn
	// because retries only ever target the final turn — mid-history
	// rewrites go through fork.
	group := ""
	maxIdx := -1
	archived := sess.ArchivedMessages()
	segStart := 0
	for i, m := range archived {
		if m.Role == "user" && m.Origin == provider.OriginUser && (m.TextContent() != "" || len(m.ContentParts) > 0) {
			segStart = i + 1
		}
	}
	for _, m := range archived[segStart:] {
		g, idx, _, tagged := variantOf(m)
		if !tagged {
			continue
		}
		if group == "" {
			group = g
		}
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	if group == "" {
		group = fmt.Sprintf("vg-%d-%s", time.Now().UnixMilli(), randToken(4))
	}
	// First retry of an untagged segment: the old answer is about to be
	// stamped index 0 (below), so the new one must take 1 — otherwise both
	// segments share index 0 and the switcher can't tell them apart.
	newIndex := maxIdx + 1
	if newIndex < 1 {
		newIndex = 1
	}

	// Re-tag the archived old segment: same group, inactive. The working
	// set's copy is about to be dropped, so only the archive matters.
	// Rows to flip = the WHOLE group (a switch-back can leave an older
	// index active far above the last user row), or — first retry of an
	// untagged segment — every row after the last user row.
	if a.dataStore != nil {
		uid := a.archiveUserID(ctx, resolved)
		if archived, err := a.dataStore.ListSessionMessages(ctx, uid, a.name, resolved); err == nil {
			var seqs []int64
			var patch map[string]any
			if maxIdx >= 0 {
				for _, m := range archived {
					if g, _, _, tagged := variantFromMeta(m.Metadata); tagged && g == group {
						seqs = append(seqs, m.Seq)
					}
				}
				patch = map[string]any{
					metaVariantGroup:  group,
					metaVariantActive: false,
				}
			} else {
				for _, m := range archived[segStart:] {
					seqs = append(seqs, m.Seq)
				}
				patch = map[string]any{
					metaVariantGroup:  group,
					metaVariantIndex:  0,
					metaVariantActive: false,
				}
			}
			if len(seqs) > 0 {
				if err := a.dataStore.UpdateSessionMessagesMeta(ctx, uid, a.name, resolved, seqs, patch); err != nil {
					slog.Warn("variant archive tag failed", "agent", a.name, "session", resolved, "error", err)
				}
			}
		} else {
			slog.Warn("variant archive list failed", "agent", a.name, "session", resolved, "error", err)
		}
	}

	// Trim the working set to just BEFORE the question's user message —
	// the re-sent turn appends a fresh user row, so keeping the old one
	// would duplicate the question in both the context and the archive.
	// Snapshot first so an IM-side /undo can still roll the retry back.
	sess.Snapshot()
	sess.ReplaceMessages(prefix[:len(prefix)-1])

	return &WebRetryPlan{
		Text:      question.TextContent(),
		ImageURLs: userImageURLs(question),
		Params: map[string]any{
			ParamKeyVariant: map[string]any{
				"group": group,
				"index": newIndex,
			},
		},
	}, nil
}

// SetVariant activates another stored variant of the last turn's answer
// group: splices the archived segment into the working set (replacing
// the currently active one, keeping any follow-up turns), re-tags the
// archive's active flags, and returns nothing — callers reload history
// to re-render.
func (a *Agent) SetVariant(ctx context.Context, sessionId, group string, index int) error {
	if group == "" {
		return errors.New("variant group is required")
	}
	resolved := a.sessions.ResolveSessionKey(sessionId)
	sess := a.sessions.GetByKey(resolved)
	if sess == nil {
		return fmt.Errorf("session %q not found", sessionId)
	}
	msgs := sess.GetMessages()
	prefix, segment, ok := lastUserTurn(msgs)
	if !ok {
		return errors.New("no previous message to retry")
	}

	// Split the working-set segment into the active variant rows (they
	// carry the group stamp) and any follow-up turns appended after the
	// retry (they don't).
	var followUp []provider.Message
	for _, m := range segment {
		if _, _, _, tagged := variantOf(m); !tagged {
			followUp = append(followUp, m)
		}
	}

	// Pull the requested segment from the archive.
	archived := sess.ArchivedMessages()
	var chosen []provider.Message
	for _, m := range archived {
		g, idx, _, tagged := variantOf(m)
		if !tagged || g != group || idx != index {
			continue
		}
		chosen = append(chosen, m)
	}
	if len(chosen) == 0 {
		return fmt.Errorf("variant %s#%d not found in session %q", group, index, sessionId)
	}

	rebuilt := append(append(prefix, chosen...), followUp...)
	sess.ReplaceMessages(rebuilt)

	// Flip the archive's active flags: chosen segment on, the rest off.
	if a.dataStore != nil {
		uid := a.archiveUserID(ctx, resolved)
		if archivedRows, err := a.dataStore.ListSessionMessages(ctx, uid, a.name, resolved); err == nil {
			var on, off []int64
			for _, m := range archivedRows {
				g, idx, _, tagged := variantFromMeta(m.Metadata)
				if !tagged || g != group {
					continue
				}
				if idx == index {
					on = append(on, m.Seq)
				} else {
					off = append(off, m.Seq)
				}
			}
			if len(off) > 0 {
				if err := a.dataStore.UpdateSessionMessagesMeta(ctx, uid, a.name, resolved, off, map[string]any{metaVariantActive: false}); err != nil {
					slog.Warn("variant deactivate failed", "agent", a.name, "session", resolved, "error", err)
				}
			}
			if len(on) > 0 {
				if err := a.dataStore.UpdateSessionMessagesMeta(ctx, uid, a.name, resolved, on, map[string]any{metaVariantActive: true}); err != nil {
					slog.Warn("variant activate failed", "agent", a.name, "session", resolved, "error", err)
				}
			}
		}
	}
	return nil
}

// workspaceScopeFor returns the workspace/history scope of a session:
// the session key for web chats (so fork branches get their own tree —
// their rows keep the original chatId), the raw chatId for IM channels
// (group chats share one workspace across sessions, legacy behavior).
func (a *Agent) workspaceScopeFor(channel, chatID, resolvedKey string) string {
	if channel == "web" {
		return resolvedKey
	}
	return chatID
}

// forkWorkspaceFiles materializes the source session's workspace files
// into the fork branch's workspace, restored to the FORK POINT state.
//
// Accuracy comes from the workspace git history (workspace.History, one
// bare repo per scope, a commit at every turn boundary whose message is
// "turn msgs=<messageCount>"): the newest commit with msgs <= keep is
// exactly the file state as of the fork point. When history is disabled
// or no such commit exists, an exact restore is impossible — a full fork
// (keep >= all messages) falls back to copying the live directory, while
// a mid-history fork deliberately copies nothing rather than leak files
// from after the fork point.
func (a *Agent) forkWorkspaceFiles(ctx context.Context, chatID, projectID, srcKey, newKey string, keep, totalMsgs int) {
	if a.workspaceStore == nil {
		return
	}
	ls, ok := a.workspaceStore.(workspace.LocalScoper)
	if !ok {
		return // S3-backed stores: no host tree to copy
	}
	srcScope := a.workspaceScopeFor("web", chatID, srcKey)
	dstScope := a.workspaceScopeFor("web", chatID, newKey)
	if projectID != "" {
		srcScope = projectID + "-" + srcScope
		dstScope = projectID + "-" + dstScope
	}
	srcDir, ok := ls.LocalScopeDir(a.name, projectID, srcKey)
	if !ok || srcDir == "" {
		return
	}
	dstDir, ok := ls.LocalScopeDir(a.name, projectID, newKey)
	if !ok || dstDir == "" || dstDir == srcDir {
		return
	}

	// Commit the live tree first so "now" is snapshotted — but tag it
	// with the FULL message count: it represents the fork moment, which
	// is only the right state for a full-history fork.
	syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if a.workspaceHistoryEnabled && a.history != nil {
		if err := a.history.Commit(syncCtx, srcScope, srcDir, fmt.Sprintf("turn msgs=%d", totalMsgs)); err != nil {
			slog.Debug("fork pre-copy commit failed", "scope", srcScope, "error", err)
		}
	}

	if a.workspaceHistoryEnabled && a.history != nil {
		if entries, err := a.history.List(syncCtx, srcScope); err == nil {
			// newest first; first entry with msgs <= keep wins
			for _, e := range entries {
				msgs, ok := parseTurnMsgs(e.Message)
				if !ok || msgs > keep {
					continue
				}
				if err := os.MkdirAll(dstDir, 0o755); err != nil {
					return
				}
				if err := a.history.Restore(syncCtx, srcScope, dstDir, e.Hash); err != nil {
					slog.Warn("fork workspace restore failed", "scope", srcScope, "commit", e.Hash, "error", err)
					return
				}
				slog.Info("fork workspace restored to turn snapshot", "scope", srcScope, "commit", e.Hash, "msgs", msgs)
				return
			}
		}
	}

	// No usable snapshot: exact restore impossible.
	if keep >= totalMsgs {
		// Full fork — the live tree IS the fork point; copy it.
		if err := copyDir(syncCtx, srcDir, dstDir); err != nil {
			slog.Warn("fork workspace copy failed", "src", srcDir, "dst", dstDir, "error", err)
		}
		return
	}
	slog.Info("fork workspace: no snapshot at fork point, branch starts with an empty workspace", "scope", srcScope, "keep", keep)
}

// parseTurnMsgs reads "turn msgs=<n>" (optionally with legacy
// "turn <RFC3339>" prefixes) from a history commit message.
func parseTurnMsgs(message string) (int, bool) {
	for _, field := range strings.Fields(message) {
		if v, ok := strings.CutPrefix(field, "msgs="); ok {
			n, err := strconv.Atoi(v)
			if err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

// copyDir recursively copies src into dst (dst created as needed).
// Skips nothing — session workspaces hold no .git (History keeps bare
// repos outside the tree).
func copyDir(ctx context.Context, src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // nothing to copy
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", src)
	}
	return os.CopyFS(dst, os.DirFS(src))
}
