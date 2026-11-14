package agent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fastclaw-ai/fastclaw/internal/provider"
	"github.com/fastclaw-ai/fastclaw/internal/session"
	"github.com/fastclaw-ai/fastclaw/internal/store"
)

// newVariantEntryTestAgent wires an sqlite-backed session manager the same
// way agentmanager does for real agents, so AppendMessage assigns archive
// seqs and the variant / fork paths exercise the exact production code
// (visibleWebChain over archived rows, UpdateSessionMessagesMeta re-tags).
func newVariantEntryTestAgent(t *testing.T) (*Agent, string) {
	t.Helper()
	st, err := store.NewDBStore("sqlite", "file:"+filepath.Join(t.TempDir(), "ventry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	mgr := session.NewManagerWithStoreForUser(t.TempDir(), session.NewStoreAdapter(st, "u1"), "u1", "agt")
	a := &Agent{name: "agt", dataStore: st, ownerUserID: "u1", agentOwnerID: "u1"}
	a.sessions = mgr
	sess := mgr.Get("web", "", "chat1", "")
	return a, sess.Key()
}

func appendTurn(a *Agent, key, q, answer string) {
	sess := a.sessions.GetByKey(key)
	sess.Append(provider.Message{Role: "user", Content: q, Origin: provider.OriginUser, Timestamp: 1})
	sess.Append(provider.Message{Role: "assistant", Content: answer, Origin: provider.OriginUser, Timestamp: 2})
}

// retryLastTurn runs PrepareWebRetry and appends the new answer carrying
// the retry's variant stamp — the same flow the SSE handler drives, minus
// the LLM.
func retryLastTurn(t *testing.T, a *Agent, key string) string {
	t.Helper()
	plan, err := a.PrepareWebRetry(context.Background(), key, 0)
	if err != nil {
		t.Fatalf("PrepareWebRetry: %v", err)
	}
	raw, _ := plan.Params[ParamKeyVariant].(map[string]any)
	// plan.Params carries the wire shape ("group"/"index"); the metadata
	// keys (variantGroup/…) are applied by tagVariantFromParams in the
	// real send path — here we stamp them directly.
	group, _ := raw["group"].(string)
	if group == "" {
		t.Fatal("retry plan carries no variant group")
	}
	sess := a.sessions.GetByKey(key)
	// The stream handler re-sends the trimmed question (plan.Text) through
	// the normal path, which appends a fresh user row before the new
	// answer — replay both here.
	sess.Append(provider.Message{Role: "user", Content: plan.Text, Origin: provider.OriginUser, Timestamp: 3})
	sess.Append(provider.Message{
		Role: "assistant", Content: "retry-answer", Origin: provider.OriginUser, Timestamp: 4,
		Metadata: map[string]any{
			metaVariantGroup:  group,
			metaVariantIndex:  1,
			metaVariantActive: true,
		},
	})
	return group
}

// SetVariant must only switch groups that sit on the LAST user turn. Once
// a follow-up turn exists, an older group is frozen — switching it via the
// API used to splice the archived segment after the WRONG question
// (lastUserTurn slices at the current last question), corrupting the LLM
// working set; the guard now rejects it and points at fork.
func TestSetVariantRejectsFrozenGroup(t *testing.T) {
	a, key := newVariantEntryTestAgent(t)
	ctx := context.Background()

	appendTurn(a, key, "q1", "a1")
	group := retryLastTurn(t, a, key)

	// Still the last turn: switching back to the original answer is legal.
	if err := a.SetVariant(ctx, key, group, 0); err != nil {
		t.Fatalf("last-turn switch must succeed: %v", err)
	}

	// Follow-up turn lands: the group is now frozen.
	appendTurn(a, key, "q2", "b1")
	err := a.SetVariant(ctx, key, group, 0)
	if err == nil || !strings.Contains(err.Error(), "earlier turn") {
		t.Fatalf("frozen group must be rejected with the fork hint, got %v", err)
	}

	// A retry anchored at the no-longer-last answer fails the stale-view
	// guard: the visible chain's last turn no longer contains its seq.
	archived := sessArchiveSeq(t, a, key, "a1")
	if archived == 0 {
		t.Fatal("a1 archive seq not found")
	}
	if _, err := a.PrepareWebRetry(ctx, key, archived); err == nil {
		t.Fatal("retry anchored at a stale entry must fail")
	}
	// Anchored at the actual last turn's answer: passes.
	b1 := sessArchiveSeq(t, a, key, "b1")
	if b1 == 0 {
		t.Fatal("b1 archive seq not found")
	}
	if _, err := a.PrepareWebRetry(ctx, key, b1); err != nil {
		t.Fatalf("retry anchored at the real last turn must succeed: %v", err)
	}
}

// History entries carry the archive seq as a stable id; fork resolves the
// anchor by that id instead of a client-recounted chain index, and an
// unknown id (stale client history) fails instead of silently forking at
// the wrong place.
func TestForkWebSessionAtEntryAndHistoryIDs(t *testing.T) {
	a, key := newVariantEntryTestAgent(t)

	appendTurn(a, key, "q1", "a1")
	appendTurn(a, key, "q2", "b1")

	hist := a.WebChatHistory(key)
	if len(hist) != 4 {
		t.Fatalf("want 4 history entries, got %d", len(hist))
	}
	var a1ID, b1ID int64
	for _, e := range hist {
		id, _ := e["id"].(int64)
		if id == 0 {
			t.Fatalf("history entry %v carries no id", e["content"])
		}
		switch e["content"] {
		case "a1":
			a1ID = id
		case "b1":
			b1ID = id
		}
	}

	// Fork keeping through a1: the branch replays exactly q1 → a1.
	newKey, err := a.ForkWebSessionAtEntry(key, a1ID)
	if err != nil {
		t.Fatalf("fork at entry: %v", err)
	}
	branch := a.WebChatHistory(newKey)
	if len(branch) != 2 || branch[0]["content"] != "q1" || branch[1]["content"] != "a1" {
		t.Fatalf("branch should replay q1,a1, got %v", branch)
	}

	// Full-history fork via the entry id of the last answer.
	fullKey, err := a.ForkWebSessionAtEntry(key, b1ID)
	if err != nil {
		t.Fatalf("fork at last entry: %v", err)
	}
	if got := a.WebChatHistory(fullKey); len(got) != 4 {
		t.Fatalf("full fork should replay 4 entries, got %d", len(got))
	}

	// Unknown id = stale client history: hard error, not a wrong-position
	// fork.
	if _, err := a.ForkWebSessionAtEntry(key, 999999); err == nil {
		t.Fatal("unknown entry id must fail")
	}
}

// webHistoryEntries surfaces Seq as the id field and omits it entirely
// when the message came from the working set (Seq == 0) — file-backed
// fallback must not advertise ids it cannot honor.
func TestWebHistoryEntriesID(t *testing.T) {
	withSeq := webHistoryEntries([]provider.Message{
		{Role: "user", Content: "q", Origin: provider.OriginUser, Seq: 7},
	})
	if got, _ := withSeq[0]["id"].(int64); got != 7 {
		t.Fatalf("entry id = %v, want 7", withSeq[0]["id"])
	}
	noSeq := webHistoryEntries([]provider.Message{
		{Role: "user", Content: "q", Origin: provider.OriginUser},
	})
	if _, ok := noSeq[0]["id"]; ok {
		t.Fatal("Seq==0 entries must not carry an id")
	}
}

// sessArchiveSeq looks up an archived message's seq by its content.
func sessArchiveSeq(t *testing.T, a *Agent, key, content string) int64 {
	t.Helper()
	for _, m := range a.sessions.GetByKey(key).ArchivedMessages() {
		if m.Content == content {
			return m.Seq
		}
	}
	return 0
}
