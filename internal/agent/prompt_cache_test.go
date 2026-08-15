package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/bus"
)

// The system prompt must be byte-stable across turns: every provider's
// prompt cache (Anthropic explicit cache_control, OpenAI-compatible
// implicit prefix caching) matches the longest identical prefix, and
// the system prompt is the first message of every request. The old
// buildDateLine embedded a second-granularity NOW timestamp directly
// into the prompt, invalidating the entire prefix on every turn. The
// volatile value now lives in buildTimeAnchorValue / the per-turn tail
// context; these tests pin that split.
func TestSystemPromptStableAcrossTime(t *testing.T) {
	store := newFakeMemoryStore()
	store.put(testAgentID, ownerUID, "SOUL.md", "# Soul\n稳定人格。")
	store.put(testAgentID, ownerUID, "IDENTITY.md", "Name: 测试agent")
	store.put(testAgentID, chatterUID, "USER.md", "Name: 品冠")
	store.put(testAgentID, chatterUID, "MEMORY.md", "- 喜欢简短回复")

	cb := newChatbotBuilder(store)
	chatterMem := cb.memory.WithUserID(chatterUID)

	first := cb.BuildSystemPromptAs(chatterUID, chatterMem)
	// Different wall-clock moments must not change a single byte.
	second := cb.BuildSystemPromptAs(chatterUID, cb.memory.WithUserID(chatterUID))

	if first != second {
		t.Fatalf("system prompt differs between two builds at different times — prefix cache would miss every turn.\nfirst bytes=%d second bytes=%d", len(first), len(second))
	}
	if strings.Contains(first, "Current date/time:") {
		t.Fatalf("system prompt still embeds the volatile NOW line; it must ride in the per-turn tail context instead")
	}
	if !strings.Contains(first, "do NOT call `date`") {
		t.Fatalf("stable time-anchor instructions missing from system prompt")
	}
}

func TestAgentModeSystemPromptStableAcrossTime(t *testing.T) {
	store := newFakeMemoryStore()
	cb := newChatbotBuilder(store)
	cb.SetPromptMode("") // resolves to agent mode

	first := cb.BuildSystemPromptAs(chatterUID, cb.memory.WithUserID(chatterUID))
	second := cb.BuildSystemPromptAs(chatterUID, cb.memory.WithUserID(chatterUID))
	if first != second {
		t.Fatalf("agent-mode system prompt differs between builds at different times")
	}
	if strings.Contains(first, "Current date/time:") {
		t.Fatalf("agent-mode system prompt still embeds the volatile NOW line")
	}
}

func TestBuildTimeAnchorValueCarriesTimestamp(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 15, 14, 23, 7, 0, loc)
	v := buildTimeAnchorValue(now)
	if !strings.Contains(v, "2026-08-15 14:23:07 +0800") {
		t.Fatalf("time anchor value = %q, want full second-granularity timestamp", v)
	}
	if !strings.Contains(v, "This is NOW") {
		t.Fatalf("time anchor value = %q, want NOW marker", v)
	}
}

// The tail context is the ONE system message appended after the latest
// user message. It must carry the NOW line on every turn (agent mode,
// DM: no sender / params / reminder parts).
func TestBuildTurnTailContextContainsTimeAnchor(t *testing.T) {
	store := newFakeMemoryStore()
	cb := newChatbotBuilder(store)
	a := &Agent{ctxBuilder: cb}

	msg := bus.InboundMessage{Channel: "web", ChatID: "c1"}
	tail := a.buildTurnTailContext(msg, chatterUID, cb.memory.WithUserID(chatterUID))
	if !strings.Contains(tail, "Current date/time:") {
		t.Fatalf("tail context missing NOW line: %q", tail)
	}
	if strings.Contains(tail, "Current Sender") {
		t.Fatalf("DM turn should not carry sender attribution: %q", tail)
	}
}

// Group turns must carry sender attribution in the TAIL, not in the
// head prefix — group chats have a different sender almost every turn.
func TestBuildTurnTailContextGroupSender(t *testing.T) {
	store := newFakeMemoryStore()
	cb := newChatbotBuilder(store)
	a := &Agent{ctxBuilder: cb}

	msg := bus.InboundMessage{Channel: "telegram", ChatID: "g1", SenderName: "alice", PeerKind: "group", UserID: "u1"}
	tail := a.buildTurnTailContext(msg, chatterUID, cb.memory.WithUserID(chatterUID))
	if !strings.Contains(tail, "username: alice") {
		t.Fatalf("group tail context missing sender: %q", tail)
	}
	if !strings.Contains(tail, "Current date/time:") {
		t.Fatalf("group tail context missing NOW line: %q", tail)
	}
}
