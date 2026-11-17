package agent

import (
	"context"
	"encoding/json"
	"log/slog"
)

// ChatEvent represents a real-time event emitted during the agent ReAct loop.
type ChatEvent struct {
	Type string         `json:"type"` // "content", "content_delta", "stream_reset", "tool_call", "tool_result", "tool_progress", "status", "steer", "error", "done", "turn_pending", "subagent_progress"
	Data map[string]any `json:"data,omitempty"`
}

const chatErrorCodeLLMProviderNotConfigured = "llm_provider_not_configured"

type chatEventsKey struct{}

// subagentEnvelopeKey marks a turn as running on behalf of a
// message_agent delegation. Events emitted inside such a turn are
// re-typed "subagent_event" with the child event nested in data, so the
// caller's session stream can show delegation progress (thinking, tool
// calls) live and on replay without mistaking it for the caller's own
// turn output.
type subagentEnvelopeKey struct{}

// SubagentEnvelope labels a delegation: the target's display name and
// agent id, plus a per-call run id so the UI can group one
// delegation's events together.
type SubagentEnvelope struct {
	Agent   string
	AgentID string
	Run     string
}

// WithSubagentEnvelope tags ctx so emitEvent envelopes this turn's events.
func WithSubagentEnvelope(ctx context.Context, env SubagentEnvelope) context.Context {
	return context.WithValue(ctx, subagentEnvelopeKey{}, env)
}

func subagentEnvelopeFromContext(ctx context.Context) (SubagentEnvelope, bool) {
	env, ok := ctx.Value(subagentEnvelopeKey{}).(SubagentEnvelope)
	return env, ok
}

// ChatEventsFromContext retrieves the events channel from context, if present.
//
// Deprecated: prefer ContextWithStream which carries the persistence
// sink + hub alongside the legacy channel. Kept for callers that only
// need the channel (tests, simple non-persistent flows).
func ChatEventsFromContext(ctx context.Context) chan<- ChatEvent {
	ch, _ := ctx.Value(chatEventsKey{}).(chan<- ChatEvent)
	return ch
}

// ContextWithChatEvents returns a new context with the events channel attached.
//
// Deprecated: prefer ContextWithStream so events also persist + publish
// to the hub for resume-on-reconnect.
func ContextWithChatEvents(ctx context.Context, ch chan<- ChatEvent) context.Context {
	return context.WithValue(ctx, chatEventsKey{}, ch)
}

// emitEvent fans one event out to every consumer registered on ctx:
//   - the persistent sink (session_events table) — assigns a seq used by
//     reconnecting clients to dedup replayed events
//   - the in-process hub (live subscribers across tabs / handlers)
//   - the legacy channel (the synchronous SSE handler that's still
//     holding the request open)
//
// Persistence is best-effort and logged on failure — a DB hiccup
// shouldn't kill the turn. Hub publishes never block (full-buffer
// subscribers are skipped). The legacy channel send respects
// ctx.Done() so the agent goroutine doesn't leak when the channel
// consumer is gone but the agent ctx is cancelled.
func emitEvent(ctx context.Context, evt ChatEvent) {
	stream := streamFromContext(ctx)

	var seq int64 = -1
	// Skip persistence for high-volume live-only events. content_delta
	// streams ~one chunk per generated token (100+ rows per turn for a
	// modest answer), which would dwarf the rest of session_events for
	// no replay value: the trailing `content` event carries the full
	// final text, so a refresh in the middle of a turn just rejoins
	// the live hub and gets the final on completion.
	//
	// stream_reset 同理：它是"上一轮尝试的增量作废"的瞬时信号，
	// 只对正盯着流式气泡的客户端有意义，重连/回放场景下没有价值
	// （重连后要么拿到重试后的正文，要么拿到 done）。
	origType := evt.Type
	persist := origType != "content_delta" && origType != "stream_reset"

	// Subagent envelope: events emitted inside a message_agent delegation
	// are re-typed "subagent_event" on the CALLER's stream (same
	// user/agent/session triple), nesting the child event in data. The
	// persistence decision above keeps using the original type, so the
	// child's content_delta stays live-only and doesn't flood
	// session_events.
	if env, ok := subagentEnvelopeFromContext(ctx); ok {
		inner, mErr := json.Marshal(evt.Data)
		if mErr != nil {
			inner = []byte("{}")
		}
		evt = ChatEvent{
			Type: "subagent_event",
			Data: map[string]any{
				"agent":   env.Agent,
				"agentId": env.AgentID,
				"run":     env.Run,
				"type":    origType,
				"event":   json.RawMessage(inner),
			},
		}
	}

	if persist && stream != nil && stream.sink != nil && stream.userID != "" && stream.sessionKey != "" {
		blob, _ := json.Marshal(evt.Data)
		s, err := stream.sink.AppendSessionEvent(ctx, stream.userID, stream.agentID, stream.sessionKey, evt.Type, blob)
		if err != nil {
			slog.Warn("persist chat event failed",
				"agent", stream.agentID, "session", stream.sessionKey,
				"type", evt.Type, "error", err)
		} else {
			seq = s
		}
	}

	if stream != nil && stream.hub != nil && stream.userID != "" && stream.sessionKey != "" {
		stream.hub.Publish(stream.userID, stream.agentID, stream.sessionKey, EventEnvelope{Seq: seq, Event: evt})
	}

	// Legacy channel path: prefer the channel held on streamCtx (set by
	// the new SSE handler); fall back to the deprecated chatEventsKey
	// channel for callers that haven't migrated.
	var ch chan<- ChatEvent
	if stream != nil {
		ch = stream.channel
	}
	if ch == nil {
		ch = ChatEventsFromContext(ctx)
	}
	if ch == nil {
		return
	}
	select {
	case ch <- evt:
	case <-ctx.Done():
	}
}
