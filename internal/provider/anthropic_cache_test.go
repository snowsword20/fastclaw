package provider

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// Leading system messages (system prompt + channel hints) must be
// JOINED into the system param. The previous implementation overwrote
// `system` with each system message, so only the last one survived and
// the main system prompt was silently dropped on the Anthropic wire.
func TestToAnthropicMessagesJoinsLeadingSystem(t *testing.T) {
	system, msgs := toAnthropicMessages([]Message{
		{Role: "system", Content: "MAIN SYSTEM PROMPT"},
		{Role: "system", Content: "## Reply Format\nsplit marker"},
		{Role: "user", Content: "你好"},
	})
	if system != "MAIN SYSTEM PROMPT\n\n## Reply Format\nsplit marker" {
		t.Fatalf("system = %q, want joined leading system messages", system)
	}
	if len(msgs) != 1 || msgs[0].Role != "user" {
		t.Fatalf("messages = %#v, want only the user message", msgs)
	}
}

// System messages that appear AFTER the conversation has started (the
// per-turn tail context) become user-role turns; Anthropic has no
// mid-conversation system role and merges consecutive user messages.
func TestToAnthropicMessagesTailSystemBecomesUser(t *testing.T) {
	system, msgs := toAnthropicMessages([]Message{
		{Role: "system", Content: "MAIN"},
		{Role: "user", Content: "第一条"},
		{Role: "system", Content: "Current date/time: 2026-08-15 14:23:07 +0800"},
	})
	if system != "MAIN" {
		t.Fatalf("system = %q, want only leading system", system)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %#v, want user + tail", msgs)
	}
	if msgs[1].Role != "user" {
		t.Fatalf("tail message role = %q, want user", msgs[1].Role)
	}
	var s string
	if err := json.Unmarshal(msgs[1].Content, &s); err != nil || s != "Current date/time: 2026-08-15 14:23:07 +0800" {
		t.Fatalf("tail content = %s, want the raw text", msgs[1].Content)
	}
}

// Breakpoints land on the FINAL content block of the last wire message
// and of the message three positions back (n-3 / n-1 — matching the
// +2 messages per exchange growth pattern), and must not disturb the
// bytes of any other block.
func TestApplyMessageCacheBreakpoints(t *testing.T) {
	mkText := func(s string) anthropicMessage {
		c, _ := json.Marshal(s)
		return anthropicMessage{Role: "user", Content: c}
	}
	toolResultContent, _ := json.Marshal([]any{
		map[string]any{"type": "tool_use", "id": "t1", "input": map[string]any{"n": 3}},
		map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "ok"},
	})

	msgs := []anthropicMessage{
		mkText("u1"), // n-5
		mkText("a1"), // n-4
		mkText("u2"), // n-3 → breakpoint
		mkText("a2"), // n-2
		{Role: "user", Content: toolResultContent}, // n-1 → breakpoint
	}

	applyMessageCacheBreakpoints(msgs)

	if !hasCacheControl(t, msgs[4]) {
		t.Fatalf("last message missing cache_control")
	}
	if !hasCacheControl(t, msgs[2]) {
		t.Fatalf("n-3 message missing cache_control")
	}
	if hasCacheControl(t, msgs[3]) || hasCacheControl(t, msgs[1]) {
		t.Fatalf("unmarked positions must stay unmarked")
	}
	// Non-last blocks untouched: the tool_use block keeps its exact bytes.
	var blocks []json.RawMessage
	if err := json.Unmarshal(msgs[4].Content, &blocks); err != nil {
		t.Fatal(err)
	}
	var first map[string]any
	if err := json.Unmarshal(blocks[0], &first); err != nil {
		t.Fatal(err)
	}
	if _, exists := first["cache_control"]; exists {
		t.Fatalf("non-last block was modified: %s", blocks[0])
	}
}

func hasCacheControl(t *testing.T, m anthropicMessage) bool {
	t.Helper()
	var blocks []json.RawMessage
	if err := json.Unmarshal(m.Content, &blocks); err == nil {
		if len(blocks) == 0 {
			return false
		}
		var last map[string]any
		if err := json.Unmarshal(blocks[len(blocks)-1], &last); err != nil {
			return false
		}
		cc, exists := last["cache_control"]
		if !exists {
			return false
		}
		m, ok := cc.(map[string]any)
		return ok && m["type"] == "ephemeral"
	}
	return strings.Contains(string(m.Content), "cache_control")
}

// End-to-end over the built HTTP body: system arrives as a block with
// cache_control, the last tool carries cache_control, and the final
// message's last block is marked.
func TestBuildRequestCacheControlWire(t *testing.T) {
	p := NewAnthropic("k", "")
	tools := []Tool{
		{Function: ToolFunction{Name: "web_search", Description: "search", Parameters: map[string]any{"type": "object"}}},
		{Function: ToolFunction{Name: "exec", Description: "run", Parameters: map[string]any{"type": "object"}}},
	}
	messages := []Message{
		{Role: "system", Content: "MAIN PROMPT"},
		{Role: "user", Content: "u1"},
		{Role: "assistant", Content: "a1"},
		{Role: "user", Content: "u2"},
		{Role: "assistant", Content: "a2"},
		{Role: "user", Content: "最新的问题"},
		{Role: "system", Content: "Current date/time: now"},
	}

	req, err := p.buildRequest(context.Background(), messages, tools, "anthropic/claude-test", 1024, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(req.Body)
	var wire struct {
		System []struct {
			Type         string `json:"type"`
			Text         string `json:"text"`
			CacheControl *struct {
				Type string `json:"type"`
			} `json:"cache_control"`
		} `json:"system"`
		Tools []struct {
			Name         string `json:"name"`
			CacheControl *struct {
				Type string `json:"type"`
			} `json:"cache_control"`
		} `json:"tools"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatalf("body not valid json: %v\n%s", err, body)
	}

	if len(wire.System) != 1 || wire.System[0].Text != "MAIN PROMPT" || wire.System[0].CacheControl == nil {
		t.Fatalf("system blocks = %#v, want single block with cache_control", wire.System)
	}
	if len(wire.Tools) != 2 || wire.Tools[1].CacheControl == nil || wire.Tools[0].CacheControl != nil {
		t.Fatalf("tools cache_control should be on last tool only: %#v", wire.Tools)
	}
	n := len(wire.Messages)
	if wire.Messages[n-1].Role != "user" {
		t.Fatalf("tail system must ride as user role, got %q", wire.Messages[n-1].Role)
	}
	var blocks []map[string]any
	if err := json.Unmarshal(wire.Messages[n-1].Content, &blocks); err != nil || len(blocks) == 0 {
		t.Fatalf("last message content = %s, want block array", wire.Messages[n-1].Content)
	}
	if blocks[len(blocks)-1]["cache_control"] == nil {
		t.Fatalf("last message last block missing cache_control: %s", wire.Messages[n-1].Content)
	}
	var n3 []map[string]any
	if err := json.Unmarshal(wire.Messages[n-3].Content, &n3); err != nil {
		t.Fatalf("n-3 content = %s, want block array (string should have been upgraded)", wire.Messages[n-3].Content)
	}
	if len(n3) == 0 || n3[len(n3)-1]["cache_control"] == nil {
		t.Fatalf("n-3 message last block missing cache_control")
	}
}
