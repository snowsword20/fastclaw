package provider

import (
	"strings"
	"testing"
)

// Anthropic-compatible gateways (e.g. Zhipu's /api/anthropic) emit
// placeholder zeros in message_start and publish the real input/cache
// totals only in the final message_delta. Real Anthropic does the
// opposite (input in message_start, output-only in message_delta).
// parseSSE must surface the real totals under both layouts.
func TestParseSSEUsageFromDeltaFallback(t *testing.T) {
	stream := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"usage":{"input_tokens":0,"output_tokens":0}}}`,
		"",
		"event: content_block_start",
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		"",
		"event: content_block_delta",
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":334,"output_tokens":16,"cache_read_input_tokens":320}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	p := NewAnthropic("k", "")
	resp, err := p.parseSSE(strings.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.InputTokens != 334 {
		t.Fatalf("InputTokens = %d, want 334 (real value only arrives in message_delta)", resp.Usage.InputTokens)
	}
	if resp.Usage.OutputTokens != 16 {
		t.Fatalf("OutputTokens = %d, want 16", resp.Usage.OutputTokens)
	}
	if resp.Usage.CacheReadTokens != 320 {
		t.Fatalf("CacheReadTokens = %d, want 320 (cache hit reported in message_delta)", resp.Usage.CacheReadTokens)
	}
}

// Real Anthropic layout: message_start carries the input/cache totals,
// message_delta only output_tokens. The delta merge must not clobber
// those values with zeros.
func TestParseSSEUsageFromStartNotClobbered(t *testing.T) {
	stream := strings.Join([]string{
		"event: message_start",
		`data: {"type":"message_start","message":{"usage":{"input_tokens":1200,"output_tokens":1,"cache_read_input_tokens":900,"cache_creation_input_tokens":200}}}`,
		"",
		"event: message_delta",
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":42}}`,
		"",
		"event: message_stop",
		`data: {"type":"message_stop"}`,
		"",
	}, "\n")

	p := NewAnthropic("k", "")
	resp, err := p.parseSSE(strings.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.InputTokens != 1200 || resp.Usage.CacheReadTokens != 900 || resp.Usage.CacheCreationTokens != 200 {
		t.Fatalf("start-event totals clobbered by delta merge: %#v", resp.Usage)
	}
	if resp.Usage.OutputTokens != 42 {
		t.Fatalf("OutputTokens = %d, want 42", resp.Usage.OutputTokens)
	}
}
