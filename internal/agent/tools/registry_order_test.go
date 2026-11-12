package tools

import "testing"

// Tool definitions serialize ahead of the message history on the LLM
// wire, so their order must be deterministic — Go map iteration yields a
// random rotation per call, which used to invalidate the provider prefix
// cache (and relocate the last-tool cache_control breakpoint) on every
// request.
func TestDefinitionsForModeDeterministicOrder(t *testing.T) {
	r := &Registry{tools: make(map[string]registeredTool)}
	names := []string{"zeta", "alpha", "mid_tool", "beta", "omega"}
	for _, n := range names {
		r.Register(n, "test tool "+n, nil, nil)
	}

	first := r.DefinitionsForMode(nil)
	for i := 0; i < 20; i++ {
		again := r.DefinitionsForMode(nil)
		if len(again) != len(first) {
			t.Fatalf("tool count changed: %d vs %d", len(again), len(first))
		}
		for j := range first {
			if first[j].Function.Name != again[j].Function.Name {
				t.Fatalf("DefinitionsForMode order unstable (map iteration leak) at pos %d: %q vs %q — prefix cache would miss every turn",
					j, first[j].Function.Name, again[j].Function.Name)
			}
		}
	}
	if first[0].Function.Name != "alpha" || first[len(first)-1].Function.Name != "zeta" {
		t.Fatalf("tools not sorted by name: first=%q last=%q", first[0].Function.Name, first[len(first)-1].Function.Name)
	}

	defs := r.Definitions()
	for i := 1; i < len(defs); i++ {
		if defs[i-1].Function.Name > defs[i].Function.Name {
			t.Fatalf("Definitions not sorted by name: %q before %q", defs[i-1].Function.Name, defs[i].Function.Name)
		}
	}
}
