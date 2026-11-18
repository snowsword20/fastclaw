package toolproviders

import "testing"

func TestMeetsAspectNamedShapes(t *testing.T) {
	caps := Capabilities{
		Declared:     true,
		AspectRatios: []string{"1:1", "3:2", "2:3"},
	}
	for _, c := range []struct {
		aspect string
		want   bool
	}{
		{"square", true},    // 1:1 declared
		{"portrait", true},  // 2:3 declared
		{"landscape", true}, // 3:2 declared
		{"SQUARE", true},    // case-insensitive
		{"  square  ", true},
		{"1:1", true},
		{"9:21", false},
		{"", true}, // nothing asked
	} {
		if got := caps.MeetsAspect(c.aspect, ""); got != c.want {
			t.Errorf("MeetsAspect(%q) = %v, want %v", c.aspect, got, c.want)
		}
	}
	// A shape-only set: portrait/landscape names must NOT pass.
	wide := Capabilities{Declared: true, AspectRatios: []string{"16:9"}}
	if wide.MeetsAspect("portrait", "") {
		t.Error("portrait must not match a 16:9-only declaration")
	}
	if !wide.MeetsAspect("landscape", "") {
		t.Error("landscape must match 16:9")
	}
	// Sizes keep their ratio-matching behavior through the name path too.
	if !caps.MeetsAspect("", "1024x1536") {
		t.Error("1024x1536 should match the 2:3 declaration")
	}
}
