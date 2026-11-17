package toolproviders

import "strings"

// Capabilities is what a provider's model can do, as the image_gen
// routing layer consumes it: the tool description tells the model what
// is askable, chain filtering skips providers that declare they can't
// meet a requirement, and the frontend (via a future REST endpoint)
// renders parameter forms from it.
//
// The authority for these fields is the provider itself: built-in
// providers answer from compile-time knowledge, the magpie provider
// fetches a live matrix from the gateway (see imagegen/magpie.go).
// An unknown model reports Declared=false — consumers must treat that
// as "capabilities unknown" (fail-open), never as "supports nothing".
type Capabilities struct {
	// AspectRatios are the W:H shapes the model outputs ("1:1", "3:4").
	AspectRatios []string `json:"aspectRatios,omitempty"`
	// Sizes are explicit pixel sizes ("1024x1024", "auto"). A provider
	// lists either sizes or ratios or both; sizes win when both exist.
	Sizes []string `json:"sizes,omitempty"`
	// Quality is the fidelity tiers the model takes ("low", "high").
	Quality []string `json:"quality,omitempty"`
	// MaxImages is how many pictures one call may return.
	MaxImages int `json:"maxImages"`
	// MaxRefImages is how many reference pictures an edit may take;
	// 0 with Declared=true means the model cannot edit at all.
	MaxRefImages int `json:"maxRefImages"`
	// Declared is whether someone authoritative answered for this model.
	// false = unknown, callers fail open instead of filtering it out.
	Declared bool `json:"declared"`
}

// CapabilityReporter is the optional interface a Provider implements to
// answer per-model capabilities. cfg is the resolved per-tenant config
// (endpoint/key/options) so a remote provider can reach its manifest.
type CapabilityReporter interface {
	Capabilities(cfg ProviderConfig, model string) Capabilities
}

// CapabilitiesOf reports the capabilities of category/name's model, or
// the zero value (Declared=false) when the provider doesn't implement
// CapabilityReporter or isn't registered.
func CapabilitiesOf(reg *Registry, category, name, model string, cfg ProviderConfig) Capabilities {
	if reg == nil {
		return Capabilities{}
	}
	p := reg.Get(category, name)
	if p == nil {
		return Capabilities{}
	}
	if cr, ok := p.(CapabilityReporter); ok {
		return cr.Capabilities(cfg, model)
	}
	return Capabilities{}
}

// MeetsAspect reports whether caps covers the aspect (or a size whose
// ratio matches). Unknown caps (Declared=false) always pass: fail-open.
func (c Capabilities) MeetsAspect(aspect, size string) bool {
	if !c.Declared {
		return true
	}
	ratios := c.aspectRatioSet()
	if len(ratios) == 0 {
		return true // nothing declared about shape
	}
	if aspect != "" {
		return ratios[normalizeRatio(aspect)]
	}
	if w, h, ok := parseSize(size); ok {
		for r := range ratios {
			if rw, rh, ok2 := parseRatio(r); ok2 && absRatio(w, h, rw, rh) {
				return true
			}
		}
		return false
	}
	return true
}

// MeetsQuality reports whether quality is a declared tier. Unknown caps
// fail open; a declared model with no quality tiers accepts anything
// (the tier is ignored downstream, not rejected here).
func (c Capabilities) MeetsQuality(quality string) bool {
	if !c.Declared || quality == "" || len(c.Quality) == 0 {
		return true
	}
	q := strings.ToLower(quality)
	for _, want := range c.Quality {
		if strings.EqualFold(want, q) {
			return true
		}
	}
	return false
}

// MeetsRefs reports whether the model can take want reference images.
// Unknown caps fail open.
func (c Capabilities) MeetsRefs(want int) bool {
	if !c.Declared {
		return true
	}
	return c.MaxRefImages >= want
}

// MeetsN reports whether one call may return n pictures.
func (c Capabilities) MeetsN(n int) bool {
	if !c.Declared || c.MaxImages == 0 {
		return true
	}
	return c.MaxImages >= n
}

func (c Capabilities) aspectRatioSet() map[string]bool {
	out := map[string]bool{}
	for _, r := range c.AspectRatios {
		if n := normalizeRatio(r); n != "" {
			out[n] = true
		}
	}
	for _, s := range c.Sizes {
		if w, h, ok := parseSize(s); ok {
			out[reduceRatio(w, h)] = true
		}
	}
	return out
}

// normalizeRatio canonicalizes "3:4"-ish input to reduced digits.
func normalizeRatio(r string) string {
	w, h, ok := parseRatio(r)
	if !ok {
		return ""
	}
	return reduceRatio(w, h)
}

func parseRatio(r string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimSpace(r), ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	var w, h int
	if !atoi(parts[0], &w) || !atoi(parts[1], &h) || w <= 0 || h <= 0 || w > 999 || h > 999 {
		return 0, 0, false
	}
	return w, h, true
}

// reduceRatio reduces w:h to lowest terms ("1792:1024" → "7:4").
func reduceRatio(w, h int) string {
	a, b := w, h
	for b != 0 {
		a, b = b, a%b
	}
	if a == 0 {
		return ""
	}
	return itoa(w/a) + ":" + itoa(h/a)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func parseSize(s string) (int, int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || s == "auto" {
		return 0, 0, false
	}
	parts := strings.SplitN(s, "x", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	var w, h int
	if !atoi(parts[0], &w) || !atoi(parts[1], &h) || w <= 0 || h <= 0 {
		return 0, 0, false
	}
	return w, h, true
}

func atoi(s string, out *int) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false
		}
		n = n*10 + int(ch-'0')
		if n > 1<<20 {
			return false
		}
	}
	*out = n
	return true
}

// absRatio reports whether w/h and rw/rh match within 2% — a declared
// 3:4 covers a 1024x1366 implementation size (512:683 reduced).
func absRatio(w, h, rw, rh int) bool {
	a := float64(w) / float64(h)
	b := float64(rw) / float64(rh)
	d := a - b
	if d < 0 {
		d = -d
	}
	return d/b < 0.02
}
