package imagegen

import (
	"strconv"
	"strings"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// Static capability answers for the built-in direct providers, from the
// vendors' public docs. These are compile-time knowledge — the magpie
// provider is the one that answers from a live manifest.

// OpenAI: gpt-image-1 takes sizes/quality/edits; dall-e-3 is fixed-shape
// and edit-less. The provider doubles as the escape hatch for any
// OpenAI-compatible images endpoint (endpoint option points elsewhere,
// model passes through) — for those, declare the real capabilities via
// the `caps` option instead of inheriting gpt-image defaults:
//
//	options: {"caps": "aspectRatios=1:1,3:2,2:3; quality=low,high; maxImages=4; maxRefImages=2"}
func (OpenAI) Capabilities(cfg toolproviders.ProviderConfig, model string) toolproviders.Capabilities {
	if caps, ok := parseCapsOption(cfg.Options["caps"]); ok {
		return caps
	}
	if strings.HasPrefix(model, "dall-e-3") {
		return toolproviders.Capabilities{
			Sizes:        []string{"1024x1024", "1792x1024", "1024x1792"},
			Quality:      []string{"standard", "hd"},
			MaxImages:    1,
			MaxRefImages: 0,
			Declared:     true,
		}
	}
	// gpt-image-1 (the default) and any newer gpt-image family member.
	return toolproviders.Capabilities{
		AspectRatios: []string{"1:1", "3:2", "2:3"},
		Sizes:        []string{"1024x1024", "1536x1024", "1024x1536", "auto"},
		Quality:      []string{"low", "medium", "high", "auto"},
		MaxImages:    10,
		MaxRefImages: 16,
		Declared:     true,
	}
}

// Fal: flux models take aspect ratios and up to 4 outputs; reference
// images need an edit-capable model — the edit_model option declares
// one, otherwise the model path itself must be an edit model.
func (Fal) Capabilities(cfg toolproviders.ProviderConfig, model string) toolproviders.Capabilities {
	if strings.Contains(strings.ToLower(model), "seedream") {
		return toolproviders.Capabilities{
			AspectRatios: []string{"1:1", "4:3", "3:4", "16:9", "9:16", "3:2", "2:3", "21:9", "9:21"},
			MaxImages:    4,
			MaxRefImages: 4,
			Declared:     true,
		}
	}
	if strings.Contains(strings.ToLower(model), "qwen-image") {
		return toolproviders.Capabilities{
			AspectRatios: []string{"1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"},
			MaxImages:    4,
			MaxRefImages: 3,
			Declared:     true,
		}
	}
	// flux family and everything else on fal
	return toolproviders.Capabilities{
		AspectRatios: []string{"1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3", "21:9", "9:21"},
		MaxImages:    4,
		MaxRefImages: refsForEditOption(cfg),
		Declared:     true,
	}
}

// refsForEditOption is 4 when the config names an image-to-image model
// (fal's edit_model option), else 0: the plain text-to-image models on
// fal can't take reference images.
func refsForEditOption(cfg toolproviders.ProviderConfig) int {
	if cfg.Options["edit_model"] != "" || strings.Contains(strings.ToLower(cfg.Options["edit_model"])+strings.ToLower(cfg.Model), "edit") {
		return 4
	}
	return 0
}

// Replicate: flux takes aspect_ratio, sdxl/ideogram vary; none of the
// routed models take reference images through this adapter.
func (Replicate) Capabilities(_ toolproviders.ProviderConfig, model string) toolproviders.Capabilities {
	m := strings.ToLower(model)
	if strings.Contains(m, "ideogram") {
		return toolproviders.Capabilities{
			AspectRatios: []string{"1:1", "16:10", "10:16", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"},
			Quality:      []string{"standard", "high"},
			MaxImages:    4,
			MaxRefImages: 0,
			Declared:     true,
		}
	}
	return toolproviders.Capabilities{
		AspectRatios: []string{"1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"},
		MaxImages:    4,
		MaxRefImages: 0,
		Declared:     true,
	}
}

// parseCapsOption decodes the `caps` provider option — semicolon-
// separated key=value lists (aspectRatios/sizes/quality are comma-
// separated; maxImages/maxRefImages are ints). ok=false when the option
// is empty or parses to nothing.
func parseCapsOption(raw string) (toolproviders.Capabilities, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return toolproviders.Capabilities{}, false
	}
	var caps toolproviders.Capabilities
	any := false
	for _, part := range strings.Split(raw, ";") {
		k, v, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if v == "" {
			continue
		}
		list := func() []string {
			out := make([]string, 0, 4)
			for _, item := range strings.Split(v, ",") {
				if item = strings.TrimSpace(item); item != "" {
					out = append(out, item)
				}
			}
			return out
		}
		switch strings.ToLower(k) {
		case "aspectratios":
			caps.AspectRatios, any = list(), true
		case "sizes":
			caps.Sizes, any = list(), true
		case "quality":
			caps.Quality, any = list(), true
		case "maximages":
			if n, err := strconv.Atoi(v); err == nil {
				caps.MaxImages, any = n, true
			}
		case "maxrefimages":
			if n, err := strconv.Atoi(v); err == nil {
				caps.MaxRefImages, any = n, true
			}
		}
	}
	caps.Declared = any
	return caps, any
}
