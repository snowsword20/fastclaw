package imagegen

import (
	"strings"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// Static capability answers for the built-in direct providers, from the
// vendors' public docs. These are compile-time knowledge — the magpie
// provider is the one that answers from a live manifest.

// OpenAI: gpt-image-1 takes sizes/quality/edits; dall-e-3 is fixed-shape
// and edit-less.
func (OpenAI) Capabilities(_ toolproviders.ProviderConfig, model string) toolproviders.Capabilities {
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
