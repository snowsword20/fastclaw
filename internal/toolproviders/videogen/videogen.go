// Package videogen bundles video-generation providers. The category is
// queue-backed by design: video takes minutes, so the provider submits a
// task to a media queue (magpie → the imgqueue plugin, which drives the
// gateway's native /v1/videos API and its account pool), polls it, and
// returns the finished clip as a MediaFile for workspace storage.
package videogen

import (
	"context"
	"fmt"
	"strings"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// Category is the tool category these providers plug into.
const Category = "video_gen"

// RegisterAll installs built-in video_gen providers in r.
func RegisterAll(r *toolproviders.Registry) {
	r.Register(&Magpie{})
	r.Register(&None{})
}

// args is the video_gen tool's argument surface (the OpenAI Videos
// shape the gateway takes, plus image_urls for image-to-video).
type args struct {
	Prompt     string
	Seconds    string
	Size       string
	Resolution string
	// Refs carries data: URLs of the first frame (image-to-video); the
	// tool layer builds them from workspace paths.
	Refs []string
}

func parseArgs(raw map[string]any) (args, error) {
	var a args
	if s, ok := raw["prompt"].(string); ok {
		a.Prompt = strings.TrimSpace(s)
	}
	if a.Prompt == "" {
		return a, fmt.Errorf("prompt is required")
	}
	if s, ok := raw["seconds"].(string); ok {
		a.Seconds = strings.TrimSpace(s)
	} else if v, ok := raw["seconds"].(float64); ok && v > 0 {
		a.Seconds = fmt.Sprintf("%d", int(v))
	}
	if s, ok := raw["size"].(string); ok {
		a.Size = strings.TrimSpace(s)
	}
	if s, ok := raw["resolution"].(string); ok {
		a.Resolution = strings.TrimSpace(s)
	}
	if rawRefs, ok := raw["image_urls"].([]string); ok {
		a.Refs = rawRefs
	} else if rawAny, ok := raw["image_urls"].([]any); ok {
		for _, v := range rawAny {
			if s, ok := v.(string); ok {
				a.Refs = append(a.Refs, s)
			}
		}
	}
	return a, nil
}

// None is a sentinel provider meaning "do not expose video_gen to the
// model" — same rule as imagegen.None / tts.None.
type None struct{}

func (None) Category() string     { return Category }
func (None) Name() string         { return "none" }
func (None) CredentialFree() bool { return true }

func (None) Execute(_ context.Context, _ toolproviders.Request) (toolproviders.Response, error) {
	return toolproviders.Response{}, toolproviders.ErrNoResults
}
