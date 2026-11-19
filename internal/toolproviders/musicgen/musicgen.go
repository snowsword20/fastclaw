// Package musicgen bundles music-generation providers. Music is
// minutes-long, queue-native work like video: the provider submits a
// media=music task to the imgqueue plugin, which dispatches to whatever
// music upstream it has configured (v0.3 ships the task plumbing with
// the upstream slot still open — tasks fail with a clear "上游未配置"
// message until one is wired in, so the tool surface is ready and zero
// code changes land when the upstream arrives).
package musicgen

import (
	"context"
	"fmt"
	"strings"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// Category is the tool category these providers plug into.
const Category = "music_gen"

// RegisterAll installs built-in music_gen providers in r.
func RegisterAll(r *toolproviders.Registry) {
	r.Register(&Magpie{})
	r.Register(&None{})
}

// args is the music_gen tool's argument surface.
type args struct {
	Prompt       string
	Style        string
	Duration     string
	Instrumental bool
	Title        string
}

func parseArgs(raw map[string]any) (args, error) {
	var a args
	if s, ok := raw["prompt"].(string); ok {
		a.Prompt = strings.TrimSpace(s)
	}
	if a.Prompt == "" {
		return a, fmt.Errorf("prompt is required")
	}
	if s, ok := raw["style"].(string); ok {
		a.Style = strings.TrimSpace(s)
	}
	if s, ok := raw["duration"].(string); ok {
		a.Duration = strings.TrimSpace(s)
	} else if v, ok := raw["duration"].(float64); ok && v > 0 {
		a.Duration = fmt.Sprintf("%d", int(v))
	}
	if b, ok := raw["instrumental"].(bool); ok {
		a.Instrumental = b
	}
	if s, ok := raw["title"].(string); ok {
		a.Title = strings.TrimSpace(s)
	}
	return a, nil
}

// Magpie generates music through a magpie gateway's imgqueue plugin.
type Magpie struct{}

func (Magpie) Category() string { return Category }
func (Magpie) Name() string     { return "magpie" }

func (g *Magpie) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	if _, err := parseArgs(req.Args); err != nil {
		return toolproviders.Response{}, err
	}
	// The queue-side music executor is a placeholder until an upstream
	// is wired (imgqueue lib/executors/music.mjs); say so up front with
	// a non-retriable error rather than burning retries on it.
	return toolproviders.Response{}, fmt.Errorf(
		"music generation: no upstream configured yet (the magpie imgqueue plugin's music executor is a placeholder); wire a music provider on the gateway side first")
}

// None is a sentinel provider meaning "do not expose music_gen to the
// model" — same rule as imagegen.None / tts.None.
type None struct{}

func (None) Category() string     { return Category }
func (None) Name() string         { return "none" }
func (None) CredentialFree() bool { return true }

func (None) Execute(_ context.Context, _ toolproviders.Request) (toolproviders.Response, error) {
	return toolproviders.Response{}, toolproviders.ErrNoResults
}
