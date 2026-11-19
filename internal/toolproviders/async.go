package toolproviders

import "context"

// Background-image marker. The image_gen tool's background mode sets this
// on the context before calling Chain.Execute, so providers that can
// queue work natively (magpie → the imgqueue plugin's task API) do that
// instead of holding one long synchronous HTTP call. Providers without a
// native queue simply ignore it and run as usual — the caller is a
// goroutine either way, so the contract is the same for every provider.
//
// The payload carries the submitting chat's routing identity so a
// task-queue provider can stamp it onto the remote task's meta: that is
// what lets fastclaw reclaim in-flight remote tasks after a restart
// (imgqueue survives fastclaw; the meta says who owns the task and where
// the finished pictures should be delivered).

type asyncImageKey struct{}

// AsyncImageMeta is the recovery/routing identity of one background
// media call (image_gen, video_gen, tts background, music_gen — the
// marker is category-agnostic; Media says which).
type AsyncImageMeta struct {
	AgentID   string
	UserID    string // fastclaw owner user
	SessionID string // workspace scope the results belong to
	ProjectID string // optional project scope
	// Channel/AccountID/ChatID name the bus address of the chat that
	// submitted the task — where the completion notice goes.
	Channel, AccountID, ChatID string
	Prompt                     string
	// Media is the category tag stamped onto the remote task's meta
	// ("image" | "video" | "tts" | "music"). A task-queue provider copies
	// it through, and restart recovery filters on it so one category's
	// recovery loop never reclaims another's tasks. Empty == "image"
	// (the original callers predate the field).
	Media string
}

// AsyncMediaName maps a Media tag to its canonical value ("" → "image").
func AsyncMediaName(media string) string {
	switch media {
	case "video", "tts", "music":
		return media
	default:
		return "image"
	}
}

// WithAsyncImage marks ctx as a background (task-queue) image call and
// attaches the submitter's routing identity.
func WithAsyncImage(ctx context.Context, meta AsyncImageMeta) context.Context {
	return context.WithValue(ctx, asyncImageKey{}, meta)
}

// AsyncImageFrom reports the background-image meta carried by ctx; ok is
// false for synchronous calls (no marker).
func AsyncImageFrom(ctx context.Context) (AsyncImageMeta, bool) {
	meta, ok := ctx.Value(asyncImageKey{}).(AsyncImageMeta)
	return meta, ok
}

// AsyncImageRequested reports whether ctx carries the background marker,
// ignoring the payload.
func AsyncImageRequested(ctx context.Context) bool {
	_, ok := ctx.Value(asyncImageKey{}).(AsyncImageMeta)
	return ok
}
