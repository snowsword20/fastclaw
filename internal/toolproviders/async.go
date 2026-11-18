package toolproviders

import "context"

// Background-image marker. The image_gen tool's background mode sets this
// on the context before calling Chain.Execute, so providers that can
// queue work natively (magpie → the imgqueue plugin's task API) do that
// instead of holding one long synchronous HTTP call. Providers without a
// native queue simply ignore it and run as usual — the caller is a
// goroutine either way, so the contract is the same for every provider.

type asyncImageKey struct{}

// WithAsyncImage marks ctx as a background (task-queue) image call.
func WithAsyncImage(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncImageKey{}, true)
}

// AsyncImageFrom reports whether ctx carries the background-image marker.
func AsyncImageFrom(ctx context.Context) bool {
	v, _ := ctx.Value(asyncImageKey{}).(bool)
	return v
}
