package tools

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/agl"
	"github.com/fastclaw-ai/fastclaw/internal/billing"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// ImageFiles is how image_gen reaches the chat's workspace: the agent
// stores each picture there (the reply shows it as /workspace/<name>, and
// the API returns it with the reply), and reads the reference images a
// call names. Session and project are the calling turn's.
type ImageFiles struct {
	Save func(ctx context.Context, sessionID, projectID, name string, data []byte, contentType string) (string, error)
	Load func(ctx context.Context, sessionID, projectID, path string) (agl.ToolFile, error)
}

// Pictures a provider returns as URLs are fetched with these limits.
const (
	maxImageBytes   = 25 << 20
	maxRefImages    = 4
	imageFetchLimit = 60 * time.Second
)

// imageHTTP fetches provider image URLs; it refuses private addresses (a
// configured endpoint could answer with any URL).
var imageHTTP = &http.Client{
	Timeout:   imageFetchLimit,
	Transport: &http.Transport{DialContext: billing.GuardedDial},
}

// RegisterImageGenChain registers the image_gen tool against a provider
// chain. Only registered when at least one provider in the chain has
// credentials configured — so an agent without image-gen keys doesn't see
// a tool it can't use. files stores the pictures in the chat's workspace
// so the model gets a short path, never the image data; nil (no
// workspace) returns the provider's URLs.
// notify, when given, receives a wake-up when a background task
// (image_gen background=true) finishes, so the agent can route it back
// into the originating chat; without it background tasks are poll-only.
func RegisterImageGenChain(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify ...func(ImageTaskNotice)) {
	if chain == nil {
		return
	}
	// "none" is a sentinel meaning the admin explicitly opted out of
	// fastclaw's image_gen. Detected anywhere in the chain → don't
	// register the tool at all so the model falls back to its own
	// native image-generation capability (or does without).
	for _, ref := range chain.Order {
		name := ref
		if i := strings.IndexByte(ref, '/'); i >= 0 {
			name = ref[:i]
		}
		if name == "none" {
			return
		}
	}
	if !chain.Available() {
		return
	}
	tasks := newImgTaskManager(chain, files)
	if len(notify) > 0 {
		tasks.notify = notify[0]
	}
	tasks.recoverAgentID = r.AgentID()
	registerImageTaskTools(r, tasks)
	// Reclaim in-flight imgqueue tasks a previous process submitted
	// (restart recovery). Best-effort and asynchronous; a no-op unless
	// a task-queue provider with surviving tasks is configured.
	go tasks.recoverRemote()
	desc := []string{
		"Generate pictures (photo, illustration, portrait, poster, sticker, logo, …) from a text description with an image model, ready to show in this chat. Use it for every request to draw, paint or make an image.",
		"Write \"prompt\" as a rich visual description: subject, style, composition, colors, mood, and any text that must appear in the picture.",
		"It returns putInReply: put that exactly in your reply so the picture is shown, plus a short line of your own. Don't describe the image data or repeat the paths otherwise.",
		"Set background=true for batches (several pictures or variations) or whenever the person shouldn't have to wait: the call returns a task id at once, a notice arrives in this chat when the pictures are ready, and image_task_status delivers them. Keep the default (sync) for a single quick picture the reply depends on.",
	}
	// Capability matrix: what each provider in the chain supports, so the
	// model asks for parameters that exist instead of discovering limits
	// through provider errors.
	if lines := capabilityLines(chain); len(lines) > 0 {
		desc = append(desc, "Providers in order, with what each supports:")
		desc = append(desc, lines...)
		desc = append(desc, "When the person asks for something no provider lists (a ratio, a quality tier, reference-image editing), say what IS supported and offer the closest option — don't call with unsupported parameters.")
	}
	props := map[string]any{
		"prompt": map[string]any{"type": "string", "description": "What the picture shows, in detail."},
		"aspect": map[string]any{"type": "string", "description": `Shape of the picture: "square", "portrait", "landscape", or an explicit ratio like "3:4"/"16:9" when the provider list above includes it. Default square.`},
		"size":   map[string]any{"type": "string", "description": "Explicit pixel size like \"1024x1024\" — only when the provider list above names sizes; otherwise prefer aspect."},
		"quality": map[string]any{"type": "string", "description": "Fidelity tier like \"low\"/\"medium\"/\"high\" — only when the provider list above names tiers; omit otherwise."},
		"n":      map[string]any{"type": "integer", "description": "How many variations (default 1). Each one is charged; respect each provider's per-call limit above."},
		"background": map[string]any{"type": "boolean", "description": "Run as a background task instead of waiting: returns a task id immediately (picture arrives later via a notice + image_task_status). Use for batches or long queues."},
	}
	if files != nil && files.Load != nil {
		desc = append(desc, "To edit a photo the person sent or base a picture on it (a new style, an avatar, a sticker of them), list it in \"images\" by its /workspace path.")
		props["images"] = filesSchema(fmt.Sprintf(`Reference images from this chat, by workspace path, e.g. ["/workspace/image_3jk7l_0.jpg"] (at most %d).`, maxRefImages))
	}
	r.Register("image_gen", strings.Join(desc, "\n"), map[string]any{
		"type":       "object",
		"properties": props,
		"required":   []string{"prompt"},
	}, func(ctx context.Context, rawArgs json.RawMessage) (string, error) {
		var args map[string]any
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("parse args: %w", err)
		}
		turn := r.forContext(ctx)
		if err := attachRefs(ctx, files, turn, args); err != nil {
			return "", err
		}
		// Requirement gate: when every provider in the chain DECLARED it
		// can't take something the call asks for, fail before any network
		// round-trip with a message the model can relay verbatim.
		if why := unionShortfall(chain, args); why != "" {
			return "", &agl.ToolError{Msg: why}
		}
		// Sync/async: the model's "background" argument decides, and the
		// caller can force either way via turn params (chat API `params`:
		// {"image_gen_mode": "background"} or "sync" — caller wins).
		bg, _ := args["background"].(bool)
		switch turn.TurnParamString("image_gen_mode") {
		case "background", "async":
			bg = true
		case "sync":
			bg = false
		}
		if bg {
			delete(args, "background")
			return tasks.submit(ctx, turn, args)
		}
		resp, err := chain.Execute(ctx, args)
		if err != nil {
			return "", err
		}
		if files == nil || files.Save == nil || len(resp.Images) == 0 {
			if len(resp.Images) > 0 {
				return urlsOnly(resp.Images), nil
			}
			return resp.Text, nil
		}
		return storeImages(ctx, files, turn, resp.Images)
	})
}

// attachRefs turns the "images" workspace paths into data: URLs for the
// providers (args["image_urls"]).
func attachRefs(ctx context.Context, files *ImageFiles, turn *Registry, args map[string]any) error {
	raw, _ := args["images"].([]any)
	delete(args, "images")
	if len(raw) == 0 {
		return nil
	}
	if files == nil || files.Load == nil {
		return &agl.ToolError{Msg: "Reference images can't be used in this chat."}
	}
	if len(raw) > maxRefImages {
		return &agl.ToolError{Msg: fmt.Sprintf("At most %d reference images.", maxRefImages)}
	}
	urls := make([]string, 0, len(raw))
	for _, v := range raw {
		p, _ := v.(string)
		f, err := files.Load(ctx, turn.SessionID(), turn.ProjectID(), p)
		if err != nil {
			return &agl.ToolError{Msg: fmt.Sprintf("Could not read %s from the workspace: %v. Use the exact /workspace path of the picture.", p, err)}
		}
		ct := http.DetectContentType(f.Bytes)
		if !strings.HasPrefix(ct, "image/") {
			return &agl.ToolError{Msg: p + " is not a picture."}
		}
		urls = append(urls, "data:"+ct+";base64,"+base64.StdEncoding.EncodeToString(f.Bytes))
	}
	args["image_urls"] = urls
	return nil
}

// storeImages saves each picture in the chat's workspace and returns the
// paths for the reply.
func storeImages(ctx context.Context, files *ImageFiles, turn *Registry, images []toolproviders.Image) (string, error) {
	_, blob, err := storeImagesAt(ctx, files, turn.SessionID(), turn.ProjectID(), images)
	return blob, err
}

// storeImagesAt is storeImages for an explicit session (the background
// task runner calls it after the submitting turn is gone). It returns the
// stored paths and the model-facing JSON blob.
func storeImagesAt(ctx context.Context, files *ImageFiles, sessionID, projectID string, images []toolproviders.Image) ([]string, string, error) {
	stamp := time.Now().Format("20060102-150405")
	var paths []string
	var failed []string
	for i, img := range images {
		data, ct := img.Data, img.ContentType
		if len(data) == 0 && img.URL != "" {
			var err error
			data, ct, err = fetchImage(ctx, img.URL)
			if err != nil {
				failed = append(failed, err.Error())
				continue
			}
		}
		if len(data) == 0 {
			continue
		}
		if ct == "" || !strings.HasPrefix(ct, "image/") {
			ct = http.DetectContentType(data)
		}
		name := fmt.Sprintf("picture-%s-%d%s", stamp, i+1, imageExt(ct))
		saved, err := files.Save(ctx, sessionID, projectID, name, data, ct)
		if err != nil {
			failed = append(failed, err.Error())
			continue
		}
		paths = append(paths, "/workspace/"+saved)
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("image_gen: the pictures were generated but could not be stored: %s", strings.Join(failed, "; "))
	}
	var reply strings.Builder
	for _, p := range paths {
		fmt.Fprintf(&reply, "![](%s)\n", p)
	}
	blob, _ := json.Marshal(map[string]any{
		"images":     paths,
		"putInReply": strings.TrimSpace(reply.String()),
	})
	return paths, string(blob), nil
}

func fetchImage(ctx context.Context, u string) ([]byte, string, error) {
	if err := billing.CheckScheme(u); err != nil {
		return nil, "", fmt.Errorf("image url: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, imageFetchLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := imageHTTP.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download image: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("download image: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxImageBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("download image: %w", err)
	}
	if len(data) > maxImageBytes {
		return nil, "", fmt.Errorf("image larger than %d MB", maxImageBytes>>20)
	}
	return data, resp.Header.Get("Content-Type"), nil
}

func imageExt(ct string) string {
	switch {
	case strings.HasPrefix(ct, "image/jpeg"):
		return ".jpg"
	case strings.HasPrefix(ct, "image/webp"):
		return ".webp"
	case strings.HasPrefix(ct, "image/gif"):
		return ".gif"
	}
	return ".png"
}

// urlsOnly is the reply without a workspace: the provider's URLs (never
// inline image data, which would flood the context).
func urlsOnly(images []toolproviders.Image) string {
	var b strings.Builder
	n := 0
	for _, img := range images {
		if img.URL == "" {
			continue
		}
		n++
		fmt.Fprintf(&b, "![image %d](%s)\n", n, img.URL)
	}
	if n == 0 {
		return "The pictures were generated but can't be shown in this chat (no workspace to store them)."
	}
	return "putInReply:\n" + b.String()
}

// capabilityLines renders the chain's per-provider capability matrix for
// the tool description: one line per "<provider>/<model>" in order.
// Providers that don't declare capabilities are marked unknown rather
// than omitted — the model should still know it can try.
func capabilityLines(chain *toolproviders.Chain) []string {
	var out []string
	for i, ref := range chain.Order {
		name, model := splitProviderRef(ref)
		caps := toolproviders.CapabilitiesOf(chain.Registry, chain.Category, name, model, chain.GetConfig(name))
		var b strings.Builder
		fmt.Fprintf(&b, "%d. %s — ", i+1, ref)
		if !caps.Declared {
			b.WriteString("capabilities unknown; common options (1:1, square) usually work — otherwise explain and pick another provider")
			out = append(out, b.String())
			continue
		}
		var parts []string
		if len(caps.Sizes) > 0 {
			parts = append(parts, "sizes "+strings.Join(caps.Sizes, "|"))
		} else if len(caps.AspectRatios) > 0 {
			parts = append(parts, "ratios "+strings.Join(caps.AspectRatios, "|"))
		}
		if len(caps.Quality) > 0 {
			parts = append(parts, "quality "+strings.Join(caps.Quality, "|"))
		}
		if caps.MaxImages > 0 {
			parts = append(parts, fmt.Sprintf("up to %d per call", caps.MaxImages))
		}
		if caps.MaxRefImages > 0 {
			parts = append(parts, fmt.Sprintf("up to %d reference images", caps.MaxRefImages))
		} else {
			parts = append(parts, "no reference images")
		}
		if len(parts) == 0 {
			b.WriteString("no capability details")
		} else {
			b.WriteString(strings.Join(parts, ", "))
		}
		out = append(out, b.String())
	}
	return out
}

// unionShortfall checks the call's asks against the UNION of the chain's
// declared capabilities and returns a relayable message when some ask is
// impossible everywhere ("" when fine). Unknown capabilities (Declared
// false) count as pass — fail-open, matching the magpie gateway's rule.
func unionShortfall(chain *toolproviders.Chain, args map[string]any) string {
	str := func(k string) string {
		s, _ := args[k].(string)
		return strings.TrimSpace(s)
	}
	aspect, size, quality := str("aspect"), str("size"), str("quality")
	refs := 0
	if raw, ok := args["image_urls"].([]string); ok {
		refs = len(raw)
	} else if raw, ok := args["image_urls"].([]any); ok {
		refs = len(raw)
	}
	n := 1
	switch v := args["n"].(type) {
	case float64:
		n = int(v)
	case int:
		n = v
	}
	if aspect == "" && size == "" && quality == "" && refs == 0 && n <= 1 {
		return ""
	}
	meetsAspect, meetsQuality, meetsRefs, meetsN := false, false, refs == 0, n <= 1
	var anyAspect, anyQuality []string
	for _, ref := range chain.Order {
		name, model := splitProviderRef(ref)
		caps := toolproviders.CapabilitiesOf(chain.Registry, chain.Category, name, model, chain.GetConfig(name))
		if caps.MeetsAspect(aspect, size) {
			meetsAspect = true
		}
		if caps.MeetsQuality(quality) {
			meetsQuality = true
		}
		if caps.MeetsRefs(refs) {
			meetsRefs = true
		}
		if caps.MeetsN(n) {
			meetsN = true
		}
		if caps.Declared {
			anyAspect = append(anyAspect, caps.AspectRatios...)
			anyAspect = append(anyAspect, caps.Sizes...)
			anyQuality = append(anyQuality, caps.Quality...)
		}
	}
	var missing []string
	if !meetsAspect {
		if seen := dedupe(anyAspect); len(seen) > 0 {
			missing = append(missing, fmt.Sprintf("shape %q (supported: %s)", orDefault(aspect, size), strings.Join(seen, ", ")))
		} else {
			missing = append(missing, fmt.Sprintf("shape %q (only standard shapes are supported)", orDefault(aspect, size)))
		}
	}
	if !meetsQuality {
		if seen := dedupe(anyQuality); len(seen) > 0 {
			missing = append(missing, fmt.Sprintf("quality %q (supported: %s)", quality, strings.Join(seen, ", ")))
		} else {
			missing = append(missing, fmt.Sprintf("quality %q (no fidelity tiers on any provider)", quality))
		}
	}
	if !meetsRefs {
		missing = append(missing, fmt.Sprintf("reference-image editing with %d image(s) (no provider in the chain can edit)", refs))
	}
	if !meetsN {
		missing = append(missing, fmt.Sprintf("%d pictures in one call (fewer per call are supported)", n))
	}
	if len(missing) == 0 {
		return ""
	}
	return "image_gen can't take this request as asked: " + strings.Join(missing, "; ") +
		". Tell the person what is supported and offer the closest option."
}

// splitProviderRef splits "fal/flux-dev" into ("fal", "flux-dev"); a
// magpie model ref keeps its own slashes ("magpie", "pool/gpt-image-2").
func splitProviderRef(ref string) (string, string) {
	if i := strings.IndexByte(ref, '/'); i >= 0 {
		return ref[:i], ref[i+1:]
	}
	return ref, ""
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func orDefault(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
