package imagegen

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// Magpie talks to a magpie gateway through its imgqueue plugin's facade
// (http://127.0.0.1:37987 by default):
//
//	GET  /v1/capabilities        the live capability matrix — declared
//	                             (admin-asserted) entries drive routing
//	POST /v1/images/generations  sync text-to-image, OpenAI images shape
//	POST /v1/images/edits        sync reference-image edit (multipart)
//
// The plugin validates requests against its matrix before proxying to
// the gateway, so pool rotation, account failover and model routing all
// stay on the magpie side. Endpoint (ProviderConfig.Endpoint) is the
// plugin's base URL; APIKey is its Bearer token.
//
// A "magpie/auto" chain reference routes by requirement: the first
// matrix model that declares it can take the call wins; an explicit
// model ("magpie/gptfree-pool/gpt-image-2.6") pins it.
type Magpie struct{}

func (Magpie) Category() string { return Category }
func (Magpie) Name() string     { return "magpie" }

// magpieCapabilityTTL bounds how stale the cached matrix may be; the
// plugin refreshes its own gateway view on a shorter cycle, so this
// only saves fastclaw the extra hop per call.
const magpieCapabilityTTL = 5 * time.Minute

type magpieMatrixEntry struct {
	Ref          string `json:"ref"`
	Label        string `json:"label"`
	CanGenerate  bool   `json:"canGenerate"`
	CanEdit      bool   `json:"canEdit"`
	Declared     bool   `json:"declared"`
	Caps         *struct {
		AspectRatios []string `json:"aspectRatios"`
		Sizes        []string `json:"sizes"`
		Quality      []string `json:"quality"`
		MaxImages    int      `json:"maxImages"`
		MaxRefImages int      `json:"maxRefImages"`
	} `json:"caps"`
}

func (e magpieMatrixEntry) capabilities() toolproviders.Capabilities {
	if e.Caps == nil {
		return toolproviders.Capabilities{Declared: false}
	}
	return toolproviders.Capabilities{
		AspectRatios: e.Caps.AspectRatios,
		Sizes:        e.Caps.Sizes,
		Quality:      e.Caps.Quality,
		MaxImages:    e.Caps.MaxImages,
		MaxRefImages: e.Caps.MaxRefImages,
		Declared:     e.Declared,
	}
}

type magpieMatrix struct {
	Models []magpieMatrixEntry `json:"models"`
}

// Package-level matrix cache keyed by plugin endpoint. Providers are
// stateless; the cache is the only shared bit and is safe for
// concurrent use.
var (
	magpieCacheMu   sync.Mutex
	magpieCacheSink = map[string]*magpieCacheEntry{}
)

type magpieCacheEntry struct {
	at     time.Time
	matrix magpieMatrix
	err    error
}

func magpieFetchMatrix(ctx context.Context, endpoint, key string, force bool) (magpieMatrix, error) {
	magpieCacheMu.Lock()
	if !force {
		if e, ok := magpieCacheSink[endpoint]; ok && time.Since(e.at) < magpieCapabilityTTL {
			m, err := e.matrix, e.err
			magpieCacheMu.Unlock()
			return m, err
		}
	}
	magpieCacheMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/capabilities", nil)
	if err != nil {
		return magpieMatrix{}, fmt.Errorf("magpie capabilities: %w", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		magpieCacheMu.Lock()
		magpieCacheSink[endpoint] = &magpieCacheEntry{at: time.Now(), err: err}
		magpieCacheMu.Unlock()
		return magpieMatrix{}, fmt.Errorf("magpie capabilities: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(io.LimitReader(resp.Body, 2048))
		err := fmt.Errorf("magpie capabilities: HTTP %d", resp.StatusCode)
		magpieCacheMu.Lock()
		magpieCacheSink[endpoint] = &magpieCacheEntry{at: time.Now(), err: err}
		magpieCacheMu.Unlock()
		return magpieMatrix{}, err
	}
	// Read the WHOLE body before unmarshaling: stopping at the JSON's
	// closing brace leaves the chunked terminator unread, and the pooled
	// (keep-alive) connection goes back dirty — the next request on it
	// waits forever for a response that never parses.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return magpieMatrix{}, fmt.Errorf("magpie capabilities read: %w", err)
	}
	var out magpieMatrix
	if err := json.Unmarshal(raw, &out); err != nil {
		return magpieMatrix{}, fmt.Errorf("magpie capabilities decode: %w", err)
	}
	magpieCacheMu.Lock()
	magpieCacheSink[endpoint] = &magpieCacheEntry{at: time.Now(), matrix: out}
	magpieCacheMu.Unlock()
	return out, nil
}

// Capabilities answers from the cached matrix: Declared=false when the
// model isn't in it (unknown — callers fail open).
func (Magpie) Capabilities(cfg toolproviders.ProviderConfig, model string) toolproviders.Capabilities {
	if cfg.Endpoint == "" || strings.EqualFold(model, "auto") {
		return toolproviders.Capabilities{}
	}
	m, err := magpieFetchMatrix(context.Background(), cfg.Endpoint, cfg.APIKey, false)
	if err != nil {
		return toolproviders.Capabilities{}
	}
	for _, e := range m.Models {
		if e.Ref == model || strings.HasSuffix(e.Ref, "/"+model) {
			return e.capabilities()
		}
	}
	return toolproviders.Capabilities{}
}

// routeFor picks the matrix model for this call: the pinned one when
// given, else the first online model whose declared capabilities meet
// the requirement. Undeclared models are a fallback only when NOTHING
// in the matrix is declared — with declared entries around, an ask none
// of them meets is a real "not supported", and blind-sending it to an
// unknown model would silently produce the wrong shape/fidelity.
func routeFor(m magpieMatrix, model string, a args) (string, error) {
	if model != "" && !strings.EqualFold(model, "auto") {
		return model, nil
	}
	var pass []magpieMatrixEntry
	anyDeclared := false
	for _, e := range m.Models {
		if e.Declared {
			anyDeclared = true
			break
		}
	}
	for _, e := range m.Models {
		if !e.CanGenerate {
			continue
		}
		if len(a.Refs) > 0 && !e.CanEdit {
			continue
		}
		caps := e.capabilities()
		if !caps.MeetsRefs(len(a.Refs)) || !caps.MeetsN(a.N) ||
			!caps.MeetsAspect(a.Aspect, a.Size) || !caps.MeetsQuality(a.Quality) {
			continue
		}
		if e.Declared {
			return e.Ref, nil
		}
		if !anyDeclared {
			pass = append(pass, e)
		}
	}
	if len(pass) > 0 {
		return pass[0].Ref, nil
	}
	return "", fmt.Errorf("magpie: no model on the gateway meets this request (refs=%d aspect=%q size=%q quality=%q n=%d); ask for less or check the imgqueue capability matrix",
		len(a.Refs), a.Aspect, a.Size, a.Quality, a.N)
}

func (g *Magpie) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	a, err := parseArgs(req.Args)
	if err != nil {
		return toolproviders.Response{}, err
	}
	endpoint := strings.TrimRight(req.Config.Endpoint, "/")
	if endpoint == "" {
		return toolproviders.Response{}, fmt.Errorf("magpie: endpoint (imgqueue 服务地址, 如 http://127.0.0.1:37987) not configured")
	}
	model, err := magpieModel(req.Config, a)
	if err != nil {
		return toolproviders.Response{}, err
	}
	// The plugin validates aspect by exact ratio membership, so a named
	// shape ("square"/"portrait"/"landscape" — the tool schema's
	// first-class values) must become a concrete declared ratio before
	// it is sent. The gate (unionShortfall) already matched it by
	// orientation; here we pick the actual ratio.
	if a.Aspect != "" {
		a.Aspect = magpieConcreteAspect(a.Aspect, g.Capabilities(req.Config, model))
	}

	// Background calls go through the plugin's task queue (fairness,
	// retries, persistence) instead of holding the sync proxy open.
	// Falls back to the sync facade when the endpoint has no task API.
	if toolproviders.AsyncImageRequested(ctx) && magpieTasksSupported(ctx, endpoint, req.Config.APIKey) {
		ctx, cancel := context.WithTimeout(ctx, magpieTaskTimeout)
		defer cancel()
		return g.executeTasks(ctx, endpoint, req.Config.APIKey, model, a)
	}

	timeout := 10 * time.Minute
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if len(a.Refs) > 0 {
		return g.edit(ctx, endpoint, req.Config.APIKey, model, a)
	}
	body := map[string]any{
		"model":  model,
		"prompt": a.Prompt,
		"n":      a.N,
	}
	if a.Size != "" {
		body["size"] = a.Size
	} else if a.Aspect != "" {
		body["aspect"] = a.Aspect
	}
	if a.Quality != "" {
		body["quality"] = strings.ToLower(a.Quality)
	}
	buf, _ := json.Marshal(body)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/images/generations", bytes.NewReader(buf))
	if err != nil {
		return toolproviders.Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Config.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+req.Config.APIKey)
	}
	return doMagpieImage(a, httpReq)
}

// magpieModel resolves "auto"/empty to a concrete matrix model.
func magpieModel(cfg toolproviders.ProviderConfig, a args) (string, error) {
	if cfg.Model != "" && !strings.EqualFold(cfg.Model, "auto") {
		return cfg.Model, nil
	}
	m, err := magpieFetchMatrix(context.Background(), cfg.Endpoint, cfg.APIKey, false)
	if err != nil {
		// No matrix (plugin down, token wrong): fail with the reason —
		// the chain should fall back to another provider, not hang.
		return "", toolproviders.Retry(fmt.Errorf("magpie auto-route: %w", err))
	}
	return routeFor(m, cfg.Model, a)
}

// magpieConcreteAspect maps a named shape to a ratio the model declares
// (first declared ratio of that orientation); undeclared caps fall back
// to the common defaults. Explicit ratios pass through untouched.
func magpieConcreteAspect(aspect string, caps toolproviders.Capabilities) string {
	name := strings.ToLower(strings.TrimSpace(aspect))
	if name != "square" && name != "portrait" && name != "landscape" {
		return aspect
	}
	fallback := map[string]string{"square": "1:1", "portrait": "2:3", "landscape": "3:2"}[name]
	if !caps.Declared || len(caps.AspectRatios) == 0 {
		return fallback
	}
	for _, r := range caps.AspectRatios {
		w, h, ok := parseRatioLocal(r)
		if !ok {
			continue
		}
		switch name {
		case "square":
			if w == h {
				return r
			}
		case "portrait":
			if h > w {
				return r
			}
		case "landscape":
			if w > h {
				return r
			}
		}
	}
	return fallback
}

// parseRatioLocal parses "W:H" without reaching into the capabilities
// package's unexported helpers.
func parseRatioLocal(r string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimSpace(r), ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	w, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || w <= 0 {
		return 0, 0, false
	}
	h, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || h <= 0 {
		return 0, 0, false
	}
	return w, h, true
}

// edit sends the reference-image form to the plugin's sync edits proxy.
func (g *Magpie) edit(ctx context.Context, endpoint, key, model string, a args) (toolproviders.Response, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("model", model)
	_ = w.WriteField("prompt", a.Prompt)
	if a.Size != "" {
		_ = w.WriteField("size", a.Size)
	} else if a.Aspect != "" {
		_ = w.WriteField("aspect", a.Aspect)
	}
	if a.Quality != "" {
		_ = w.WriteField("quality", strings.ToLower(a.Quality))
	}
	for i, ref := range a.Refs {
		data, ct, err := decodeDataURL(ref)
		if err != nil {
			return toolproviders.Response{}, fmt.Errorf("magpie: reference image %d: %w", i+1, err)
		}
		h := make(map[string][]string)
		h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="image"; filename="ref%d%s"`, i+1, extFor(ct))}
		h["Content-Type"] = []string{ct}
		part, err := w.CreatePart(h)
		if err != nil {
			return toolproviders.Response{}, err
		}
		if _, err := part.Write(data); err != nil {
			return toolproviders.Response{}, err
		}
	}
	if err := w.Close(); err != nil {
		return toolproviders.Response{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/images/edits", &buf)
	if err != nil {
		return toolproviders.Response{}, err
	}
	httpReq.Header.Set("Content-Type", w.FormDataContentType())
	if key != "" {
		httpReq.Header.Set("Authorization", "Bearer "+key)
	}
	return doMagpieImage(a, httpReq)
}

// doMagpieImage runs the request and folds the OpenAI-images response
// into the chain's Response shape (images as bytes for the workspace).
func doMagpieImage(a args, httpReq *http.Request) (toolproviders.Response, error) {
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie request: %w", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		err := fmt.Errorf("magpie HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return toolproviders.Response{}, toolproviders.Retry(err)
		}
		return toolproviders.Response{}, err
	}
	var out struct {
		Data []struct {
			URL     string `json:"url"`
			B64JSON string `json:"b64_json"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 200<<20)).Decode(&out); err != nil {
		return toolproviders.Response{}, fmt.Errorf("magpie decode: %w", err)
	}
	images := make([]toolproviders.Image, 0, len(out.Data))
	urls := make([]string, 0, len(out.Data))
	b64s := make([]string, 0, len(out.Data))
	for _, d := range out.Data {
		switch {
		case d.URL != "":
			urls = append(urls, d.URL)
			images = append(images, toolproviders.Image{URL: d.URL})
		case d.B64JSON != "":
			data, err := base64.StdEncoding.DecodeString(d.B64JSON)
			if err != nil {
				continue
			}
			b64s = append(b64s, d.B64JSON)
			images = append(images, toolproviders.Image{Data: data, ContentType: "image/png"})
		}
	}
	if len(images) == 0 {
		return toolproviders.Response{}, toolproviders.ErrNoResults
	}
	text := renderURLs(a.Prompt, urls)
	if text == "" {
		text = renderB64(a.Prompt, b64s)
	}
	return toolproviders.Response{Text: text, Images: images}, nil
}
