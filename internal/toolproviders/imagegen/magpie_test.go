package imagegen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// fakeImgQueue stands in for the imgqueue plugin: capabilities matrix +
// sync generation/edits proxies, recording what fastclaw sent.
func fakeImgQueue(t *testing.T, matrix string) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		rec.caps++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, matrix)
	})
	mux.HandleFunc("POST /v1/images/generations", func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		body, _ := io.ReadAll(r.Body)
		rec.gens = append(rec.gens, string(body))
		rec.mu.Unlock()
		var req map[string]any
		json.Unmarshal(body, &req)
		if q, _ := req["quality"].(string); q == "ultra" {
			w.WriteHeader(422)
			io.WriteString(w, `{"error":"bad quality","missing":[{"field":"quality","value":"ultra","supported":"low|high"}]}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[{"b64_json":"` + tinyPNGB64 + `"}],"usage":{"input_tokens":1}}`)
	})
	mux.HandleFunc("POST /v1/images/edits", func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		ct := r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		rec.edits = append(rec.edits, ct+" "+fmt.Sprint(strings.Count(string(body), "image")))
		rec.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[{"b64_json":"` + tinyPNGB64 + `"}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, rec
}

type recorder struct {
	mu    sync.Mutex
	caps  int
	gens  []string
	edits []string
}

const tinyPNGB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="

const testMatrix = `{
 "ok": true, "format": 1,
 "models": [
  {"ref":"pool/declared-editor","label":"Editor","canGenerate":true,"canEdit":true,"declared":true,
   "caps":{"aspectRatios":["1:1","3:4"],"quality":["low","high"],"maxImages":2,"maxRefImages":2}},
  {"ref":"pool/plain","label":"Plain","canGenerate":true,"canEdit":false,"declared":false,
   "caps":null}
 ],
 "undeclared": ["pool/plain"]
}`

func magpieChain(srv *httptest.Server, model string) *toolproviders.Chain {
	reg := toolproviders.NewRegistry()
	RegisterAll(reg)
	return &toolproviders.Chain{
		Category:     Category,
		Order:        []string{"magpie/" + model},
		AutoFallback: false,
		Registry:     reg,
		GetConfig: func(string) toolproviders.ProviderConfig {
			return toolproviders.ProviderConfig{APIKey: "tok", Endpoint: srv.URL}
		},
	}
}

func TestMagpieGenerates(t *testing.T) {
	srv, rec := fakeImgQueue(t, testMatrix)
	resp, err := magpieChain(srv, "pool/declared-editor").Execute(context.Background(), map[string]any{
		"prompt": "a cat", "aspect": "3:4", "quality": "high", "n": 1,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(resp.Images) != 1 || len(resp.Images[0].Data) == 0 {
		t.Fatalf("want one inline image, got %+v", resp.Images)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.gens) != 1 || !strings.Contains(rec.gens[0], `"aspect":"3:4"`) {
		t.Fatalf("aspect not passed through: %v", rec.gens)
	}
	if !strings.Contains(rec.gens[0], `"quality":"high"`) {
		t.Fatalf("quality not passed through: %v", rec.gens)
	}
}

func TestMagpieEditsWithRefs(t *testing.T) {
	srv, rec := fakeImgQueue(t, testMatrix)
	ref := "data:image/png;base64," + tinyPNGB64
	resp, err := magpieChain(srv, "pool/declared-editor").Execute(context.Background(), map[string]any{
		"prompt": "make it a sticker", "image_urls": []string{ref},
	})
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if len(resp.Images) != 1 {
		t.Fatalf("want one image back")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.edits) != 1 {
		t.Fatalf("edits not proxied: %v", rec.edits)
	}
}

func TestMagpieAutoRoutesByRequirement(t *testing.T) {
	srv, rec := fakeImgQueue(t, testMatrix)
	// refs + 3:4 + low → only the declared editor qualifies
	resp, err := magpieChain(srv, "auto").Execute(context.Background(), map[string]any{
		"prompt": "sticker of us", "aspect": "3:4", "quality": "low",
		"image_urls": []string{"data:image/png;base64," + tinyPNGB64},
	})
	if err != nil {
		t.Fatalf("auto route: %v", err)
	}
	if len(resp.Images) != 1 {
		t.Fatalf("want image")
	}
	// Check edits landed, then RELEASE before the second Execute — the
	// fake server's handlers take the same mutex, holding it across the
	// next request deadlocks them against the test goroutine.
	(func() {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.edits) != 1 {
			t.Fatalf("auto should have gone to the editor via edits: %v", rec.edits)
		}
	})()

	// An ask nothing declares (9:21 + ultra) fails BEFORE any provider call.
	_, err = magpieChain(srv, "auto").Execute(context.Background(), map[string]any{
		"prompt": "x", "aspect": "9:21", "quality": "ultra",
	})
	if err == nil || !strings.Contains(err.Error(), "no model") {
		t.Fatalf("want routing failure, got %v", err)
	}
}

func TestMagpiePlugin422IsFatal(t *testing.T) {
	srv, _ := fakeImgQueue(t, testMatrix)
	// The pinned declared model + a quality the plugin's matrix rejects:
	// the plugin's 422 must surface as a NON-retriable error (the chain
	// shouldn't pointlessly loop other providers on a hard miss).
	_, err := magpieChain(srv, "pool/declared-editor").Execute(context.Background(), map[string]any{
		"prompt": "x", "quality": "ultra",
	})
	if err == nil || !strings.Contains(err.Error(), "422") {
		t.Fatalf("want 422 error surfaced, got %v", err)
	}
	var re *toolproviders.RetriableError
	if errors.As(err, &re) {
		t.Fatalf("422 should be fatal, got retriable: %v", err)
	}
}

func TestMagpieCapabilitiesFromMatrix(t *testing.T) {
	srv, _ := fakeImgQueue(t, testMatrix)
	cfg := toolproviders.ProviderConfig{APIKey: "tok", Endpoint: srv.URL}
	caps := Magpie{}.Capabilities(cfg, "pool/declared-editor")
	if !caps.Declared || caps.MaxRefImages != 2 || !caps.MeetsRefs(2) || caps.MeetsRefs(3) {
		t.Fatalf("declared caps wrong: %+v", caps)
	}
	if !caps.MeetsAspect("3:4", "") || caps.MeetsAspect("9:21", "") {
		t.Fatalf("aspect gate wrong: %+v", caps)
	}
	// Unknown model → fail-open
	if (Magpie{}.Capabilities(cfg, "pool/nope")).Declared {
		t.Fatal("unknown model must be undeclared")
	}
}
