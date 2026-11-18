package imagegen

// Task-mode tests: the imgqueue plugin's /v1/tasks surface exercised
// through the async-image context marker — submit, poll, fetch, delete
// on cancel, and the sync-facade fallback when no task API exists.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// fakeImgQueueTasks is an imgqueue stand-in with the full task API.
// Tasks complete as soon as they're first polled (unless mode says
// otherwise), mirroring the plugin's one-picture-per-task contract.
func fakeImgQueueTasks(t *testing.T, mode string) (*httptest.Server, *taskRecorder) {
	t.Helper()
	rec := &taskRecorder{}
	var mu sync.Mutex
	tasks := map[string]*fakeImgqTask{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		io.WriteString(w, testMatrix)
	})
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		io.WriteString(w, `{"ok":true,"name":"magpie-imgqueue"}`)
	})
	mux.HandleFunc("POST /v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		id := fmt.Sprintf("t%d", len(tasks)+1)
		tasks[id] = &fakeImgqTask{id: id, status: "queued", request: string(body)}
		rec.submits = append(rec.submits, string(body))
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"queued"},"poll":"/v1/tasks/%s"}`, id, id)
	})
	mux.HandleFunc("GET /v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		path := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
		if strings.HasSuffix(path, "/image") {
			w.Header().Set("Content-Type", "image/png")
			w.Write([]byte("\x89PNG fake-task-image"))
			return
		}
		mu.Lock()
		defer mu.Unlock()
		task, ok := tasks[path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		if task.status == "queued" { // complete on first poll, per mode
			switch mode {
			case "fail":
				task.status = "failed"
			case "stuck":
				task.status = "running"
			default:
				task.status = "done"
			}
		}
		switch task.status {
		case "done":
			fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"done","result":{"file":%q,"mime":"image/png"}}}`, path, path+".png")
		case "failed":
			fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"failed","error":"gateway sad"}}`, path)
		default:
			fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"running"}}`, path)
		}
	})
	mux.HandleFunc("DELETE /v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		mu.Lock()
		defer mu.Unlock()
		rec.deletes = append(rec.deletes, strings.TrimPrefix(r.URL.Path, "/v1/tasks/"))
		io.WriteString(w, `{"ok":true}`)
	})
	mux.HandleFunc("POST /v1/images/generations", func(w http.ResponseWriter, r *http.Request) {
		rec.auth(t, w, r)
		rec.syncGens++
		io.WriteString(w, `{"data":[{"b64_json":"`+tinyPNGB64+`"}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, rec
}

type fakeImgqTask struct {
	id, status, request string
}

type taskRecorder struct {
	mu       sync.Mutex
	submits  []string
	deletes  []string
	syncGens int
}

func (rec *taskRecorder) auth(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer tok" {
		w.WriteHeader(401)
		return
	}
}

func (rec *taskRecorder) snapshot() (submits, deletes []string, syncGens int) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string{}, rec.submits...), append([]string{}, rec.deletes...), rec.syncGens
}

func TestMagpieTaskModeSubmitsAndPolls(t *testing.T) {
	srv, rec := fakeImgQueueTasks(t, "")
	ctx := toolproviders.WithAsyncImage(context.Background())
	resp, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "a cat", "aspect": "3:4", "quality": "high",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(resp.Images) != 1 || string(resp.Images[0].Data) != "\x89PNG fake-task-image" {
		t.Fatalf("want the task image back, got %+v", resp.Images)
	}
	submits, deletes, syncGens := rec.snapshot()
	if len(submits) != 1 || !strings.Contains(submits[0], `"aspect":"3:4"`) || !strings.Contains(submits[0], "fastclaw") {
		t.Fatalf("submit body wrong: %v", submits)
	}
	if syncGens != 0 {
		t.Fatalf("async mode must not touch the sync facade, %d calls", syncGens)
	}
	if len(deletes) != 0 {
		t.Fatalf("successful run must not delete: %v", deletes)
	}
}

func TestMagpieTaskModeNSubmitsOneTaskPerPicture(t *testing.T) {
	srv, rec := fakeImgQueueTasks(t, "")
	ctx := toolproviders.WithAsyncImage(context.Background())
	resp, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "two cats", "n": 2,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(resp.Images) != 2 {
		t.Fatalf("want 2 pictures, got %d", len(resp.Images))
	}
	submits, _, _ := rec.snapshot()
	if len(submits) != 2 {
		t.Fatalf("one task per picture, got %d submits", len(submits))
	}
}

func TestMagpieTaskModeFailureIsRetriable(t *testing.T) {
	srv, _ := fakeImgQueueTasks(t, "fail")
	ctx := toolproviders.WithAsyncImage(context.Background())
	_, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "doomed",
	})
	if err == nil || !strings.Contains(err.Error(), "gateway sad") {
		t.Fatalf("want task failure surfaced, got %v", err)
	}
	var re *toolproviders.RetriableError
	if !errors.As(err, &re) {
		t.Fatalf("task failure must be retriable so the chain falls back, got %v", err)
	}
}

func TestMagpieTaskModeCancelDeletesRemoteTasks(t *testing.T) {
	srv, rec := fakeImgQueueTasks(t, "stuck")
	ctx, cancel := context.WithCancel(toolproviders.WithAsyncImage(context.Background()))
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()
	_, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "stuck in queue",
	})
	if err == nil {
		t.Fatal("cancelled execute must fail")
	}
	// The deferred DELETE fires asynchronously — wait for it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, deletes, _ := rec.snapshot()
		if len(deletes) >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancelled task was not deleted remotely: %v", deletes)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMagpieAsyncFallsBackToSyncWhenNoTaskAPI(t *testing.T) {
	// No /v1/health on this server: the probe fails and the async call
	// degrades to the synchronous facade (today's behavior).
	srv, rec := fakeImgQueue(t, testMatrix)
	ctx := toolproviders.WithAsyncImage(context.Background())
	resp, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "a cat",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(resp.Images) != 1 {
		t.Fatalf("want image from sync facade")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.gens) != 1 {
		t.Fatalf("sync facade not used: %v", rec.gens)
	}
}

// The tool layer (image_gen background mode) relies on the marker being
// invisible to providers that don't implement task mode.
func TestAsyncMarkerIsFalseByDefault(t *testing.T) {
	if toolproviders.AsyncImageFrom(context.Background()) {
		t.Fatal("plain context must not read as async")
	}
	if !toolproviders.AsyncImageFrom(toolproviders.WithAsyncImage(context.Background())) {
		t.Fatal("marker roundtrip failed")
	}
}

// Named shapes must reach the plugin as concrete declared ratios — its
// validator matches aspect by exact membership.
func TestMagpieNamedAspectBecomesConcreteRatio(t *testing.T) {
	srv, rec := fakeImgQueueTasks(t, "")
	ctx := toolproviders.WithAsyncImage(context.Background())
	if _, err := magpieChain(srv, "pool/declared-editor").Execute(ctx, map[string]any{
		"prompt": "a cat", "aspect": "square",
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	submits, _, _ := rec.snapshot()
	if len(submits) != 1 || !strings.Contains(submits[0], `"aspect":"1:1"`) {
		t.Fatalf("square should be sent as the declared 1:1 ratio: %v", submits)
	}
}

func TestMagpieConcreteAspect(t *testing.T) {
	declared := toolproviders.Capabilities{Declared: true, AspectRatios: []string{"3:2", "2:3", "1:1"}}
	cases := map[string]string{
		"square":    "1:1",
		"portrait":  "2:3",
		"landscape": "3:2",
		"16:9":      "16:9", // explicit ratios pass through
		"":          "",
	}
	for in, want := range cases {
		if got := magpieConcreteAspect(in, declared); got != want {
			t.Errorf("magpieConcreteAspect(%q) = %q, want %q", in, got, want)
		}
	}
	// Undeclared caps fall back to the common defaults.
	if got := magpieConcreteAspect("portrait", toolproviders.Capabilities{}); got != "2:3" {
		t.Errorf("undeclared fallback = %q, want 2:3", got)
	}
}
