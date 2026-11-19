package tools

// media_tasks_test.go — background video/tts task flows through the
// generic manager (the video twin of image_tasks_test.go's gated
// provider dance).

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// gatedMediaProvider holds one background call until released, then
// returns the queued outcome (providerOutcome comes from
// image_tasks_test.go — same package).
type gatedMediaProvider struct {
	category string
	mu       sync.Mutex
	entered  chan struct{}
	release  chan providerOutcome
	sawAsync bool
}

func newGatedMediaProvider(category string) *gatedMediaProvider {
	return &gatedMediaProvider{
		category: category,
		entered:  make(chan struct{}),
		release:  make(chan providerOutcome),
	}
}

func (g *gatedMediaProvider) Category() string     { return g.category }
func (g *gatedMediaProvider) Name() string         { return "gated" }
func (g *gatedMediaProvider) CredentialFree() bool { return true }

func (g *gatedMediaProvider) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	g.mu.Lock()
	g.sawAsync = toolproviders.AsyncImageRequested(ctx)
	meta, _ := toolproviders.AsyncImageFrom(ctx)
	g.mu.Unlock()
	if meta.Media == "" && g.sawAsync {
		t := req.Args["prompt"]
		if s, ok := t.(string); ok && s == "" {
			t = req.Args["text"]
		}
		_ = t
	}
	close(g.entered)
	out := <-g.release
	return out.resp, out.err
}

func (g *gatedMediaProvider) asyncSeen() (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, meta := toolproviders.AsyncImageMeta{}, ""
	_ = meta
	return g.sawAsync, ""
}

func mediaTaskRegistry(t *testing.T, p *gatedMediaProvider, files *ImageFiles, notify func(MediaTaskNotice), register func(*Registry, *toolproviders.Chain, *ImageFiles, func(MediaTaskNotice), interface{})) *Registry {
	t.Helper()
	reg := toolproviders.NewRegistry()
	reg.Register(p)
	r := NewRegistry(t.TempDir(), t.TempDir())
	r.SetSessionID("web:chat_9")
	r.SetMessageContext("web", "acc_1", "chat_9")
	register(r, &toolproviders.Chain{
		Category: p.category, Order: []string{"gated/model"}, Registry: reg,
		GetConfig: func(string) toolproviders.ProviderConfig { return toolproviders.ProviderConfig{} },
	}, files, notify, nil)
	return r
}

func TestVideoGenBackgroundStoresAndNotifies(t *testing.T) {
	p := newGatedMediaProvider("video_gen")
	notices := make(chan MediaTaskNotice, 4)
	var saved []string
	files := &ImageFiles{
		Save: func(_ context.Context, session, project, name string, _ []byte, _ string) (string, error) {
			saved = append(saved, session+"/"+name)
			return name, nil
		},
	}
	r := mediaTaskRegistry(t, p, files, func(n MediaTaskNotice) { notices <- n },
		func(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify func(MediaTaskNotice), _ interface{}) {
			RegisterVideoGenChain(r, chain, files, notify, nil)
		})

	out, err := r.Execute(context.Background(), "video_gen", `{"prompt":"a cat walking","seconds":"4"}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task   string `json:"task"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &sub); err != nil || sub.Task == "" || sub.Status != "submitted" {
		t.Fatalf("submit result %q (%v)", out, err)
	}
	if !strings.HasPrefix(sub.Task, "videotask_") {
		t.Fatalf("video task id should carry the videotask_ prefix, got %q", sub.Task)
	}
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background chain never started")
	}

	p.release <- providerOutcome{resp: toolproviders.Response{
		Media: []toolproviders.MediaFile{{Data: []byte("FAKEFTYPVIDEO"), ContentType: "video/mp4"}},
	}}

	n := waitNotice(t, notices)
	if n.Channel != "web" || n.ChatID != "chat_9" || !strings.Contains(n.Text, sub.Task) || !strings.Contains(n.Text, "视频") {
		t.Fatalf("notice routed wrong: %+v", n)
	}
	if len(saved) != 1 || !strings.HasPrefix(saved[0], "web:chat_9/video-") {
		t.Fatalf("saved %v", saved)
	}

	// The generic dispatcher and the category pair answer the same.
	for _, tool := range []string{"media_task_status", "video_task_status"} {
		st, err := r.Execute(context.Background(), tool, `{"task":"`+sub.Task+`"}`)
		if err != nil {
			t.Fatalf("%s: %v", tool, err)
		}
		var res struct {
			Status     string   `json:"status"`
			Files      []string `json:"files"`
			PutInReply string   `json:"putInReply"`
		}
		if err := json.Unmarshal([]byte(st), &res); err != nil || res.Status != "done" {
			t.Fatalf("%s status %q (%v)", tool, st, err)
		}
		if len(res.Files) != 1 || !strings.HasPrefix(res.Files[0], "/workspace/video-") {
			t.Fatalf("%s files %v", tool, res.Files)
		}
		// Non-image media never renders as an inline ![]() image.
		if strings.Contains(res.PutInReply, "![](") {
			t.Fatalf("%s putInReply should be a plain link, got %q", tool, res.PutInReply)
		}
	}
}

func TestTTSBackgroundSubmitsAndDelivers(t *testing.T) {
	p := newGatedMediaProvider("tts")
	notices := make(chan MediaTaskNotice, 4)
	r := mediaTaskRegistry(t, p, nil, func(n MediaTaskNotice) { notices <- n },
		func(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify func(MediaTaskNotice), _ interface{}) {
			RegisterTTSChain(r, chain, files, notify, nil)
		})

	out, err := r.Execute(context.Background(), "tts", `{"text":"你好世界","background":true}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task string `json:"task"`
	}
	if err := json.Unmarshal([]byte(out), &sub); err != nil || !strings.HasPrefix(sub.Task, "ttstask_") {
		t.Fatalf("submit result %q (%v)", out, err)
	}
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background chain never started")
	}

	// A queue provider returns the clip as Media; the manager stores it.
	p.release <- providerOutcome{resp: toolproviders.Response{
		Media:    []toolproviders.MediaFile{{Data: []byte("ID3FAKEMP3"), ContentType: "audio/mpeg"}},
		TaskRefs: []string{"queue_1"},
	}}

	n := waitNotice(t, notices)
	if !strings.Contains(n.Text, sub.Task) || !strings.Contains(n.Text, "语音") {
		t.Fatalf("notice wrong: %+v", n)
	}
	st, err := r.Execute(context.Background(), "tts_task_status", `{"task":"`+sub.Task+`"}`)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Status    string   `json:"status"`
		Files     []string `json:"files"`
		QueueTask string   `json:"queueTask"`
		Error     string   `json:"error"`
	}
	if err := json.Unmarshal([]byte(st), &res); err != nil {
		t.Fatalf("status %q (%v)", st, err)
	}
	if res.QueueTask != "queue_1" {
		t.Fatalf("queueTask %q", res.QueueTask)
	}
	// No workspace and no share service wired in this test: the clip
	// was generated but is undeliverable — the same honest failure the
	// image path reports, naming the category.
	if res.Status != "failed" || !strings.Contains(res.Error, "no retrievable tts result") {
		t.Fatalf("status %q", st)
	}
}

// mediaTaskLookupAcrossCategories verifies the registry-level prefix
// dispatch (MediaTaskLookup / MediaTaskStatus) across two categories.
func TestMediaTaskLookupAcrossCategories(t *testing.T) {
	reg := toolproviders.NewRegistry()
	pv := newGatedMediaProvider("video_gen")
	reg.Register(pv)
	r := NewRegistry(t.TempDir(), t.TempDir())
	RegisterVideoGenChain(r, &toolproviders.Chain{
		Category: "video_gen", Order: []string{"gated/model"}, Registry: reg,
		GetConfig: func(string) toolproviders.ProviderConfig { return toolproviders.ProviderConfig{} },
	}, nil, nil, nil)

	out, err := r.Execute(context.Background(), "video_gen", `{"prompt":"x"}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task string `json:"task"`
	}
	_ = json.Unmarshal([]byte(out), &sub)
	<-pv.entered

	if _, ok := r.MediaTaskLookup(sub.Task); !ok {
		t.Fatalf("lookup should find the running task %q", sub.Task)
	}
	if _, ok := r.MediaTaskLookup("imgtask_999"); ok {
		t.Fatal("lookup must not find ids of unregistered categories")
	}
	// Cancel through the generic tool.
	if _, err := r.Execute(context.Background(), "media_task_cancel", `{"task":"`+sub.Task+`"}`); err != nil {
		t.Fatal(err)
	}
	pv.release <- providerOutcome{err: context.Canceled}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, _ := r.MediaTaskLookup(sub.Task)
		if snap != nil && snap["status"] == "cancelled" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("task never reached cancelled")
}
