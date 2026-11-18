package tools

// image_tasks_test.go — background mode through the real tool surface:
// image_gen(background=true) submits, the chain runs detached with the
// async marker set, completion stores pictures into the submitting
// chat's workspace and fires the notifier, and the companion tools
// poll/cancel.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// gatedImageProvider blocks in Execute until released (or the context
// dies), recording what the background call handed it.
type gatedImageProvider struct {
	entered chan struct{} // closed when Execute starts
	release chan providerOutcome
	mu      sync.Mutex
	sawAsync bool
}

type providerOutcome struct {
	resp toolproviders.Response
	err  error
}

func newGatedImageProvider() *gatedImageProvider {
	return &gatedImageProvider{entered: make(chan struct{}), release: make(chan providerOutcome)}
}

func (g *gatedImageProvider) Category() string     { return "image_gen" }
func (g *gatedImageProvider) Name() string         { return "fal" }
func (g *gatedImageProvider) CredentialFree() bool { return true }
func (g *gatedImageProvider) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	g.mu.Lock()
	g.sawAsync = toolproviders.AsyncImageFrom(ctx)
	g.mu.Unlock()
	close(g.entered)
	select {
	case o := <-g.release:
		return o.resp, o.err
	case <-ctx.Done():
		return toolproviders.Response{}, ctx.Err()
	}
}

func (g *gatedImageProvider) asyncSeen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sawAsync
}

func imageTaskRegistry(t *testing.T, p *gatedImageProvider, files *ImageFiles, notify func(ImageTaskNotice)) *Registry {
	t.Helper()
	reg := toolproviders.NewRegistry()
	reg.Register(p)
	r := NewRegistry(t.TempDir(), t.TempDir())
	r.SetSessionID("web:chat_7")
	r.SetMessageContext("web", "acc_1", "chat_7")
	RegisterImageGenChain(r, &toolproviders.Chain{
		Category: "image_gen", Order: []string{"fal/seedream"}, Registry: reg,
		GetConfig: func(string) toolproviders.ProviderConfig { return toolproviders.ProviderConfig{} },
	}, files, notify)
	return r
}

func waitNotice(t *testing.T, ch chan ImageTaskNotice) ImageTaskNotice {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("no completion notice arrived")
		return ImageTaskNotice{}
	}
}

func TestImageGenBackgroundSubmitsStoresAndNotifies(t *testing.T) {
	p := newGatedImageProvider()
	notices := make(chan ImageTaskNotice, 4)
	var saved []string
	files := &ImageFiles{
		Save: func(_ context.Context, session, project, name string, _ []byte, _ string) (string, error) {
			saved = append(saved, session+"/"+name)
			return name, nil
		},
	}
	r := imageTaskRegistry(t, p, files, func(n ImageTaskNotice) { notices <- n })

	out, err := r.Execute(context.Background(), "image_gen", `{"prompt":"a cat poster","background":true}`)
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
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background chain never started")
	}
	if !p.asyncSeen() {
		t.Fatal("background chain must carry the async-image marker")
	}

	p.release <- providerOutcome{resp: toolproviders.Response{Images: []toolproviders.Image{
		{Data: testPNG, ContentType: "image/png"},
	}}}

	n := waitNotice(t, notices)
	if n.Channel != "web" || n.ChatID != "chat_7" || !strings.Contains(n.Text, sub.Task) || !strings.Contains(n.Text, "已完成") {
		t.Fatalf("notice routed wrong: %+v", n)
	}

	// The stored picture belongs to the submitting chat's workspace.
	if len(saved) != 1 || !strings.HasPrefix(saved[0], "web:chat_7/picture-") {
		t.Fatalf("saved %v", saved)
	}

	st, err := r.Execute(context.Background(), "image_task_status", `{"task":"`+sub.Task+`"}`)
	if err != nil {
		t.Fatal(err)
	}
	var res struct {
		Status     string   `json:"status"`
		Images     []string `json:"images"`
		PutInReply string   `json:"putInReply"`
	}
	if err := json.Unmarshal([]byte(st), &res); err != nil || res.Status != "done" {
		t.Fatalf("status %q (%v)", st, err)
	}
	if len(res.Images) != 1 || !strings.HasPrefix(res.Images[0], "/workspace/picture-") {
		t.Fatalf("images %v", res.Images)
	}
	if !strings.Contains(res.PutInReply, "![](/workspace/") {
		t.Fatalf("putInReply %q", res.PutInReply)
	}

	// Idempotent: a second fetch returns the same paths.
	again, err := r.Execute(context.Background(), "image_task_status", `{"task":"`+sub.Task+`"}`)
	if err != nil || !strings.Contains(again, res.Images[0]) {
		t.Fatalf("re-fetch %q (%v)", again, err)
	}
}

func TestImageGenBackgroundFailureNotifies(t *testing.T) {
	p := newGatedImageProvider()
	notices := make(chan ImageTaskNotice, 4)
	r := imageTaskRegistry(t, p, nil, func(n ImageTaskNotice) { notices <- n })

	out, err := r.Execute(context.Background(), "image_gen", `{"prompt":"doomed","background":true}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task string `json:"task"`
	}
	json.Unmarshal([]byte(out), &sub)

	p.release <- providerOutcome{err: errFail("provider exploded")}

	n := waitNotice(t, notices)
	if !strings.Contains(n.Text, "失败") || !strings.Contains(n.Text, sub.Task) {
		t.Fatalf("failure notice %q", n.Text)
	}
	st, err := r.Execute(context.Background(), "image_task_status", `{"task":"`+sub.Task+`"}`)
	if err != nil || !strings.Contains(st, `"failed"`) || !strings.Contains(st, "provider exploded") {
		t.Fatalf("status %q (%v)", st, err)
	}
}

func TestImageGenBackgroundCancel(t *testing.T) {
	p := newGatedImageProvider()
	notices := make(chan ImageTaskNotice, 4)
	r := imageTaskRegistry(t, p, nil, func(n ImageTaskNotice) { notices <- n })

	out, err := r.Execute(context.Background(), "image_gen", `{"prompt":"never finishes","background":true}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task string `json:"task"`
	}
	json.Unmarshal([]byte(out), &sub)

	// Cancel while the provider is still blocked; the companion tool
	// reports it and the background context unwinds the provider.
	res, err := r.Execute(context.Background(), "image_task_cancel", `{"task":"`+sub.Task+`"}`)
	if err != nil || !strings.Contains(res, "Cancelled") {
		t.Fatalf("cancel %q (%v)", res, err)
	}
	st, err := r.Execute(context.Background(), "image_task_status", `{"task":"`+sub.Task+`"}`)
	if err != nil || !strings.Contains(st, `"cancelled"`) {
		t.Fatalf("status %q (%v)", st, err)
	}
	// Give the goroutine a moment to observe the cancellation; a notice
	// for a cancelled task must NOT be delivered.
	select {
	case n := <-notices:
		t.Fatalf("cancel must not notify: %+v", n)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestImageTaskStatusUnknownId(t *testing.T) {
	p := newGatedImageProvider()
	r := imageTaskRegistry(t, p, nil, nil)
	if _, err := r.Execute(context.Background(), "image_task_status", `{"task":"imgtask_99"}`); err == nil {
		t.Fatal("unknown task id must error")
	}
}

type errFail string

func (e errFail) Error() string { return string(e) }
