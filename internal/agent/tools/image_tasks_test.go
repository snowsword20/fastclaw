package tools

// image_tasks_test.go — background mode through the real tool surface:
// image_gen(background=true) submits, the chain runs detached with the
// async marker set, completion stores pictures into the submitting
// chat's workspace and fires the notifier, and the companion tools
// poll/cancel.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/imagegen"
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
	_, g.sawAsync = toolproviders.AsyncImageFrom(ctx)
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
	}, files, notify, nil)
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

// params {"image_gen_mode":"background"} forces the background path even
// when the model didn't pass background=true.
func TestImageGenParamsForceBackground(t *testing.T) {
	p := newGatedImageProvider()
	notices := make(chan ImageTaskNotice, 4)
	r := imageTaskRegistry(t, p, nil, func(n ImageTaskNotice) { notices <- n })
	r.SetTurnParams(map[string]any{"image_gen_mode": "background"})

	out, err := r.Execute(context.Background(), "image_gen", `{"prompt":"a cat"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"status":"submitted"`) {
		t.Fatalf("params must force background (got %q)", out)
	}
	select {
	case <-p.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background chain never started")
	}
	// "sync" must win over the model's background=true. The sync path
	// blocks on the provider, so feed it from a goroutine.
	p2 := newGatedImageProvider()
	r2 := imageTaskRegistry(t, p2, nil, func(ImageTaskNotice) {})
	r2.SetTurnParams(map[string]any{"image_gen_mode": "sync"})
	go func() {
		<-p2.entered
		p2.release <- providerOutcome{resp: toolproviders.Response{
			Images: []toolproviders.Image{{URL: "https://cdn.example/x.png"}},
		}}
	}()
	out2, err := r2.Execute(context.Background(), "image_gen", `{"prompt":"x","background":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out2, `"status":"submitted"`) {
		t.Fatalf("mode=sync must override background=true (got %q)", out2)
	}
}

// ---------- restart recovery ----------

// fakeRecoveryQueue is a minimal imgqueue stand-in for recovery tests:
// a task list plus per-task poll/image endpoints. Poll counts let a
// test flip a task's state between polls (running → done).
type fakeRecoveryQueue struct {
	mu     sync.Mutex
	polls  map[string]int
	states map[string]string // id → status served for the NEXT poll
}

func newFakeRecoveryQueue(t *testing.T, listJSON string) *httptest.Server {
	t.Helper()
	q := &fakeRecoveryQueue{polls: map[string]int{}, states: map[string]string{
		"t-done": "done", "t-run": "done", // list states; polls serve same
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		q.mu.Lock()
		defer q.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(listJSON))
	})
	mux.HandleFunc("GET /v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/"), "/image")
		q.mu.Lock()
		q.polls[id]++
		status := q.states[id]
		q.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/image") {
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write([]byte("\x89PNG recovered-image"))
			return
		}
		if status == "done" {
			fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"done","result":{"file":%q,"mime":"image/png"}}}`, id, id+".png")
			return
		}
		fmt.Fprintf(w, `{"ok":true,"task":{"id":%q,"status":"running"}}`, id)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestImageTaskRecoveryFromQueue(t *testing.T) {
	now := time.Now().UnixMilli()
	meta := func(agent, chat, session string) string {
		return fmt.Sprintf(`{"origin":"fastclaw","agentID":%q,"userID":"u_1","sessionID":%q,"projectID":"","channel":"web","accountID":"acc_1","chatID":%q,"prompt":"p"}`, agent, session, chat)
	}
	listJSON := fmt.Sprintf(`{"ok":true,"tasks":[
		{"id":"t-done","status":"done","finishedAt":%d,"meta":%s},
		{"id":"t-run","status":"running","finishedAt":0,"meta":%s},
		{"id":"t-other","status":"running","finishedAt":0,"meta":%s}
	]}`, now, meta("agtA", "chat_9", "web:chat_9"), meta("agtA", "chat_9", "web:chat_9"), meta("agtB", "chat_x", "web:chat_x"))
	srv := newFakeRecoveryQueue(t, listJSON)

	notices := make(chan ImageTaskNotice, 8)
	var saved []string
	files := &ImageFiles{
		Save: func(_ context.Context, session, _, name string, _ []byte, _ string) (string, error) {
			saved = append(saved, session+"/"+name)
			return name, nil
		},
	}
	reg := toolproviders.NewRegistry()
	imagegen.RegisterAll(reg)
	chain := &toolproviders.Chain{
		Category: "image_gen", Order: []string{"magpie/auto"}, Registry: reg,
		GetConfig: func(string) toolproviders.ProviderConfig {
			return toolproviders.ProviderConfig{APIKey: "tok", Endpoint: srv.URL}
		},
	}
	m := newImgTaskManager(chain, files)
	m.recoverAgentID = "agtA"
	doneID := make(chan string, 1)
	m.notify = func(n ImageTaskNotice) {
		// Notice text carries the task id; remember the first one (the
		// done-listed task). Non-blocking so later notices never wedge
		// this callback.
		if len(doneID) == 0 {
			if m := taskIDRe.FindString(n.Text); m != "" {
				doneID <- m
			}
		}
		notices <- n
	}

	m.recoverRemote() // synchronous; resumeTask goroutines keep polling

	// t-done was delivered immediately (1 save + 1 notice).
	waitNotice(t, notices)
	if len(saved) != 1 || !strings.HasPrefix(saved[0], "web:chat_9/picture-") {
		t.Fatalf("recovered picture save = %v", saved)
	}

	// t-run resumed polling and completed (its poll serves done).
	waitNotice(t, notices)
	if len(saved) != 2 {
		t.Fatalf("resumed task save count = %d", len(saved))
	}

	// t-other (different agent) must not be claimed: exactly 2 notices.
	select {
	case n := <-notices:
		t.Fatalf("unexpected extra notice: %+v", n)
	case <-time.After(400 * time.Millisecond):
	}

	// The recovered local task surfaces its remote id. Task ids are
	// process-global, so take the done task's id from its notice text
	// instead of assuming a fixed number.
	doneNotice := <-doneID
	st, err := m.status(doneNotice)
	if err != nil || !strings.Contains(st, `"queueTask":"t-done"`) {
		t.Fatalf("status %q (%v)", st, err)
	}
}

// taskIDRe extracts an imgtask id out of a notice text.
var taskIDRe = regexp.MustCompile(`imgtask_\d+`)

// A caller webhook (params image_task_callback + secret) receives the
// terminal outcome with a matching HMAC signature.
func TestImageTaskCallback(t *testing.T) {
	var mu sync.Mutex
	var gotBody string
	var gotSig string
	hooks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		mu.Lock()
		gotBody, gotSig = string(buf[:n]), r.Header.Get("X-Fastclaw-Signature")
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer hooks.Close()

	p := newGatedImageProvider()
	notices := make(chan ImageTaskNotice, 4)
	r := imageTaskRegistry(t, p, nil, func(n ImageTaskNotice) { notices <- n })
	r.SetTurnParams(map[string]any{
		"image_task_callback":        hooks.URL,
		"image_task_callback_secret": "sekret",
	})

	out, err := r.Execute(context.Background(), "image_gen", `{"prompt":"a cat","background":true}`)
	if err != nil {
		t.Fatal(err)
	}
	var sub struct {
		Task string `json:"task"`
	}
	json.Unmarshal([]byte(out), &sub)
	p.release <- providerOutcome{err: errFail("boom")}

	waitNotice(t, notices)
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		body, sig := gotBody, gotSig
		mu.Unlock()
		if body != "" {
			if !strings.Contains(body, `"event":"image_task.failed"`) || !strings.Contains(body, sub.Task) || !strings.Contains(body, "boom") {
				t.Fatalf("callback body %q", body)
			}
			mac := hmac.New(sha256.New, []byte("sekret"))
			mac.Write([]byte(body))
			if sig != hex.EncodeToString(mac.Sum(nil)) {
				t.Fatalf("signature mismatch: %q", sig)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("callback never arrived")
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// With a claim lease wired, a second replica's recovery is a no-op: the
// first holder keeps the tasks (and the notices).
func TestImageTaskRecoveryLeaseGates(t *testing.T) {
	fakeLeaser := &fakeTaskLeaser{}
	listJSON := fmt.Sprintf(`{"ok":true,"tasks":[{"id":"t-run","status":"running","finishedAt":0,"meta":%s}]}`,
		func() string {
			return fmt.Sprintf(`{"origin":"fastclaw","agentID":%q,"userID":"u","sessionID":"web:c1","projectID":"","channel":"web","accountID":"a","chatID":"c1","prompt":"p"}`, "agtA")
		}())
	srv := newFakeRecoveryQueue(t, listJSON)

	notices := make(chan ImageTaskNotice, 8)
	mk := func() *imgTaskManager {
		reg := toolproviders.NewRegistry()
		imagegen.RegisterAll(reg)
		chain := &toolproviders.Chain{
			Category: "image_gen", Order: []string{"magpie/auto"}, Registry: reg,
			GetConfig: func(string) toolproviders.ProviderConfig {
				return toolproviders.ProviderConfig{APIKey: "tok", Endpoint: srv.URL}
			},
		}
		m := newImgTaskManager(chain, nil)
		m.recoverAgentID = "agtA"
		m.notify = func(n ImageTaskNotice) { notices <- n }
		m.st = fakeLeaser
		m.holderID = fmt.Sprintf("replica-%d", imgTaskSeq.Add(100))
		return m
	}

	first := mk()
	first.recoverRemote() // acquires
	waitNotice(t, notices)

	second := mk()
	second.recoverRemote() // lease held by "another replica" → skip

	select {
	case n := <-notices:
		t.Fatalf("second replica must not claim: %+v", n)
	case <-time.After(400 * time.Millisecond):
	}
}

// fakeTaskLeaser grants the first acquire only — the second replica
// (different holder id) is refused, as channel_leases does.
type fakeTaskLeaser struct {
	mu        sync.Mutex
	holder    string
	lastRenew bool
}

func (f *fakeTaskLeaser) AcquireChannelLease(_ context.Context, _, accountID, holderID string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder == "" || f.holder == holderID {
		f.holder = holderID
		return true, nil
	}
	return false, nil
}

func (f *fakeTaskLeaser) RenewChannelLease(_ context.Context, _, _, holderID string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastRenew = f.holder == holderID
	return f.lastRenew, nil
}

func (f *fakeTaskLeaser) ReleaseChannelLease(_ context.Context, _, _, holderID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder == holderID {
		f.holder = ""
	}
	return nil
}
