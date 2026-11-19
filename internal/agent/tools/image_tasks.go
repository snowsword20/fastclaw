package tools

// Background image tasks — image_gen(background=true) plus its companion
// tools image_task_status / image_task_cancel.
//
// The surface mirrors exec(run_in_background) + bash_output/kill_shell:
// submitting returns an id immediately, the provider chain then runs in a
// goroutine (with the async-image marker set, so magpie delegates to the
// imgqueue plugin's task queue), and the model either polls or — when the
// turn has already ended — gets woken up by a notice the manager injects
// back into the chat through the notifier the agent wires in
// (loop.go RegisterImageGenChain), the same way a fired cron job does.

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/agl"
	"github.com/fastclaw-ai/fastclaw/internal/share"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/imagegen"
)

// imageTaskLimit bounds a background task's whole life (queue wait +
// generation + storage). Slightly above the magpie task path's own 30
// minutes so the provider can finish its cleanup.
const imageTaskLimit = 35 * time.Minute

// imageTaskRetention is how long a finished task stays queryable.
const imageTaskRetention = 24 * time.Hour

// Claim-lease timing, copied from the channel lease constants
// (internal/channels/lease.go): renew at 1/3 TTL so two missed renews
// still leave a takeover window.
const (
	imageTaskLeaseTTL     = 30 * time.Second
	imageTaskLeaseRenew   = 10 * time.Second
	imageTaskLeaseChannel = "image_task"
)

// imgTaskHolderID names this process in claim leases — the tools-layer
// twin of the gateway's boot UUID. Minted once per process.
var imgTaskHolderID = func() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "imgtask-holder-unknown"
	}
	return hex.EncodeToString(b)
}()

// imgTaskSeq numbers tasks process-globally so ids never collide across
// agents (an /v1/image-tasks/{id} lookup sweeps the user's agents).
var imgTaskSeq atomic.Int64

// ImageTaskNotice is the wake-up the manager sends into the originating
// chat when a background task reaches a deliverable state (done/failed).
// Channel/AccountID/ChatID are the bus address captured at submit time.
type ImageTaskNotice struct {
	Channel, AccountID, ChatID string
	Text                       string
}

// imgTaskManager owns every background image_gen task of one agent.
// Like the shell manager it lives as long as the agent process — ids do
// not survive a restart (remote imgqueue tasks do, and recoverRemote
// re-attaches them).
type imgTaskManager struct {
	mu     sync.Mutex
	tasks  map[string]*imgTask
	chain  *toolproviders.Chain
	files  *ImageFiles
	notify func(ImageTaskNotice)
	// recoverAgentID is the owning agent's id, set at registration;
	// recovery claims only queue tasks whose meta names it.
	recoverAgentID string
	// st, when set, gates recovery with a per-agent claim lease so two
	// replicas don't reclaim (and double-notify) the same queue tasks.
	// store.Store satisfies imageTaskLeaser; tests can fake just the
	// three methods.
	st         imageTaskLeaser
	// holderID names this replica in the lease; defaults to the
	// process-wide imgTaskHolderID (tests inject distinct values).
	holderID   string
	recoverMu  sync.Mutex
	recoverCtx context.Context    // cancelled when the lease is lost
	recoverCap context.CancelFunc // nil when the lease was never taken
}

// holder resolves the lease identity.
func (m *imgTaskManager) holder() string {
	if m.holderID != "" {
		return m.holderID
	}
	return imgTaskHolderID
}

// imageTaskLeaser is the slice of store.Store the claim lease needs.
type imageTaskLeaser interface {
	AcquireChannelLease(ctx context.Context, channel, accountID, holderID string, ttl time.Duration) (bool, error)
	RenewChannelLease(ctx context.Context, channel, accountID, holderID string, ttl time.Duration) (bool, error)
	ReleaseChannelLease(ctx context.Context, channel, accountID, holderID string) error
}

type imgTask struct {
	id     string
	status string // running | done | failed | cancelled
	prompt string
	createdAt,
	finishedAt time.Time

	// Session identity + bus address captured from the submitting turn:
	// pictures are stored into that chat's workspace from the background
	// goroutine, and the completion notice is routed back to the chat.
	sessionID string
	projectID string
	channel,
	accountID,
	chatID string

	// queueTask is the remote task id a task-queue provider (magpie →
	// imgqueue) reported back in Response.TaskRefs; empty for providers
	// without a native queue.
	queueTask string

	// Caller webhook (params image_task_callback / _secret), POSTed
	// once on completion. In-memory like the id — a restart drops it.
	callbackURL    string
	callbackSecret string

	cancel context.CancelFunc

	// Results, written once on completion.
	images     []string
	putInReply string
	err        string
}

func newImgTaskManager(chain *toolproviders.Chain, files *ImageFiles) *imgTaskManager {
	return &imgTaskManager{
		tasks: map[string]*imgTask{},
		chain: chain,
		files: files,
	}
}

// submit starts a background run of the chain and returns the model-facing
// acknowledgement. ctx is the tool call's context; its values (billing
// turn) carry over, its cancellation does not.
func (m *imgTaskManager) submit(ctx context.Context, turn *Registry, args map[string]any) (string, error) {
	prompt, _ := args["prompt"].(string)

	m.mu.Lock()
	m.purgeLocked()
	t := &imgTask{
		id:        fmt.Sprintf("imgtask_%d", imgTaskSeq.Add(1)),
		status:    "running",
		prompt:    prompt,
		createdAt: time.Now(),
		sessionID: turn.SessionID(),
		projectID: turn.ProjectID(),
		channel:   turn.MessageChannel(),
		accountID: turn.MessageAccountID(),
		chatID:    turn.MessageChatID(),
		callbackURL:    strings.TrimSpace(turn.TurnParamString("image_task_callback")),
		callbackSecret: turn.TurnParamString("image_task_callback_secret"),
	}
	m.tasks[t.id] = t
	m.mu.Unlock()

	// Routing identity rides on the context: task-queue providers stamp
	// it into the remote task's meta, which is what lets a restarted
	// process recognize and reclaim its own in-flight tasks (see
	// recoverRemote).
	meta := toolproviders.AsyncImageMeta{
		AgentID:   turn.AgentID(),
		UserID:    turn.OwnerUserID(),
		SessionID: t.sessionID,
		ProjectID: t.projectID,
		Channel:   t.channel,
		AccountID: t.accountID,
		ChatID:    t.chatID,
		Prompt:    prompt,
	}
	taskCtx, cancel := context.WithTimeout(
		toolproviders.WithAsyncImage(context.WithoutCancel(ctx), meta), imageTaskLimit)
	t.cancel = cancel
	go m.run(t, taskCtx, cancel, args)

	blob, _ := json.Marshal(map[string]any{
		"task":   t.id,
		"status": "submitted",
		"hint":   "Background generation queued. Poll image_task_status with this task id; when it finishes, a notice also arrives in this chat — fetch it then and put putInReply in your reply.",
	})
	return string(blob), nil
}

// run executes the chain in the background and files the outcome. It is
// the only writer of a task's results, and only while the task is still
// "running" (a cancel marks it terminal first and wins).
func (m *imgTaskManager) run(t *imgTask, ctx context.Context, cancel context.CancelFunc, args map[string]any) {
	defer cancel()

	resp, err := m.chain.Execute(ctx, args)

	m.mu.Lock()
	if t.status != "running" { // cancelled while in flight
		m.mu.Unlock()
		return
	}
	t.finishedAt = time.Now()
	switch {
	case err != nil && errors.Is(ctx.Err(), context.Canceled):
		// A deliberate cancel (image_task_cancel) marked the task
		// terminal already; this is the goroutine catching up. A
		// deadline expiry, though, is a failure worth reporting.
		t.status = "cancelled"
	case err != nil:
		t.status = "failed"
		t.err = err.Error()
	default:
		if m.files == nil || m.files.Save == nil || len(resp.Images) == 0 {
			t.status = "done"
			t.putInReply = urlsOnly(resp.Images)
			for _, img := range resp.Images {
				if img.URL != "" {
					t.images = append(t.images, img.URL)
				}
			}
			if len(t.images) == 0 && strings.Contains(t.putInReply, "can't be shown") {
				t.status = "failed"
				t.err = "the provider returned no retrievable picture"
			}
			break
		}
		paths, reply, serr := storeImagesAt(ctx, m.files, t.sessionID, t.projectID, resp.Images)
		if serr != nil {
			t.status = "failed"
			t.err = serr.Error()
			break
		}
		t.status = "done"
		t.images = paths
		t.putInReply = reply
	}
	t.queueTask = strings.Join(resp.TaskRefs, ",")
	done := t.status == "done"
	failed := t.status == "failed"
	brief := taskBrief(t.prompt)
	id, count, why := t.id, len(t.images), t.err
	notify := m.notify
	channel := t.channel
	m.mu.Unlock()

	if notify != nil && channel != "" {
		if done {
			notify(ImageTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
				Text: fmt.Sprintf("图片任务已完成：%s「%s」生成 %d 张。调用 image_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到图片。若你已在之前的回复里把这张图发给用户，简短确认即可，不要重复发图。", id, brief, count, id)})
		} else if failed {
			notify(ImageTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
				Text: fmt.Sprintf("图片任务失败：%s「%s」：%s。把原因简要告诉用户；如需重试可再发起一次 image_gen。", id, brief, why)})
		}
	}
	if t.callbackURL != "" {
		go m.deliverCallback(t, done)
	}
}

// deliverCallback POSTs the terminal outcome to the caller's webhook
// (params image_task_callback). Links come from the share service when
// configured; otherwise the workspace paths travel as-is. Single POST,
// 10s timeout, no retry — delivery is best-effort.
func (m *imgTaskManager) deliverCallback(t *imgTask, done bool) {
	images := make([]string, 0, len(t.images))
	for _, p := range t.images {
		if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
			images = append(images, p)
			continue
		}
		if link, ok := m.shareLink(t, p); ok {
			images = append(images, link)
		}
	}
	event := "image_task.failed"
	if done {
		event = "image_task.completed"
	}
	payload, _ := json.Marshal(map[string]any{
		"event":      event,
		"task":       t.id,
		"status":     t.status,
		"prompt":     t.prompt,
		"images":     images,
		"error":      t.err,
		"finishedAt": t.finishedAt.UnixMilli(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.callbackURL, strings.NewReader(string(payload)))
	if err != nil {
		slog.Warn("image task callback: bad url", "task", t.id, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if t.callbackSecret != "" {
		mac := hmac.New(sha256.New, []byte(t.callbackSecret))
		mac.Write(payload)
		req.Header.Set("X-Fastclaw-Signature", hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("image task callback failed", "task", t.id, "url", t.callbackURL, "error", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
	if resp.StatusCode >= 300 {
		slog.Warn("image task callback non-2xx", "task", t.id, "status", resp.StatusCode)
	}
}

// shareLink turns a stored workspace picture into a 7-day download
// link via the share service (no-op ok=false when sharing is off or
// the picture can't be read back).
func (m *imgTaskManager) shareLink(t *imgTask, path string) (string, bool) {
	sh := share.Default()
	if sh == nil || m.files == nil || m.files.Load == nil {
		return "", false
	}
	f, err := m.files.Load(context.Background(), t.sessionID, t.projectID, path)
	if err != nil {
		return "", false
	}
	link, err := sh.Create(context.Background(), f.Bytes, f.Name)
	if err != nil {
		return "", false
	}
	return link, true
}

// lookup returns the API-facing snapshot of one task (the
// /v1/image-tasks handler consumes this); ok=false when unknown.
func (m *imgTaskManager) lookup(id string) (map[string]any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, false
	}
	out := map[string]any{
		"task":      t.id,
		"status":    t.status,
		"prompt":    t.prompt,
		"createdAt": t.createdAt.UnixMilli(),
	}
	if t.queueTask != "" {
		out["queueTask"] = t.queueTask
	}
	if !t.finishedAt.IsZero() {
		out["finishedAt"] = t.finishedAt.UnixMilli()
	}
	switch t.status {
	case "running":
		out["elapsed"] = time.Since(t.createdAt).Round(time.Second).String()
	case "done":
		// For API consumers the workspace paths are useless — hand out
		// share download links when the service is configured, falling
		// back to the raw paths (the caller can resolve them through
		// the session-files API) when it isn't.
		links := make([]string, 0, len(t.images))
		allLinked := len(t.images) > 0
		for _, p := range t.images {
			if link, ok := m.shareLink(t, p); ok {
				links = append(links, link)
			} else {
				links = append(links, p)
				allLinked = false
			}
		}
		out["images"] = links
		out["imagesAreDownloadLinks"] = allLinked
		out["putInReply"] = t.putInReply
	case "failed":
		out["error"] = t.err
	}
	return out, true
}

// status renders the model-facing snapshot; a done task can be fetched
// any number of times (paths stay in the workspace).
func (m *imgTaskManager) status(id string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return "", &agl.ToolError{Msg: fmt.Sprintf("no such image task %q (call image_gen with background=true first; ids are valid only within the same agent process)", id)}
	}
	out := map[string]any{"task": t.id, "status": t.status, "prompt": t.prompt}
	if t.queueTask != "" {
		out["queueTask"] = t.queueTask
	}
	switch t.status {
	case "running":
		out["elapsed"] = time.Since(t.createdAt).Round(time.Second).String()
		out["advice"] = "Still generating. End your turn now (tell the person the picture is on its way) — a notice arrives in this chat when it's done; don't keep polling in this turn."
	case "done":
		out["images"] = t.images
		out["putInReply"] = t.putInReply
	case "failed":
		out["error"] = t.err
	}
	blob, _ := json.Marshal(out)
	return string(blob), nil
}

// cancel stops a running task (the magpie path also deletes the remote
// imgqueue job); terminal tasks are reported as such.
func (m *imgTaskManager) cancelTask(id string) (string, error) {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return "", &agl.ToolError{Msg: fmt.Sprintf("no such image task %q", id)}
	}
	if t.status != "running" {
		st := t.status
		m.mu.Unlock()
		return fmt.Sprintf("Already finished (status=%s).", st), nil
	}
	t.status = "cancelled"
	cancel := t.cancel
	t.finishedAt = time.Now()
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return fmt.Sprintf("Cancelled %s.", id), nil
}

// purgeLocked drops terminal tasks past their retention; called on each
// submit so no extra sweeper goroutine is needed.
func (m *imgTaskManager) purgeLocked() {
	cutoff := time.Now().Add(-imageTaskRetention)
	for id, t := range m.tasks {
		if t.status != "running" && t.finishedAt.Before(cutoff) {
			delete(m.tasks, id)
		}
	}
}

// ---------- restart recovery ----------
//
// fastclaw's task ids live in memory, but a magpie/imgqueue task lives
// on the plugin side (tasks.json + image files, days of retention).
// Every submitted task's meta therefore carries the submitting chat's
// routing identity, and after a restart recoverRemote sweeps the queue
// for tasks stamped with this agent's id and re-attaches them:
//
//	queued/running → rebuild a local imgTask and resume polling
//	done           → fetch the picture, store it, notify the chat
//	failed/cancelled → dropped silently (a restart must not storm
//	                   the chat with old failures)
//
// Only magpie-backed tasks are recoverable; providers without a remote
// queue cannot survive the process that ran them. Multi-replica
// deployments are gated by the per-agent claim lease (see recoverRemote):
// one holder recovers, the others skip.

// recoverRemote reclaims this agent's in-flight imgqueue tasks after a
// restart. Called once, asynchronously, when the image tools register.
// With a store wired, a per-agent claim lease (channel_leases table)
// first gates concurrent replicas: the loser boots without touching the
// queue; the winner holds the lease for the process lifetime — losing
// it later (stolen after a stall) cancels every recovery poll so the
// new holder takes over without double delivery. Network errors are
// logged and swallowed: the tasks stay on imgqueue (its own retention
// cleans them up eventually) and nothing breaks.
func (m *imgTaskManager) recoverRemote() {
	name := "magpie"
	cfg := m.chain.GetConfig(name)
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	if endpoint == "" || m.chain.Registry == nil || m.chain.Registry.Get(m.chain.Category, name) == nil {
		return
	}
	m.mu.Lock()
	agentID := m.recoverAgentID
	m.mu.Unlock()
	if agentID == "" {
		return
	}

	// Recovery work hangs off recoverCtx so a lost lease (another
	// replica took over) cancels the polls before they could deliver a
	// duplicate notice.
	ctx, cancel := context.WithCancel(context.Background())
	m.recoverMu.Lock()
	m.recoverCtx, m.recoverCap = ctx, cancel
	m.recoverMu.Unlock()
	if m.st != nil {
		ok, err := m.st.AcquireChannelLease(ctx, imageTaskLeaseChannel, agentID, m.holder(), imageTaskLeaseTTL)
		if err != nil || !ok {
			slog.Info("image task recovery: claim lease not taken, skipping",
				"agent", agentID, "error", err)
			cancel()
			return
		}
		go m.renewLease(ctx, agentID)
	}
	slog.Info("image task recovery: claim lease held", "agent", agentID)

	listCtx, cancelList := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelList()
	tasks, err := imagegen.ListQueueTasks(listCtx, endpoint, cfg.APIKey, func(t imagegen.QueueTask) bool {
		return t.Meta != nil && t.Meta.Origin == "fastclaw" && t.Meta.AgentID == agentID
	})
	if err != nil {
		slog.Warn("image task recovery: queue unreachable", "error", err)
		return
	}
	for _, rt := range tasks {
		switch rt.Status {
		case "queued", "running":
			m.resumeTask(rt)
		case "done":
			// Only fetch recent completions; a picture whose imgqueue
			// retention already swept it is beyond recovery.
			if rt.FinishedAt > 0 && time.Since(time.UnixMilli(rt.FinishedAt)) < imageTaskRetention {
				m.finishRecovered(ctx, endpoint, cfg.APIKey, rt)
			}
		default:
			// failed/cancelled while we were down — drop silently.
		}
	}
}

// renewLease keeps the claim lease alive for the process lifetime;
// losing it cancels the recovery context so the polls stop and the new
// holder takes over.
func (m *imgTaskManager) renewLease(ctx context.Context, agentID string) {
	ticker := time.NewTicker(imageTaskLeaseRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ok, err := m.st.RenewChannelLease(ctx, imageTaskLeaseChannel, agentID, m.holder(), imageTaskLeaseTTL)
		if err != nil {
			continue // transient store trouble — hold optimistically
		}
		if !ok {
			slog.Warn("image task recovery: claim lease lost, cancelling recovery",
				"agent", agentID)
			m.recoverMu.Lock()
			if m.recoverCap != nil {
				m.recoverCap()
			}
			m.recoverMu.Unlock()
			return
		}
	}
}

// resumeTask rebuilds a local imgTask for a queued/running remote task
// and restarts the polling loop against it.
func (m *imgTaskManager) resumeTask(rt imagegen.QueueTask) {
	meta := rt.Meta
	m.mu.Lock()
	n := imgTaskSeq.Add(1)
	t := &imgTask{
		id:        fmt.Sprintf("imgtask_%d", n),
		status:    "running",
		prompt:    meta.Prompt,
		createdAt: time.Now(),
		sessionID: meta.SessionID,
		projectID: meta.ProjectID,
		channel:   meta.Channel,
		accountID: meta.AccountID,
		chatID:    meta.ChatID,
		queueTask: rt.ID,
	}
	m.tasks[t.id] = t
	m.mu.Unlock()

	// Hang the recovered poll off the lease's context: losing the claim
	// (another replica took over) cancels this poll before it could
	// double-deliver.
	recoverCtx := context.Background()
	m.recoverMu.Lock()
	if m.recoverCtx != nil {
		recoverCtx = m.recoverCtx
	}
	m.recoverMu.Unlock()
	taskCtx, cancel := context.WithTimeout(recoverCtx, imageTaskLimit)
	t.cancel = cancel
	go m.runRecovered(t, taskCtx, cancel, rt.ID)
	slog.Info("image task recovery: resumed", "task", t.id, "queue", rt.ID, "chat", t.chatID)
}

// finishRecovered handles a task that finished while fastclaw was down:
// fetch the picture, store it into the submitting chat's workspace, and
// deliver the completion notice.
func (m *imgTaskManager) finishRecovered(ctx context.Context, endpoint, key string, rt imagegen.QueueTask) {
	meta := rt.Meta
	m.mu.Lock()
	n := imgTaskSeq.Add(1)
	t := &imgTask{
		id:        fmt.Sprintf("imgtask_%d", n),
		status:    "running",
		prompt:    meta.Prompt,
		createdAt: time.Now(),
		sessionID: meta.SessionID,
		projectID: meta.ProjectID,
		channel:   meta.Channel,
		accountID: meta.AccountID,
		chatID:    meta.ChatID,
		queueTask: rt.ID,
	}
	m.tasks[t.id] = t
	m.mu.Unlock()

	img, err := imagegen.FetchQueueTaskImage(ctx, endpoint, key, rt.ID)
	m.mu.Lock()
	if t.status != "running" {
		m.mu.Unlock()
		return
	}
	t.finishedAt = time.Now()
	switch {
	case err != nil:
		t.status = "failed"
		t.err = fmt.Sprintf("recovered task %s: picture not retrievable: %v", rt.ID, err)
	case m.files == nil || m.files.Save == nil:
		t.status = "done"
		t.images = []string{fmt.Sprintf("queue task %s (picture stays on the imgqueue side; no workspace to store it)", rt.ID)}
		t.putInReply = ""
	default:
		if paths, reply, serr := storeImagesAt(ctx, m.files, t.sessionID, t.projectID, []toolproviders.Image{img}); serr != nil {
			t.status = "failed"
			t.err = serr.Error()
		} else {
			t.status = "done"
			t.images = paths
			t.putInReply = reply
		}
	}
	id, count := t.id, len(t.images)
	notify := m.notify
	channel := t.channel
	m.mu.Unlock()

	if notify != nil && channel != "" && t.status == "done" {
		notify(ImageTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
			Text: fmt.Sprintf("图片任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 张。调用 image_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到图片。", id, taskBrief(t.prompt), count, id)})
	}
	slog.Info("image task recovery: finished", "task", t.id, "queue", rt.ID, "status", t.status)
}

// runRecovered polls one recovered remote task to a terminal state and
// files the outcome — the recovery twin of run(), speaking to imgqueue
// directly instead of the provider chain (the chain would resubmit).
func (m *imgTaskManager) runRecovered(t *imgTask, ctx context.Context, cancel context.CancelFunc, remoteID string) {
	defer cancel()
	name := "magpie"
	cfg := m.chain.GetConfig(name)
	endpoint := strings.TrimRight(cfg.Endpoint, "/")

	var doneImage toolproviders.Image
	var failure string
	interval := 2 * time.Second
	consecutive := 0
	for {
		if ctx.Err() != nil {
			failure = "recovery wait cancelled"
			break
		}
		task, err := imagegen.PollQueueTask(ctx, endpoint, cfg.APIKey, remoteID)
		if err != nil {
			if consecutive++; consecutive >= 5 {
				failure = fmt.Sprintf("queue unreachable: %v", err)
				break
			}
		} else {
			consecutive = 0
			switch task.Status {
			case "done":
				img, ferr := imagegen.FetchQueueTaskImage(ctx, endpoint, cfg.APIKey, task.ID)
				if ferr != nil {
					failure = ferr.Error()
				} else {
					doneImage = img
				}
			case "failed", "cancelled":
				failure = fmt.Sprintf("%s: %s", task.Status, strings.TrimSpace(task.Error))
			}
			if task.Status == "done" || task.Status == "failed" || task.Status == "cancelled" {
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
		if next := interval * 2; next > 5*time.Second {
			interval = 5 * time.Second
		}
	}

	m.mu.Lock()
	if t.status != "running" {
		m.mu.Unlock()
		return
	}
	t.finishedAt = time.Now()
	switch {
	case failure != "":
		t.status = "failed"
		t.err = failure
	case m.files == nil || m.files.Save == nil:
		t.status = "done"
		t.images = []string{fmt.Sprintf("queue task %s (picture stays on the imgqueue side; no workspace to store it)", remoteID)}
		t.putInReply = ""
	default:
		if paths, reply, serr := storeImagesAt(ctx, m.files, t.sessionID, t.projectID, []toolproviders.Image{doneImage}); serr != nil {
			t.status = "failed"
			t.err = serr.Error()
		} else {
			t.status = "done"
			t.images = paths
			t.putInReply = reply
		}
	}
	id, count := t.id, len(t.images)
	notify := m.notify
	channel := t.channel
	m.mu.Unlock()

	if notify != nil && channel != "" && t.status == "done" {
		notify(ImageTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
			Text: fmt.Sprintf("图片任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 张。调用 image_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到图片。", id, taskBrief(t.prompt), count, id)})
	}
}

// taskBrief shortens a prompt for notice texts.
func taskBrief(prompt string) string {
	r := []rune(strings.TrimSpace(prompt))
	if len(r) > 40 {
		r = append(r[:40], []rune("…")...)
	}
	return string(r)
}

const imageTaskStatusDescription = `Check a background image task started with image_gen(background=true) and, when it is done, get the pictures.

Returns JSON with:
  - "status": "running" (with elapsed time — poll again later), "done", "failed" (with "error"), or "cancelled"
  - when done: "images" (workspace paths or URLs) and "putInReply" — put that exactly in your reply so the pictures are shown, plus a short line of your own

A done task can be fetched again at any time within a day. When a notice about a finished task arrives in this chat, call this tool with the task id it names and deliver the pictures.`

const imageTaskCancelDescription = `Cancel a background image task started with image_gen(background=true). Idempotent — cancelling an already-finished task just reports its status.`

func registerImageTaskTools(r *Registry, m *imgTaskManager) {
	r.Register("image_task_status", imageTaskStatusDescription, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task": map[string]any{
				"type":        "string",
				"description": `Task id returned by image_gen(background=true), e.g. "imgtask_1".`,
			},
		},
		"required": []string{"task"},
	}, func(ctx context.Context, raw json.RawMessage) (string, error) {
		var args struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return "", fmt.Errorf("image_task_status: parse args: %w", err)
		}
		if args.Task == "" {
			return "", fmt.Errorf("image_task_status: task is required")
		}
		return m.status(args.Task)
	})

	r.Register("image_task_cancel", imageTaskCancelDescription, map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task": map[string]any{
				"type":        "string",
				"description": "Task id returned by image_gen(background=true).",
			},
		},
		"required": []string{"task"},
	}, func(ctx context.Context, raw json.RawMessage) (string, error) {
		var args struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return "", fmt.Errorf("image_task_cancel: parse args: %w", err)
		}
		if args.Task == "" {
			return "", fmt.Errorf("image_task_cancel: task is required")
		}
		return m.cancelTask(args.Task)
	})
}
