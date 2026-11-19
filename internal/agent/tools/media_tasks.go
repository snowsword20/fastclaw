package tools

// Background media tasks — image_gen / video_gen / tts / music_gen in
// background mode, plus their companion task-status/cancel tools.
//
// One manager implementation serves every media category; a mediaCategory
// value supplies the names, notice texts and limits that differ. The
// surface mirrors exec(run_in_background) + bash_output/kill_shell:
// submitting returns an id immediately, the provider chain then runs in
// a goroutine (with the async-media marker set, so magpie delegates to
// the imgqueue plugin's task queue), and the model either polls or —
// when the turn has already ended — gets woken up by a notice the
// manager injects back into the chat through the notifier the agent
// wires in (loop.go registerMediaChains), the same way a fired cron job
// does.
//
// History: this file generalizes the original image_tasks.go; the image
// category keeps its exact strings and behavior.

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/agl"
	"github.com/fastclaw-ai/fastclaw/internal/share"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/queueclient"
)

// mediaTaskRetention is how long a finished task stays queryable.
const mediaTaskRetention = 24 * time.Hour

// Claim-lease timing, copied from the channel lease constants
// (internal/channels/lease.go): renew at 1/3 TTL so two missed renews
// still leave a takeover window.
const (
	mediaTaskLeaseTTL   = 30 * time.Second
	mediaTaskLeaseRenew = 10 * time.Second
)

// mediaTaskHolderID names this process in claim leases — the tools-layer
// twin of the gateway's boot UUID. Minted once per process.
var mediaTaskHolderID = func() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "mediatask-holder-unknown"
	}
	return hex.EncodeToString(b)
}()

// mediaTaskSeq numbers tasks process-globally so ids never collide across
// agents or categories (an /v1/media-tasks/{id} lookup sweeps the user's
// agents).
var mediaTaskSeq atomic.Int64

// MediaTaskNotice is the wake-up a manager sends into the originating
// chat when a background task reaches a deliverable state (done/failed).
// Channel/AccountID/ChatID are the bus address captured at submit time.
type MediaTaskNotice struct {
	Channel, AccountID, ChatID string
	Text                       string
}

// mediaCategory is everything that differs between the four media task
// flavors. name must match toolproviders.AsyncMediaName values.
type mediaCategory struct {
	name          string // "image" | "video" | "tts" | "music"
	toolName      string // "image_gen" — named in notices and error texts
	statusTool    string // "image_task_status" — the poll tool the notice names
	idPrefix      string // "imgtask" — task ids: imgtask_12, videotask_3…
	leaseChannel  string // "image_task" — restart-recovery claim lease key
	callbackParam string // turn param carrying the caller webhook url
	eventPrefix   string // webhook event: image_task.completed…
	labelCN       string // 图片/视频/语音/音乐 — notice wording
	unitCN        string // 张/个/段 — notice wording
	// promptArg is the tool arg carrying the prompt text ("prompt" for
	// image/video/music, "text" for tts).
	promptArg string
	// resultKey is the JSON key the status/lookup payloads list result
	// files under. image keeps the original "images" key (the model
	// contract and API consumers predate the generalization); the other
	// categories use "files".
	resultKey string
	// limit bounds a background task's whole life.
	limit time.Duration
	// doneNotice / failNotice render the chat wake-up texts.
	doneNotice func(id, brief string, count int) string
	failNotice func(id, brief, why string) string
	// recoveredNotice is the done-notice variant for tasks that finished
	// while fastclaw was down.
	recoveredNotice func(id, brief string, count int) string
}

// imageTaskCategory preserves the original image_gen strings verbatim.
var imageTaskCategory = mediaCategory{
	name:          "image",
	toolName:      "image_gen",
	statusTool:    "image_task_status",
	idPrefix:      "imgtask",
	leaseChannel:  "image_task",
	callbackParam: "image_task_callback",
	eventPrefix:   "image_task",
	labelCN:       "图片",
	unitCN:        "张",
	promptArg:     "prompt",
	resultKey:     "images",
	limit:         35 * time.Minute,
	doneNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("图片任务已完成：%s「%s」生成 %d 张。调用 image_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到图片。若你已在之前的回复里把这张图发给用户，简短确认即可，不要重复发图。", id, brief, count, id)
	},
	failNotice: func(id, brief, why string) string {
		return fmt.Sprintf("图片任务失败：%s「%s」：%s。把原因简要告诉用户；如需重试可再发起一次 image_gen。", id, brief, why)
	},
	recoveredNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("图片任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 张。调用 image_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到图片。", id, brief, count, id)
	},
}

// mediaTaskLeaser is the slice of store.Store the claim lease needs.
type mediaTaskLeaser interface {
	AcquireChannelLease(ctx context.Context, channel, accountID, holderID string, ttl time.Duration) (bool, error)
	RenewChannelLease(ctx context.Context, channel, accountID, holderID string, ttl time.Duration) (bool, error)
	ReleaseChannelLease(ctx context.Context, channel, accountID, holderID string) error
}

type mediaTask struct {
	id     string
	status string // running | done | failed | cancelled
	prompt string
	createdAt,
	finishedAt time.Time

	// Session identity + bus address captured from the submitting turn:
	// results are stored into that chat's workspace from the background
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

	// Caller webhook (params <category>_task_callback / _secret),
	// POSTed once on completion. In-memory like the id — a restart drops it.
	callbackURL    string
	callbackSecret string

	cancel context.CancelFunc

	// Results, written once on completion.
	files      []string // stored workspace paths (or provider URLs)
	putInReply string
	err        string
}

// mediaTaskManager owns every background task of one media category for
// one agent. Like the shell manager it lives as long as the agent
// process — ids do not survive a restart (remote imgqueue tasks do, and
// recoverRemote re-attaches them).
type mediaTaskManager struct {
	cat    mediaCategory
	mu     sync.Mutex
	tasks  map[string]*mediaTask
	chain  *toolproviders.Chain
	files  *ImageFiles
	notify func(MediaTaskNotice)
	// recoverAgentID is the owning agent's id, set at registration;
	// recovery claims only queue tasks whose meta names it.
	recoverAgentID string
	// st, when set, gates recovery with a per-agent claim lease so two
	// replicas don't reclaim (and double-notify) the same queue tasks.
	st         mediaTaskLeaser
	holderID   string
	recoverMu  sync.Mutex
	recoverCtx context.Context    // cancelled when the lease is lost
	recoverCap context.CancelFunc // nil when the lease was never taken
}

func newMediaTaskManager(cat mediaCategory, chain *toolproviders.Chain, files *ImageFiles) *mediaTaskManager {
	return &mediaTaskManager{
		cat:   cat,
		tasks: map[string]*mediaTask{},
		chain: chain,
		files: files,
	}
}

// holder resolves the lease identity.
func (m *mediaTaskManager) holder() string {
	if m.holderID != "" {
		return m.holderID
	}
	return mediaTaskHolderID
}

// submit starts a background run of the chain and returns the model-facing
// acknowledgement. ctx is the tool call's context; its values (billing
// turn) carry over, its cancellation does not.
func (m *mediaTaskManager) submit(ctx context.Context, turn *Registry, args map[string]any) (string, error) {
	prompt, _ := args[m.cat.promptArg].(string)
	if s, ok := args["prompt"].(string); prompt == "" && ok {
		prompt = s // video_gen/music_gen send prompt; tts sends text — cover both
	}

	m.mu.Lock()
	m.purgeLocked()
	t := &mediaTask{
		id:             fmt.Sprintf("%s_%d", m.cat.idPrefix, mediaTaskSeq.Add(1)),
		status:         "running",
		prompt:         prompt,
		createdAt:      time.Now(),
		sessionID:      turn.SessionID(),
		projectID:      turn.ProjectID(),
		channel:        turn.MessageChannel(),
		accountID:      turn.MessageAccountID(),
		chatID:         turn.MessageChatID(),
		callbackURL:    strings.TrimSpace(turn.TurnParamString(m.cat.callbackParam)),
		callbackSecret: turn.TurnParamString(m.cat.callbackParam + "_secret"),
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
		Media:     m.cat.name,
	}
	taskCtx, cancel := context.WithTimeout(
		toolproviders.WithAsyncImage(context.WithoutCancel(ctx), meta), m.cat.limit)
	t.cancel = cancel
	go m.run(t, taskCtx, cancel, args)

	blob, _ := json.Marshal(map[string]any{
		"task":   t.id,
		"status": "submitted",
		"hint":   fmt.Sprintf("Background %s generation queued. Poll %s with this task id; when it finishes, a notice also arrives in this chat — fetch it then and put putInReply in your reply.", m.cat.name, m.cat.statusTool),
	})
	return string(blob), nil
}

// mediaOfResponse normalizes a provider response's payloads into one
// list the storage sink understands. Images and Media are the same
// shape to it; MEDIA: lines (the classic TTS contract — a tmp file the
// chat pipeline attaches) are consumed here so a background tts task
// running a synchronous provider still lands its audio in the workspace.
func mediaOfResponse(resp toolproviders.Response) []toolproviders.Image {
	out := make([]toolproviders.Image, 0, len(resp.Images)+len(resp.Media))
	out = append(out, resp.Images...)
	for _, mf := range resp.Media {
		out = append(out, toolproviders.Image{URL: mf.URL, Data: mf.Data, ContentType: mf.ContentType})
	}
	for _, path := range extractMediaPaths(resp.Text) {
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}
		out = append(out, toolproviders.Image{Data: data, ContentType: http.DetectContentType(data)})
	}
	return out
}

// run executes the chain in the background and files the outcome. It is
// the only writer of a task's results, and only while the task is still
// "running" (a cancel marks it terminal first and wins).
func (m *mediaTaskManager) run(t *mediaTask, ctx context.Context, cancel context.CancelFunc, args map[string]any) {
	defer cancel()

	resp, err := m.chain.Execute(ctx, args)
	payload := mediaOfResponse(resp)

	m.mu.Lock()
	if t.status != "running" { // cancelled while in flight
		m.mu.Unlock()
		return
	}
	t.finishedAt = time.Now()
	switch {
	case err != nil && errors.Is(ctx.Err(), context.Canceled):
		// A deliberate cancel (task_cancel) marked the task terminal
		// already; this is the goroutine catching up. A deadline
		// expiry, though, is a failure worth reporting.
		t.status = "cancelled"
	case err != nil:
		t.status = "failed"
		t.err = err.Error()
	default:
		if m.files == nil || m.files.Save == nil || len(payload) == 0 {
			// No workspace (or nothing usable): URL payloads pass
			// through as-is; byte payloads from queue providers get a
			// 7-day share link when the share service is configured —
			// otherwise the result was generated but is undeliverable.
			t.status = "done"
			t.putInReply = urlsOnly(payload)
			for _, img := range payload {
				if img.URL != "" {
					t.files = append(t.files, img.URL)
				}
			}
			if len(t.files) == 0 {
				if links, ok := m.sharePayloads(ctx, payload); ok {
					t.files = links
					var b strings.Builder
					for i, l := range links {
						fmt.Fprintf(&b, "[media file %d](%s)\n", i+1, l)
					}
					t.putInReply = "putInReply:\n" + b.String()
				} else {
					t.status = "failed"
					t.err = fmt.Sprintf("the provider returned no retrievable %s result (no workspace to store it in)", m.cat.name)
				}
			}
			break
		}
		paths, reply, serr := storeMediaAt(ctx, m.files, t.sessionID, t.projectID, payload)
		if serr != nil {
			t.status = "failed"
			t.err = serr.Error()
			break
		}
		t.status = "done"
		t.files = paths
		t.putInReply = reply
	}
	t.queueTask = strings.Join(resp.TaskRefs, ",")
	done := t.status == "done"
	failed := t.status == "failed"
	brief := taskBrief(t.prompt)
	id, count, why := t.id, len(t.files), t.err
	notify := m.notify
	channel := t.channel
	m.mu.Unlock()

	if notify != nil && channel != "" {
		if done {
			notify(MediaTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
				Text: m.cat.doneNotice(id, brief, count)})
		} else if failed {
			notify(MediaTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
				Text: m.cat.failNotice(id, brief, why)})
		}
	}
	if t.callbackURL != "" {
		go m.deliverCallback(t, done)
	}
}

// deliverCallback POSTs the terminal outcome to the caller's webhook
// (params <category>_task_callback). Links come from the share service
// when configured; otherwise the workspace paths travel as-is. Single
// POST, 10s timeout, no retry — delivery is best-effort.
func (m *mediaTaskManager) deliverCallback(t *mediaTask, done bool) {
	files := make([]string, 0, len(t.files))
	for _, p := range t.files {
		if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
			files = append(files, p)
			continue
		}
		if link, ok := m.shareLink(t, p); ok {
			files = append(files, link)
		}
	}
	event := m.cat.eventPrefix + ".failed"
	if done {
		event = m.cat.eventPrefix + ".completed"
	}
	payload, _ := json.Marshal(map[string]any{
		"event":      event,
		"task":       t.id,
		"status":     t.status,
		"prompt":     t.prompt,
		"files":      files,
		"error":      t.err,
		"finishedAt": t.finishedAt.UnixMilli(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.callbackURL, strings.NewReader(string(payload)))
	if err != nil {
		slog.Warn("media task callback: bad url", "task", t.id, "error", err)
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
		slog.Warn("media task callback failed", "task", t.id, "url", t.callbackURL, "error", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
	if resp.StatusCode >= 300 {
		slog.Warn("media task callback non-2xx", "task", t.id, "status", resp.StatusCode)
	}
}

// sharePayloads turns byte payloads into share links when the share
// service is up; ok=false when nothing could be shared.
func (m *mediaTaskManager) sharePayloads(ctx context.Context, payloads []toolproviders.Image) ([]string, bool) {
	sh := share.Default()
	if sh == nil {
		return nil, false
	}
	var links []string
	for i, p := range payloads {
		if len(p.Data) == 0 {
			continue
		}
		link, err := sh.Create(ctx, p.Data, fmt.Sprintf("%s-%d%s", m.cat.name, i+1, mediaExt(p.ContentType)))
		if err != nil {
			continue
		}
		links = append(links, link)
	}
	return links, len(links) > 0
}

// shareLink turns a stored workspace file into a 7-day download link
// via the share service (no-op ok=false when sharing is off or the file
// can't be read back).
func (m *mediaTaskManager) shareLink(t *mediaTask, path string) (string, bool) {
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
// /v1/media-tasks handler consumes this); ok=false when unknown.
func (m *mediaTaskManager) lookup(id string) (map[string]any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, false
	}
	out := map[string]any{
		"task":      t.id,
		"media":     m.cat.name,
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
		links := make([]string, 0, len(t.files))
		allLinked := len(t.files) > 0
		for _, p := range t.files {
			if link, ok := m.shareLink(t, p); ok {
				links = append(links, link)
			} else {
				links = append(links, p)
				allLinked = false
			}
		}
		out[m.cat.resultKey] = links
		out["filesAreDownloadLinks"] = allLinked
		out["putInReply"] = t.putInReply
	case "failed":
		out["error"] = t.err
	}
	return out, true
}

// status renders the model-facing snapshot; a done task can be fetched
// any number of times (paths stay in the workspace).
func (m *mediaTaskManager) status(id string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return "", &agl.ToolError{Msg: fmt.Sprintf("no such %s task %q (call %s in background mode first; ids are valid only within the same agent process)", m.cat.name, id, m.cat.toolName)}
	}
	out := map[string]any{"task": t.id, "status": t.status, "prompt": t.prompt}
	if t.queueTask != "" {
		out["queueTask"] = t.queueTask
	}
	switch t.status {
	case "running":
		out["elapsed"] = time.Since(t.createdAt).Round(time.Second).String()
		out["advice"] = "Still generating. End your turn now (tell the person the result is on its way) — a notice arrives in this chat when it's done; don't keep polling in this turn."
	case "done":
		out[m.cat.resultKey] = t.files
		out["putInReply"] = t.putInReply
	case "failed":
		out["error"] = t.err
	}
	blob, _ := json.Marshal(out)
	return string(blob), nil
}

// cancel stops a running task (the magpie path also deletes the remote
// imgqueue job); terminal tasks are reported as such.
func (m *mediaTaskManager) cancelTask(id string) (string, error) {
	m.mu.Lock()
	t, ok := m.tasks[id]
	if !ok {
		m.mu.Unlock()
		return "", &agl.ToolError{Msg: fmt.Sprintf("no such %s task %q", m.cat.name, id)}
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
func (m *mediaTaskManager) purgeLocked() {
	cutoff := time.Now().Add(-mediaTaskRetention)
	for id, t := range m.tasks {
		if t.status != "running" && t.finishedAt.Before(cutoff) {
			delete(m.tasks, id)
		}
	}
}

// ---------- restart recovery ----------
//
// fastclaw's task ids live in memory, but a magpie/imgqueue task lives
// on the plugin side (tasks.json + result files, days of retention).
// Every submitted task's meta therefore carries the submitting chat's
// routing identity AND the media tag, and after a restart recoverRemote
// sweeps the queue for tasks stamped with this agent's id and this
// category's media, then re-attaches them:
//
//	queued/running → rebuild a local task and resume polling
//	done           → fetch the file, store it, notify the chat
//	failed/cancelled → dropped silently (a restart must not storm
//	                   the chat with old failures)
//
// Only magpie-backed tasks are recoverable; providers without a remote
// queue cannot survive the process that ran them. Multi-replica
// deployments are gated by the per-agent claim lease (per category, so
// one replica can hold image recovery while another holds video's):
// one holder recovers, the others skip.

// recoverRemote reclaims this agent's in-flight imgqueue tasks of this
// category after a restart. Called once, asynchronously, when the media
// tools register.
func (m *mediaTaskManager) recoverRemote() {
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
		ok, err := m.st.AcquireChannelLease(ctx, m.cat.leaseChannel, agentID, m.holder(), mediaTaskLeaseTTL)
		if err != nil || !ok {
			slog.Info("media task recovery: claim lease not taken, skipping",
				"media", m.cat.name, "agent", agentID, "error", err)
			cancel()
			return
		}
		go m.renewLease(ctx, agentID)
	}
	slog.Info("media task recovery: claim lease held", "media", m.cat.name, "agent", agentID)

	listCtx, cancelList := context.WithTimeout(ctx, 2*time.Minute)
	defer cancelList()
	tasks, err := queueclient.List(listCtx, endpoint, cfg.APIKey, func(t queueclient.Task) bool {
		return t.Meta != nil && t.Meta.Origin == "fastclaw" &&
			t.Meta.AgentID == agentID &&
			toolproviders.AsyncMediaName(t.Meta.Media) == m.cat.name
	})
	if err != nil {
		slog.Warn("media task recovery: queue unreachable", "media", m.cat.name, "error", err)
		return
	}
	for _, rt := range tasks {
		switch rt.Status {
		case "queued", "running":
			m.resumeTask(rt)
		case "done":
			// Only fetch recent completions; a file whose imgqueue
			// retention already swept it is beyond recovery.
			if rt.FinishedAt > 0 && time.Since(time.UnixMilli(rt.FinishedAt)) < mediaTaskRetention {
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
func (m *mediaTaskManager) renewLease(ctx context.Context, agentID string) {
	ticker := time.NewTicker(mediaTaskLeaseRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ok, err := m.st.RenewChannelLease(ctx, m.cat.leaseChannel, agentID, m.holder(), mediaTaskLeaseTTL)
		if err != nil {
			continue // transient store trouble — hold optimistically
		}
		if !ok {
			slog.Warn("media task recovery: claim lease lost, cancelling recovery",
				"media", m.cat.name, "agent", agentID)
			m.recoverMu.Lock()
			if m.recoverCap != nil {
				m.recoverCap()
			}
			m.recoverMu.Unlock()
			return
		}
	}
}

// resumeTask rebuilds a local task for a queued/running remote task
// and restarts the polling loop against it.
func (m *mediaTaskManager) resumeTask(rt queueclient.Task) {
	meta := rt.Meta
	m.mu.Lock()
	n := mediaTaskSeq.Add(1)
	t := &mediaTask{
		id:        fmt.Sprintf("%s_%d", m.cat.idPrefix, n),
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
	taskCtx, cancel := context.WithTimeout(recoverCtx, m.cat.limit)
	t.cancel = cancel
	go m.runRecovered(t, taskCtx, cancel, rt.ID)
	slog.Info("media task recovery: resumed", "media", m.cat.name, "task", t.id, "queue", rt.ID, "chat", t.chatID)
}

// fetchRecovered downloads a finished remote task's first result file
// as an Image payload (the storage sink is content-type agnostic).
func (m *mediaTaskManager) fetchRecovered(ctx context.Context, endpoint, key, queueID string) (toolproviders.Image, error) {
	data, ct, err := queueclient.FetchFile(ctx, endpoint, key, queueID, 0)
	if err != nil {
		return toolproviders.Image{}, err
	}
	if ct == "" {
		ct = http.DetectContentType(data)
	}
	return toolproviders.Image{Data: data, ContentType: ct}, nil
}

// fileOneResult stores a single result payload and files it on the
// task; shared by finishRecovered and runRecovered.
func (m *mediaTaskManager) fileOneResult(ctx context.Context, t *mediaTask, remoteID string, payload toolproviders.Image, fetchErr error) {
	m.mu.Lock()
	if t.status != "running" {
		m.mu.Unlock()
		return
	}
	t.finishedAt = time.Now()
	switch {
	case fetchErr != nil:
		t.status = "failed"
		t.err = fmt.Sprintf("recovered task %s: result not retrievable: %v", remoteID, fetchErr)
	case m.files == nil || m.files.Save == nil:
		t.status = "done"
		t.files = []string{fmt.Sprintf("queue task %s (result stays on the imgqueue side; no workspace to store it)", remoteID)}
		t.putInReply = ""
	default:
		if paths, reply, serr := storeMediaAt(ctx, m.files, t.sessionID, t.projectID, []toolproviders.Image{payload}); serr != nil {
			t.status = "failed"
			t.err = serr.Error()
		} else {
			t.status = "done"
			t.files = paths
			t.putInReply = reply
		}
	}
	id, count := t.id, len(t.files)
	notify := m.notify
	channel := t.channel
	m.mu.Unlock()

	if notify != nil && channel != "" && t.status == "done" {
		notify(MediaTaskNotice{Channel: channel, AccountID: t.accountID, ChatID: t.chatID,
			Text: m.cat.recoveredNotice(id, taskBrief(t.prompt), count)})
	}
}

// finishRecovered handles a task that finished while fastclaw was down:
// fetch the file, store it into the submitting chat's workspace, and
// deliver the completion notice.
func (m *mediaTaskManager) finishRecovered(ctx context.Context, endpoint, key string, rt queueclient.Task) {
	meta := rt.Meta
	m.mu.Lock()
	n := mediaTaskSeq.Add(1)
	t := &mediaTask{
		id:        fmt.Sprintf("%s_%d", m.cat.idPrefix, n),
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

	payload, err := m.fetchRecovered(ctx, endpoint, key, rt.ID)
	m.fileOneResult(ctx, t, rt.ID, payload, err)
	slog.Info("media task recovery: finished", "media", m.cat.name, "task", t.id, "queue", rt.ID, "status", t.status)
}

// runRecovered polls one recovered remote task to a terminal state and
// files the outcome — the recovery twin of run(), speaking to imgqueue
// directly instead of the provider chain (the chain would resubmit).
func (m *mediaTaskManager) runRecovered(t *mediaTask, ctx context.Context, cancel context.CancelFunc, remoteID string) {
	defer cancel()
	name := "magpie"
	cfg := m.chain.GetConfig(name)
	endpoint := strings.TrimRight(cfg.Endpoint, "/")

	var donePayload toolproviders.Image
	var failure string
	interval := 2 * time.Second
	consecutive := 0
	for {
		if ctx.Err() != nil {
			failure = "recovery wait cancelled"
			break
		}
		task, err := queueclient.Poll(ctx, endpoint, cfg.APIKey, remoteID)
		if err != nil {
			if consecutive++; consecutive >= 5 {
				failure = fmt.Sprintf("queue unreachable: %v", err)
				break
			}
		} else {
			consecutive = 0
			switch task.Status {
			case "done":
				payload, ferr := m.fetchRecovered(ctx, endpoint, cfg.APIKey, task.ID)
				if ferr != nil {
					failure = ferr.Error()
				} else {
					donePayload = payload
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

	if failure != "" {
		m.mu.Lock()
		if t.status == "running" {
			t.status = "failed"
			t.err = failure
			t.finishedAt = time.Now()
		}
		m.mu.Unlock()
		return
	}
	m.fileOneResult(ctx, t, remoteID, donePayload, nil)
}

// taskBrief shortens a prompt for notice texts.
func taskBrief(prompt string) string {
	r := []rune(strings.TrimSpace(prompt))
	if len(r) > 40 {
		r = append(r[:40], []rune("…")...)
	}
	return string(r)
}

// registerMediaTaskTools registers this category's status/cancel pair.
func registerMediaTaskTools(r *Registry, m *mediaTaskManager) {
	statusName := m.cat.statusTool
	cancelName := strings.TrimSuffix(statusName, "_status") + "_cancel"
	statusDesc := fmt.Sprintf(`Check a background %s task started with %s in background mode and, when it is done, get the result files.

Returns JSON with:
  - "status": "running" (with elapsed time — poll again later), "done", "failed" (with "error"), or "cancelled"
  - when done: "files" (workspace paths or URLs) and "putInReply" — put that exactly in your reply so the result is shown, plus a short line of your own

A done task can be fetched again at any time within a day. When a notice about a finished task arrives in this chat, call this tool with the task id it names and deliver the result.`, m.cat.name, m.cat.toolName)
	cancelDesc := fmt.Sprintf(`Cancel a background %s task started with %s in background mode. Idempotent — cancelling an already-finished task just reports its status.`, m.cat.name, m.cat.toolName)

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"task": map[string]any{
				"type":        "string",
				"description": fmt.Sprintf(`Task id returned by %s background mode, e.g. %q.`, m.cat.toolName, m.cat.idPrefix+"_1"),
			},
		},
		"required": []string{"task"},
	}
	r.Register(statusName, statusDesc, schema, func(ctx context.Context, raw json.RawMessage) (string, error) {
		var args struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return "", fmt.Errorf("%s: parse args: %w", statusName, err)
		}
		if args.Task == "" {
			return "", fmt.Errorf("%s: task is required", statusName)
		}
		return m.status(args.Task)
	})
	r.Register(cancelName, cancelDesc, schema, func(ctx context.Context, raw json.RawMessage) (string, error) {
		var args struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return "", fmt.Errorf("%s: parse args: %w", cancelName, err)
		}
		if args.Task == "" {
			return "", fmt.Errorf("%s: task is required", cancelName)
		}
		return m.cancelTask(args.Task)
	})
}

// ---------- generic media storage sink ----------

// storeMediaAt stores generated payloads (any media kind) in the chat's
// workspace and returns the paths plus the model-facing blob. Image
// payloads keep the original picture-* naming and ![]() reply format;
// video/audio get their own prefixes and plain markdown links (chat UIs
// don't render ![](video.mp4) as an inline player).
func storeMediaAt(ctx context.Context, files *ImageFiles, sessionID, projectID string, payloads []toolproviders.Image) ([]string, string, error) {
	stamp := time.Now().Format("20060102-150405")
	var paths []string
	var failed []string
	for i, p := range payloads {
		data, ct := p.Data, p.ContentType
		if len(data) == 0 && p.URL != "" {
			var err error
			data, ct, err = fetchImage(ctx, p.URL)
			if err != nil {
				failed = append(failed, err.Error())
				continue
			}
		}
		if len(data) == 0 {
			continue
		}
		if ct == "" {
			ct = http.DetectContentType(data)
		}
		name := fmt.Sprintf("%s-%s-%d%s", mediaFilePrefix(ct), stamp, i+1, mediaExt(ct))
		saved, err := files.Save(ctx, sessionID, projectID, name, data, ct)
		if err != nil {
			failed = append(failed, err.Error())
			continue
		}
		paths = append(paths, "/workspace/"+saved)
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("media task: the results were generated but could not be stored: %s", strings.Join(failed, "; "))
	}
	var reply strings.Builder
	for i, p := range paths {
		if isImagePayload(i < len(payloads) && strings.HasPrefix(payloads[i].ContentType, "image/")) {
			fmt.Fprintf(&reply, "![](%s)\n", p)
		} else {
			fmt.Fprintf(&reply, "[media file %d](%s)\n", i+1, p)
		}
	}
	blob, _ := json.Marshal(map[string]any{
		"files":      paths,
		"putInReply": strings.TrimSpace(reply.String()),
	})
	return paths, string(blob), nil
}

// isImagePayload keeps images in the ![]() inline format.
func isImagePayload(yes bool) bool { return yes }

// extractMediaPaths picks up "MEDIA:<path>" lines from a tool result
// (the classic audio attachment contract; the agent loop's twin lives in
// loop.go). A background media task uses it to consume a synchronous
// provider's tmp-file output into the workspace instead.
func extractMediaPaths(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "MEDIA:"); ok && rest != "" {
			out = append(out, rest)
		}
	}
	return out
}

// mediaFilePrefix picks the workspace filename prefix by content type.
func mediaFilePrefix(ct string) string {
	switch {
	case strings.HasPrefix(ct, "video/"):
		return "video"
	case strings.HasPrefix(ct, "audio/"):
		return "audio"
	}
	return "picture"
}

// mediaExt maps a content type to a file extension (any media kind).
func mediaExt(ct string) string {
	switch {
	case strings.HasPrefix(ct, "image/jpeg"):
		return ".jpg"
	case strings.HasPrefix(ct, "image/webp"):
		return ".webp"
	case strings.HasPrefix(ct, "image/gif"):
		return ".gif"
	case strings.HasPrefix(ct, "image/"):
		return ".png"
	case strings.HasPrefix(ct, "video/webm"):
		return ".webm"
	case strings.HasPrefix(ct, "video/quicktime"):
		return ".mov"
	case strings.HasPrefix(ct, "video/x-matroska"):
		return ".mkv"
	case strings.HasPrefix(ct, "video/"):
		return ".mp4"
	case strings.HasPrefix(ct, "audio/wav"), strings.HasPrefix(ct, "audio/x-wav"), strings.HasPrefix(ct, "audio/wave"):
		return ".wav"
	case strings.HasPrefix(ct, "audio/ogg"), strings.HasPrefix(ct, "audio/opus"):
		return ".opus"
	case strings.HasPrefix(ct, "audio/aac"):
		return ".aac"
	case strings.HasPrefix(ct, "audio/flac"):
		return ".flac"
	case strings.HasPrefix(ct, "audio/mp4"), strings.HasPrefix(ct, "audio/x-m4a"):
		return ".m4a"
	case strings.HasPrefix(ct, "audio/mpeg"), strings.HasPrefix(ct, "audio/mp3"):
		return ".mp3"
	}
	// sniffed fallback for odd types
	return ".bin"
}
