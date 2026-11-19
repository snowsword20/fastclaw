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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/agl"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// imageTaskLimit bounds a background task's whole life (queue wait +
// generation + storage). Slightly above the magpie task path's own 30
// minutes so the provider can finish its cleanup.
const imageTaskLimit = 35 * time.Minute

// imageTaskRetention is how long a finished task stays queryable.
const imageTaskRetention = 24 * time.Hour

// ImageTaskNotice is the wake-up the manager sends into the originating
// chat when a background task reaches a deliverable state (done/failed).
// Channel/AccountID/ChatID are the bus address captured at submit time.
type ImageTaskNotice struct {
	Channel, AccountID, ChatID string
	Text                       string
}

// imgTaskManager owns every background image_gen task of one agent.
// Like the shell manager it lives as long as the agent process — ids do
// not survive a restart.
type imgTaskManager struct {
	mu     sync.Mutex
	seq    int
	tasks  map[string]*imgTask
	chain  *toolproviders.Chain
	files  *ImageFiles
	notify func(ImageTaskNotice)
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
	m.seq++
	t := &imgTask{
		id:        fmt.Sprintf("imgtask_%d", m.seq),
		status:    "running",
		prompt:    prompt,
		createdAt: time.Now(),
		sessionID: turn.SessionID(),
		projectID: turn.ProjectID(),
		channel:   turn.MessageChannel(),
		accountID: turn.MessageAccountID(),
		chatID:    turn.MessageChatID(),
	}
	m.tasks[t.id] = t
	m.mu.Unlock()

	taskCtx, cancel := context.WithTimeout(
		toolproviders.WithAsyncImage(context.WithoutCancel(ctx)), imageTaskLimit)
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
