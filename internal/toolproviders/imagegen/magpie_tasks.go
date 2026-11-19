package imagegen

// magpie_tasks.go — the imgqueue plugin's native task-queue mode.
//
// When the call runs in the background (toolproviders.WithAsyncImage,
// set by the image_gen tool's background mode), the provider submits
// imgqueue tasks instead of using the synchronous proxy endpoints:
//
//	POST   /v1/tasks            submit (JSON generation, multipart edit)
//	GET    /v1/tasks/{id}       poll status (queued/running/done/…)
//	GET    /v1/tasks/{id}/image fetch the finished picture (Bearer)
//	DELETE /v1/tasks/{id}       cancel + remove (queued or running)
//
// That buys the plugin's queue fairness, concurrency cap, retry/backoff
// and crash persistence — the very reasons the plugin exists — while the
// caller just sees one long-running Execute that returns pictures at the
// end. Failures come back retriable so the chain still falls back to the
// next provider (e.g. openai), exactly like the synchronous path.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/queueclient"
)

// magpieTaskTimeout bounds one background call end to end, including the
// plugin's own queue wait and retries (its per-attempt gateway timeout
// defaults to 10 minutes with up to 3 attempts, so a tight queue can
// legitimately take a while).
const magpieTaskTimeout = 30 * time.Minute

// maxMagpieImageBytes caps one fetched task picture, matching the tool
// layer's download ceiling (internal/agent/tools maxImageBytes).
const maxMagpieImageBytes = 25 << 20

// magpieTasksSupported reports whether the endpoint runs an imgqueue
// service with the task API (probe cached by the shared client).
func magpieTasksSupported(ctx context.Context, endpoint, key string) bool {
	return queueclient.HealthSupported(ctx, endpoint, key)
}

// imgqueueTask is the poll-relevant slice of the plugin's public task.
type imgqueueTask struct {
	ID     string `json:"id"`
	Status string `json:"status"` // queued | running | done | failed | cancelled
	Error  string `json:"error"`
	Result *struct {
		File          string `json:"file"`
		Mime          string `json:"mime"`
		RevisedPrompt string `json:"revisedPrompt"`
	} `json:"result"`
	Meta *QueueTaskMeta `json:"meta"`
}

// QueueTaskMeta mirrors the meta fastclaw stamps onto submitted tasks
// (routing identity for restart recovery).
type QueueTaskMeta struct {
	Origin    string `json:"origin"`
	AgentID   string `json:"agentID"`
	UserID    string `json:"userID"`
	SessionID string `json:"sessionID"`
	ProjectID string `json:"projectID"`
	Channel   string `json:"channel"`
	AccountID string `json:"accountID"`
	ChatID    string `json:"chatID"`
	Prompt    string `json:"prompt"`
	// Media is the category tag the queue dispatches on; a missing value
	// means image (imgqueue v0.2 tasks predate the field).
	Media string `json:"media,omitempty"`
}

// QueueTask is the exported view of one imgqueue task, for the tool
// layer's restart recovery.
type QueueTask struct {
	ID         string
	Status     string // queued | running | done | failed | cancelled
	Error      string
	FinishedAt int64 // ms epoch; 0 unless terminal
	Meta       *QueueTaskMeta
}

// ListQueueTasks pulls the queue's task list, keeping only entries the
// filter accepts (nil filter = all, up to the plugin's 400-task cap).
func ListQueueTasks(ctx context.Context, endpoint, key string, accept func(QueueTask) bool) ([]QueueTask, error) {
	rows, err := queueclient.List(ctx, endpoint, key, func(t queueclient.Task) bool {
		return accept == nil || accept(queueTaskOf(t))
	})
	if err != nil {
		return nil, err
	}
	out := make([]QueueTask, 0, len(rows))
	for _, t := range rows {
		out = append(out, queueTaskOf(t))
	}
	return out, nil
}

// queueTaskOf maps a queueclient task onto this package's view.
func queueTaskOf(t queueclient.Task) QueueTask {
	qt := QueueTask{ID: t.ID, Status: t.Status, Error: t.Error, FinishedAt: t.FinishedAt}
	if m := t.Meta; m != nil {
		qt.Meta = &QueueTaskMeta{
			Origin: m.Origin, AgentID: m.AgentID, UserID: m.UserID,
			SessionID: m.SessionID, ProjectID: m.ProjectID,
			Channel: m.Channel, AccountID: m.AccountID, ChatID: m.ChatID,
			Prompt: m.Prompt, Media: m.Media,
		}
	}
	return qt
}

// PollQueueTask fetches one task's current state.
func PollQueueTask(ctx context.Context, endpoint, key, id string) (*QueueTask, error) {
	t, err := queueclient.Poll(ctx, endpoint, key, id)
	if err != nil {
		return nil, err
	}
	qt := QueueTask{ID: t.ID, Status: t.Status, Error: t.Error}
	if t.Meta != nil {
		qt.Meta = &QueueTaskMeta{
			Origin: t.Meta.Origin, AgentID: t.Meta.AgentID, UserID: t.Meta.UserID,
			SessionID: t.Meta.SessionID, ProjectID: t.Meta.ProjectID,
			Channel: t.Meta.Channel, AccountID: t.Meta.AccountID, ChatID: t.Meta.ChatID,
			Prompt: t.Meta.Prompt, Media: t.Meta.Media,
		}
	}
	return &qt, nil
}

// FetchQueueTaskImage downloads a finished task's picture (first file,
// /file with /image fallback for older plugin versions).
func FetchQueueTaskImage(ctx context.Context, endpoint, key, id string) (toolproviders.Image, error) {
	data, ct, err := queueclient.FetchFile(ctx, endpoint, key, id, 0)
	if err != nil {
		return toolproviders.Image{}, err
	}
	if ct == "" || !strings.HasPrefix(ct, "image/") {
		ct = http.DetectContentType(data)
	}
	return toolproviders.Image{Data: data, ContentType: ct}, nil
}

// executeTasks runs the background path: submit a.N tasks (the plugin
// produces one picture per task), poll them to a terminal state, then
// fetch the finished pictures. A partial success (some tasks done) is
// still a success; only an empty result falls through to the chain's
// next provider. The remote task ids travel back in Response.TaskRefs
// so the tool layer can reconcile against the queue (status display,
// restart recovery).
func (g *Magpie) executeTasks(ctx context.Context, endpoint, key, model string, a args) (toolproviders.Response, error) {
	ids, err := g.submitTasks(ctx, endpoint, key, model, a)
	if err != nil {
		return toolproviders.Response{}, err
	}
	// Cancellation (task cancelled, provider deadline): stop the remote
	// work too, best-effort, on a context that outlives the caller.
	defer func() {
		if ctx.Err() != nil {
			for _, id := range ids {
				go magpieDeleteTask(context.WithoutCancel(ctx), endpoint, key, id)
			}
		}
	}()

	images, b64s := make([]toolproviders.Image, 0, len(ids)), make([]string, 0, len(ids))
	var failures []string
	pending := make(map[string]int, len(ids)) // id → consecutive poll errors
	for _, id := range ids {
		pending[id] = 0
	}
	interval := 2 * time.Second
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie task wait cancelled: %w", err))
		}
		for id := range pending {
			t, perr := magpiePollTask(ctx, endpoint, key, id)
			if perr != nil {
				if n := pending[id] + 1; n >= 5 {
					delete(pending, id)
					failures = append(failures, fmt.Sprintf("%s: %v", id, perr))
				} else {
					pending[id] = n
				}
				continue
			}
			pending[id] = 0
			switch t.Status {
			case "done":
				delete(pending, id)
				img, err := magpieFetchTaskImage(ctx, endpoint, key, t)
				if err != nil {
					failures = append(failures, fmt.Sprintf("%s: %v", id, err))
					continue
				}
				images = append(images, img)
				b64s = append(b64s, base64Of(img))
			case "failed", "cancelled":
				delete(pending, id)
				failures = append(failures, fmt.Sprintf("%s: %s (%s)", id, t.Status, strings.TrimSpace(t.Error)))
			}
		}
		if len(pending) > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(interval):
			}
			if next := interval * 2; next > 5*time.Second {
				interval = 5 * time.Second
			} else {
				interval = next
			}
		}
	}
	if len(images) == 0 {
		return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie: all %d image task(s) failed: %s", len(ids), strings.Join(failures, "; ")))
	}
	return toolproviders.Response{Text: renderB64(a.Prompt, b64s), Images: images, TaskRefs: ids}, nil
}

// submitTasks posts one imgqueue task per requested picture (generation
// as JSON, reference-image edits as multipart, matching the sync path).
// Every task's meta carries the submitting chat's routing identity —
// imgqueue persists it, and that is what lets a restarted fastclaw
// recognize its own in-flight tasks and reclaim them.
func (g *Magpie) submitTasks(ctx context.Context, endpoint, key, model string, a args) ([]string, error) {
	var meta map[string]any
	if m, ok := toolproviders.AsyncImageFrom(ctx); ok {
		meta = map[string]any{
			"origin":    "fastclaw",
			"agentID":   m.AgentID,
			"userID":    m.UserID,
			"sessionID": m.SessionID,
			"projectID": m.ProjectID,
			"channel":   m.Channel,
			"accountID": m.AccountID,
			"chatID":    m.ChatID,
			"prompt":    a.Prompt,
			"media":     toolproviders.AsyncMediaName(m.Media),
		}
	} else {
		meta = map[string]any{"origin": "fastclaw", "media": "image"}
	}
	ids := make([]string, 0, a.N)
	for i := 0; i < a.N; i++ {
		var (
			httpReq *http.Request
			err     error
		)
		if len(a.Refs) > 0 {
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
			for j, ref := range a.Refs {
				data, ct, derr := decodeDataURL(ref)
				if derr != nil {
					return nil, fmt.Errorf("magpie: reference image %d: %w", j+1, derr)
				}
				h := make(map[string][]string)
				h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="image"; filename="ref%d%s"`, j+1, extFor(ct))}
				h["Content-Type"] = []string{ct}
				part, perr := w.CreatePart(h)
				if perr != nil {
					return nil, perr
				}
				if _, err := part.Write(data); err != nil {
					return nil, err
				}
			}
			if err := w.Close(); err != nil {
				return nil, err
			}
			httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/tasks", &buf)
			if err != nil {
				return nil, err
			}
			httpReq.Header.Set("Content-Type", w.FormDataContentType())
		} else {
			body := map[string]any{
				"model":  model,
				"prompt": a.Prompt,
				// One picture per task — n belongs to the sync facade only.
				"meta": meta,
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
			httpReq, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/tasks", bytes.NewReader(buf))
			if err != nil {
				return nil, err
			}
			httpReq.Header.Set("Content-Type", "application/json")
		}
		if key != "" {
			httpReq.Header.Set("Authorization", "Bearer "+key)
		}
		id, err := magpiePostTask(httpReq)
		if err != nil {
			// Stop whatever already got queued so a failed batch leaves
			// no orphans behind.
			for _, done := range ids {
				go magpieDeleteTask(context.WithoutCancel(ctx), endpoint, key, done)
			}
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func magpiePostTask(httpReq *http.Request) (string, error) {
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", toolproviders.Retry(fmt.Errorf("magpie task submit: %w", err))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode != http.StatusAccepted {
		err := fmt.Errorf("magpie task submit: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		// 429 = queue full, 5xx = plugin trouble — both worth the next
		// provider. Anything else (e.g. 422 validation) is fatal, same
		// rule as the sync facade.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return "", toolproviders.Retry(err)
		}
		return "", err
	}
	var out struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Task.ID == "" {
		return "", toolproviders.Retry(fmt.Errorf("magpie task submit: unexpected answer: %s", strings.TrimSpace(string(body))))
	}
	return out.Task.ID, nil
}

func magpiePollTask(ctx context.Context, endpoint, key, id string) (*imgqueueTask, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v1/tasks/"+id, nil)
	if err != nil {
		return nil, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("poll: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Task imgqueueTask `json:"task"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return &out.Task, nil
}

// magpieFetchTaskImage downloads the finished picture; the plugin keeps
// it on disk (retention defaults to 14 days) and serves the raw bytes.
func magpieFetchTaskImage(ctx context.Context, endpoint, key string, t *imgqueueTask) (toolproviders.Image, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/v1/tasks/"+t.ID+"/image", nil)
	if err != nil {
		return toolproviders.Image{}, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return toolproviders.Image{}, fmt.Errorf("fetch image: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return toolproviders.Image{}, fmt.Errorf("fetch image: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	// Same ceiling the sync path enforces via the tool layer's fetch.
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxMagpieImageBytes+1))
	if err != nil {
		return toolproviders.Image{}, fmt.Errorf("fetch image: %w", err)
	}
	if len(data) > maxMagpieImageBytes {
		return toolproviders.Image{}, fmt.Errorf("image larger than %d MB", maxMagpieImageBytes>>20)
	}
	ct := resp.Header.Get("Content-Type")
	if t.Result != nil && t.Result.Mime != "" {
		ct = t.Result.Mime
	}
	if ct == "" || !strings.HasPrefix(ct, "image/") {
		ct = http.DetectContentType(data)
	}
	return toolproviders.Image{Data: data, ContentType: ct}, nil
}


func magpieDeleteTask(ctx context.Context, endpoint, key, id string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint+"/v1/tasks/"+id, nil)
	if err != nil {
		return
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
	resp.Body.Close()
}

// base64Of renders an image's bytes for renderB64's inline text — the
// tool layer replaces that text with workspace paths before the model
// sees it; it only surfaces verbatim in workspace-less chats.
func base64Of(img toolproviders.Image) string {
	return base64.StdEncoding.EncodeToString(img.Data)
}
