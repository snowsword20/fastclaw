// Package queueclient speaks the imgqueue plugin's task API
// (POST/GET/DELETE /v1/tasks, GET /v1/health) for any media category.
//
// The protocol is the generic API surface documented in
// docs/image-gen-providers.md §2: any service implementing these
// endpoints can act as a task-queue provider. media ("image" | "video" |
// "tts" | "music") rides on the submit body and the task meta; imgqueue
// v0.3+ dispatches per media, and restart recovery filters on
// meta.media so each category's recovery loop only reclaims its own
// tasks.
package queueclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Meta mirrors the routing identity fastclaw stamps onto submitted
// tasks (the queue persists it; that is what lets a restarted process
// recognize and reclaim its own in-flight tasks).
type Meta struct {
	Origin    string `json:"origin"`
	AgentID   string `json:"agentID"`
	UserID    string `json:"userID"`
	SessionID string `json:"sessionID"`
	ProjectID string `json:"projectID"`
	Channel   string `json:"channel"`
	AccountID string `json:"accountID"`
	ChatID    string `json:"chatID"`
	Prompt    string `json:"prompt"`
	// Media is the category tag ("image"/"video"/"tts"/"music"); the
	// queue treats a missing value as image (v0.2 compatibility).
	Media string `json:"media,omitempty"`
}

// Task is the poll-relevant slice of the queue's public task.
type Task struct {
	ID         string `json:"id"`
	Status     string `json:"status"` // queued | running | done | failed | cancelled
	Error      string `json:"error"`
	FinishedAt int64  `json:"finishedAt"` // ms epoch; 0 unless terminal
	Meta       *Meta  `json:"meta"`
}

// taskEnvelope is the GET /v1/tasks/{id} response shape.
type taskEnvelope struct {
	Task struct {
		Task
		Result *struct {
			Files []struct {
				File string `json:"file"`
				Mime string `json:"mime"`
			} `json:"files"`
			File string `json:"file"`
			Mime string `json:"mime"`
		} `json:"result"`
	} `json:"task"`
}

// FetchResult carries a finished task's result files (index 0 plus any
// extras — music tasks can hand back audio + video).
type FetchResult struct {
	Data        []byte
	ContentType string
}

// healthSink caches /v1/health probes per endpoint for 5 minutes — the
// same rule as the capability matrix, so a plain gateway in front of
// the endpoint falls back to the synchronous facade instead of failing
// every call.
var (
	healthMu   sync.Mutex
	healthSink = map[string]healthProbe{}
)

type healthProbe struct {
	at time.Time
	ok bool
}

// HealthSupported probes GET /v1/health (5s timeout) and reports
// whether the endpoint answers like an imgqueue service.
func HealthSupported(ctx context.Context, endpoint, key string) bool {
	healthMu.Lock()
	if p, ok := healthSink[endpoint]; ok && time.Since(p.at) < 5*time.Minute {
		healthMu.Unlock()
		return p.ok
	}
	healthMu.Unlock()

	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/health", nil)
	if err != nil {
		return false
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	ok := err == nil && resp.StatusCode == http.StatusOK
	if err == nil {
		var out struct {
			OK bool `json:"ok"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out)
		resp.Body.Close()
		ok = out.OK
	}
	healthMu.Lock()
	healthSink[endpoint] = healthProbe{at: time.Now(), ok: ok}
	healthMu.Unlock()
	return ok
}

// Submit posts one task (JSON body: model/prompt/media + meta…) and
// returns the queue's task id. Retryable failures (429/5xx, network)
// come back wrapped so the provider chain can fall back.
func Submit(ctx context.Context, endpoint, key string, body map[string]any) (string, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/v1/tasks", bytes.NewReader(buf))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", retry(fmt.Errorf("task submit: %w", err))
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if resp.StatusCode != http.StatusAccepted {
		err := fmt.Errorf("task submit: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		// 429 = queue full, 5xx = plugin trouble — both worth the next
		// provider; anything else (e.g. 422 validation) is fatal.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return "", retry(err)
		}
		return "", err
	}
	var out struct {
		Task struct {
			ID string `json:"id"`
		} `json:"task"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Task.ID == "" {
		return "", retry(fmt.Errorf("task submit: unexpected answer: %s", strings.TrimSpace(string(raw))))
	}
	return out.Task.ID, nil
}

// Poll fetches one task's current state.
func Poll(ctx context.Context, endpoint, key, id string) (*Task, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/tasks/"+id, nil)
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
	var out taskEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	t := out.Task.Task
	return &t, nil
}

// List pulls the queue's task list, keeping the entries the filter
// accepts (nil = all, up to the plugin's 400-task cap).
func List(ctx context.Context, endpoint, key string, accept func(Task) bool) ([]Task, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/tasks?limit=400", nil)
	if err != nil {
		return nil, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list tasks: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Tasks []Task `json:"tasks"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	matched := make([]Task, 0, len(out.Tasks))
	for _, t := range out.Tasks {
		if accept == nil || accept(t) {
			matched = append(matched, t)
		}
	}
	return matched, nil
}

// FetchFile downloads a finished task's result file (index selects
// among multi-file results). It tries /file first and falls back to the
// legacy /image alias for index 0 against older plugin versions.
func FetchFile(ctx context.Context, endpoint, key, id string, index int) ([]byte, string, error) {
	base := strings.TrimRight(endpoint, "/") + "/v1/tasks/" + id
	paths := []string{fmt.Sprintf("%s/file?index=%d", base, index)}
	if index == 0 {
		paths = append(paths, base+"/image")
	}
	var lastErr error
	for _, p := range paths {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p, nil)
		if err != nil {
			return nil, "", err
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("fetch file: %w", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			lastErr = fmt.Errorf("fetch file: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxFileBytes+1))
		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("fetch file: %w", err)
			continue
		}
		if len(data) > maxFileBytes {
			lastErr = fmt.Errorf("file larger than %d MB", maxFileBytes>>20)
			continue
		}
		return data, ct, nil
	}
	return nil, "", lastErr
}

// maxFileBytes caps one fetched result file (videos are big; the tool
// layer's workspace storage handles the rest).
const maxFileBytes = 200 << 20

// Delete cancels + removes a remote task, best-effort (queue removal on
// cancellation so no orphan keeps running).
func Delete(ctx context.Context, endpoint, key, id string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(endpoint, "/")+"/v1/tasks/"+id, nil)
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

// retry wraps err so the provider chain treats it as retriable.
func retry(err error) error {
	return &retriable{err}
}

type retriable struct{ err error }

func (r *retriable) Error() string { return r.err.Error() }
func (r *retriable) Unwrap() error { return r.err }

// Retriable reports whether err came from a retryable queueclient call.
func Retriable(err error) bool {
	_, ok := err.(*retriable)
	return ok
}
