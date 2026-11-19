package videogen

// magpie.go — the imgqueue plugin's task-queue mode for video.
//
// Video is minutes-long work, so this provider is queue-native: it
// submits a media=video task to the imgqueue plugin (POST /v1/tasks),
// polls it to a terminal state, and downloads the finished clip
// (GET /v1/tasks/{id}/file). The plugin in turn drives the magpie
// gateway's native /v1/videos API; account pooling and provider routing
// are the gateway's business, exactly as for images.
//
// There is no synchronous facade: without a reachable imgqueue service
// (queueclient.HealthSupported false) the call fails with a clear,
// non-retriable message instead of holding a minutes-long HTTP request.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/queueclient"
)

// Magpie generates videos through a magpie gateway's imgqueue plugin.
type Magpie struct{}

func (Magpie) Category() string { return Category }
func (Magpie) Name() string     { return "magpie" }

// videoTaskTimeout bounds one task end to end, including the plugin's
// own queue wait and retries. Videos are slow; 45 minutes covers a
// queued 2-minute clip on a busy pool.
const videoTaskTimeout = 45 * time.Minute

// poll cadence for a submitted task: 2s growing to 10s.
var pollSchedule = []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}

func (g *Magpie) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	a, err := parseArgs(req.Args)
	if err != nil {
		return toolproviders.Response{}, err
	}
	endpoint := strings.TrimRight(req.Config.Endpoint, "/")
	if endpoint == "" {
		return toolproviders.Response{}, fmt.Errorf("magpie video: endpoint not configured")
	}
	if !queueclient.HealthSupported(ctx, endpoint, req.Config.APIKey) {
		return toolproviders.Response{}, fmt.Errorf(
			"magpie video: %s has no imgqueue task service (needs the magpie-imgqueue plugin v0.3+); video generation is queue-only",
			endpoint)
	}

	body := map[string]any{
		"media":  "video",
		"prompt": a.Prompt,
		"meta":   submitMeta(ctx, a.Prompt),
	}
	if model := req.Config.Model; model != "" && model != "auto" {
		body["model"] = model
	}
	if a.Seconds != "" {
		body["seconds"] = a.Seconds
	}
	if a.Size != "" {
		body["size"] = a.Size
	}
	if a.Resolution != "" {
		body["resolution"] = a.Resolution
	}
	if len(a.Refs) > 0 {
		// Image-to-video: the first reference frame travels as a data URL.
		body["input_reference"] = a.Refs[0]
	}

	id, err := queueclient.Submit(ctx, endpoint, req.Config.APIKey, body)
	if err != nil {
		return toolproviders.Response{}, err
	}
	// Cancellation (task cancelled, provider deadline): stop the remote
	// work too, best-effort, on a context that outlives the caller.
	defer func() {
		if ctx.Err() != nil {
			go queueclient.Delete(context.WithoutCancel(ctx), endpoint, req.Config.APIKey, id)
		}
	}()

	var failure string
	var clip toolproviders.MediaFile
	consecutive := 0
	interval := pollSchedule[0]
	for {
		if ctx.Err() != nil {
			return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie video wait cancelled: %w", ctx.Err()))
		}
		task, perr := queueclient.Poll(ctx, endpoint, req.Config.APIKey, id)
		if perr != nil {
			if consecutive++; consecutive >= 5 {
				failure = fmt.Sprintf("queue unreachable: %v", perr)
				break
			}
		} else {
			consecutive = 0
			switch task.Status {
			case "done":
				data, ct, ferr := queueclient.FetchFile(ctx, endpoint, req.Config.APIKey, id, 0)
				if ferr != nil {
					failure = ferr.Error()
				} else {
					clip = toolproviders.MediaFile{Data: data, ContentType: ct, Filename: "video.mp4"}
				}
			case "failed", "cancelled":
				failure = fmt.Sprintf("%s: %s", task.Status, strings.TrimSpace(task.Error))
			}
			if failure != "" || clip.Data != nil {
				break
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
		if next := interval * 2; next > pollSchedule[len(pollSchedule)-1] {
			interval = pollSchedule[len(pollSchedule)-1]
		} else {
			interval = next
		}
	}
	if failure != "" {
		return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie video task %s: %s", id, failure))
	}
	return toolproviders.Response{
		Text:     fmt.Sprintf("Generated video (queue task %s).", id),
		Media:    []toolproviders.MediaFile{clip},
		TaskRefs: []string{id},
	}, nil
}

// submitMeta stamps the routing identity (recovery) onto the task,
// mirroring imagegen's submitTasks.
func submitMeta(ctx context.Context, prompt string) map[string]any {
	if m, ok := toolproviders.AsyncImageFrom(ctx); ok {
		return map[string]any{
			"origin":    "fastclaw",
			"agentID":   m.AgentID,
			"userID":    m.UserID,
			"sessionID": m.SessionID,
			"projectID": m.ProjectID,
			"channel":   m.Channel,
			"accountID": m.AccountID,
			"chatID":    m.ChatID,
			"prompt":    prompt,
			"media":     "video",
		}
	}
	return map[string]any{"origin": "fastclaw", "media": "video"}
}
