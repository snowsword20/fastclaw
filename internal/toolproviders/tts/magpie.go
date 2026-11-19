package tts

// magpie.go — the imgqueue plugin's task-queue mode for speech.
//
// The other TTS providers are synchronous (seconds-long) API calls.
// This one exists for the background mode: when the tts tool runs with
// background=true and a magpie chain is configured, the speech call
// becomes a queue task (media=tts) — long texts no longer hold a
// connection, the queue retries on gateway hiccups, and the finished
// clip lands in the chat's workspace with a completion notice.
//
// In the chain it sits AFTER the direct providers: sync mode (the
// default) never reaches it unless the admin pins it, because the sync
// facade is just the gateway's /v1/audio/speech passthrough, which the
// plugin also proxies — but direct providers already do that natively.
// Its real job is the async marker path below.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders/queueclient"
)

// Magpie synthesizes speech through a magpie gateway's imgqueue plugin.
type Magpie struct{}

func (Magpie) Category() string { return Category }
func (Magpie) Name() string     { return "magpie" }

// speechTaskTimeout bounds one task end to end (queue wait included).
const speechTaskTimeout = 15 * time.Minute

func (g *Magpie) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) {
	a, err := parseArgs(req.Args)
	if err != nil {
		return toolproviders.Response{}, err
	}
	endpoint := strings.TrimRight(req.Config.Endpoint, "/")
	if endpoint == "" {
		return toolproviders.Response{}, fmt.Errorf("magpie tts: endpoint not configured")
	}

	// Background marker set → go through the task queue (persistence,
	// retries, restart recovery). Sync call → drive the queue task
	// inline; a speech synthesis is seconds-long, so the task round-trip
	// costs little and buys the queue's retry semantics.
	queue := toolproviders.AsyncImageRequested(ctx)

	body := map[string]any{
		"media": "tts",
		"model": req.Config.Model,
		"input": a.Text,
	}
	if a.Voice != "" {
		body["voice"] = a.Voice
	}
	if m, ok := toolproviders.AsyncImageFrom(ctx); ok {
		body["meta"] = map[string]any{
			"origin": "fastclaw", "agentID": m.AgentID, "userID": m.UserID,
			"sessionID": m.SessionID, "projectID": m.ProjectID,
			"channel": m.Channel, "accountID": m.AccountID, "chatID": m.ChatID,
			"prompt": a.Text, "media": "tts",
		}
	}

	waitCtx := ctx
	if queue {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), speechTaskTimeout)
		defer cancel()
	}

	id, err := queueclient.Submit(waitCtx, endpoint, req.Config.APIKey, body)
	if err != nil {
		return toolproviders.Response{}, err
	}
	defer func() {
		if waitCtx.Err() != nil {
			go queueclient.Delete(context.WithoutCancel(waitCtx), endpoint, req.Config.APIKey, id)
		}
	}()

	var failure string
	var clip toolproviders.MediaFile
	consecutive := 0
	interval := 2 * time.Second
	for {
		if waitCtx.Err() != nil {
			return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie tts wait cancelled: %w", waitCtx.Err()))
		}
		task, perr := queueclient.Poll(waitCtx, endpoint, req.Config.APIKey, id)
		if perr != nil {
			if consecutive++; consecutive >= 5 {
				failure = fmt.Sprintf("queue unreachable: %v", perr)
				break
			}
		} else {
			consecutive = 0
			switch task.Status {
			case "done":
				data, ct, ferr := queueclient.FetchFile(waitCtx, endpoint, req.Config.APIKey, id, 0)
				if ferr != nil {
					failure = ferr.Error()
				} else {
					clip = toolproviders.MediaFile{Data: data, ContentType: ct, Filename: "speech.mp3"}
				}
			case "failed", "cancelled":
				failure = fmt.Sprintf("%s: %s", task.Status, strings.TrimSpace(task.Error))
			}
			if failure != "" || clip.Data != nil {
				break
			}
		}
		select {
		case <-waitCtx.Done():
		case <-time.After(interval):
		}
		if next := interval * 2; next > 5*time.Second {
			interval = 5 * time.Second
		} else {
			interval = next
		}
	}
	if failure != "" {
		return toolproviders.Response{}, toolproviders.Retry(fmt.Errorf("magpie tts task %s: %s", id, failure))
	}

	if !queue {
		// Synchronous call: keep the classic contract — MEDIA: line for
		// the chat pipeline to attach the clip to this very reply.
		return writeAudio(clip.Data, extOf(clip.ContentType))
	}
	// Background: hand the bytes to the task manager, which stores them
	// in the workspace and notifies the chat.
	return toolproviders.Response{
		Text:     fmt.Sprintf("Generated speech (queue task %s).", id),
		Media:    []toolproviders.MediaFile{clip},
		TaskRefs: []string{id},
	}, nil
}

// extOf maps an audio content type to a file extension.
func extOf(ct string) string {
	switch {
	case strings.Contains(ct, "wav"):
		return "wav"
	case strings.Contains(ct, "ogg"), strings.Contains(ct, "opus"):
		return "opus"
	case strings.Contains(ct, "aac"):
		return "aac"
	case strings.Contains(ct, "flac"):
		return "flac"
	}
	return "mp3"
}
