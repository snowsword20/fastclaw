package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/fastclaw-ai/fastclaw/internal/store"
	"github.com/fastclaw-ai/fastclaw/internal/toolproviders"
)

// ttsTaskCategory is the tts flavor of the generic media task manager
// (see media_tasks.go). The prompt arg is "text" — the tts tool's input
// field.
var ttsTaskCategory = mediaCategory{
	name:          "tts",
	toolName:      "tts",
	statusTool:    "tts_task_status",
	idPrefix:      "ttstask",
	leaseChannel:  "tts_task",
	callbackParam: "tts_task_callback",
	eventPrefix:   "tts_task",
	labelCN:       "语音",
	unitCN:        "段",
	promptArg:     "text",
	resultKey:     "files",
	limit:         20 * time.Minute,
	doneNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("语音任务已完成：%s「%s」生成 %d 段。调用 tts_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户听到语音。若你已在之前的回复里把该语音发给用户，简短确认即可，不要重复发送。", id, brief, count, id)
	},
	failNotice: func(id, brief, why string) string {
		return fmt.Sprintf("语音任务失败：%s「%s」：%s。把原因简要告诉用户；如需重试可再发起一次 tts。", id, brief, why)
	},
	recoveredNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("语音任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 段。调用 tts_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户听到语音。", id, brief, count, id)
	},
}

// RegisterTTSChain registers the tts tool against a provider chain. Absent
// credentials ⇒ the tool isn't visible to the agent at all. Background
// mode (background=true, or turn param tts_mode=background) submits a
// media task instead of holding the call: useful for long texts and for
// queue-backed providers (magpie/imgqueue) where the clip lands in the
// workspace with a completion notice.
func RegisterTTSChain(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify func(MediaTaskNotice), st store.Store) {
	if chain == nil {
		return
	}
	// "none" is a sentinel meaning the admin explicitly opted out of
	// fastclaw's tts. Detected anywhere in the chain → don't register
	// the tool at all so the model falls back to its own native audio
	// capability (or does without).
	for _, ref := range chain.Order {
		name := ref
		if i := strings.IndexByte(ref, '/'); i >= 0 {
			name = ref[:i]
		}
		if name == "none" {
			return
		}
	}
	if !chain.Available() {
		return
	}

	var tasks *mediaTaskManager
	startTasks := func() *mediaTaskManager {
		if tasks != nil {
			return tasks
		}
		tasks = newMediaTaskManager(ttsTaskCategory, chain, files)
		if notify != nil {
			tasks.notify = notify
		}
		tasks.recoverAgentID = r.AgentID()
		tasks.st = st
		r.RegisterMediaTasks(tasks)
		registerMediaTaskTools(r, tasks)
		go tasks.recoverRemote()
		return tasks
	}

	desc := "Convert text to speech. Uses a configurable provider chain (OpenAI tts-1, MiniMax speech-02, …) with automatic fallback. The audio file is attached to the chat message automatically. Set background=true for long texts (or when tts_mode=background is set by the caller): the call returns a task id at once and tts_task_status delivers the clip when a notice says it's done."
	r.Register("tts", desc, map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"text": map[string]interface{}{
				"type":        "string",
				"description": "Text to synthesize",
			},
			"voice": map[string]interface{}{
				"type":        "string",
				"description": "Voice id (provider-specific; default picked automatically)",
			},
			"background": map[string]interface{}{
				"type":        "boolean",
				"description": "Run as a background task instead of waiting: returns a task id immediately (the clip arrives later via a notice + tts_task_status). Use for long texts.",
			},
		},
		"required": []string{"text"},
	}, func(ctx context.Context, rawArgs json.RawMessage) (string, error) {
		var args map[string]any
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("parse args: %w", err)
		}
		turn := r.forContext(ctx)
		bg, _ := args["background"].(bool)
		switch turn.TurnParamString("tts_mode") {
		case "background", "async":
			bg = true
		case "sync":
			bg = false
		}
		if bg {
			delete(args, "background")
			return startTasks().submit(ctx, turn, args)
		}
		resp, err := chain.Execute(ctx, args)
		if err != nil {
			return "", err
		}
		return resp.Text, nil
	})
}
