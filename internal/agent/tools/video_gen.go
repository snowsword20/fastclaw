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

// videoTaskCategory is the video_gen flavor of the generic media task
// manager (see media_tasks.go).
var videoTaskCategory = mediaCategory{
	name:          "video",
	toolName:      "video_gen",
	statusTool:    "video_task_status",
	idPrefix:      "videotask",
	leaseChannel:  "video_task",
	callbackParam: "video_task_callback",
	eventPrefix:   "video_task",
	labelCN:       "视频",
	unitCN:        "个",
	promptArg:     "prompt",
	resultKey:     "files",
	limit:         50 * time.Minute,
	doneNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("视频任务已完成：%s「%s」生成 %d 个。调用 video_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到视频。若你已在之前的回复里把该视频发给用户，简短确认即可，不要重复发送。", id, brief, count, id)
	},
	failNotice: func(id, brief, why string) string {
		return fmt.Sprintf("视频任务失败：%s「%s」：%s。把原因简要告诉用户；如需重试可再发起一次 video_gen。", id, brief, why)
	},
	recoveredNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("视频任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 个。调用 video_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户看到视频。", id, brief, count, id)
	},
}

// RegisterVideoGenChain registers the video_gen tool against a provider
// chain. Video is minutes-long, queue-native work, so the tool is
// background-only: every call submits a task and returns an id at once
// (the twin of image_gen's background=true, minus the sync mode that
// doesn't exist for video). Reference images (first frame) ride along
// through the same workspace-path mechanism as image_gen.
func RegisterVideoGenChain(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify func(MediaTaskNotice), st store.Store) {
	if chain == nil {
		return
	}
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
	tasks := newMediaTaskManager(videoTaskCategory, chain, files)
	if notify != nil {
		tasks.notify = notify
	}
	tasks.recoverAgentID = r.AgentID()
	tasks.st = st
	r.RegisterMediaTasks(tasks)
	registerMediaTaskTools(r, tasks)
	go tasks.recoverRemote()

	desc := []string{
		"Generate a short video clip from a text description with a video model. Videos take minutes to make, so every call runs as a background task: you get a task id at once, a notice arrives in this chat when the clip is ready, and video_task_status delivers it.",
		"Write \"prompt\" as a rich description: subject, action, camera movement, style, mood.",
		"For image-to-video (animating a picture the person sent), list it in \"images\" by its /workspace path — it becomes the first frame.",
		"After submitting, end your turn and tell the person the video is on its way; when the completion notice arrives, call video_task_status and put putInReply in your reply.",
	}
	props := map[string]any{
		"prompt":     map[string]any{"type": "string", "description": "What the video shows, in detail."},
		"seconds":    map[string]any{"type": "string", "description": `Length in seconds, e.g. "4" or "8". Omit to let the model pick.`},
		"size":       map[string]any{"type": "string", "description": `Frame shape: an explicit ratio like "16:9"/"9:16"/"1:1" or pixels like "1280x720". Omit for the model default.`},
		"resolution": map[string]any{"type": "string", "description": `Fidelity tier like "720p"/"1080p" — only when the provider supports it; omit otherwise.`},
	}
	if files != nil && files.Load != nil {
		props["images"] = filesSchema(`First-frame image from this chat, by workspace path, e.g. ["/workspace/image_3jk7l_0.jpg"] (at most 1).`)
	}
	r.Register("video_gen", strings.Join(desc, "\n"), map[string]any{
		"type":       "object",
		"properties": props,
		"required":   []string{"prompt"},
	}, func(ctx context.Context, rawArgs json.RawMessage) (string, error) {
		var args map[string]any
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("parse args: %w", err)
		}
		turn := r.forContext(ctx)
		if err := attachRefs(ctx, files, turn, args); err != nil {
			return "", err
		}
		return tasks.submit(ctx, turn, args)
	})
}

// musicTaskCategory is the music_gen flavor of the generic media task
// manager. The tool surface ships now; generation starts failing with a
// clear "upstream not configured" message until a music upstream lands
// on the queue side (imgqueue's music executor placeholder).
var musicTaskCategory = mediaCategory{
	name:          "music",
	toolName:      "music_gen",
	statusTool:    "music_task_status",
	idPrefix:      "mustask",
	leaseChannel:  "music_task",
	callbackParam: "music_task_callback",
	eventPrefix:   "music_task",
	labelCN:       "音乐",
	unitCN:        "段",
	promptArg:     "prompt",
	resultKey:     "files",
	limit:         40 * time.Minute,
	doneNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("音乐任务已完成：%s「%s」生成 %d 段。调用 music_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户听到音乐。若你已在之前的回复里把该音乐发给用户，简短确认即可，不要重复发送。", id, brief, count, id)
	},
	failNotice: func(id, brief, why string) string {
		return fmt.Sprintf("音乐任务失败：%s「%s」：%s。把原因简要告诉用户；如需重试可再发起一次 music_gen。", id, brief, why)
	},
	recoveredNotice: func(id, brief string, count int) string {
		return fmt.Sprintf("音乐任务已完成（fastclaw 重启前提交）：%s「%s」生成 %d 段。调用 music_task_status 查询 task %q，把返回 JSON 里的 putInReply 原样放进你的回复，让用户听到音乐。", id, brief, count, id)
	},
}

// RegisterMusicGenChain registers the music_gen tool against a provider
// chain — background-only like video_gen.
func RegisterMusicGenChain(r *Registry, chain *toolproviders.Chain, files *ImageFiles, notify func(MediaTaskNotice), st store.Store) {
	if chain == nil {
		return
	}
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
	tasks := newMediaTaskManager(musicTaskCategory, chain, files)
	if notify != nil {
		tasks.notify = notify
	}
	tasks.recoverAgentID = r.AgentID()
	tasks.st = st
	r.RegisterMediaTasks(tasks)
	registerMediaTaskTools(r, tasks)
	go tasks.recoverRemote()

	desc := []string{
		"Generate music from a text description. Music takes minutes, so every call runs as a background task: you get a task id at once, a notice arrives when the track is ready, and music_task_status delivers it.",
		"Write \"prompt\" describing genre, instruments, tempo and mood, e.g. \"upbeat lofi hip hop with a warm Rhodes piano, 85 bpm\".",
		"After submitting, end your turn and tell the person the music is on its way; when the completion notice arrives, call music_task_status and put putInReply in your reply.",
	}
	props := map[string]any{
		"prompt":       map[string]any{"type": "string", "description": "What the music sounds like: genre, instruments, tempo, mood."},
		"style":        map[string]any{"type": "string", "description": `Style tag, e.g. "lofi", "cinematic", "pop" (provider-specific; optional).`},
		"duration":     map[string]any{"type": "string", "description": `Length in seconds, e.g. "60". Omit for the model default.`},
		"instrumental": map[string]any{"type": "boolean", "description": "True for a track without vocals."},
		"title":        map[string]any{"type": "string", "description": "Optional track title."},
	}
	r.Register("music_gen", strings.Join(desc, "\n"), map[string]any{
		"type":       "object",
		"properties": props,
		"required":   []string{"prompt"},
	}, func(ctx context.Context, rawArgs json.RawMessage) (string, error) {
		var args map[string]any
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return "", fmt.Errorf("parse args: %w", err)
		}
		turn := r.forContext(ctx)
		return tasks.submit(ctx, turn, args)
	})
}
