# 生图 Provider 接入指南（内置 openai / fal / replicate / magpie 与自定义接入）

fastclaw 的 `image_gen` 工具由 provider 链驱动（`tools.categories.image_gen` 的 primary + fallbacks）。本文说明每种接入方式、能力矩阵怎么声明、异步（后台任务）行为，以及没有现成 provider 时怎么接你自己的服务。

## 基础概念

- **配置位置**：管理后台「工具设置」或 `tools.providers`（每 provider 一份 `{apiKey, endpoint, options}`）+ `tools.categories`（链：`"<provider>/<model>"`）。数据库部署存 configs 表（kind=setting，name=tools.providers / tools.categories）。
- **能力矩阵**：模型在调用前会看到每个 provider 声明支持什么（比例/尺寸/质量档/每调用张数/垫图张数），避免拿不支持的参数去撞墙。声明方式各 provider 不同（下表）。
- **异步任务**：`image_gen` 带 `background=true`（或调用方 params `{"image_gen_mode":"background"}`）时，任务在后台跑，完成后 fastclaw 把唤醒消息注入原会话、API 调用方可用 `GET /v1/image-tasks/{id}` 轮询、或用 params `image_task_callback`（+ 可选 `image_task_callback_secret` 做 HMAC-SHA256 签名头 `X-Fastclaw-Signature`）收回调。

## 三种接入方式

### 1. OpenAI 兼容服务（零代码，绝大多数场景）

硅基流动、各类中转、自建 OpenAI-shape 网关都属于这类。

```
tools.providers.openai = {apiKey, endpoint: "https://你的服务/v1/images/generations",
                          options: {"caps": "aspectRatios=1:1,3:2,2:3; quality=low,high; maxImages=4; maxRefImages=2"}}
tools.categories.image_gen = {primary: "openai/你的模型名"}
```

- endpoint 留空即官方 API；`model` 来自链 ref 的后缀，原样透传
- 不配 `caps` 时按模型名家族推断（gpt-image 家族 / dall-e-3）；自定义模型名**务必配 caps**，否则矩阵是 gpt-image 的默认值，可能声明了你服务不支持的能力
- 垫图（参考图编辑）走 `/images/edits`（endpoint 里的 `/images/generations` 字面替换），仅 gpt-image 前缀模型启用
- 异步：同步直连（fastclaw 内部并发），无远端队列——`background` 模式由 fastclaw 协程承载，重启后不可恢复

### 2. 自定义网关（magpie 协议 = 自定义协议，推荐给自建排队服务）

magpie provider 不绑定 magpie 本体——它说的是一个**通用 API 面**，你自己的服务实现这几个端点即可被 fastclaw 当作完整 provider 使用（能力矩阵、排队、重试、恢复全支持）：

```
GET  /v1/capabilities        → {models:[{ref,label,canGenerate,canEdit,declared,caps:{aspectRatios,sizes,quality,maxImages,maxRefImages}}]}
POST /v1/images/generations  → OpenAI images 形状 {data:[{url|b64_json}]}（同步面）
POST /v1/images/edits        → multipart 同步面（可选）
POST /v1/tasks               → 提交异步任务 → 202 {task:{id}}；body 的 meta 字段为自由 JSON（fastclaw 会写入路由身份，原样保存即可）
GET  /v1/tasks/{id}          → {task:{id,status,result:{mime}}};  status ∈ queued|running|done|failed|cancelled
GET  /v1/tasks/{id}/image    → 图片字节（Bearer 鉴权）
DELETE /v1/tasks/{id}        → 取消（fastclaw 在调用方取消时调用，可选）
GET  /v1/health              → {ok:true}（探测任务 API 是否可用，可选）
```

- 接入：`tools.providers.magpie = {apiKey, endpoint: "http://你的服务:端口"}`，`primary: "magpie/auto"` 或 `magpie/<模型ref>`
- 能力矩阵：网关的 `/v1/capabilities` 返回什么就声明什么（`declared:true` 的条目参与严格路由；全 undeclared 时 fail-open）
- 异步：原生支持。任务 meta 里 fastclaw 会写 `origin/agentID/chatID` 等身份标签用于**重启认领**（fastclaw 重启后扫描队列认领自己的在途任务并投递）——meta 是自由字段，你的服务不理解它也没关系
- 多副本部署：认领由 `channel_leases` 表的租约锁门（per-agent，30s TTL/10s 续租），单实例恢复
- 参考实现：magpie-imgqueue 插件（`~/.magpie-imgqueue`，队列/重试/面板/webhook 全带）

### 3. 内部 fork 写 Go provider（自家私有协议）

实现 `toolproviders.Provider` 三件套并注册：

```go
// internal/toolproviders/imagegen/your.go
type Your struct{}
func (Your) Category() string { return Category }
func (Your) Name() string     { return "your" }
func (Your) Execute(ctx context.Context, req toolproviders.Request) (toolproviders.Response, error) { ... }

// RegisterAll 里 r.Register(&Your{})；admin UI 会自动出现该条目（NeedsKey/NeedsURL 全开）
```

- 慢服务支持异步：`toolproviders.WithAsyncImage(ctx, meta)` 标记后台调用；完成后把远端任务 ID 放进 `Response.TaskRefs`（工具层会记录并用于 status/恢复）
- 能力矩阵：实现 `Capabilities(cfg, model) toolproviders.Capabilities`（`Declared:true` 参与严格路由）
- 错误分级：可重试（网络/429/5xx/无结果）返回 `toolproviders.Retry(err)` 让链降级到下一个 provider；参数类错误直接返回

## 对照表

| 接法 | 代码量 | 能力矩阵 | 异步任务 | 重启恢复 |
|---|---|---|---|---|
| openai 兼容填 endpoint | 零 | options.caps 或家族推断 | fastclaw 协程承载 | 无远端队列，不可恢复 |
| magpie 协议自建网关 | 自建服务侧 | 网关 capabilities 返回 | 原生（/v1/tasks） | 有（meta 身份标签） |
| 写 Go provider | Go 开发 | 代码实现 | 自己实现（约定 TaskRefs） | 自己实现 |

## 相关端点与参数

- `GET /v1/image-tasks/{id}`：任务直查（apikey 鉴权；done 时 images 为 7 天 share 下载链接，`imagesAreDownloadLinks:false` 表示是 workspace 路径需走 session-files API）。注意 id 进程内存活——fastclaw 重启后旧号 404，队列任务会在新号下被认领
- params：`image_gen_mode`（background/sync）、`image_task_callback`、`image_task_callback_secret`
- 完成通知：会话内注入（同 cron 机制）；`GET /v1/tasks/{id}` 的 imgqueue 面板对照字段为 `queueTask`
