# Workspace Version History（工作区版本历史）

工作区快照子系统：每个回合边界对会话工作区做一次**整树快照**，供三处消费——
面板里的版本历史列表/回滚（`GET/POST /api/agents/{id}/sessions/{sid}/history[/restore]`）、
回答重新生成（变体）的 fork 工作区回放（`forkWorkspaceFiles`）、以及未来的按快照恢复能力。

状态：**已实现，双后端**。本文描述 2026-11 的云模式改造（内容寻址快照）之后的架构。

## 快照语义（两个后端一致）

- **回合边界整树快照**，不在每次文件写入时打点：沙箱 exec 经 bind mount
  的写入对本进程不可见，回合边界是唯一能同时覆盖「文件工具 + exec + 上传」
  的一致点。
- **净树跳过**：与最近一份快照完全一致则不产生新快照（git 靠 `status
  --porcelain`，云后端靠清单 diff）。
- **fork 坐标**：提交信息固定为 `turn msgs=<n>`（n=会话可见消息数），
  fork 取「msgs ≤ keep 的最新一份」即为 fork 点的文件状态。
- **恢复 = 覆盖式（overlay）**：目标里多出来的文件不动（等价于
  `git checkout <hash> -- .`），不删除。
- **尽力而为**：快照失败只记日志，绝不失败一个回合。

## 两种后端

选择发生在 gateway 启动时（`internal/gateway/history_backend.go`），按对象
存储类型决定，一次性注入所有 agent manager 与 setup server：

| 对象存储 | 后端 | 清单位置 | blob 位置 |
|---|---|---|---|
| local（默认） | `workspace.History`（git 裸仓库） | git 提交 | git 对象库 |
| S3 兼容（MinIO/R2/S3/OSS/B2） | `workspace.CloudHistory` | SQL 库（D2，默认，sqlite/postgres 均可）或对象存储（D1，`workspaceHistory.manifestStore="object"`） | **永远在对象存储** |

- **git 后端**：`~/.fastclaw/workspace-history/<scope>.git` 裸仓库。裸仓库
  刻意放在工作区之外——工作区里出现 `.git` 会被 agent 自己看到/破坏。行为
  自功能引入以来零变化。
- **云后端**：为多 pod 而生。任何 pod 都能对任意 scope 提交/列出/恢复，
  因为唯一真相源是共享的对象存储（+ SQL 库）。

### 云后端的数据布局

blob（内容寻址，payload 永不进 SQL 库——与 store 层"二进制不进库"铁律一致）：

```
<prefix>/history/blobs/<sha256-hex>
```

清单（每回合一个 JSON：path → {hash, size, modtime}）：

```
D1: <prefix>/history/manifests/<agent>/<scope>/snap-<纳秒20位>-<4hex>.json
D2: 表 workspace_history_snapshots（agent_id, project_id, session_key,
    snap_id 唯一, msgs, message, manifest TEXT, created_at）
```

快照 id 内嵌 **20 位零填充纳秒时间戳**，因此字典序 = 时间序——对象存储
List 无需服务端排序；同纳秒（同一时钟刻度）的 id 无相对顺序，无碍：fork
匹配按 msgs 而非 id 序。

### 提交流程（CloudHistory.Commit）

1. 列举存储端当前树（`StoreSource` = 文件工具同款通道）；
2. 与最近清单按 path 比对 (size, modtime)——未变文件直接复用哈希**不重读**，
   变了的才 Get 流式过 sha256、spool 到临时文件再按哈希上传（单遍读，
   不占内存；blob 键即哈希，必须先算后传）；
3. 整树映射与最近清单相同 → 跳过（净树）；
4. 写清单。两 pod 并发提交同一 scope 天然安全：blob 幂等、清单只追加。

## 云模式的已知限制

- **Docker+S3 的 exec 产物滞后**：Docker 沙箱 /workspace 是 pod 本地 bind
  mount，exec 产物要到沙箱**空闲驱逐**才 flush 进对象存储（默认 10 分钟，
  远程后端 E2B/BoxLite 则是每次 exec 后同步）。快照=存储端状态，因此可能
  短暂落后沙箱实况。这是既有持久化缺口，非快照子系统引入。
- **无保留策略/GC**：每个脏回合加一份清单；blob 随历史增长（内容寻址使
  未来按引用计数回收很容易）。与 git 后端同款限制。
- **同 size 同 mtime 的重写**会被快路径当作未变（概率极低，下一回合补上）。
  mtime 未知（=0）的文件永不走快路径，始终重读。
- **恢复不删除多余文件**（overlay 语义）：想清空目标请先清工作区再恢复。

## 配置

```jsonc
// configs 表 workspaceHistory 命名空间（系统级），per-agent 可用 *bool 覆盖 enabled
{
  "enabled": true,
  "manifestStore": "auto"   // auto|sql → SQL 库(D2)；object → 对象存储(D1)
}
```

本地单实例不需要任何新配置——`local` 对象存储自动走 git 后端。

## 多 pod 验证清单

`deploy/multi-pod/docker-compose.yaml`（pod-a :18953 / pod-b :18954，共享
Postgres + MinIO）：

1. pod A 开 workspaceHistory，对 agent 发一条带 write_file 的消息；
2. pod B `GET /api/agents/{id}/sessions/{sid}/history` → 应见快照（跨 pod 可见）；
3. pod B 对旧快照 `POST …/history/restore` → pod A 读文件应回到旧值；
4. pod B 对中途消息 fork → 分支会话工作区文件 = fork 点状态；
5. psql 见 `workspace_history_snapshots` 行、MinIO 控制台见 `history/blobs/*`；
6. 把 manifestStore 切 `object`（D1）重复 2-3。
