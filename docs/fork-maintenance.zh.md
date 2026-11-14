# Fork 维护手册：双副本对齐 + 上游同步 + 部署

> 写给本 fork 的维护者。上游 = `fastclaw-ai/fastclaw`；本 fork 相对上游的
> 增量清单见 README「本 Fork 相对上游的增量」一节（随时
> `git log origin/dev..dev --oneline` 复核）。

## 双副本架构

| 副本 | 路径 | 角色 |
|---|---|---|
| Windows | `E:\aaprojects\githubrepo\fastclaw` | 开发 / 评审副本 |
| WSL Ubuntu | `~/githubpro/fastclaw` | 部署副本：`bin/fastclaw gateway` + Postgres，systemd 用户服务 `fastclaw.service`（监听 18953） |

两个副本都是完整仓库，远程配置一致：

- `origin` = fastclaw-ai/fastclaw（**上游，只读，永远不 push**）
- `mine` = snowsword20/fastclaw（fork，**唯一推送目标**，一律 `git push mine dev`）

分支语义：

- `origin/dev` = 上游镜像（fetch 自动更新，不养本地镜像分支）
- `dev` = 集成分支 = 上游 + 自有改动；**服务二进制只从 dev 构建**
- `feat/*` = 短命功能分支：并行 session、大功能各自开分支，完工合回 dev

## 对齐纪律（两份副本不跑散的铁律）

1. **任何副本 push 之后，另一副本立刻 pull**：`git pull mine dev`。这是收尾
   动作的一部分，不做等于给下次分叉埋雷。
2. **动手改代码前先对齐**：`git status` 确认干净 + `git pull mine dev` 确认最新。
3. **并行工作必须走各自 `feat/*` 分支**，谁完工谁合并。历史上两次 dev 分叉
   事故（2026-11-13 最严重的一次）都是两个 session 同时在 dev 上各自提交造成的。
4. **不跑没提交的代码**：重建二进制上线之前，改动必须已提交并推送——
   保证"线上跑的 = git 里某个 dev 提交"这条等式永远成立。

## 跟上游

```bash
git fetch origin
git merge origin/dev      # 建议每周一次，或看到上游有好东西就合
go test ./internal/...    # 全绿才算合并完成
git push mine dev
```

- 两边副本均已开启 `git rerere.enabled true`：重复冲突自动复用上次的解法
  （自有改动集中在上游也高频修改的文件里，同一处冲突会反复出现）。
- **冲突高发区**（自有补丁所在，合并前重点盯）：
  `internal/agent/{loop.go, variants.go, prompt_modules.go, context.go, tools/exec_guard.go}`、
  `internal/provider/{anthropic.go, provider.go}`、
  `internal/session/{manager.go, store_adapter.go}`、
  `internal/store/database.go`（session_messages seq 1 起始）、
  `internal/setup/{handlers.go, server.go}`、`Makefile`（bundled skills 同步行）。
- 能上游化的通用修复（如 exec 安全闸）优先提 PR 回上游——提回一个，
  私有补丁集就永久少一个。

## 部署（WSL 副本）

```bash
cd ~/githubpro/fastclaw
git pull mine dev
make bundle-skills bundle-docs                 # 同步嵌入的 skills / apidocs
go build -o bin/fastclaw ./cmd/fastclaw        # web 嵌入产物没改就跳过 build-web
git tag run/$(date +%m-%d) && git push mine run/$(date +%m-%d)   # 回滚锚点
systemctl --user restart fastclaw
systemctl --user is-active fastclaw            # 验证：active
ss -tlnp | grep fastclaw                       # 验证：监听 18953
```

回滚（一条路走到黑之前先试试它）：

```bash
git checkout run/MM-DD && go build -o bin/fastclaw ./cmd/fastclaw \
  && systemctl --user restart fastclaw
```

## 历史备注

2026-11-13 两副本 dev 分叉（Windows 独有 prompt-cache 精修 + exec_guard，
WSL 独有 math-practice + thinking 流），次日人工合并（0f5ab9d → a48142f →
4f19d31），本文档的对齐纪律即由此而来。
