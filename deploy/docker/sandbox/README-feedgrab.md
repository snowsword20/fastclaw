# feedgrab-enabled sandbox image

A custom fastclaw sandbox image that bundles [feedgrab](https://github.com/iBigQiang/feedgrab)
so an agent can fetch/digest content from any URL via the `feedgrab` CLI inside
the exec sandbox — no per-call pip/npm round-trips.

This builds **on top of** the official `thinkany/fastclaw-sandbox:latest`
(Python + Node + Camoufox) and adds:

- `feedgrab[all]` Python package (all platform extras: telegram, browser,
  stealth, twitter, wechat, xhs, feishu)
- Playwright + Patchright Chromium binaries (~300MB, pre-installed)
- `build-essential` (for C-extension deps like `curl_cffi`)

## Files

| File | Purpose |
|------|---------|
| `Dockerfile.feedgrab` | The image recipe. Layers deps separately from source for fast rebuilds. |
| `build-feedgrab.sh` | Build helper. Auto-locates the build context (parent of both repos) and wires proxy args. |
| `seed-feedgrab-data.sh` | One-time: copies your `.env` + `sessions/` into a session's host workspace dir. |

---

## Architecture (why this layout)

```
┌──────────────────────────────────────────────────────────┐
│  Image: myrepo/feedgrab-sandbox:latest                   │
│                                                          │
│  Layer 0: thinkany/fastclaw-sandbox:latest   (cached)    │
│           Python 3 · Node 22 · Camoufox · fonts · proxy  │
│  Layer B: build-essential                     (cached)   │
│  Layer C: playwright/patchright chromium      (cached)   │
│  Layer D: feedgrab[all] deps  (reruns on dep bump)       │
│  Layer E: feedgrab source      (reruns on code edit ~10s)│
└──────────────────────────────────────────────────────────┘
                      ▲ docker create (per session)
┌─────────────────────┴───────────────────────────────────┐
│  Container runtime — /workspace is a host RW bind-mount  │
│                                                          │
│  /workspace/.feedgrab/                                   │
│  ├── .env              ← credentials (NOT in image)      │
│  └── sessions/         ← Playwright/cookie login state   │
│                                                          │
│  Agent exec call:                                        │
│    feedgrab <url>                                        │
│    (FEEDGRAB_DATA_DIR is baked into the image env)       │
└──────────────────────────────────────────────────────────┘
```

**Key design choices:**

- **Code in image, secrets on host.** `.env` and `sessions/` are never baked in
  — they'd leak on `docker push` and force a rebuild on every credential
  rotation. They live in `/workspace/.feedgrab/`, which is the only RW
  bind-mount in the fastclaw sandbox and syncs back to the durable host store.
- **Layered Dockerfile.** Editing a `.py` file only reruns the thin source
  layer (~10-30s). Only a dependency bump (`pyproject.toml`) reruns the heavy
  deps layer. Browser binaries install once and stay cached.
- **Build context = parent of both repos.** The official `build.sh` assumes
  context == sandbox dir, but we need feedgrab's source visible to COPY.
  `build-feedgrab.sh` auto-locates the common parent so you don't think about it.

---

## Quick start

### 1. Build the image

```bash
cd E:/project/github/fastclaw/deploy/docker/sandbox

# Local build (default tag: latest, default name: myrepo/feedgrab-sandbox)
./build-feedgrab.sh

# With a proxy for GFW networks (playwright/pip need it to download)
./build-feedgrab.sh --proxy http://host.docker.internal:7890

# Custom name / tag
./build-feedgrab.sh -i ghcr.io/you/feedgrab-sandbox -t v1

# Build + push
./build-feedgrab.sh --push
```

`build-feedgrab.sh` defaults `FEEDGRAB_DIR` to `../../../../../feedgrab`
(sibling of fastclaw). If your feedgrab checkout is elsewhere:

```bash
./build-feedgrab.sh --feedgrab-dir /path/to/feedgrab
# or
FEEDGRAB_DIR=/path/to/feedgrab ./build-feedgrab.sh
```

### 2. Point fastclaw at the image

In the dashboard: **Settings → Runtime → Sandbox**

| Field | Value |
|-------|-------|
| Sandbox enabled | on |
| Backend | docker |
| Image | `myrepo/feedgrab-sandbox:latest` (or whatever you tagged) |

Or via CLI:

```bash
fastclaw agents config <agent> set sandbox.enabled true
fastclaw agents config <agent> set sandbox.backend docker
fastclaw agents config <agent> set sandbox.image myrepo/feedgrab-sandbox:latest
```

### 3. Seed feedgrab data (once per session)

The image has no credentials. Drop yours into the session's host workspace:

```bash
./seed-feedgrab-data.sh <session-workspace-dir>
# e.g.
./seed-feedgrab-data.sh ~/.fastclaw/workspaces/agt_xxx/sess_yyy
```

This copies your feedgrab checkout's `.env` and `sessions/` into
`<session-workspace-dir>/.feedgrab/`, which the container sees at
`/workspace/.feedgrab/`.

> **Finding the session workspace dir.** It's under `$FASTCLAW_HOME` on the
> host. The dashboard's Sessions panel and `fastclaw agents files ls` can help
> you locate it. For a quick test you can also `docker exec` into a running
> sandbox and call feedgrab directly.

### 4. Use it

From the agent chat, the agent invokes the `exec` tool:

```bash
# Public content — works immediately
feedgrab https://example.com/feed.xml

# Login-gated platforms — work once .env + sessions/ are seeded
feedgrab https://t.me/somechannel
feedgrab https://twitter.com/user/status/123
```

Or, for manual testing, `docker exec` into the sandbox:

```bash
docker exec -it <sandbox-container> feedgrab <url>
```

---

## Updating feedgrab

Two paths, depending on what changed.

### Code-only edit (most common) — rebuild in seconds

Layer E (source) reruns; everything above stays cached:

```bash
# edit feedgrab/feedgrab/*.py ...
cd E:/project/github/fastclaw/deploy/docker/sandbox
./build-feedgrab.sh           # ~10-30s
# restart agent / sandbox pool to pick up the new image
```

### Dependency bump — rebuild in minutes

When `pyproject.toml` changes, Layer D reruns (~2-5min):

```bash
./build-feedgrab.sh --proxy http://host.docker.internal:7890
```

### Hot-patch without rebuilding (debugging only)

Drop the new source into the session workspace and override `PYTHONPATH`:

```bash
# inside the container, or via the exec tool
cp -r /new/feedgrab/feedgrab /workspace/feedgrab
export PYTHONPATH=/workspace:$PYTHONPATH
feedgrab <url>     # picks up /workspace/feedgrab over the image's copy
```

This is for iterating. Bake the final version into the image so it's reliable
across fresh containers.

### Credentials / login rotation — never rebuild

`.env` and `sessions/` live on the host, not in the image. Just update them:

```bash
./seed-feedgrab-data.sh <session-workspace-dir>   # overwrites with current .env/sessions
```

Or, for an expired login on one platform, re-login inside the sandbox:

```bash
docker exec -it <sandbox-container> feedgrab login <platform>
# writes to /workspace/.feedgrab/sessions/ — persists on the host
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `playwright install` hangs during build | GFW. Pass `--proxy http://host.docker.internal:7890` to `build-feedgrab.sh`. |
| `pip install` fails on `curl_cffi`/`XClientTransaction` | C-extension compile. Layer B installs `build-essential`; if you removed it, add it back. |
| `feedgrab` works on RSS but fails on Twitter/XHS | Login state missing. Run `seed-feedgrab-data.sh`, or `feedgrab login <platform>` in the sandbox. |
| `FEEDGRAB_DATA_DIR` points nowhere | The image bakes in `/workspace/.feedgrab/sessions`. Seed it via `seed-feedgrab-data.sh`; the dir must exist on the host workspace store. |
| Camoufox works but Playwright Chromium doesn't | They're separate runtimes. Both are installed in Layer C. If you skipped it, rebuild without removing that layer. |
| Image too large | Expected ~2GB (base Camoufox ~1GB + two Chromiums ~300MB + feedgrab deps). To slim: drop `[all]` and install only the extras you use (edit Layer D). |

---

## Layout recap

```
fastclaw/deploy/docker/sandbox/
├── Dockerfile              # official (untouched)
├── Dockerfile.feedgrab     # ← this feature
├── build-feedgrab.sh       # ← build helper
├── seed-feedgrab-data.sh   # ← data seeding helper
├── build.sh                # official (untouched)
├── e2b.Dockerfile          # official (untouched)
├── e2b.toml                # official (untouched)
└── README-feedgrab.md      # ← this doc
```

Official files are untouched, so upstream updates merge cleanly. Only the three
`.feedgrab` files are yours.
