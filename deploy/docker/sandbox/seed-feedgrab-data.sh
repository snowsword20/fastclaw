#!/usr/bin/env bash
# Seed feedgrab runtime data (.env + sessions/) into a fastclaw session workspace.
#
# Why this exists: feedgrab's code is baked into the sandbox image, but its
# CREDENTIALS and LOGIN STATE must NOT be — they're secrets that change often
# and would leak if pushed to a registry. The fastclaw docker sandbox mounts
# the session's host workspace dir at /workspace (RW), so we drop the data
# there and feedgrab reads it via FEEDGRAB_DATA_DIR=/workspace/.feedgrab/sessions.
#
# This data lives on the HOST and persists across container restarts. You only
# run this once per fastclaw session (or when credentials rotate).
#
# Usage:
#   seed-feedgrab-data.sh <session-workspace-dir> [--source <feedgrab-dir>]
#
#   <session-workspace-dir>  the host dir that fastclaw bind-mounts at /workspace
#                            for the target session. Find it via the dashboard:
#                            Agents → <agent> → Sessions → locate the session,
#                            or check $FASTCLAW_HOME on the host.
#   --source <feedgrab-dir>  where your .env and sessions/ live. Defaults to the
#                            feedgrab checkout alongside this fastclaw repo.
#
# Example:
#   seed-feedgrab-data.sh ~/.fastclaw/workspaces/agt_xxx/sess_yyy
#   seed-feedgrab-data.sh ~/.fastclaw/workspaces/agt_xxx/sess_yyy \
#       --source E:/project/github/feedgrab

set -euo pipefail

if [[ $# -lt 1 ]]; then
  sed -n '2,28p' "$0"
  exit 2
fi

DEST="$1"
shift || true
SOURCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,28p' "$0"; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# sandbox(1)→docker(2)→deploy(3)→fastclaw(4)→github parent. Matches
# build-feedgrab.sh's REPO_ROOT so the default SOURCE resolves the same way
# (feedgrab as a sibling of fastclaw). The previous ../../.. landed inside
# fastclaw/ itself and looked for fastclaw/feedgrab, which never exists.
REPO_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)
SOURCE=${SOURCE:-"$REPO_ROOT/feedgrab"}

# Resolve Windows-style paths (E:/...) to something bash cp understands on Git Bash.
# `cygpath -u` handles this; fall back to the raw value if cygpath isn't around.
to_posix() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -u "$1"
  else
    echo "$1"
  fi
}

DEST=$(to_posix "$DEST")
SOURCE=$(to_posix "$SOURCE")

if [[ ! -d "$DEST" ]]; then
  echo "ERROR: session workspace dir does not exist: $DEST" >&2
  echo "       Create it first, or double-check the path from the dashboard." >&2
  exit 1
fi
if [[ ! -d "$SOURCE" ]]; then
  echo "ERROR: feedgrab source dir not found: $SOURCE" >&2
  echo "       Pass --source /path/to/feedgrab" >&2
  exit 1
fi

TARGET="$DEST/.feedgrab"

# The session workspace is bind-mounted RW into the sandbox at /workspace,
# and under FASTCLAW_SANDBOX_ENFORCE (or any sandbox-first config) the
# container runs as root, so files it created — including a pre-existing
# .feedgrab/ from an earlier feedgrab run — are owned by root. A non-root
# operator can't mkdir/cp inside it. Detect that and fall back to copying
# THROUGH a running fastclaw sandbox container (which sees the same /workspace
# as root), so seeding works without passwordless sudo.
VIA_CONTAINER=0
if ! mkdir -p "$TARGET/sessions" 2>/dev/null; then
  # Permission denied — likely root-owned .feedgrab/ from a sandbox run.
  # Find a running fastclaw sandbox whose /workspace maps to this $DEST.
  CID=$(docker ps --filter label=fastclaw=sandbox --format '{{.ID}}' 2>/dev/null | while read -r id; do
    src=$(docker inspect "$id" --format '{{range .Mounts}}{{if eq .Destination "/workspace"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)
    if [[ "$src" == "$DEST" ]]; then echo "$id"; break; fi
  done | head -1)
  if [[ -z "$CID" ]]; then
    echo "ERROR: cannot write to $TARGET (permission denied) and no running" >&2
    echo "       fastclaw sandbox maps to $DEST." >&2
    echo "       Either run this script with sudo, or trigger a sandbox for" >&2
    echo "       this session first (send the agent any message) and re-run." >&2
    exit 1
  fi
  echo "ℹ $TARGET is root-owned; seeding through sandbox container $CID (as root)."
  VIA_CONTAINER=1
  docker exec "$CID" mkdir -p /workspace/.feedgrab/sessions 2>/dev/null
fi

copied=0

# .env — credentials (cookies/tokens for all platforms). REQUIRED for login-only
# platforms (Twitter, XHS, WeChat, zsxq, ...). Optional for public RSS/web.
if [[ -f "$SOURCE/.env" ]]; then
  if [[ $VIA_CONTAINER -eq 1 ]]; then
    docker cp "$SOURCE/.env" "$CID:/workspace/.feedgrab/.env" 2>/dev/null
  else
    cp "$SOURCE/.env" "$TARGET/.env"
  fi
  echo "✓ copied .env → $TARGET/.env"
  copied=1
else
  echo "⚠ no .env found at $SOURCE/.env — public-only content will work,"
  echo "  but login-only platforms (Twitter/XHS/WeChat/zsxq) won't."
fi

# sessions/ — Playwright/Telethon/cookie login state. Optional but recommended:
# without it the agent has to re-login on first use (interactive, painful in a
# sandbox). Copy existing login state if present.
if [[ -d "$SOURCE/sessions" ]] && [[ -n $(ls -A "$SOURCE/sessions" 2>/dev/null) ]]; then
  if [[ $VIA_CONTAINER -eq 1 ]]; then
    docker cp "$SOURCE/sessions/." "$CID:/workspace/.feedgrab/sessions/" 2>/dev/null
  else
    cp -r "$SOURCE/sessions/." "$TARGET/sessions/"
  fi
  echo "✓ copied sessions/ → $TARGET/sessions/"
  copied=1
else
  echo "ℹ no sessions/ at $SOURCE/sessions — first login-gated call will need"
  echo "  an interactive \`feedgrab login <platform>\` inside the sandbox."
fi

if [[ $copied -eq 0 ]]; then
  echo
  echo "Nothing was copied. Point --source at a feedgrab checkout that has"
  echo "either .env or sessions/ populated." >&2
  exit 1
fi

echo
echo "==> done. Inside the sandbox container, feedgrab now sees:"
echo "    /workspace/.feedgrab/.env         (credentials)"
echo "    /workspace/.feedgrab/sessions/    (login state)"
echo
echo 'Test from an exec tool call or `docker exec`:'
echo "    feedgrab https://example.com/feed.xml"
