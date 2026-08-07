#!/usr/bin/env bash
# Build the feedgrab-enabled fastclaw sandbox image.
#
# Why a dedicated script: the Dockerfile.feedgrab needs a build context that
# contains BOTH fastclaw/ and feedgrab/ as siblings, so it can COPY feedgrab
# source in. The official build.sh assumes context == the sandbox dir; this
# script auto-locates the common parent and wires the proxy args for you.
#
# Usage:
#   build-feedgrab.sh                                  # local build, tag latest
#   build-feedgrab.sh -t v1                            # custom tag
#   build-feedgrab.sh -i myrepo/feedgrab-sandbox       # custom image name
#   build-feedgrab.sh --proxy http://host.docker.internal:7890   # with proxy
#   build-feedgrab.sh --push                           # build + push
#   build-feedgrab.sh --platform linux/amd64,linux/arm64 --push   # multi-arch
#
# After building, point the gateway at it via Settings → Runtime → Sandbox →
# Image (or set during onboard). Default: myrepo/feedgrab-sandbox:latest.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# fastclaw/deploy/docker/sandbox/  →  ... →  fastclaw/  →  E:/project/github/
# sandbox(1)→docker(2)→deploy(3)→fastclaw(4)→github parent. The build context
# is that github parent (contains both fastclaw/ and feedgrab/ as siblings).
REPO_ROOT=$(cd "$SCRIPT_DIR/../../../.." && pwd)
# feedgrab is a sibling of fastclaw by default. Override with FEEDGRAB_DIR.
FEEDGRAB_DIR=${FEEDGRAB_DIR:-"$REPO_ROOT/feedgrab"}
CONTEXT_DIR=$(cd "$FEEDGRAB_DIR/.." && pwd)

IMAGE_NAME=${IMAGE_NAME:-myrepo/feedgrab-sandbox}
TAG=latest
PUSH=0
PLATFORM=""
PROXY=""

usage() {
  sed -n '2,20p' "$0"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--tag)
      TAG="$2"; shift 2 ;;
    -i|--image)
      IMAGE_NAME="$2"; shift 2 ;;
    --proxy)
      PROXY="$2"; shift 2 ;;
    --push)
      PUSH=1; shift ;;
    --platform)
      PLATFORM="$2"; shift 2 ;;
    --feedgrab-dir)
      FEEDGRAB_DIR="$2"
      CONTEXT_DIR=$(cd "$FEEDGRAB_DIR/.." && pwd)
      shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Sanity: feedgrab source must exist or COPY will fail at build time.
if [[ ! -f "$FEEDGRAB_DIR/pyproject.toml" ]]; then
  echo "ERROR: feedgrab source not found at: $FEEDGRAB_DIR" >&2
  echo "       Set FEEDGRAB_DIR or pass --feedgrab-dir /path/to/feedgrab" >&2
  exit 1
fi

REF="${IMAGE_NAME}:${TAG}"

echo "==> building ${REF}"
echo "    Dockerfile: ${SCRIPT_DIR}/Dockerfile.feedgrab"
echo "    context:    ${CONTEXT_DIR}"
echo "    feedgrab:   ${FEEDGRAB_DIR}"
if [[ -n "$PROXY" ]]; then
  echo "    proxy:      ${PROXY}"
fi

# Build proxy args. Only passed when --proxy is given so non-proxy builds stay
# clean (no empty-arg surprises).
PROXY_ARGS=()
if [[ -n "$PROXY" ]]; then
  PROXY_ARGS=(--build-arg "HTTPS_PROXY=${PROXY}" --build-arg "HTTP_PROXY=${PROXY}")
fi

if [[ -n "$PLATFORM" ]]; then
  # buildx path — required for multi-arch and for --push without local load.
  docker buildx build \
    --platform "$PLATFORM" \
    "${PROXY_ARGS[@]}" \
    $([[ $PUSH -eq 1 ]] && echo --push || echo --load) \
    -t "$REF" \
    -f "${SCRIPT_DIR}/Dockerfile.feedgrab" \
    "$CONTEXT_DIR"
else
  docker build \
    "${PROXY_ARGS[@]}" \
    -t "$REF" \
    -f "${SCRIPT_DIR}/Dockerfile.feedgrab" \
    "$CONTEXT_DIR"
  if [[ $PUSH -eq 1 ]]; then
    echo "==> pushing ${REF}"
    docker push "$REF"
  fi
fi

echo
echo "==> done: ${REF}"
echo
echo "Use it via Settings → Runtime → Sandbox → Image:"
echo "    ${REF}"
echo
echo "Then populate feedgrab data once (see README-feedgrab.md):"
echo "    mkdir -p <session-workspace>/.feedgrab/sessions"
echo "    cp <your>/.env <session-workspace>/.feedgrab/.env"
