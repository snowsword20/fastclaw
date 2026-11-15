#!/usr/bin/env python3
"""One-shot migration of a LOCALFS fastclaw deployment into an S3-compatible
R2 bucket, so the gateway can switch from the local object store to the
cloud one without losing files or fork history.

Two phases (run both by default):

  workspaces  ~/.fastclaw/workspaces/<agent>/...  →  s3://<bucket>/<agent>/...
              Key layout is identical to workspace.LocalFS.scopeDir, which is
              exactly what workspace.S3.key() expects — a 1:1 copy.

  history     ~/.fastclaw/workspace-history/<key>.git (git bare repos, the
              local History backend) → cloud snapshots: content-addressed
              blobs at <prefix>/history/blobs/<sha256> in the bucket plus one
              row per commit in the workspace_history_snapshots SQL table
              (the D2/SQL manifest store — the gateway's default "auto" mode).

Writes go through the Cloudflare R2 REST API using wrangler's OAuth token
(`wrangler login` on this host), so no S3 API token is needed for the
migration itself. The gateway itself still needs a real R2 API token — mint
one in the dashboard and put it in the env the gateway reads.

Idempotent: blobs are content-addressed (re-upload is a no-op), snapshot ids
are derived from commit time (INSERT ... ON CONFLICT DO NOTHING).

Usage (from the repo root, on the gateway host):
    python3 deploy/multi-pod/migrate-to-r2.py            # both phases
    python3 deploy/multi-pod/migrate-to-r2.py workspaces # files only
    python3 deploy/multi-pod/migrate-to-r2.py history    # fork snapshots only
"""
import datetime
import hashlib
import json
import mimetypes
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request

HOME = os.path.expanduser(os.environ.get("FASTCLAW_HOME", "~/.fastclaw"))
ACCOUNT_ID = "27bec8625c6abae0466aba12e59daa75"
BUCKET = "fastclaw"
# Must match the gateway's FASTCLAW_OBJECT_STORE_PREFIX (empty → bucket root;
# history blobs always live under "<prefix>/history/blobs/").
PREFIX = ""
WRANGLER_TOML = ("/mnt/c/Users/Administrator/AppData/Roaming/xdg.config"
                 "/.wrangler/config/default.toml")
PG_CONTAINER = "fastclaw-postgres"
PG_USER = "fastclaw"
PG_DB = "fastclaw"

_objects_base = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/r2/buckets/{BUCKET}/objects/"


def oauth_token():
    with open(WRANGLER_TOML) as f:
        for line in f:
            if line.strip().startswith("oauth_token"):
                return line.split('"')[1]
    raise SystemExit(f"no oauth_token in {WRANGLER_TOML} — run `wrangler login` first")


def r2(method, key, data=None, content_type=None, _retried=False):
    """One R2 object API call. On 401 (expired OAuth) refresh via wrangler
    (whoami re-negotiates the token) and retry once."""
    headers = {"Authorization": f"Bearer {oauth_token()}"}
    if content_type:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(_objects_base + key, data=data, method=method,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, (r.read() if method == "GET" else b"")
    except urllib.error.HTTPError as e:
        if e.code == 401 and not _retried:
            subprocess.run(["wrangler", "whoami"], capture_output=True)
            return r2(method, key, data, content_type, _retried=True)
        raise SystemExit(f"{method} {key} -> HTTP {e.code}: {e.read()[:300]}")
    except urllib.error.URLError as e:
        if not _retried:
            time.sleep(2)
            return r2(method, key, data, content_type, _retried=True)
        raise SystemExit(f"{method} {key} -> {e}")


def put_object(key, data, content_type):
    for attempt in range(3):
        try:
            r2("PUT", key, data, content_type)
            return
        except SystemExit as e:
            if attempt == 2:
                raise
            print(f"  retry ({e})", file=sys.stderr)
            time.sleep(2 * (attempt + 1))


def psql(sql, stdin=None):
    """Run SQL in the gateway's postgres container. -tA = tuples-only."""
    cmd = ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER,
           "-d", PG_DB, "-v", "ON_ERROR_STOP=1", "-tAc", sql]
    if stdin is not None:
        cmd = ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER,
               "-d", PG_DB, "-v", "ON_ERROR_STOP=1"]
    r = subprocess.run(cmd, input=stdin, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"psql failed: {r.stderr.strip()[:500]}")
    return r.stdout.strip()


# ---------------------------------------------------------------------------
# Phase 1: workspace files
# ---------------------------------------------------------------------------

def migrate_workspaces():
    root = os.path.join(HOME, "workspaces")
    if not os.path.isdir(root):
        print(f"no workspaces dir at {root}, skipping")
        return
    n = 0
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            key = os.path.relpath(full, root).replace(os.sep, "/")
            ctype = mimetypes.guess_type(name)[0] or "application/octet-stream"
            with open(full, "rb") as f:
                data = f.read()
            put_object(key, data, ctype)
            n += 1
            if n % 25 == 0:
                print(f"  {n} files uploaded...")
    print(f"workspaces: {n} files -> s3://{BUCKET}/{PREFIX}")


# ---------------------------------------------------------------------------
# Phase 2: git fork history → cloud snapshots
# ---------------------------------------------------------------------------

def resolve_scope(key):
    """Map a flat scope key (repo name) to (agent_id, project_id, session).
    HistScope.Key() is "<project>-<session>" or bare "<session>"; session
    keys contain dashes themselves, so let the DB pick the real row."""
    safe = key.replace("'", "''")
    row = psql(
        "SELECT agent_id, project_id, session_key FROM sessions "
        f"WHERE session_key='{safe}' OR project_id||'-'||session_key='{safe}' "
        "ORDER BY (project_id='') DESC, updated_at DESC LIMIT 1")
    if row:
        agent_id, project_id, session = (c.strip() for c in row.split("|", 2))
        return agent_id, project_id, session
    # Deleted session: ownership survives on disk as the agent's session
    # dir (the DB purges the row but the workspace tree stays). Project
    # membership is unrecoverable; "" matches how web sessions are stored.
    sessions_root = os.path.join(HOME, "workspaces")
    for agent in sorted(os.listdir(sessions_root)):
        if os.path.isdir(os.path.join(sessions_root, agent, "sessions", key)):
            return agent, "", key
    return None


def parse_msgs(message):
    for field in message.split():
        if field.startswith("msgs="):
            try:
                return int(field[5:])
            except ValueError:
                pass
    return 0


def git(*args):
    r = subprocess.run(["git", *args], capture_output=True)
    if r.returncode != 0:
        raise SystemExit(f"git {' '.join(args[:3])}...: {r.stderr.decode()[:300]}")
    return r.stdout


def migrate_history():
    hist = os.path.join(HOME, "workspace-history")
    repos = sorted(p for p in os.listdir(hist) if p.endswith(".git")) if os.path.isdir(hist) else []
    print(f"history: {len(repos)} repos under {hist}")
    sha1_to_sha256 = {}   # git blob sha1 → (sha256 hex, size), dedupes reads+uploads
    blob_keys_uploaded = set()
    total_commits = 0

    for repo_name in repos:
        key = repo_name[:-len(".git")]
        scope = resolve_scope(key)
        if scope is None:
            print(f"  {key}: no sessions row — cannot place manifest, SKIPPED")
            continue
        agent_id, project_id, session = scope
        gitdir = f"--git-dir={os.path.join(hist, repo_name)}"
        log = git(gitdir, "log", "--reverse", "--pretty=%H|%ct|%s").decode()
        commits = [l.split("|", 2) for l in log.splitlines() if l.strip()]
        print(f"  {key}: agent={agent_id} project={project_id!r} "
              f"session={session!r} ({len(commits)} commits)")

        for sha, ctime, subject in commits:
            ctime = int(ctime)
            files = {}
            for line in git(gitdir, "ls-tree", "-r", sha).decode().splitlines():
                meta, path = line.split("\t", 1)
                mode, otype, blob_sha = meta.split()
                if otype != "blob":  # submodules / symlinks: not workspace files
                    print(f"    skip non-blob {path!r} ({otype})")
                    continue
                if blob_sha not in sha1_to_sha256:
                    data = git(gitdir, "cat-file", "blob", blob_sha)
                    h = hashlib.sha256(data).hexdigest()
                    sha1_to_sha256[blob_sha] = (h, len(data))
                    blob_key = f"{PREFIX}/history/blobs/{h}".lstrip("/")
                    if blob_key not in blob_keys_uploaded:
                        put_object(blob_key, data, "application/octet-stream")
                        blob_keys_uploaded.add(blob_key)
                h, size = sha1_to_sha256[blob_sha]
                # m=0: git has no per-file mtime; 0 disables the snapshot
                # fast path for migrated files (safe, just re-hashes).
                files[path] = {"h": h, "s": size, "m": 0}

            # Deterministic, order-preserving snapshot id (matches
            # newSnapshotID's "snap-<020d nanos>-<4hex>" shape).
            nanos = ctime * 10**9
            manifest = {
                "id": f"snap-{nanos:020d}-{secrets.token_hex(2)}",
                "message": subject,
                "msgs": parse_msgs(subject),
                "time": nanos,
                "files": files,
            }
            doc = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
            # Dollar-quote the JSON/message so quotes inside can't break SQL.
            tag = f"$m{secrets.token_hex(4)}$"
            created = datetime.datetime.fromtimestamp(
                ctime, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            psql(None, stdin="\n".join([
                "BEGIN;",
                "INSERT INTO workspace_history_snapshots "
                "(snap_id, agent_id, project_id, session_key, msgs, message, manifest, created_at) "
                "VALUES (" + ", ".join([
                    f"'{manifest['id']}'",
                    f"'{agent_id}'",
                    f"'{project_id}'",
                    f"'{session}'",
                    str(manifest["msgs"]),
                    f"{tag}{subject}{tag}",
                    f"{tag}{doc}{tag}",
                    f"TIMESTAMP '{created}'",
                ]) + ") ON CONFLICT (snap_id) DO NOTHING;",
                "COMMIT;",
            ]) + "\n")
            total_commits += 1
    print(f"history: {total_commits} snapshots, {len(blob_keys_uploaded)} blobs "
          f"-> s3://{BUCKET}/{PREFIX}/history/blobs/ + SQL manifests")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "all"
    started = time.time()
    if phase in ("all", "workspaces"):
        migrate_workspaces()
    if phase in ("all", "history"):
        migrate_history()
    print(f"done in {time.time() - started:.1f}s")
