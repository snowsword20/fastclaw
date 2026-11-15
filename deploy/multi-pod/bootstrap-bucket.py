#!/usr/bin/env python3
"""Create the fastclaw bucket on the local RustFS object store.

Zero-dependency (stdlib only) SigV4 PutBucket, run from the WSL host:
    python3 deploy/multi-pod/bootstrap-bucket.py
Replaces the aws-cli bootstrap container — alpine's CDN is unreachable
from this network and the injected clash proxy 502s compose-network
traffic, so there is no reliable in-container way to install a client.
"""
import datetime
import hashlib
import hmac
import urllib.error
import urllib.request

HOST = "127.0.0.1:19000"  # published port of multi-pod-objectstore-1
AK = "minioadmin"
SK = "minioadmin"
REGION = "us-east-1"
BUCKET = "fastclaw"

now = datetime.datetime.now(datetime.timezone.utc)
amzdate = now.strftime("%Y%m%dT%H%M%SZ")
datestamp = now.strftime("%Y%m%d")
empty_sha = hashlib.sha256(b"").hexdigest()

canonical_uri = "/" + BUCKET
headers = {
    "host": HOST,
    "x-amz-content-sha256": empty_sha,
    "x-amz-date": amzdate,
}
signed_headers = ";".join(sorted(headers))
canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted(headers.items()))
canonical_request = "\n".join(
    ["PUT", canonical_uri, "", canonical_headers, signed_headers, empty_sha]
)
algorithm = "AWS4-HMAC-SHA256"
scope = f"{datestamp}/{REGION}/s3/aws4_request"
string_to_sign = "\n".join(
    [algorithm, amzdate, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
)


def sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


k_signing = sign(
    sign(sign(sign(("AWS4" + SK).encode(), datestamp), REGION), "s3"),
    "aws4_request",
)
signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
auth = (
    f"{algorithm} Credential={AK}/{scope}, "
    f"SignedHeaders={signed_headers}, Signature={signature}"
)

req = urllib.request.Request(
    f"http://{HOST}{canonical_uri}",
    method="PUT",
    headers={
        "Authorization": auth,
        "x-amz-date": amzdate,
        "x-amz-content-sha256": empty_sha,
    },
)
try:
    with urllib.request.urlopen(req, timeout=15) as r:
        print(f"bucket '{BUCKET}' ready (HTTP {r.status})")
except urllib.error.HTTPError as e:
    body = e.read()[:200].decode(errors="replace")
    # 409 BucketAlreadyOwnedByYou = fine, idempotent re-run
    print(f"HTTP {e.code}: {body}")
