package workspace

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// ObjectHistoryStore is the object-store side of CloudHistory: it stores
// content-addressed snapshot blobs and (optionally) snapshot manifests in
// the same S3-compatible bucket as the workspace itself. Key layout, all
// under the store's prefix:
//
//	<prefix>/history/blobs/<sha256-hex>                      — BlobStore
//	<prefix>/history/manifests/<agent>/<scope>/<id>.json     — ManifestStore (D1)
//
// Blobs are content-addressed: the hex name IS the payload hash, so
// repeated uploads of identical content are idempotent no-ops on the
// bucket and any pod can serve any blob. Manifests are tiny JSON
// documents (path → {hash, size, modtime} per file); ids embed a zero-
// padded nanosecond timestamp so lexicographic key order == time order.
//
// Use NewObjectHistoryStore rather than constructing the struct directly.
type ObjectHistoryStore struct {
	client *minio.Client
	bucket string
	prefix string // "history" under the workspace store's prefix, or bucket root
}

// Compile-time proof ObjectHistoryStore satisfies both cloud storage
// roles — as the D1 deployment it acts as ManifestStore directly, and as
// the blob side of every cloud deployment (blobs never live in SQL).
var (
	_ BlobStore    = (*ObjectHistoryStore)(nil)
	_ ManifestStore = (*ObjectHistoryStore)(nil)
)

// NewObjectHistoryStore builds the blob+manifest store from the same
// config block as the workspace S3 store. Returns a wrapped error so the
// gateway can abort startup with a readable message on misconfiguration.
func NewObjectHistoryStore(cfg S3Config) (*ObjectHistoryStore, error) {
	client, err := newMinioClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ObjectHistoryStore{
		client: client,
		bucket: cfg.Bucket,
		prefix: strings.Trim(cfg.Prefix+"/history", "/"),
	}, nil
}

func (h *ObjectHistoryStore) blobKey(hash string) string {
	return h.prefix + "/blobs/" + hash
}

func (h *ObjectHistoryStore) manifestPrefix(scope HistScope) string {
	return h.prefix + "/manifests/" + sanitizeScope(scope.AgentID) + "/" + sanitizeScope(scope.Key()) + "/"
}

func (h *ObjectHistoryStore) manifestKey(scope HistScope, id string) string {
	return h.manifestPrefix(scope) + id + ".json"
}

// PutBlob stores one content-addressed blob. Idempotent: same hash, same
// bytes, so concurrent pods re-uploading the same blob converge.
func (h *ObjectHistoryStore) PutBlob(ctx context.Context, hash string, r io.Reader, size int64) error {
	if !isBlobHash(hash) {
		return fmt.Errorf("workspace history: invalid blob hash %q", hash)
	}
	_, err := h.client.PutObject(ctx, h.bucket, h.blobKey(hash), r, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}

// GetBlob reads one blob back. Unknown hashes surface ErrNotFound.
func (h *ObjectHistoryStore) GetBlob(ctx context.Context, hash string) (io.ReadCloser, error) {
	if !isBlobHash(hash) {
		return nil, fmt.Errorf("workspace history: invalid blob hash %q", hash)
	}
	obj, err := h.client.GetObject(ctx, h.bucket, h.blobKey(hash), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// Same early-Stat as S3.Get: turn a missing key into ErrNotFound up
	// front instead of on first Read.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		return nil, mapS3Err(err)
	}
	return obj, nil
}

// Save writes one manifest document. The record's ID must already be set
// (CloudHistory assigns it — the store only persists bytes).
func (h *ObjectHistoryStore) Save(ctx context.Context, scope HistScope, m *ManifestRecord) error {
	if m.ID == "" {
		return fmt.Errorf("workspace history: manifest id required")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("workspace history: marshal manifest: %w", err)
	}
	_, err = h.client.PutObject(ctx, h.bucket, h.manifestKey(scope, m.ID),
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/json",
		})
	return err
}

// List reads every manifest of the scope, newest first. The id's zero-
// padded timestamp makes descending key order the time order, so the
// listing never needs a server-side sort. No manifests yields an empty
// slice (not an error).
func (h *ObjectHistoryStore) List(ctx context.Context, scope HistScope) ([]ManifestRecord, error) {
	prefix := h.manifestPrefix(scope)
	var ids []string
	for obj := range h.client.ListObjects(ctx, h.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		name = strings.TrimSuffix(name, ".json")
		if name == "" || name == obj.Key {
			continue // not a manifest document
		}
		ids = append(ids, name)
	}
	// Newest first: zero-padded snapshot ids sort lexicographically by
	// embedded nanosecond timestamp.
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	out := make([]ManifestRecord, 0, len(ids))
	for _, id := range ids {
		obj, err := h.client.GetObject(ctx, h.bucket, h.manifestKey(scope, id), minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(obj)
		closeErr := obj.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		var m ManifestRecord
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("workspace history: manifest %s: %w", id, err)
		}
		if m.ID == "" {
			m.ID = id
		}
		out = append(out, m)
	}
	return out, nil
}

// newSnapshotID mints a time-ordered, collision-resistant manifest id:
// "snap-<zero-padded nanos>-<4 hex random>". The padding is load-bearing —
// it is what makes lexicographic order equal time order (see List).
func newSnapshotID() string {
	var rnd [2]byte
	_, _ = rand.Read(rnd[:])
	return fmt.Sprintf("snap-%020d-%s", time.Now().UnixNano(), hex.EncodeToString(rnd[:]))
}

func isBlobHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
