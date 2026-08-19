// Package store defines the metadata index: the gateway-local source of truth
// for buckets, keys and object metadata (see docs/DESIGN.md §5). Object bytes
// live on Swarm; the index maps S3 semantics (ordered listings, ETags,
// user metadata, low-latency HEAD) onto Swarm references.
package store

import (
	"context"
	"errors"
	"time"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotEmpty = errors.New("bucket not empty")
	ErrObjectNotFound = errors.New("object not found")
)

type Bucket struct {
	Name      string
	CreatedAt time.Time
	// BatchID is the bucket-default postage batch (may be empty; the gateway
	// default applies then).
	BatchID string
}

type Object struct {
	Bucket       string
	Key          string
	SwarmRef     string // hex Swarm reference; empty for zero-byte objects
	BatchID      string // postage batch that stamped this object's upload
	Size         int64
	ETag         string // hex MD5, unquoted
	ContentType  string
	StorageClass string
	UserMetadata map[string]string
	LastModified time.Time
}

// Store is the metadata index. Implementations must be safe for concurrent
// use. The in-memory implementation is for development and tests; SQLite and
// Postgres implementations are planned (design §5).
type Store interface {
	CreateBucket(ctx context.Context, b Bucket) error
	GetBucket(ctx context.Context, name string) (*Bucket, error)
	ListBuckets(ctx context.Context) ([]Bucket, error)
	// DeleteBucket fails with ErrBucketNotEmpty when objects remain.
	DeleteBucket(ctx context.Context, name string) error

	// PutObject upserts; overwriting a key is last-writer-wins, as in S3.
	PutObject(ctx context.Context, o Object) error
	GetObject(ctx context.Context, bucket, key string) (*Object, error)
	// DeleteObject is idempotent: deleting an absent key returns nil.
	DeleteObject(ctx context.Context, bucket, key string) error
	// ListObjects returns up to limit objects whose keys begin with prefix and
	// sort strictly after `after`, in ascending key order.
	ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]Object, error)
}
