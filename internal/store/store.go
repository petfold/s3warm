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
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrBucketNotEmpty     = errors.New("bucket not empty")
	ErrObjectNotFound     = errors.New("object not found")
	ErrUploadNotFound     = errors.New("multipart upload not found")
	ErrPreconditionFailed = errors.New("precondition failed")
)

// PutCondition constrains an object write; it is evaluated atomically with
// the write (design §10: conditional writes are trivial index constraints).
type PutCondition struct {
	// IfMatch requires the key to exist with this ETag ("*" = any existing).
	IfMatch string
	// IfNoneMatch "*" requires the key to not exist; a specific ETag
	// requires the current object to not have it.
	IfNoneMatch string
}

// Ok reports whether the condition holds against the current state of the
// key: exists and, when it does, its ETag.
func (c *PutCondition) Ok(exists bool, etag string) bool {
	if c == nil {
		return true
	}
	if c.IfMatch != "" {
		if !exists || (c.IfMatch != "*" && c.IfMatch != etag) {
			return false
		}
	}
	if c.IfNoneMatch != "" {
		if exists && (c.IfNoneMatch == "*" || c.IfNoneMatch == etag) {
			return false
		}
	}
	return true
}

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
	// Parts is non-empty for composite (multipart) objects: the ordered
	// part list the gateway stitches on reads (design §7). SwarmRef is
	// empty for composites.
	Parts []Part
}

// Part is one part of a multipart upload / composite object.
type Part struct {
	PartNumber   int
	SwarmRef     string
	Size         int64
	ETag         string // hex MD5 of the part, unquoted
	LastModified time.Time
}

// MultipartUpload is an in-progress upload session (design §7).
type MultipartUpload struct {
	UploadID     string
	Bucket       string
	Key          string
	Initiated    time.Time
	ContentType  string
	StorageClass string
	UserMetadata map[string]string
	BatchID      string
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
	// A non-nil cond is checked atomically with the write and fails with
	// ErrPreconditionFailed.
	PutObject(ctx context.Context, o Object, cond *PutCondition) error
	GetObject(ctx context.Context, bucket, key string) (*Object, error)
	// DeleteObject is idempotent: deleting an absent key returns nil.
	DeleteObject(ctx context.Context, bucket, key string) error
	// ListObjects returns up to limit objects whose keys begin with prefix and
	// sort strictly after `after`, in ascending key order.
	ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]Object, error)

	CreateMultipartUpload(ctx context.Context, u MultipartUpload) error
	// GetMultipartUpload fails with ErrUploadNotFound unless bucket, key and
	// id all match an in-progress upload.
	GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUpload, error)
	// PutPart upserts a part by number.
	PutPart(ctx context.Context, uploadID string, p Part) error
	// ListParts returns up to limit parts with numbers strictly greater than
	// afterPart, in ascending order.
	ListParts(ctx context.Context, uploadID string, afterPart, limit int) ([]Part, error)
	// ListMultipartUploads returns a bucket's in-progress uploads whose keys
	// begin with prefix, ordered by (key, uploadID).
	ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]MultipartUpload, error)
	// DeleteMultipartUpload removes the upload and its parts.
	DeleteMultipartUpload(ctx context.Context, uploadID string) error
}
