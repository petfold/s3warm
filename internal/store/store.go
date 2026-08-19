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
	ErrSnapshotNotFound   = errors.New("snapshot not found")
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
	// Encryption is the bucket-default SSE algorithm: "" or "AES256"
	// (design §12 — mapped to swarm-encrypt).
	Encryption string
	// Versioning is "" (never enabled), "Enabled" or "Suspended" (design §11).
	Versioning string
	// HeadRoot/CommitSeq track the bucket's commit chain (design §5): the
	// Swarm manifest root of the latest commit and its sequence number.
	HeadRoot  string
	CommitSeq int64
	// CORS holds the bucket's CORS rules as JSON ("" = not configured).
	CORS string
	// Tags holds the bucket tag set as JSON ("" = none).
	Tags string
	// Owner is the tenant that created the bucket (design §8 layer 2).
	// Empty means root-owned: visible to root keys only, like any bucket,
	// but never to tenant keys.
	Owner string
	// ACT fields (design §8): ACT marks the bucket ACT-protected — every
	// object is uploaded with swarm-act under the node's publisher key.
	// ActHistory is the bucket's ACT history address; ActGrantees is the
	// grantee-list reference ("" until the first grant).
	ACT         bool
	ActHistory  string
	ActGrantees string
}

// Snapshot is a labeled commit root (design §5): restoring one is an atomic
// whole-bucket rollback.
type Snapshot struct {
	Bucket    string
	Label     string
	Root      string
	Seq       int64
	CreatedAt time.Time
}

type Object struct {
	Bucket      string
	Key         string
	SwarmRef    string // hex Swarm reference; empty for zero-byte objects
	BatchID     string // postage batch that stamped this object's upload
	Size        int64
	ETag        string // hex MD5, unquoted
	ContentType string
	// ContentEncoding as stored (any aws-chunked transport token stripped).
	ContentEncoding string
	StorageClass    string
	UserMetadata    map[string]string
	LastModified    time.Time
	// ChecksumAlgorithm/Checksum hold the flexible checksum recorded at
	// write time (algorithm uppercase, value base64), when the client
	// provided one.
	ChecksumAlgorithm string
	Checksum          string
	// Encrypted marks SSE objects: the Swarm reference embeds the
	// decryption key and must stay private (design §12).
	Encrypted bool
	// Tags holds the object's tag set as JSON ("" = none). Tags are
	// per-version and mutable in place.
	Tags string
	// Versioning fields (design §11). VersionID is "null" for writes into
	// never-versioned or suspended buckets; VSeq orders a key's versions
	// (write-time UnixNano); DeleteMarker rows shadow the key.
	VersionID    string
	VSeq         int64
	IsLatest     bool
	DeleteMarker bool
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
	Encrypted    bool
	Tags         string // tag set JSON, applied to the completed object
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
	// SetBucketEncryption sets the bucket-default SSE algorithm ("" clears).
	SetBucketEncryption(ctx context.Context, bucket, algorithm string) error
	// SetBucketVersioning sets the versioning status ("Enabled"/"Suspended").
	SetBucketVersioning(ctx context.Context, bucket, status string) error
	// SetBucketHead records the bucket's latest commit root and sequence.
	SetBucketHead(ctx context.Context, bucket, root string, seq int64) error
	// SetBucketCORS sets the bucket's CORS rules JSON ("" clears).
	SetBucketCORS(ctx context.Context, bucket, corsJSON string) error
	// SetBucketTagging sets the bucket tag set JSON ("" clears).
	SetBucketTagging(ctx context.Context, bucket, tagsJSON string) error
	// SetBucketACT records the bucket's ACT history address and grantee-list
	// reference (design §8).
	SetBucketACT(ctx context.Context, bucket, history, grantees string) error
	// SetObjectTags replaces one version's tag set in place (versionID "" =
	// the latest version).
	SetObjectTags(ctx context.Context, bucket, key, versionID, tagsJSON string) error

	PutSnapshot(ctx context.Context, s Snapshot) error
	GetSnapshot(ctx context.Context, bucket, label string) (*Snapshot, error)
	ListSnapshots(ctx context.Context, bucket string) ([]Snapshot, error)
	// RestoreBucket atomically replaces the bucket's entire object set and
	// points its head at the given commit (design §5: rollback is O(1) on
	// Swarm; the index swap happens here).
	RestoreBucket(ctx context.Context, bucket string, objects []Object, root string, seq int64) error

	// PutObject inserts o as its key's latest version. The caller sets
	// o.VersionID ("null" for unversioned/suspended writes — an existing row
	// with the same version id is replaced) and o.VSeq. A non-nil cond is
	// checked atomically against the current latest (a delete marker counts
	// as absent) and fails with ErrPreconditionFailed.
	PutObject(ctx context.Context, o Object, cond *PutCondition) error
	// GetObject returns the key's latest version — possibly a delete marker;
	// callers decide how to surface those.
	GetObject(ctx context.Context, bucket, key string) (*Object, error)
	GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*Object, error)
	// DeleteObject removes every version of the key. Idempotent.
	DeleteObject(ctx context.Context, bucket, key string) error
	// DeleteVersion permanently removes one version (delete markers
	// included), promoting the next-newest to latest, and returns the
	// removed row. ErrObjectNotFound when absent.
	DeleteVersion(ctx context.Context, bucket, key, versionID string) (*Object, error)
	// ListObjects returns up to limit latest, non-delete-marker objects
	// whose keys begin with prefix and sort strictly after `after`, in
	// ascending key order.
	ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]Object, error)
	// ListVersions returns up to limit version rows (delete markers
	// included) ordered by key ascending then newest-first, starting
	// strictly after the (keyMarker, versionMarker) position; an empty
	// versionMarker starts after all versions of keyMarker.
	ListVersions(ctx context.Context, bucket, prefix, keyMarker, versionMarker string, limit int) ([]Object, error)

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
