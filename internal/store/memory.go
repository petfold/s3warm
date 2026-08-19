package store

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-memory Store for development and tests.
type Memory struct {
	mu      sync.RWMutex
	buckets map[string]*memBucket
	uploads map[string]*memUpload
}

type memBucket struct {
	meta      Bucket
	objects   map[string]Object
	snapshots map[string]Snapshot
}

type memUpload struct {
	meta  MultipartUpload
	parts map[int]Part
}

func NewMemory() *Memory {
	return &Memory{
		buckets: make(map[string]*memBucket),
		uploads: make(map[string]*memUpload),
	}
}

func (m *Memory) CreateBucket(_ context.Context, b Bucket) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[b.Name]; ok {
		return ErrBucketExists
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	m.buckets[b.Name] = &memBucket{
		meta:      b,
		objects:   make(map[string]Object),
		snapshots: make(map[string]Snapshot),
	}
	return nil
}

func (m *Memory) GetBucket(_ context.Context, name string) (*Bucket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[name]
	if !ok {
		return nil, ErrBucketNotFound
	}
	meta := b.meta
	return &meta, nil
}

func (m *Memory) ListBuckets(_ context.Context) ([]Bucket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Bucket, 0, len(m.buckets))
	for _, b := range m.buckets {
		out = append(out, b.meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *Memory) DeleteBucket(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[name]
	if !ok {
		return ErrBucketNotFound
	}
	if len(b.objects) > 0 {
		return ErrBucketNotEmpty
	}
	delete(m.buckets, name)
	return nil
}

func (m *Memory) SetBucketEncryption(_ context.Context, bucket, algorithm string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return ErrBucketNotFound
	}
	b.meta.Encryption = algorithm
	return nil
}

func (m *Memory) SetBucketHead(_ context.Context, bucket, root string, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return ErrBucketNotFound
	}
	b.meta.HeadRoot, b.meta.CommitSeq = root, seq
	return nil
}

func (m *Memory) PutSnapshot(_ context.Context, s Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[s.Bucket]
	if !ok {
		return ErrBucketNotFound
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	b.snapshots[s.Label] = s
	return nil
}

func (m *Memory) GetSnapshot(_ context.Context, bucket, label string) (*Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return nil, ErrBucketNotFound
	}
	s, ok := b.snapshots[label]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	return &s, nil
}

func (m *Memory) ListSnapshots(_ context.Context, bucket string) ([]Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return nil, ErrBucketNotFound
	}
	out := make([]Snapshot, 0, len(b.snapshots))
	for _, s := range b.snapshots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out, nil
}

func (m *Memory) RestoreBucket(_ context.Context, bucket string, objects []Object, root string, seq int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return ErrBucketNotFound
	}
	b.objects = make(map[string]Object, len(objects))
	for _, o := range objects {
		o.Bucket = bucket
		b.objects[o.Key] = o
	}
	b.meta.HeadRoot, b.meta.CommitSeq = root, seq
	return nil
}

func (m *Memory) PutObject(_ context.Context, o Object, cond *PutCondition) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[o.Bucket]
	if !ok {
		return ErrBucketNotFound
	}
	cur, exists := b.objects[o.Key]
	if !cond.Ok(exists, cur.ETag) {
		return ErrPreconditionFailed
	}
	b.objects[o.Key] = o
	return nil
}

func (m *Memory) GetObject(_ context.Context, bucket, key string) (*Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return nil, ErrBucketNotFound
	}
	o, ok := b.objects[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	return &o, nil
}

func (m *Memory) DeleteObject(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return ErrBucketNotFound
	}
	delete(b.objects, key)
	return nil
}

func (m *Memory) ListObjects(_ context.Context, bucket, prefix, after string, limit int) ([]Object, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[bucket]
	if !ok {
		return nil, ErrBucketNotFound
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit >= 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]Object, 0, len(keys))
	for _, k := range keys {
		out = append(out, b.objects[k])
	}
	return out, nil
}

func (m *Memory) CreateMultipartUpload(_ context.Context, u MultipartUpload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[u.Bucket]; !ok {
		return ErrBucketNotFound
	}
	if u.Initiated.IsZero() {
		u.Initiated = time.Now().UTC()
	}
	m.uploads[u.UploadID] = &memUpload{meta: u, parts: make(map[int]Part)}
	return nil
}

func (m *Memory) GetMultipartUpload(_ context.Context, bucket, key, uploadID string) (*MultipartUpload, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.uploads[uploadID]
	if !ok || u.meta.Bucket != bucket || u.meta.Key != key {
		return nil, ErrUploadNotFound
	}
	meta := u.meta
	return &meta, nil
}

func (m *Memory) PutPart(_ context.Context, uploadID string, p Part) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[uploadID]
	if !ok {
		return ErrUploadNotFound
	}
	u.parts[p.PartNumber] = p
	return nil
}

func (m *Memory) ListParts(_ context.Context, uploadID string, afterPart, limit int) ([]Part, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.uploads[uploadID]
	if !ok {
		return nil, ErrUploadNotFound
	}
	nums := make([]int, 0, len(u.parts))
	for n := range u.parts {
		if n > afterPart {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	if limit >= 0 && len(nums) > limit {
		nums = nums[:limit]
	}
	out := make([]Part, 0, len(nums))
	for _, n := range nums {
		out = append(out, u.parts[n])
	}
	return out, nil
}

func (m *Memory) ListMultipartUploads(_ context.Context, bucket, prefix string) ([]MultipartUpload, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.buckets[bucket]; !ok {
		return nil, ErrBucketNotFound
	}
	var out []MultipartUpload
	for _, u := range m.uploads {
		if u.meta.Bucket == bucket && strings.HasPrefix(u.meta.Key, prefix) {
			out = append(out, u.meta)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}

func (m *Memory) DeleteMultipartUpload(_ context.Context, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.uploads[uploadID]; !ok {
		return ErrUploadNotFound
	}
	delete(m.uploads, uploadID)
	return nil
}
