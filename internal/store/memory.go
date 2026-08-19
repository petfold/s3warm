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
}

type memBucket struct {
	meta    Bucket
	objects map[string]Object
}

func NewMemory() *Memory {
	return &Memory{buckets: make(map[string]*memBucket)}
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
	m.buckets[b.Name] = &memBucket{meta: b, objects: make(map[string]Object)}
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
