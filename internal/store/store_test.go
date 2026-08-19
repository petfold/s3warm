package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// The conformance suite runs against every Store implementation.
func forEachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) {
		fn(t, NewMemory())
	})
	t.Run("sqlite", func(t *testing.T) {
		s, err := OpenSQLite(filepath.Join(t.TempDir(), "index.db"))
		if err != nil {
			t.Fatalf("OpenSQLite: %v", err)
		}
		t.Cleanup(func() { s.Close() })
		fn(t, s)
	})
}

func obj(bucket, key string) Object {
	return Object{
		Bucket:       bucket,
		Key:          key,
		SwarmRef:     "ref-" + key,
		BatchID:      "batch-" + key,
		Size:         int64(len(key)),
		ETag:         "etag-" + key,
		ContentType:  "text/plain",
		StorageClass: "STANDARD",
		UserMetadata: map[string]string{"origin": "test"},
		LastModified: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC),
	}
}

func TestBucketLifecycle(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()

		if _, err := s.GetBucket(ctx, "nope"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("GetBucket(missing) = %v", err)
		}
		if err := s.CreateBucket(ctx, Bucket{Name: "b1", BatchID: "batch1"}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		if err := s.CreateBucket(ctx, Bucket{Name: "b1"}); !errors.Is(err, ErrBucketExists) {
			t.Fatalf("CreateBucket(dup) = %v", err)
		}
		b, err := s.GetBucket(ctx, "b1")
		if err != nil || b.Name != "b1" || b.BatchID != "batch1" || b.CreatedAt.IsZero() {
			t.Fatalf("GetBucket = %+v, %v", b, err)
		}

		if err := s.CreateBucket(ctx, Bucket{Name: "a0"}); err != nil {
			t.Fatal(err)
		}
		bs, err := s.ListBuckets(ctx)
		if err != nil || len(bs) != 2 || bs[0].Name != "a0" || bs[1].Name != "b1" {
			t.Fatalf("ListBuckets = %+v, %v", bs, err)
		}

		if err := s.PutObject(ctx, obj("b1", "k"), nil); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrBucketNotEmpty) {
			t.Fatalf("DeleteBucket(non-empty) = %v", err)
		}
		if err := s.DeleteObject(ctx, "b1", "k"); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteBucket(ctx, "b1"); err != nil {
			t.Fatalf("DeleteBucket: %v", err)
		}
		if err := s.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("DeleteBucket(missing) = %v", err)
		}
	})
}

func TestObjectLifecycle(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()

		if err := s.PutObject(ctx, obj("nope", "k"), nil); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("PutObject(missing bucket) = %v", err)
		}
		if err := s.CreateBucket(ctx, Bucket{Name: "b"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetObject(ctx, "b", "k"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("GetObject(missing) = %v", err)
		}
		if _, err := s.GetObject(ctx, "nope", "k"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("GetObject(missing bucket) = %v", err)
		}

		want := obj("b", "k")
		if err := s.PutObject(ctx, want, nil); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetObject(ctx, "b", "k")
		if err != nil {
			t.Fatal(err)
		}
		if got.SwarmRef != want.SwarmRef || got.BatchID != want.BatchID || got.ETag != want.ETag || got.Size != want.Size ||
			got.ContentType != want.ContentType || got.StorageClass != want.StorageClass ||
			got.UserMetadata["origin"] != "test" || !got.LastModified.Equal(want.LastModified) {
			t.Fatalf("GetObject = %+v, want %+v", got, want)
		}

		// Overwrite is last-writer-wins.
		want.SwarmRef = "ref-2"
		if err := s.PutObject(ctx, want, nil); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.GetObject(ctx, "b", "k"); got.SwarmRef != "ref-2" {
			t.Fatalf("overwrite: SwarmRef = %q", got.SwarmRef)
		}

		// Delete is idempotent.
		if err := s.DeleteObject(ctx, "b", "k"); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteObject(ctx, "b", "k"); err != nil {
			t.Fatalf("DeleteObject(absent) = %v", err)
		}
		if err := s.DeleteObject(ctx, "nope", "k"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("DeleteObject(missing bucket) = %v", err)
		}
	})
}

func TestPutObjectConditional(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.CreateBucket(ctx, Bucket{Name: "b"}); err != nil {
			t.Fatal(err)
		}

		// If-None-Match "*": create-only.
		if err := s.PutObject(ctx, obj("b", "k"), &PutCondition{IfNoneMatch: "*"}); err != nil {
			t.Fatalf("create-only on absent key: %v", err)
		}
		if err := s.PutObject(ctx, obj("b", "k"), &PutCondition{IfNoneMatch: "*"}); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("create-only on existing key = %v, want ErrPreconditionFailed", err)
		}

		// If-Match: overwrite only the expected state.
		if err := s.PutObject(ctx, obj("b", "k"), &PutCondition{IfMatch: "etag-k"}); err != nil {
			t.Fatalf("if-match with current etag: %v", err)
		}
		if err := s.PutObject(ctx, obj("b", "k"), &PutCondition{IfMatch: "stale-etag"}); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("if-match with stale etag = %v, want ErrPreconditionFailed", err)
		}
		if err := s.PutObject(ctx, obj("b", "k"), &PutCondition{IfMatch: "*"}); err != nil {
			t.Fatalf("if-match any-existing: %v", err)
		}
		if err := s.PutObject(ctx, obj("b", "absent"), &PutCondition{IfMatch: "*"}); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("if-match on absent key = %v, want ErrPreconditionFailed", err)
		}
	})
}

func TestListObjects(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.CreateBucket(ctx, Bucket{Name: "b"}); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"a/1", "a/2", "ab", "b", "c/1"} {
			if err := s.PutObject(ctx, obj("b", k), nil); err != nil {
				t.Fatal(err)
			}
		}

		keys := func(prefix, after string, limit int) []string {
			t.Helper()
			os, err := s.ListObjects(ctx, "b", prefix, after, limit)
			if err != nil {
				t.Fatalf("ListObjects(%q,%q,%d): %v", prefix, after, limit, err)
			}
			out := make([]string, len(os))
			for i, o := range os {
				out[i] = o.Key
			}
			return out
		}
		equal := func(got, want []string) {
			t.Helper()
			if len(got) != len(want) {
				t.Fatalf("got %v, want %v", got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("got %v, want %v", got, want)
				}
			}
		}

		equal(keys("", "", 100), []string{"a/1", "a/2", "ab", "b", "c/1"})
		equal(keys("a/", "", 100), []string{"a/1", "a/2"})
		equal(keys("a", "", 100), []string{"a/1", "a/2", "ab"})
		equal(keys("", "a/2", 100), []string{"ab", "b", "c/1"})
		equal(keys("a", "a/1", 100), []string{"a/2", "ab"})
		equal(keys("", "", 2), []string{"a/1", "a/2"})
		equal(keys("", "zzz", 100), nil)
		equal(keys("nope/", "", 100), nil)

		if _, err := s.ListObjects(ctx, "missing", "", "", 10); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("ListObjects(missing bucket) = %v", err)
		}
	})
}

func TestSQLitePersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")

	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateBucket(ctx, Bucket{Name: "b", BatchID: "batch"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutObject(ctx, obj("b", "k"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.GetObject(ctx, "b", "k")
	if err != nil || got.SwarmRef != "ref-k" || got.UserMetadata["origin"] != "test" {
		t.Fatalf("after reopen: %+v, %v", got, err)
	}
	b, err := s2.GetBucket(ctx, "b")
	if err != nil || b.BatchID != "batch" {
		t.Fatalf("after reopen: %+v, %v", b, err)
	}
}
