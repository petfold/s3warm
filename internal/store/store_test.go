package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	t.Run("postgres", func(t *testing.T) {
		fn(t, openTestPostgres(t))
	})
}

// openTestPostgres connects to the server named by S3WARM_TEST_POSTGRES
// (a postgres:// DSN) and isolates the test in a throwaway schema. Skipped
// when the variable is unset — CI provides a server, `make test-postgres`
// runs one locally.
func openTestPostgres(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("S3WARM_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("S3WARM_TEST_POSTGRES not set")
	}
	schema := fmt.Sprintf("s3warm_test_%d", atomic.AddInt64(&testSchemaSeq, 1))
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		admin.Exec("DROP SCHEMA " + schema + " CASCADE") //nolint:errcheck
		admin.Close()
	})
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	s, err := OpenPostgres(dsn + sep + "search_path=" + schema)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

var testSchemaSeq int64

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

func TestVersioning(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.CreateBucket(ctx, Bucket{Name: "b"}); err != nil {
			t.Fatal(err)
		}
		put := func(versionID string, vseq int64, marker bool) Object {
			o := obj("b", "k")
			o.VersionID, o.VSeq, o.DeleteMarker = versionID, vseq, marker
			o.SwarmRef = "ref-" + versionID
			if err := s.PutObject(ctx, o, nil); err != nil {
				t.Fatalf("put %s: %v", versionID, err)
			}
			return o
		}

		put("v1", 1, false)
		put("v2", 2, false)
		latest, err := s.GetObject(ctx, "b", "k")
		if err != nil || latest.VersionID != "v2" || !latest.IsLatest {
			t.Fatalf("latest = %+v, %v", latest, err)
		}
		if v, err := s.GetObjectVersion(ctx, "b", "k", "v1"); err != nil || v.SwarmRef != "ref-v1" || v.IsLatest {
			t.Fatalf("v1 = %+v, %v", v, err)
		}

		// Delete marker shadows the key.
		put("v3", 3, true)
		latest, _ = s.GetObject(ctx, "b", "k")
		if !latest.DeleteMarker {
			t.Fatalf("latest should be a delete marker: %+v", latest)
		}
		if objs, _ := s.ListObjects(ctx, "b", "", "", 10); len(objs) != 0 {
			t.Fatalf("shadowed key listed: %+v", objs)
		}
		// Conditional create-only succeeds through a delete marker.
		o := obj("b", "k")
		o.VersionID, o.VSeq = "v4", 4
		if err := s.PutObject(ctx, o, &PutCondition{IfNoneMatch: "*"}); err != nil {
			t.Fatalf("create-only through marker: %v", err)
		}

		// Version listing: newest first per key.
		vs, err := s.ListVersions(ctx, "b", "", "", "", -1)
		if err != nil || len(vs) != 4 {
			t.Fatalf("versions = %d, %v", len(vs), err)
		}
		if vs[0].VersionID != "v4" || vs[3].VersionID != "v1" || !vs[0].IsLatest || vs[1].IsLatest {
			t.Fatalf("order/latest wrong: %+v", vs)
		}
		// Pagination after (k, v3).
		vs, _ = s.ListVersions(ctx, "b", "", "k", "v3", -1)
		if len(vs) != 2 || vs[0].VersionID != "v2" {
			t.Fatalf("after marker: %+v", vs)
		}

		// Removing the latest promotes the next-newest.
		removed, err := s.DeleteVersion(ctx, "b", "k", "v4")
		if err != nil || removed.VersionID != "v4" {
			t.Fatalf("DeleteVersion = %+v, %v", removed, err)
		}
		latest, _ = s.GetObject(ctx, "b", "k")
		if latest.VersionID != "v3" || !latest.IsLatest || !latest.DeleteMarker {
			t.Fatalf("promotion failed: %+v", latest)
		}
		if _, err := s.DeleteVersion(ctx, "b", "k", "v4"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("double delete = %v", err)
		}

		// Suspended-style write: replacing the "null" version keeps one row.
		put("null", 5, false)
		put("null", 6, false)
		vs, _ = s.ListVersions(ctx, "b", "", "", "", -1)
		if len(vs) != 4 { // v1, v2, v3, null
			t.Fatalf("null replace: %d versions: %+v", len(vs), vs)
		}
		latest, _ = s.GetObject(ctx, "b", "k")
		if latest.VersionID != "null" || latest.VSeq != 6 {
			t.Fatalf("null latest: %+v", latest)
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

// TestConcurrentConditionalCreate races create-only writers of one key:
// exactly one may win. SQLite serializes via its single writer; Postgres
// must do it with the per-key advisory lock (multi-gateway, design §10).
func TestConcurrentConditionalCreate(t *testing.T) {
	forEachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		if err := s.CreateBucket(ctx, Bucket{Name: "race"}); err != nil {
			t.Fatal(err)
		}
		const writers = 16
		var wins int64
		var wg sync.WaitGroup
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				o := obj("race", "the-key")
				o.ETag = fmt.Sprintf("etag-%d", i)
				o.VersionID = fmt.Sprintf("v%d", i)
				o.VSeq = int64(i)
				err := s.PutObject(ctx, o, &PutCondition{IfNoneMatch: "*"})
				switch {
				case err == nil:
					atomic.AddInt64(&wins, 1)
				case errors.Is(err, ErrPreconditionFailed):
				default:
					t.Errorf("writer %d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		if wins != 1 {
			t.Fatalf("create-only race: %d writers succeeded, want exactly 1", wins)
		}
	})
}
