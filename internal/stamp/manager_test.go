package stamp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/petfold/s3warm/internal/bee"
)

func newFakeStampAPI(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stamps/{id}", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		id := r.PathValue("id")
		st := map[string]any{
			"batchID": id, "exists": true, "usable": true,
			"utilization": 8, "utilizationRatio": 0.25,
			"depth": 21, "bucketDepth": 16, "amount": "1000",
			"batchTTL": 7200, "immutableFlag": false,
		}
		switch id {
		case "unknown":
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":404,"message":"issue on get stamp"}`)
			return
		case "unusable":
			st["usable"] = false
		case "full":
			st["immutableFlag"] = true
			st["utilizationRatio"] = 1.0
		case "expired":
			st["batchTTL"] = 0
		}
		json.NewEncoder(w).Encode(st)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestManager(t *testing.T, hits *atomic.Int32) *Manager {
	srv := newFakeStampAPI(t, hits)
	return NewManager(bee.New(srv.URL), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestCheck(t *testing.T) {
	var hits atomic.Int32
	m := newTestManager(t, &hits)
	ctx := context.Background()

	tests := []struct {
		id     string
		reason string // empty = expect nil
	}{
		{"good", ""},
		{"unknown", "batch not found on the node"},
		{"unusable", "batch is not usable (yet)"},
		{"full", "immutable batch is out of capacity"},
		{"expired", "batch has expired"},
	}
	for _, tc := range tests {
		err := m.Check(ctx, tc.id)
		if tc.reason == "" {
			if err != nil {
				t.Fatalf("Check(%q) = %v, want nil", tc.id, err)
			}
			continue
		}
		var be *BatchError
		if !errors.As(err, &be) || be.Reason != tc.reason {
			t.Fatalf("Check(%q) = %v, want reason %q", tc.id, err, tc.reason)
		}
	}
}

func TestCheckAllowsWhenBeeUnreachable(t *testing.T) {
	m := NewManager(bee.New("http://127.0.0.1:1"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	// A transient failure must not block writes — only positive diagnoses do.
	if err := m.Check(context.Background(), "whatever"); err != nil {
		t.Fatalf("Check with unreachable bee = %v, want nil", err)
	}
}

func TestGetCaches(t *testing.T) {
	var hits atomic.Int32
	m := newTestManager(t, &hits)
	ctx := context.Background()

	i1, err := m.Get(ctx, "good")
	if err != nil || !i1.Usable {
		t.Fatalf("Get: %+v, %v", i1, err)
	}
	if _, err := m.Get(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("expected 1 upstream fetch, got %d", n)
	}
	if ttl := i1.TTLRemaining(); ttl <= 0 || ttl.Seconds() > 7200 {
		t.Fatalf("TTLRemaining = %v", ttl)
	}
}

func TestRatioFallback(t *testing.T) {
	i := &Info{Stamp: bee.Stamp{Utilization: 24, Depth: 21, BucketDepth: 16}}
	if r := i.Ratio(); r != 0.75 {
		t.Fatalf("Ratio = %v, want 0.75", r)
	}
}
