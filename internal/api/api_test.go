package api_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/petfold/s3warm/internal/api"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/config"
	"github.com/petfold/s3warm/internal/store"
)

// newFakeBee is an in-memory stand-in for a Bee node: POST /bytes stores a
// blob under its sha256 (shaped like a Swarm reference), GET /bytes/{ref}
// serves it with Range support.
func newFakeBee(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	blobs := map[string][]byte{}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /bytes", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("swarm-postage-batch-id") == "" {
			w.WriteHeader(http.StatusPaymentRequired)
			io.WriteString(w, `{"code":402,"message":"batch not found"}`)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sum := sha256.Sum256(data)
		ref := hex.EncodeToString(sum[:])
		mu.Lock()
		blobs[ref] = data
		mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"reference": ref})
	})
	mux.HandleFunc("GET /bytes/{ref}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		data, ok := blobs[r.PathValue("ref")]
		mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":404,"message":"not found"}`)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// newGateway runs a gateway in anonymous mode against a fake Bee.
func newGateway(t *testing.T) string {
	t.Helper()
	fakeBee := newFakeBee(t)
	cfg := &config.Config{
		BeeAPI:   fakeBee.URL,
		BatchID:  strings.Repeat("ab", 32),
		Region:   "us-east-1",
		Deferred: true,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(api.New(cfg, store.NewMemory(), bee.New(fakeBee.URL), logger))
	t.Cleanup(srv.Close)
	return srv.URL
}

func do(t *testing.T, method, url string, body io.Reader, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	if resp.StatusCode != wantStatus {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("%s %s: status %d, want %d\nbody: %s", method, url, resp.StatusCode, wantStatus, b)
	}
	return resp
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestObjectRoundTrip(t *testing.T) {
	base := newGateway(t)

	do(t, http.MethodPut, base+"/demo", nil, nil, http.StatusOK).Body.Close()

	// PUT
	content := "hello swarm"
	resp := do(t, http.MethodPut, base+"/demo/docs/hello.txt", strings.NewReader(content),
		map[string]string{"Content-Type": "text/plain", "x-amz-meta-origin": "test"}, http.StatusOK)
	resp.Body.Close()
	if got, want := resp.Header.Get("ETag"), `"`+md5hex(content)+`"`; got != want {
		t.Fatalf("ETag = %s, want %s", got, want)
	}
	if resp.Header.Get("x-swarm-reference") == "" {
		t.Fatal("missing x-swarm-reference header")
	}

	// GET
	resp = do(t, http.MethodGet, base+"/demo/docs/hello.txt", nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != content {
		t.Fatalf("body = %q", body)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := resp.Header.Get("x-amz-meta-origin"); got != "test" {
		t.Fatalf("x-amz-meta-origin = %q", got)
	}

	// Range GET
	resp = do(t, http.MethodGet, base+"/demo/docs/hello.txt", nil,
		map[string]string{"Range": "bytes=0-4"}, http.StatusPartialContent)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" {
		t.Fatalf("range body = %q", body)
	}

	// HEAD
	resp = do(t, http.MethodHead, base+"/demo/docs/hello.txt", nil, nil, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(content)) {
		t.Fatalf("HEAD Content-Length = %q", got)
	}

	// Conditional GET
	resp = do(t, http.MethodGet, base+"/demo/docs/hello.txt", nil,
		map[string]string{"If-None-Match": `"` + md5hex(content) + `"`}, http.StatusNotModified)
	resp.Body.Close()

	// CopyObject: O(1), same reference
	resp = do(t, http.MethodPut, base+"/demo/copy.txt", nil,
		map[string]string{"x-amz-copy-source": "/demo/docs/hello.txt"}, http.StatusOK)
	resp.Body.Close()
	resp = do(t, http.MethodGet, base+"/demo/copy.txt", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != content {
		t.Fatalf("copied body = %q", body)
	}

	// DELETE, then 404 with the S3 error envelope
	do(t, http.MethodDelete, base+"/demo/docs/hello.txt", nil, nil, http.StatusNoContent).Body.Close()
	resp = do(t, http.MethodGet, base+"/demo/docs/hello.txt", nil, nil, http.StatusNotFound)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<Code>NoSuchKey</Code>") {
		t.Fatalf("expected NoSuchKey error, got: %s", body)
	}

	// Bucket not empty
	resp = do(t, http.MethodDelete, base+"/demo", nil, nil, http.StatusConflict)
	resp.Body.Close()

	// Empty it and delete
	do(t, http.MethodDelete, base+"/demo/copy.txt", nil, nil, http.StatusNoContent).Body.Close()
	do(t, http.MethodDelete, base+"/demo", nil, nil, http.StatusNoContent).Body.Close()
}

type listResult struct {
	KeyCount              int    `xml:"KeyCount"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
}

func listV2(t *testing.T, url string) listResult {
	t.Helper()
	resp := do(t, http.MethodGet, url, nil, nil, http.StatusOK)
	defer resp.Body.Close()
	var out listResult
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode list result: %v", err)
	}
	return out
}

func TestListObjectsV2DelimiterAndPagination(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/pages", nil, nil, http.StatusOK).Body.Close()
	// Zero-byte puts: indexed without touching Bee.
	for _, key := range []string{"a/1", "a/2", "b", "c/1", "c/2", "d"} {
		do(t, http.MethodPut, base+"/pages/"+key, nil, nil, http.StatusOK).Body.Close()
	}

	// Page 1: common prefix a/ + key b.
	res := listV2(t, base+"/pages?list-type=2&delimiter=/&max-keys=2")
	if res.KeyCount != 2 || !res.IsTruncated {
		t.Fatalf("page 1: KeyCount=%d IsTruncated=%v", res.KeyCount, res.IsTruncated)
	}
	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0].Prefix != "a/" {
		t.Fatalf("page 1 prefixes: %+v", res.CommonPrefixes)
	}
	if len(res.Contents) != 1 || res.Contents[0].Key != "b" {
		t.Fatalf("page 1 contents: %+v", res.Contents)
	}

	// Page 2: common prefix c/ + key d, listing complete.
	res = listV2(t, base+"/pages?list-type=2&delimiter=/&max-keys=2&continuation-token="+res.NextContinuationToken)
	if res.IsTruncated {
		t.Fatal("page 2 should not be truncated")
	}
	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0].Prefix != "c/" {
		t.Fatalf("page 2 prefixes: %+v", res.CommonPrefixes)
	}
	if len(res.Contents) != 1 || res.Contents[0].Key != "d" {
		t.Fatalf("page 2 contents: %+v", res.Contents)
	}

	// Prefix filter without delimiter.
	res = listV2(t, base+"/pages?list-type=2&prefix=a/")
	if len(res.Contents) != 2 || res.Contents[0].Key != "a/1" || res.Contents[1].Key != "a/2" {
		t.Fatalf("prefix listing: %+v", res.Contents)
	}
}

func TestDeleteObjectsBatch(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/batch", nil, nil, http.StatusOK).Body.Close()
	for _, key := range []string{"x", "y"} {
		do(t, http.MethodPut, base+"/batch/"+key, nil, nil, http.StatusOK).Body.Close()
	}
	payload := `<Delete><Object><Key>x</Key></Object><Object><Key>y</Key></Object><Object><Key>ghost</Key></Object></Delete>`
	resp := do(t, http.MethodPost, base+"/batch?delete", strings.NewReader(payload), nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{"<Key>x</Key>", "<Key>y</Key>", "<Key>ghost</Key>"} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("missing %s in: %s", want, body)
		}
	}
	do(t, http.MethodDelete, base+"/batch", nil, nil, http.StatusNoContent).Body.Close()
}

func TestBucketBasics(t *testing.T) {
	base := newGateway(t)

	// Invalid names rejected.
	resp := do(t, http.MethodPut, base+"/UPPER", nil, nil, http.StatusBadRequest)
	resp.Body.Close()
	resp = do(t, http.MethodPut, base+"/xy", nil, nil, http.StatusBadRequest)
	resp.Body.Close()

	do(t, http.MethodPut, base+"/valid-bucket", nil, nil, http.StatusOK).Body.Close()
	// Duplicate → 409.
	do(t, http.MethodPut, base+"/valid-bucket", nil, nil, http.StatusConflict).Body.Close()
	do(t, http.MethodHead, base+"/valid-bucket", nil, nil, http.StatusOK).Body.Close()
	do(t, http.MethodHead, base+"/missing-bucket", nil, nil, http.StatusNotFound).Body.Close()

	resp = do(t, http.MethodGet, base+"/", nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<Name>valid-bucket</Name>") {
		t.Fatalf("ListBuckets: %s", body)
	}

	// GetBucketLocation: us-east-1 renders as empty constraint.
	resp = do(t, http.MethodGet, base+"/valid-bucket?location", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "LocationConstraint") {
		t.Fatalf("GetBucketLocation: %s", body)
	}

	// Multipart is explicitly NotImplemented for now.
	resp = do(t, http.MethodPost, base+"/valid-bucket/big.bin?uploads", nil, nil, http.StatusNotImplemented)
	resp.Body.Close()
}
