package api_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/petfold/s3warm/internal/api"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/config"
	"github.com/petfold/s3warm/internal/manifest"
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
		if r.Header.Get("swarm-encrypt") == "true" {
			// Encrypted references are 64 bytes: hash plus decryption key.
			ref += hex.EncodeToString(sum[:])
		}
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
	mux.HandleFunc("GET /stamps/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		json.NewEncoder(w).Encode(map[string]any{
			"batchID": id, "exists": true, "usable": id != unusableBatch,
			"utilization": 8, "utilizationRatio": 0.25,
			"depth": 21, "bucketDepth": 16, "amount": "1000",
			"batchTTL": 7200, "immutableFlag": false,
		})
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
		BeeAPI:  fakeBee.URL,
		BatchID: testBatch,
		Region:  "us-east-1",
		Ack:     "node",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := store.NewMemory()
	beeClient := bee.New(fakeBee.URL)
	commits := manifest.NewCommitter(st, beeClient, testBatch, true, nil, logger)
	srv := httptest.NewServer(api.New(cfg, st, beeClient, nil, commits, logger))
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

var (
	testBatch     = strings.Repeat("ab", 32)
	unusableBatch = strings.Repeat("00", 32)
)

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
	if got := resp.Header.Get("x-swarm-postage-batch-id"); got != testBatch {
		t.Fatalf("x-swarm-postage-batch-id = %q", got)
	}
	if ttl, err := strconv.Atoi(resp.Header.Get("x-swarm-batch-ttl")); err != nil || ttl <= 0 || ttl > 7200 {
		t.Fatalf("x-swarm-batch-ttl = %q, %v", resp.Header.Get("x-swarm-batch-ttl"), err)
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
	if got := resp.Header.Get("x-swarm-postage-batch-id"); got != testBatch {
		t.Fatalf("GET x-swarm-postage-batch-id = %q", got)
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

func TestConditionalPut(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/cond", nil, nil, http.StatusOK).Body.Close()

	// Create-only succeeds, then fails on the existing key.
	do(t, http.MethodPut, base+"/cond/k", strings.NewReader("v1"),
		map[string]string{"If-None-Match": "*"}, http.StatusOK).Body.Close()
	do(t, http.MethodPut, base+"/cond/k", strings.NewReader("v2"),
		map[string]string{"If-None-Match": "*"}, http.StatusPreconditionFailed).Body.Close()

	// If-Match with the current ETag succeeds; a stale one fails.
	do(t, http.MethodPut, base+"/cond/k", strings.NewReader("v2"),
		map[string]string{"If-Match": `"` + md5hex("v1") + `"`}, http.StatusOK).Body.Close()
	do(t, http.MethodPut, base+"/cond/k", strings.NewReader("v3"),
		map[string]string{"If-Match": `"` + md5hex("v1") + `"`}, http.StatusPreconditionFailed).Body.Close()

	// Copy-source conditionals.
	do(t, http.MethodPut, base+"/cond/copy", nil, map[string]string{
		"x-amz-copy-source":          "/cond/k",
		"x-amz-copy-source-if-match": `"` + md5hex("v2") + `"`,
	}, http.StatusOK).Body.Close()
	do(t, http.MethodPut, base+"/cond/copy2", nil, map[string]string{
		"x-amz-copy-source":          "/cond/k",
		"x-amz-copy-source-if-match": `"` + md5hex("stale") + `"`,
	}, http.StatusPreconditionFailed).Body.Close()
}

func TestGetObjectAttributes(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/attrs", nil, nil, http.StatusOK).Body.Close()
	content := "attribute me"
	crc := crc32.ChecksumIEEE([]byte(content))
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc)
	cksum := base64.StdEncoding.EncodeToString(sum)
	do(t, http.MethodPut, base+"/attrs/k", strings.NewReader(content),
		map[string]string{"x-amz-checksum-crc32": cksum}, http.StatusOK).Body.Close()

	resp := do(t, http.MethodGet, base+"/attrs/k?attributes", nil,
		map[string]string{"x-amz-object-attributes": "ETag,ObjectSize,StorageClass,Checksum"}, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(body)
	for _, want := range []string{
		"<ETag>" + md5hex(content) + "</ETag>",
		fmt.Sprintf("<ObjectSize>%d</ObjectSize>", len(content)),
		"<StorageClass>STANDARD</StorageClass>",
		"<ChecksumCRC32>" + cksum + "</ChecksumCRC32>",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %s in %s", want, out)
		}
	}
	// Unselected attributes are omitted.
	resp = do(t, http.MethodGet, base+"/attrs/k?attributes", nil,
		map[string]string{"x-amz-object-attributes": "ObjectSize"}, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(body), "ETag") {
		t.Fatalf("unrequested ETag present: %s", body)
	}
	// Missing selection header is an error.
	do(t, http.MethodGet, base+"/attrs/k?attributes", nil, nil, http.StatusBadRequest).Body.Close()
}

func TestCORS(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/corsy", nil, nil, http.StatusOK).Body.Close()

	// No config yet.
	do(t, http.MethodGet, base+"/corsy?cors", nil, nil, http.StatusNotFound).Body.Close()

	cfg := `<CORSConfiguration>
		<CORSRule><AllowedOrigin>prefix*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><ExposeHeader>x-amz-meta-x</ExposeHeader></CORSRule>
		<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>PUT</AllowedMethod></CORSRule>
	</CORSConfiguration>`
	do(t, http.MethodPut, base+"/corsy?cors", strings.NewReader(cfg), nil, http.StatusOK).Body.Close()

	// Actual request with a matching origin gets decorated.
	resp := do(t, http.MethodGet, base+"/corsy?list-type=2", nil,
		map[string]string{"Origin": "prefix.example"}, http.StatusOK)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "prefix.example" {
		t.Fatalf("allow-origin = %q", got)
	}
	if got := resp.Header.Get("Access-Control-Expose-Headers"); got != "x-amz-meta-x" {
		t.Fatalf("expose-headers = %q", got)
	}
	// Wildcard rule answers with a literal *.
	resp = do(t, http.MethodPut, base+"/corsy/obj", strings.NewReader("x"),
		map[string]string{"Origin": "anything.example"}, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("wildcard allow-origin = %q", got)
	}
	// Non-matching origin: no headers, request unharmed.
	resp = do(t, http.MethodGet, base+"/corsy?list-type=2", nil,
		map[string]string{"Origin": "elsewhere"}, http.StatusOK)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unexpected CORS headers for non-matching origin")
	}

	// Preflights: match → 200, mismatch → 403, no origin/method → 400.
	resp = do(t, http.MethodOptions, base+"/corsy/obj", nil,
		map[string]string{"Origin": "prefix.app", "Access-Control-Request-Method": "GET"}, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET" {
		t.Fatalf("preflight allow-methods = %q", got)
	}
	do(t, http.MethodOptions, base+"/corsy/obj", nil,
		map[string]string{"Origin": "elsewhere", "Access-Control-Request-Method": "DELETE"}, http.StatusForbidden).Body.Close()
	do(t, http.MethodOptions, base+"/corsy/obj", nil,
		map[string]string{"Origin": "prefix.app"}, http.StatusBadRequest).Body.Close()

	// Delete config: preflights forbidden again.
	do(t, http.MethodDelete, base+"/corsy?cors", nil, nil, http.StatusNoContent).Body.Close()
	do(t, http.MethodOptions, base+"/corsy/obj", nil,
		map[string]string{"Origin": "prefix.app", "Access-Control-Request-Method": "GET"}, http.StatusForbidden).Body.Close()
}

func TestVersioningLifecycle(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/ver", nil, nil, http.StatusOK).Body.Close()

	// Enable versioning.
	cfg := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	do(t, http.MethodPut, base+"/ver?versioning", strings.NewReader(cfg), nil, http.StatusOK).Body.Close()
	resp := do(t, http.MethodGet, base+"/ver?versioning", nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<Status>Enabled</Status>") {
		t.Fatalf("versioning config: %s", body)
	}

	// Two writes → two version ids.
	resp = do(t, http.MethodPut, base+"/ver/doc", strings.NewReader("first"), nil, http.StatusOK)
	resp.Body.Close()
	v1 := resp.Header.Get("x-amz-version-id")
	resp = do(t, http.MethodPut, base+"/ver/doc", strings.NewReader("second"), nil, http.StatusOK)
	resp.Body.Close()
	v2 := resp.Header.Get("x-amz-version-id")
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids: %q, %q", v1, v2)
	}

	// Latest and by-version reads.
	resp = do(t, http.MethodGet, base+"/ver/doc", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "second" || resp.Header.Get("x-amz-version-id") != v2 {
		t.Fatalf("latest = %q (%s)", body, resp.Header.Get("x-amz-version-id"))
	}
	resp = do(t, http.MethodGet, base+"/ver/doc?versionId="+v1, nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "first" {
		t.Fatalf("v1 body = %q", body)
	}

	// Plain delete inserts a marker; the key reads as absent.
	resp = do(t, http.MethodDelete, base+"/ver/doc", nil, nil, http.StatusNoContent)
	resp.Body.Close()
	markerID := resp.Header.Get("x-amz-version-id")
	if resp.Header.Get("x-amz-delete-marker") != "true" || markerID == "" {
		t.Fatalf("marker headers: %+v", resp.Header)
	}
	resp = do(t, http.MethodGet, base+"/ver/doc", nil, nil, http.StatusNotFound)
	resp.Body.Close()
	if resp.Header.Get("x-amz-delete-marker") != "true" {
		t.Fatal("404 without delete-marker header")
	}
	// Old versions remain readable by id.
	do(t, http.MethodGet, base+"/ver/doc?versionId="+v2, nil, nil, http.StatusOK).Body.Close()

	// The version listing shows two versions and one marker.
	resp = do(t, http.MethodGet, base+"/ver?versions", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if c := strings.Count(string(body), "<Version>"); c != 2 {
		t.Fatalf("versions = %d in %s", c, body)
	}
	if c := strings.Count(string(body), "<DeleteMarker>"); c != 1 {
		t.Fatalf("markers = %d in %s", c, body)
	}

	// Removing the marker undeletes the key.
	do(t, http.MethodDelete, base+"/ver/doc?versionId="+markerID, nil, nil, http.StatusNoContent).Body.Close()
	resp = do(t, http.MethodGet, base+"/ver/doc", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "second" {
		t.Fatalf("after undelete: %q", body)
	}

	// Permanently removing the latest promotes the previous version.
	do(t, http.MethodDelete, base+"/ver/doc?versionId="+v2, nil, nil, http.StatusNoContent).Body.Close()
	resp = do(t, http.MethodGet, base+"/ver/doc", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "first" {
		t.Fatalf("after version delete: %q", body)
	}

	// Suspend: writes take the null version id.
	cfg = `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`
	do(t, http.MethodPut, base+"/ver?versioning", strings.NewReader(cfg), nil, http.StatusOK).Body.Close()
	resp = do(t, http.MethodPut, base+"/ver/doc", strings.NewReader("suspended"), nil, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("x-amz-version-id"); got != "" {
		t.Fatalf("suspended PUT must not return a version id, got %q", got)
	}
	// But the write landed as the "null" version.
	resp = do(t, http.MethodGet, base+"/ver/doc?versionId=null", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "suspended" {
		t.Fatalf("null version body = %q", body)
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/chain", nil, nil, http.StatusOK).Body.Close()

	put := func(key, val string) {
		t.Helper()
		do(t, http.MethodPut, base+"/chain/"+key, strings.NewReader(val), nil, http.StatusOK).Body.Close()
	}
	get := func(key string, wantStatus int) string {
		t.Helper()
		resp := do(t, http.MethodGet, base+"/chain/"+key, nil, nil, wantStatus)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(b)
	}

	// State v1: two objects, snapshot it.
	put("a.txt", "version-one")
	put("b.txt", "keep-me")
	resp := do(t, http.MethodPut, base+"/chain?x-swarm-snapshot=v1", nil, nil, http.StatusOK)
	var snap struct {
		Root string `json:"root"`
		Seq  int64  `json:"seq"`
	}
	json.NewDecoder(resp.Body).Decode(&snap)
	resp.Body.Close()
	if snap.Root == "" || snap.Seq != 1 {
		t.Fatalf("snapshot v1: %+v", snap)
	}

	// HeadBucket exposes the commit head.
	resp = do(t, http.MethodHead, base+"/chain", nil, nil, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("x-swarm-bucket-root"); got != snap.Root {
		t.Fatalf("head root = %q, want %q", got, snap.Root)
	}

	// Mutate: overwrite, delete, add — then snapshot v2.
	put("a.txt", "version-two")
	do(t, http.MethodDelete, base+"/chain/b.txt", nil, nil, http.StatusNoContent).Body.Close()
	put("c.txt", "newcomer")
	resp = do(t, http.MethodPut, base+"/chain?x-swarm-snapshot=v2", nil, nil, http.StatusOK)
	var snap2 struct {
		Root string `json:"root"`
		Seq  int64  `json:"seq"`
	}
	json.NewDecoder(resp.Body).Decode(&snap2)
	resp.Body.Close()
	if snap2.Root == snap.Root || snap2.Seq != 2 {
		t.Fatalf("snapshot v2: %+v", snap2)
	}

	// Both snapshots listed.
	resp = do(t, http.MethodGet, base+"/chain?x-swarm-snapshot", nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `"v1"`) || !strings.Contains(string(body), `"v2"`) {
		t.Fatalf("snapshot list: %s", body)
	}

	// Rollback to v1 by label: the whole bucket state returns atomically.
	do(t, http.MethodPost, base+"/chain?x-swarm-restore=v1", nil, nil, http.StatusOK).Body.Close()
	if got := get("a.txt", http.StatusOK); got != "version-one" {
		t.Fatalf("after restore a.txt = %q", got)
	}
	if got := get("b.txt", http.StatusOK); got != "keep-me" {
		t.Fatalf("after restore b.txt = %q", got)
	}
	get("c.txt", http.StatusNotFound)

	// Roll forward to v2 by raw commit root.
	do(t, http.MethodPost, base+"/chain?x-swarm-restore="+snap2.Root, nil, nil, http.StatusOK).Body.Close()
	if got := get("a.txt", http.StatusOK); got != "version-two" {
		t.Fatalf("after re-restore a.txt = %q", got)
	}
	get("b.txt", http.StatusNotFound)
	if got := get("c.txt", http.StatusOK); got != "newcomer" {
		t.Fatalf("after re-restore c.txt = %q", got)
	}
}

func TestSSE(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/sse", nil, nil, http.StatusOK).Body.Close()

	// Per-request SSE-S3: encrypted, reference suppressed, header echoed.
	resp := do(t, http.MethodPut, base+"/sse/secret", strings.NewReader("hidden"),
		map[string]string{"x-amz-server-side-encryption": "AES256"}, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("x-amz-server-side-encryption"); got != "AES256" {
		t.Fatalf("SSE header = %q", got)
	}
	if resp.Header.Get("x-swarm-reference") != "" {
		t.Fatal("encrypted reference must not be exposed")
	}
	resp = do(t, http.MethodGet, base+"/sse/secret", nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hidden" || resp.Header.Get("x-amz-server-side-encryption") != "AES256" ||
		resp.Header.Get("x-swarm-reference") != "" {
		t.Fatalf("encrypted GET: body=%q sse=%q ref=%q", body,
			resp.Header.Get("x-amz-server-side-encryption"), resp.Header.Get("x-swarm-reference"))
	}

	// SSE-KMS and SSE-C are rejected.
	do(t, http.MethodPut, base+"/sse/kms", strings.NewReader("x"),
		map[string]string{"x-amz-server-side-encryption": "aws:kms"}, http.StatusNotImplemented).Body.Close()

	// Bucket default encryption.
	do(t, http.MethodGet, base+"/sse?encryption", nil, nil, http.StatusNotFound).Body.Close()
	sseCfg := `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`
	do(t, http.MethodPut, base+"/sse?encryption", strings.NewReader(sseCfg), nil, http.StatusOK).Body.Close()
	resp = do(t, http.MethodGet, base+"/sse?encryption", nil, nil, http.StatusOK)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<SSEAlgorithm>AES256</SSEAlgorithm>") {
		t.Fatalf("GetBucketEncryption: %s", body)
	}
	// Plain PUT now encrypts by default.
	resp = do(t, http.MethodPut, base+"/sse/defaulted", strings.NewReader("x"), nil, http.StatusOK)
	resp.Body.Close()
	if resp.Header.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatal("bucket default encryption not applied")
	}
	// Clear it again.
	do(t, http.MethodDelete, base+"/sse?encryption", nil, nil, http.StatusNoContent).Body.Close()
	resp = do(t, http.MethodPut, base+"/sse/plain", strings.NewReader("x"), nil, http.StatusOK)
	resp.Body.Close()
	if resp.Header.Get("x-amz-server-side-encryption") != "" {
		t.Fatal("encryption applied after config delete")
	}
}

func TestChecksums(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/cksum", nil, nil, http.StatusOK).Body.Close()

	content := "checksum me"
	crc := crc32.ChecksumIEEE([]byte(content))
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc)
	good := base64.StdEncoding.EncodeToString(sum)

	// Correct checksum accepted and echoed.
	resp := do(t, http.MethodPut, base+"/cksum/k", strings.NewReader(content),
		map[string]string{"x-amz-checksum-crc32": good}, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("x-amz-checksum-crc32"); got != good {
		t.Fatalf("PUT checksum echo = %q, want %q", got, good)
	}

	// Returned on GET only with checksum mode enabled.
	resp = do(t, http.MethodGet, base+"/cksum/k", nil, nil, http.StatusOK)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.Header.Get("x-amz-checksum-crc32") != "" {
		t.Fatal("checksum returned without checksum mode")
	}
	resp = do(t, http.MethodGet, base+"/cksum/k", nil,
		map[string]string{"x-amz-checksum-mode": "ENABLED"}, http.StatusOK)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := resp.Header.Get("x-amz-checksum-crc32"); got != good {
		t.Fatalf("GET checksum = %q, want %q", got, good)
	}

	// Wrong checksum rejected, object untouched.
	resp = do(t, http.MethodPut, base+"/cksum/k2", strings.NewReader(content),
		map[string]string{"x-amz-checksum-crc32": "AAAAAA=="}, http.StatusBadRequest)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<Code>BadDigest</Code>") {
		t.Fatalf("bad checksum error: %s", body)
	}
	do(t, http.MethodGet, base+"/cksum/k2", nil, nil, http.StatusNotFound).Body.Close()
}

func TestPutRejectsBadBatch(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/badbatch", nil, nil, http.StatusOK).Body.Close()

	// A positively-diagnosed batch problem must fail the PUT synchronously
	// (design §6 ack policy) with the SwarmPostageError extension code.
	resp := do(t, http.MethodPut, base+"/badbatch/key.txt", strings.NewReader("data"),
		map[string]string{"x-swarm-postage-batch-id": unusableBatch}, http.StatusPaymentRequired)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<Code>SwarmPostageError</Code>") {
		t.Fatalf("expected SwarmPostageError, got: %s", body)
	}
}

func TestBucketBasics(t *testing.T) {
	base := newGateway(t)

	// Invalid names rejected.
	resp := do(t, http.MethodPut, base+"/UPPER", nil, nil, http.StatusBadRequest)
	resp.Body.Close()
	resp = do(t, http.MethodPut, base+"/xy", nil, nil, http.StatusBadRequest)
	resp.Body.Close()

	do(t, http.MethodPut, base+"/valid-bucket", nil, nil, http.StatusOK).Body.Close()
	// Re-creating an owned bucket succeeds in us-east-1, as on AWS.
	do(t, http.MethodPut, base+"/valid-bucket", nil, nil, http.StatusOK).Body.Close()
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

}

func TestMultipartRoundTrip(t *testing.T) {
	base := newGateway(t)
	do(t, http.MethodPut, base+"/mpu", nil, nil, http.StatusOK).Body.Close()

	// Initiate.
	resp := do(t, http.MethodPost, base+"/mpu/big.bin?uploads", nil,
		map[string]string{"Content-Type": "application/x-thing", "x-amz-meta-src": "mpu"}, http.StatusOK)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&initiated); err != nil || initiated.UploadID == "" {
		t.Fatalf("initiate: %v (%+v)", err, initiated)
	}
	resp.Body.Close()
	uid := initiated.UploadID

	// Two parts: 5 MiB + a small tail crossing a part boundary on read.
	part1 := bytes.Repeat([]byte("s3warm-part-one!"), 5*1024*1024/16)
	part2 := []byte("tail-of-the-object")
	etag1, etag2 := md5hex(string(part1)), md5hex(string(part2))

	resp = do(t, http.MethodPut, base+"/mpu/big.bin?partNumber=1&uploadId="+uid,
		bytes.NewReader(part1), nil, http.StatusOK)
	resp.Body.Close()
	if got := resp.Header.Get("ETag"); got != `"`+etag1+`"` {
		t.Fatalf("part 1 ETag = %s", got)
	}
	do(t, http.MethodPut, base+"/mpu/big.bin?partNumber=2&uploadId="+uid,
		bytes.NewReader(part2), nil, http.StatusOK).Body.Close()

	// ListParts sees both.
	resp = do(t, http.MethodGet, base+"/mpu/big.bin?uploadId="+uid, nil, nil, http.StatusOK)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "<PartNumber>1</PartNumber>") ||
		!strings.Contains(string(body), "<PartNumber>2</PartNumber>") {
		t.Fatalf("ListParts: %s", body)
	}

	// Wrong ETag → InvalidPart; wrong order → InvalidPartOrder.
	complete := func(payload string, wantStatus int) string {
		t.Helper()
		resp := do(t, http.MethodPost, base+"/mpu/big.bin?uploadId="+uid,
			strings.NewReader(payload), nil, wantStatus)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(b)
	}
	if out := complete(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"deadbeef"</ETag></Part></CompleteMultipartUpload>`,
		http.StatusBadRequest); !strings.Contains(out, "<Code>InvalidPart</Code>") {
		t.Fatalf("wrong etag: %s", out)
	}
	if out := complete(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag2, etag1),
		http.StatusBadRequest); !strings.Contains(out, "<Code>InvalidPartOrder</Code>") {
		t.Fatalf("wrong order: %s", out)
	}

	// Complete: S3 multipart ETag algebra.
	sum1, _ := hex.DecodeString(etag1)
	sum2, _ := hex.DecodeString(etag2)
	concat := md5.Sum(append(sum1, sum2...))
	wantETag := hex.EncodeToString(concat[:]) + "-2"
	out := complete(fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag1, etag2),
		http.StatusOK)
	if !strings.Contains(out, wantETag) {
		t.Fatalf("complete ETag: want %s in %s", wantETag, out)
	}

	// Full GET: stitched content matches.
	want := append(append([]byte{}, part1...), part2...)
	resp = do(t, http.MethodGet, base+"/mpu/big.bin", nil, nil, http.StatusOK)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, want) {
		t.Fatalf("stitched GET: %d bytes, want %d", len(got), len(want))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-thing" {
		t.Fatalf("Content-Type = %q", ct)
	}

	// Range GET across the part boundary.
	boundary := int64(len(part1))
	rangeSpec := fmt.Sprintf("bytes=%d-%d", boundary-4, boundary+3)
	resp = do(t, http.MethodGet, base+"/mpu/big.bin", nil,
		map[string]string{"Range": rangeSpec}, http.StatusPartialContent)
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(got, want[boundary-4:boundary+4]) {
		t.Fatalf("boundary range = %q, want %q", got, want[boundary-4:boundary+4])
	}

	// Abort a second upload; it disappears.
	resp = do(t, http.MethodPost, base+"/mpu/other.bin?uploads", nil, nil, http.StatusOK)
	var second struct {
		UploadID string `xml:"UploadId"`
	}
	xml.NewDecoder(resp.Body).Decode(&second)
	resp.Body.Close()
	do(t, http.MethodDelete, base+"/mpu/other.bin?uploadId="+second.UploadID, nil, nil, http.StatusNoContent).Body.Close()
	do(t, http.MethodDelete, base+"/mpu/other.bin?uploadId="+second.UploadID, nil, nil, http.StatusNotFound).Body.Close()
}
