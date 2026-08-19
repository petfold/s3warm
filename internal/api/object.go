package api

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/auth"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/metrics"
	"github.com/petfold/s3warm/internal/store"
)

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	b, err := s.store.GetBucket(ctx, bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	// Conditional PUT (design §10): the authoritative check runs atomically
	// with the index write; this early check just fails fast before bytes move.
	var cond *store.PutCondition
	if im, inm := r.Header.Get("If-Match"), r.Header.Get("If-None-Match"); im != "" || inm != "" {
		cond = &store.PutCondition{IfMatch: trimETag(im), IfNoneMatch: trimETag(inm)}
		cur, err := s.store.GetObject(ctx, bucket, key)
		exists := err == nil
		var curETag string
		if exists {
			curETag = cur.ETag
		}
		if !exists && cond.IfMatch != "" {
			// If-Match against a nonexistent key is NoSuchKey, not 412.
			s.writeError(w, r, errNoSuchKey)
			return
		}
		if !cond.Ok(exists, curETag) {
			s.writeError(w, r, errPreconditionFailed)
			return
		}
	}

	batch := s.resolveBatch(r, b)
	// Synchronous batch validation (design §6, §9): a positively-diagnosed
	// batch problem must surface on the PUT, never after the ack.
	if batch != "" && r.ContentLength != 0 {
		if err := s.stamps.Check(ctx, batch); err != nil {
			s.writeError(w, r, errSwarmPostage.withMessage(err.Error()))
			return
		}
	}

	encrypt, apiErr := s.resolveSSE(r, b)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}

	res, apiErr := s.uploadBody(r, batch, r.Header.Get("x-amz-storage-class"), encrypt)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}

	obj := store.Object{
		Bucket:            bucket,
		Key:               key,
		SwarmRef:          res.Ref,
		BatchID:           batch,
		Size:              res.Size,
		ETag:              res.ETag,
		ContentType:       r.Header.Get("Content-Type"),
		ContentEncoding:   storedContentEncoding(r.Header.Get("Content-Encoding")),
		StorageClass:      storageClassOf(r.Header.Get("x-amz-storage-class")),
		UserMetadata:      userMetadata(r.Header),
		LastModified:      time.Now().UTC(),
		ChecksumAlgorithm: res.ChecksumAlgorithm,
		Checksum:          res.Checksum,
		Encrypted:         encrypt,
	}
	// On a precondition race the object is not indexed; the stray stamped
	// bytes simply expire (design §6).
	if err := s.store.PutObject(ctx, obj, cond); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	if res.Ref != "" && !encrypt {
		// For encrypted objects the 64-byte reference embeds the decryption
		// key, so it stays private (design §12).
		w.Header().Set("x-swarm-reference", res.Ref)
	}
	if encrypt {
		w.Header().Set("x-amz-server-side-encryption", "AES256")
	}
	s.setBatchHeaders(ctx, w.Header(), batch)
	if res.Checksum != "" {
		w.Header().Set("x-amz-checksum-"+strings.ToLower(res.ChecksumAlgorithm), res.Checksum)
	}
	w.Header().Set("ETag", `"`+res.ETag+`"`)
	w.WriteHeader(http.StatusOK)
	metrics.ObjectBytesIn.Add(float64(res.Size))
	s.commits.Notify(bucket)
}

// resolveSSE decides whether a write is encrypted (design §12): request SSE
// header, then bucket default, then the gateway-wide -encrypt flag. SSE-C
// and SSE-KMS are rejected.
func (s *Server) resolveSSE(r *http.Request, b *store.Bucket) (bool, *apiError) {
	h := r.Header
	if h.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		h.Get("x-amz-server-side-encryption-customer-key") != "" {
		e := errNotImplemented.withMessage("SSE-C is not supported; use SSE-S3 (AES256)")
		return false, &e
	}
	switch sse := h.Get("x-amz-server-side-encryption"); sse {
	case "AES256":
		return true, nil
	case "":
		return s.cfg.Encrypt || b.Encryption == "AES256", nil
	default: // aws:kms, aws:kms:dsse
		e := errNotImplemented.withMessage("only SSE-S3 (AES256) is supported, got " + sse)
		return false, &e
	}
}

// resolveBatch picks the postage batch for a write: request header override,
// then bucket default, then gateway default (design §9).
func (s *Server) resolveBatch(r *http.Request, b *store.Bucket) string {
	if batch := r.Header.Get("x-swarm-postage-batch-id"); batch != "" {
		return batch
	}
	if b.BatchID != "" {
		return b.BatchID
	}
	return s.cfg.BatchID
}

// uploadResult is the outcome of streaming a request body to Swarm.
type uploadResult struct {
	Ref               string // empty for zero-byte bodies
	Size              int64
	ETag              string // hex MD5, unquoted
	ChecksumAlgorithm string // uppercase, when a flexible checksum was requested
	Checksum          string // base64
}

// uploadBody streams r's body to Bee while hashing — MD5 for the S3 ETag,
// SHA-256 to enforce the signed x-amz-content-sha256, plus any requested
// flexible checksum — decoding and verifying aws-chunked framing (signed
// chunks and trailers) on the way. Zero-byte bodies are not uploaded. On
// error nothing is indexed by the caller; stray stamped bytes expire with
// their batch (design §6).
func (s *Server) uploadBody(r *http.Request, batch, storageClass string, encrypt bool) (*uploadResult, *apiError) {
	md5h := md5.New()
	writers := []io.Writer{md5h}
	rawSHA := r.Header.Get("X-Amz-Content-Sha256")
	expectedSHA := strings.ToLower(rawSHA)
	var sha hash.Hash
	if len(expectedSHA) == 64 {
		sha = sha256.New()
		writers = append(writers, sha)
	}

	ck, apiErr := parseChecksumRequest(r.Header)
	if apiErr != nil {
		return nil, apiErr
	}
	var ckHash hash.Hash
	if ck != nil {
		ckHash = checksumAlgorithms[ck.alg]()
		writers = append(writers, ckHash)
	}

	body := io.Reader(r.Body)
	length := r.ContentLength
	var chunked *auth.ChunkedReader
	if isAwsChunked(r) {
		var sc *auth.StreamContext
		if id := identityFrom(r.Context()); id != nil {
			sc = id.Stream
		}
		if strings.HasPrefix(rawSHA, "STREAMING-AWS4-HMAC-SHA256") && sc == nil {
			e := errInvalidRequest.withMessage("signed streaming payload requires SigV4 authentication")
			return nil, &e
		}
		chunked = auth.NewChunkedReader(r.Body, sc)
		body = chunked
		length = -1
		if dcl := r.Header.Get("x-amz-decoded-content-length"); dcl != "" {
			if n, err := strconv.ParseInt(dcl, 10, 64); err == nil {
				length = n
			}
		}
	}
	counted := &countingReader{r: io.TeeReader(body, io.MultiWriter(writers...))}

	chunkedErr := func() *apiError {
		if chunked == nil || chunked.Err() == nil {
			return nil
		}
		if errors.Is(chunked.Err(), auth.ErrChunkSignature) {
			return &apiError{"SignatureDoesNotMatch", http.StatusForbidden, chunked.Err().Error()}
		}
		e := errInvalidRequest.withMessage(chunked.Err().Error())
		return &e
	}

	var ref string
	if length != 0 {
		var err error
		ref, err = s.bee.UploadBytes(r.Context(), counted, bee.UploadOptions{
			BatchID:         batch,
			Encrypt:         encrypt,
			RedundancyLevel: s.redundancyFor(storageClass),
			Deferred:        s.cfg.Ack != "network",
			ContentLength:   length,
		})
		if err != nil {
			if e := chunkedErr(); e != nil {
				return nil, e
			}
			e := beeError(err)
			return nil, &e
		}
	} else if chunked != nil {
		// Zero-byte chunked body: still drain the framing so signatures and
		// trailers are verified.
		if _, err := io.Copy(io.Discard, counted); err != nil {
			if e := chunkedErr(); e != nil {
				return nil, e
			}
			e := errInvalidRequest.withMessage(err.Error())
			return nil, &e
		}
	}

	md5sum := md5h.Sum(nil)
	if sha != nil {
		if got := hex.EncodeToString(sha.Sum(nil)); got != expectedSHA {
			e := errSHA256Mismatch
			return nil, &e
		}
	}
	if cmd5 := r.Header.Get("Content-MD5"); cmd5 != "" {
		want, err := base64.StdEncoding.DecodeString(cmd5)
		if err != nil || !bytes.Equal(want, md5sum) {
			e := errBadDigest
			return nil, &e
		}
	}

	res := &uploadResult{Ref: ref, Size: counted.n, ETag: hex.EncodeToString(md5sum)}
	if ck != nil {
		digest := base64.StdEncoding.EncodeToString(ckHash.Sum(nil))
		expected := ck.expected
		if ck.inTrailer && chunked != nil {
			expected = chunked.Trailer().Get("x-amz-checksum-" + ck.alg)
		}
		if expected != "" && expected != digest {
			e := errBadDigest.withMessage("The " + strings.ToUpper(ck.alg) + " you specified did not match the calculated checksum.")
			return nil, &e
		}
		res.ChecksumAlgorithm = strings.ToUpper(ck.alg)
		res.Checksum = digest
	}
	return res, nil
}

// storedContentEncoding strips the aws-chunked transport token from a
// Content-Encoding list, preserving the rest — S3 stores only the content's
// own encodings.
func storedContentEncoding(header string) string {
	if header == "" {
		return ""
	}
	var kept []string
	for _, enc := range strings.Split(header, ",") {
		if e := strings.TrimSpace(enc); e != "" && !strings.EqualFold(e, "aws-chunked") {
			kept = append(kept, e)
		}
	}
	return strings.Join(kept, ", ")
}

// isAwsChunked reports whether the request body uses aws-chunked framing.
// Only the STREAMING-* payload type is authoritative: clients may send
// Content-Encoding: aws-chunked without actually framing the body.
func isAwsChunked(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-")
}

// setBatchHeaders surfaces batch identity and estimated remaining life
// (design §9: expiry is a real S3 difference, so it is made visible).
func (s *Server) setBatchHeaders(ctx context.Context, h http.Header, batch string) {
	if batch == "" {
		return
	}
	h.Set("x-swarm-postage-batch-id", batch)
	if info, err := s.stamps.Get(ctx, batch); err == nil {
		h.Set("x-swarm-batch-ttl", strconv.FormatInt(int64(info.TTLRemaining().Seconds()), 10))
	}
}

// handleGetObject serves GetObject (withBody) and HeadObject.
func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string, withBody bool) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	obj, err := s.store.GetObject(ctx, bucket, key)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	h := w.Header()
	h.Set("ETag", `"`+obj.ETag+`"`)
	h.Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))

	if code, ok := checkConditionals(r, obj); !ok {
		if code == http.StatusNotModified {
			w.WriteHeader(code)
		} else {
			s.writeError(w, r, errPreconditionFailed)
		}
		return
	}

	h.Set("Accept-Ranges", "bytes")
	ct := obj.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	h.Set("Content-Type", ct)
	if obj.ContentEncoding != "" {
		h.Set("Content-Encoding", obj.ContentEncoding)
	}
	// Response header overrides — authenticated-only in S3 and covered by
	// the signature; commonly carried by presigned URLs.
	for param, hdr := range map[string]string{
		"response-content-type":        "Content-Type",
		"response-content-language":    "Content-Language",
		"response-expires":             "Expires",
		"response-cache-control":       "Cache-Control",
		"response-content-disposition": "Content-Disposition",
		"response-content-encoding":    "Content-Encoding",
	} {
		if val := r.URL.Query().Get(param); val != "" {
			h.Set(hdr, val)
		}
	}
	h.Set("x-amz-storage-class", obj.StorageClass)
	for k, v := range obj.UserMetadata {
		// Direct map write: Go's Set would canonicalize to X-Amz-Meta-Foo and
		// clients would round-trip the metadata key as "Foo"; AWS emits
		// all-lowercase.
		h["x-amz-meta-"+k] = []string{v}
	}
	if obj.SwarmRef != "" && !obj.Encrypted {
		h.Set("x-swarm-reference", obj.SwarmRef)
	}
	if obj.Encrypted {
		h.Set("x-amz-server-side-encryption", "AES256")
	}
	// The stored checksum covers the full object, so it must not accompany
	// partial (ranged or part-numbered) responses — clients validate response
	// bodies against it.
	if obj.Checksum != "" && strings.EqualFold(r.Header.Get("x-amz-checksum-mode"), "ENABLED") &&
		r.Header.Get("Range") == "" && r.URL.Query().Get("partNumber") == "" {
		h.Set("x-amz-checksum-"+strings.ToLower(obj.ChecksumAlgorithm), obj.Checksum)
	}
	s.setBatchHeaders(ctx, h, obj.BatchID)

	if pn := r.URL.Query().Get("partNumber"); pn != "" {
		// GetObject/HeadObject by part: rewrite into the part's byte range.
		if apiErr := preparePartRequest(r, h, obj, pn); apiErr != nil {
			s.writeError(w, r, *apiErr)
			return
		}
	}

	if withBody && len(obj.Parts) > 0 {
		s.serveComposite(w, r, obj)
		return
	}
	if !withBody || obj.SwarmRef == "" {
		h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	resp, err := s.bee.DownloadBytes(ctx, obj.SwarmRef, r.Header.Get("Range"))
	if err != nil {
		s.writeError(w, r, beeError(err))
		return
	}
	defer resp.Body.Close()
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		h.Set("Content-Length", cl)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		h.Set("Content-Range", cr)
	}
	w.WriteHeader(resp.StatusCode) // 200, or 206 for ranges
	n, _ := io.Copy(w, resp.Body)  // client may hang up mid-stream
	metrics.ObjectBytesOut.Add(float64(n))
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// Deleting an absent key succeeds, as in S3. The bytes on Swarm expire
	// with their postage batch (design §6).
	if err := s.store.DeleteObject(ctx, bucket, key); err != nil && !errors.Is(err, store.ErrObjectNotFound) {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
	s.commits.Notify(bucket)
}

func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Objects) == 0 {
		s.writeError(w, r, errMalformedXML)
		return
	}
	if len(req.Objects) > 1000 {
		s.writeError(w, r, errInvalidArgument.withMessage("the batch delete request contains more than 1000 keys"))
		return
	}

	result := deleteResult{Xmlns: s3Xmlns}
	for _, o := range req.Objects {
		if err := s.store.DeleteObject(ctx, bucket, o.Key); err != nil && !errors.Is(err, store.ErrObjectNotFound) {
			result.Errors = append(result.Errors, deleteError{Key: o.Key, Code: "InternalError", Message: err.Error()})
			continue
		}
		if !req.Quiet {
			result.Deleted = append(result.Deleted, deletedEntry{Key: o.Key})
		}
	}
	writeXML(w, http.StatusOK, result)
	s.commits.Notify(bucket)
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	if strings.Contains(r.Header.Get("x-amz-copy-source"), "?versionId=") {
		s.notImplemented(w, r, "CopyObject with versionId")
		return
	}
	srcBucket, srcKey, apiErr := parseCopySource(r)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	srcObj, err := s.store.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if !copySourceConditionals(r, srcObj) {
		s.writeError(w, r, errPreconditionFailed)
		return
	}

	directive := strings.ToUpper(r.Header.Get("x-amz-metadata-directive"))
	if directive == "" {
		directive = "COPY"
	}
	if srcBucket == bucket && srcKey == key && directive != "REPLACE" {
		s.writeError(w, r, errInvalidRequest.withMessage(
			"this copy request is illegal because it is trying to copy an object to itself without changing the object's metadata"))
		return
	}

	// Server-side copy on a content-addressed store is a metadata operation:
	// the new key points at the same Swarm reference (design §6).
	obj := *srcObj
	obj.Bucket, obj.Key = bucket, key
	obj.LastModified = time.Now().UTC()
	if directive == "REPLACE" {
		obj.ContentType = r.Header.Get("Content-Type")
		obj.UserMetadata = userMetadata(r.Header)
		if sc := r.Header.Get("x-amz-storage-class"); sc != "" {
			obj.StorageClass = storageClassOf(sc)
		}
	}
	if err := s.store.PutObject(ctx, obj, nil); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Xmlns,
		LastModified: xmlTime(obj.LastModified),
		ETag:         `"` + obj.ETag + `"`,
	})
	s.commits.Notify(bucket)
}

// checkConditionals evaluates If-Match / If-None-Match / If-Modified-Since /
// If-Unmodified-Since. Returns (status, false) when a precondition fails.
func checkConditionals(r *http.Request, obj *store.Object) (int, bool) {
	etag := `"` + obj.ETag + `"`
	modified := obj.LastModified.Truncate(time.Second)

	if im := r.Header.Get("If-Match"); im != "" && !etagMatch(im, etag) {
		return http.StatusPreconditionFailed, false
	}
	if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && modified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagMatch(inm, etag) {
			return http.StatusNotModified, false
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !modified.After(t) {
			return http.StatusNotModified, false
		}
	}
	return 0, true
}

// serveComposite streams a multipart (composite) object by mapping the
// requested byte range onto the ordered part list and issuing consecutive
// sub-range reads against Bee — the join happens here, fully streaming
// (design §7).
func (s *Server) serveComposite(w http.ResponseWriter, r *http.Request, obj *store.Object) {
	ctx := r.Context()
	h := w.Header()

	start, length := int64(0), obj.Size
	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		st, en, ok, satisfiable := parseRangeSpec(spec, obj.Size)
		if !satisfiable {
			s.writeError(w, r, errInvalidRange)
			return
		}
		if ok { // unparseable/multi-range specs serve the full body, as S3 does
			start, length = st, en-st+1
			status = http.StatusPartialContent
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", st, en, obj.Size))
		}
	}
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(status)

	var served, partOffset int64
	remaining := length
	for _, p := range obj.Parts {
		if remaining <= 0 {
			break
		}
		partStart, partEnd := partOffset, partOffset+p.Size-1
		partOffset += p.Size
		if p.Size == 0 || partEnd < start {
			continue
		}
		lo := max(start-partStart, 0)
		hi := min(p.Size-1, lo+remaining-1)
		resp, err := s.bee.DownloadBytes(ctx, p.SwarmRef, fmt.Sprintf("bytes=%d-%d", lo, hi))
		if err != nil {
			// Headers are already written; the truncated body is all we can
			// signal with. Log loudly.
			s.log.Error("composite read failed mid-stream",
				"bucket", obj.Bucket, "key", obj.Key, "part", p.PartNumber, "err", err)
			return
		}
		n, err := io.Copy(w, resp.Body)
		resp.Body.Close()
		served += n
		remaining -= n
		if err != nil {
			return // client hangup or upstream failure
		}
	}
	metrics.ObjectBytesOut.Add(float64(served))
}

// preparePartRequest maps a GET/HEAD `partNumber` onto the part's byte range
// (S3 semantics: a non-multipart object is one part; out-of-range part
// numbers are 400 InvalidPart). Sets x-amz-mp-parts-count for composites.
func preparePartRequest(r *http.Request, h http.Header, obj *store.Object, pnStr string) *apiError {
	n, err := strconv.Atoi(pnStr)
	if err != nil || n < 1 || n > maxPartNumber {
		e := errInvalidArgument.withMessage("invalid partNumber")
		return &e
	}
	if len(obj.Parts) == 0 {
		if n != 1 {
			e := errInvalidPart
			return &e
		}
		if obj.Size > 0 {
			r.Header.Set("Range", "bytes=0-")
		}
		return nil
	}
	if n > len(obj.Parts) {
		e := errInvalidPart
		return &e
	}
	h.Set("x-amz-mp-parts-count", strconv.Itoa(len(obj.Parts)))
	var start int64
	for _, p := range obj.Parts[:n-1] {
		start += p.Size
	}
	if size := obj.Parts[n-1].Size; size > 0 {
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+size-1))
	}
	return nil
}

// parseRangeSpec parses a single-range header against size (end inclusive).
// ok=false means "ignore the header, serve the whole body" (S3's behavior
// for multi-range or malformed specs); satisfiable=false means 416.
func parseRangeSpec(spec string, size int64) (start, end int64, ok, satisfiable bool) {
	v, found := strings.CutPrefix(spec, "bytes=")
	if !found || strings.Contains(v, ",") {
		return 0, 0, false, true
	}
	first, last, found := strings.Cut(strings.TrimSpace(v), "-")
	if !found {
		return 0, 0, false, true
	}
	if first == "" { // suffix form: last N bytes
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil {
			return 0, 0, false, true
		}
		if n <= 0 || size == 0 {
			return 0, 0, false, false
		}
		return max(size-n, 0), size - 1, true, true
	}
	st, err := strconv.ParseInt(first, 10, 64)
	if err != nil || st < 0 {
		return 0, 0, false, true
	}
	if st >= size {
		return 0, 0, false, false
	}
	en := size - 1
	if last != "" {
		en, err = strconv.ParseInt(last, 10, 64)
		if err != nil {
			return 0, 0, false, true
		}
		if en < st {
			return 0, 0, false, true
		}
		en = min(en, size-1)
	}
	return st, en, true, true
}

// copySourceConditionals evaluates x-amz-copy-source-if-* against the
// source object; false means 412 PreconditionFailed.
func copySourceConditionals(r *http.Request, src *store.Object) bool {
	etag := `"` + src.ETag + `"`
	modified := src.LastModified.Truncate(time.Second)
	if v := r.Header.Get("x-amz-copy-source-if-match"); v != "" && !etagMatch(v, etag) {
		return false
	}
	if v := r.Header.Get("x-amz-copy-source-if-none-match"); v != "" && etagMatch(v, etag) {
		return false
	}
	if v := r.Header.Get("x-amz-copy-source-if-modified-since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && !modified.After(t) {
			return false
		}
	}
	if v := r.Header.Get("x-amz-copy-source-if-unmodified-since"); v != "" {
		if t, err := http.ParseTime(v); err == nil && modified.After(t) {
			return false
		}
	}
	return true
}

// trimETag normalizes a conditional-header ETag value: quotes stripped,
// "*" preserved.
func trimETag(v string) string {
	return strings.Trim(strings.TrimSpace(v), `"`)
}

func etagMatch(headerValue, etag string) bool {
	for _, part := range strings.Split(headerValue, ",") {
		if p := strings.TrimSpace(part); p == "*" || p == etag {
			return true
		}
	}
	return false
}

func userMetadata(h http.Header) map[string]string {
	var m map[string]string
	for name, values := range h {
		if suffix, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-"); ok && len(values) > 0 {
			if m == nil {
				m = make(map[string]string)
			}
			m[suffix] = values[0]
		}
	}
	return m
}

// redundancyFor maps an S3 storage class onto a Swarm erasure-coding level
// (design §12).
func (s *Server) redundancyFor(storageClass string) int {
	switch strings.ToUpper(storageClass) {
	case "REDUCED_REDUNDANCY", "SWARM_NONE":
		return 0
	case "SWARM_MEDIUM":
		return 1
	case "SWARM_STRONG":
		return 2
	case "SWARM_INSANE":
		return 3
	case "SWARM_PARANOID":
		return 4
	default:
		return s.cfg.RedundancyLevel
	}
}

func storageClassOf(header string) string {
	if header == "" {
		return "STANDARD"
	}
	return header
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
