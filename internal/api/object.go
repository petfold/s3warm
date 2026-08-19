package api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	b, err := s.store.GetBucket(ctx, bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	// Stream the body to Bee while hashing: MD5 for the S3 ETag, SHA-256 to
	// enforce the signed x-amz-content-sha256 (design §6).
	md5h := md5.New()
	writers := []io.Writer{md5h}
	expectedSHA := strings.ToLower(r.Header.Get("X-Amz-Content-Sha256"))
	var sha hash.Hash
	if len(expectedSHA) == 64 {
		sha = sha256.New()
		writers = append(writers, sha)
	}
	body := &countingReader{r: io.TeeReader(r.Body, io.MultiWriter(writers...))}

	batch := r.Header.Get("x-swarm-postage-batch-id")
	if batch == "" {
		batch = b.BatchID
	}
	if batch == "" {
		batch = s.cfg.BatchID
	}

	var ref string
	if r.ContentLength != 0 {
		ref, err = s.bee.UploadBytes(ctx, body, bee.UploadOptions{
			BatchID:         batch,
			Encrypt:         s.cfg.Encrypt,
			RedundancyLevel: s.redundancyFor(r.Header.Get("x-amz-storage-class")),
			Deferred:        s.cfg.Deferred,
			ContentLength:   r.ContentLength,
		})
		if err != nil {
			s.writeError(w, r, beeError(err))
			return
		}
	}
	// Zero-byte objects (e.g. directory markers) are indexed without a Swarm
	// upload; MD5 of the empty payload still applies.

	md5sum := md5h.Sum(nil)
	if sha != nil {
		if got := hex.EncodeToString(sha.Sum(nil)); got != expectedSHA {
			// The upload happened, but the object is not indexed; the stray
			// stamped bytes simply expire (design §6).
			s.writeError(w, r, errSHA256Mismatch)
			return
		}
	}
	if cmd5 := r.Header.Get("Content-MD5"); cmd5 != "" {
		want, err := base64.StdEncoding.DecodeString(cmd5)
		if err != nil || !bytes.Equal(want, md5sum) {
			s.writeError(w, r, errBadDigest)
			return
		}
	}

	etag := hex.EncodeToString(md5sum)
	obj := store.Object{
		Bucket:       bucket,
		Key:          key,
		SwarmRef:     ref,
		Size:         body.n,
		ETag:         etag,
		ContentType:  r.Header.Get("Content-Type"),
		StorageClass: storageClassOf(r.Header.Get("x-amz-storage-class")),
		UserMetadata: userMetadata(r.Header),
		LastModified: time.Now().UTC(),
	}
	if err := s.store.PutObject(ctx, obj); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	if ref != "" && !s.cfg.Encrypt {
		// For encrypted objects the 64-byte reference embeds the decryption
		// key, so it stays private (design §12).
		w.Header().Set("x-swarm-reference", ref)
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
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
	h.Set("x-amz-storage-class", obj.StorageClass)
	for k, v := range obj.UserMetadata {
		h.Set("x-amz-meta-"+k, v)
	}
	if obj.SwarmRef != "" && !s.cfg.Encrypt {
		h.Set("x-swarm-reference", obj.SwarmRef)
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
	io.Copy(w, resp.Body)          //nolint:errcheck // client may hang up mid-stream
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
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	src := r.Header.Get("x-amz-copy-source")
	if strings.Contains(src, "?versionId=") {
		s.notImplemented(w, r, "CopyObject with versionId")
		return
	}
	unescaped, err := url.PathUnescape(src)
	if err != nil {
		s.writeError(w, r, errInvalidArgument.withMessage("invalid x-amz-copy-source"))
		return
	}
	srcBucket, srcKey, ok := strings.Cut(strings.TrimPrefix(unescaped, "/"), "/")
	if !ok || srcKey == "" {
		s.writeError(w, r, errInvalidArgument.withMessage("x-amz-copy-source must be of the form bucket/key"))
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
	if err := s.store.PutObject(ctx, obj); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Xmlns,
		LastModified: xmlTime(obj.LastModified),
		ETag:         `"` + obj.ETag + `"`,
	})
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
