package api

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

// Multipart uploads (design §7): parts stream straight to /bytes with no
// staging; completion validates the part list and writes a composite object
// whose reads are stitched by serveComposite. Swarm has no server-side
// concatenation and part boundaries almost never align with chunk-tree
// subtrees, so a single root hash cannot be assembled from part hashes;
// async consolidation into one canonical reference is a planned follow-up.

const (
	maxPartNumber = 10000
	minPartSize   = 5 * 1024 * 1024 // all parts except the last
)

func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	b, err := s.store.GetBucket(ctx, bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	batch := s.resolveBatch(r, b)
	if batch != "" {
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
	tags, apiErr := parseTaggingHeader(r.Header.Get("x-amz-tagging"))
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	tagsJSON, apiErr := tagsToJSON(tags)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}

	var id [16]byte
	rand.Read(id[:]) //nolint:errcheck // crypto/rand.Read never fails
	upload := store.MultipartUpload{
		UploadID:     hex.EncodeToString(id[:]),
		Bucket:       bucket,
		Key:          key,
		Initiated:    time.Now().UTC(),
		ContentType:  r.Header.Get("Content-Type"),
		StorageClass: storageClassOf(r.Header.Get("x-amz-storage-class")),
		UserMetadata: userMetadata(r.Header),
		BatchID:      batch,
		Encrypted:    encrypt,
		Tags:         tagsJSON,
	}
	if err := s.store.CreateMultipartUpload(ctx, upload); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if encrypt {
		w.Header().Set("x-amz-server-side-encryption", "AES256")
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns:    s3Xmlns,
		Bucket:   bucket,
		Key:      key,
		UploadID: upload.UploadID,
	})
}

func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	partNumber, apiErr := parsePartNumber(r)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	upload, err := s.store.GetMultipartUpload(ctx, bucket, key, r.URL.Query().Get("uploadId"))
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	res, apiErr := s.uploadBody(r, upload.BatchID, upload.StorageClass, upload.Encrypted)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	part := store.Part{
		PartNumber:   partNumber,
		SwarmRef:     res.Ref,
		Size:         res.Size,
		ETag:         res.ETag,
		LastModified: time.Now().UTC(),
	}
	if err := s.store.PutPart(ctx, upload.UploadID, part); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if upload.Encrypted {
		w.Header().Set("x-amz-server-side-encryption", "AES256")
	}
	w.Header().Set("ETag", `"`+res.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

// handleUploadPartCopy copies from an existing object into a part.
// A whole-object copy of a simple object is O(1) (same reference); a ranged
// copy re-streams the range through the gateway. Composite sources are not
// supported yet.
func (s *Server) handleUploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	partNumber, apiErr := parsePartNumber(r)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	upload, err := s.store.GetMultipartUpload(ctx, bucket, key, r.URL.Query().Get("uploadId"))
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	srcObj, apiErr := s.getCopySource(r)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if !copySourceConditionals(r, srcObj) {
		s.writeError(w, r, errPreconditionFailed)
		return
	}
	if len(srcObj.Parts) > 0 {
		s.notImplemented(w, r, "UploadPartCopy from a composite (multipart) source")
		return
	}

	part := store.Part{PartNumber: partNumber, LastModified: time.Now().UTC()}
	if rangeSpec := r.Header.Get("x-amz-copy-source-range"); rangeSpec == "" || srcObj.SwarmRef == "" {
		// Whole-object copy on a content-addressed store: reuse the reference.
		part.SwarmRef = srcObj.SwarmRef
		part.Size = srcObj.Size
		part.ETag = srcObj.ETag
	} else {
		// x-amz-copy-source-range is strictly bytes=first-last, within bounds
		// (no clamping, no suffix/open forms).
		var st, en int64
		if _, err := fmt.Sscanf(rangeSpec, "bytes=%d-%d", &st, &en); err != nil ||
			fmt.Sprintf("bytes=%d-%d", st, en) != rangeSpec ||
			st > en || en >= srcObj.Size {
			s.writeError(w, r, errInvalidRange)
			return
		}
		resp, err := s.bee.DownloadBytes(ctx, srcObj.SwarmRef,
			bee.DownloadOptions{Range: fmt.Sprintf("bytes=%d-%d", st, en), Strategy: s.cfg.FetchStrategy})
		if err != nil {
			s.writeError(w, r, beeError(err))
			return
		}
		defer resp.Body.Close()
		md5h := md5.New()
		ref, err := s.bee.UploadBytes(ctx, io.TeeReader(resp.Body, md5h),
			s.uploadOptions(upload.BatchID, upload.StorageClass, upload.Encrypted, en-st+1))
		if err != nil {
			s.writeError(w, r, beeError(err))
			return
		}
		part.SwarmRef = ref
		part.Size = en - st + 1
		part.ETag = hex.EncodeToString(md5h.Sum(nil))
	}

	if err := s.store.PutPart(ctx, upload.UploadID, part); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns:        s3Xmlns,
		LastModified: xmlTime(part.LastModified),
		ETag:         `"` + part.ETag + `"`,
	})
}

func (s *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	b, err := s.store.GetBucket(ctx, bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var req completeMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil || len(req.Parts) == 0 {
		s.writeError(w, r, errMalformedXML)
		return
	}

	upload, err := s.store.GetMultipartUpload(ctx, bucket, key, r.URL.Query().Get("uploadId"))
	if err != nil {
		// Complete must be retry-idempotent: SDKs re-send it, and by then the
		// upload bookkeeping is gone. If the object at this key is the result
		// of completing exactly these parts, report success again.
		if errors.Is(err, store.ErrUploadNotFound) {
			if etag, ok := s.alreadyCompleted(ctx, bucket, key, &req); ok {
				s.writeCompleteResult(w, r, bucket, key, etag)
				return
			}
		}
		s.writeError(w, r, storeError(err))
		return
	}

	stored, err := s.store.ListParts(ctx, upload.UploadID, 0, -1)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	byNumber := make(map[int]store.Part, len(stored))
	for _, p := range stored {
		byNumber[p.PartNumber] = p
	}

	// Validate the requested list: strictly ascending (checked over the whole
	// list first, matching S3's error precedence), every part uploaded with a
	// matching ETag, and S3's minimum size for all but the last.
	prev := 0
	for _, want := range req.Parts {
		if want.PartNumber <= prev {
			s.writeError(w, r, errInvalidPartOrder)
			return
		}
		prev = want.PartNumber
	}
	parts := make([]store.Part, 0, len(req.Parts))
	var totalSize int64
	for i, want := range req.Parts {
		p, ok := byNumber[want.PartNumber]
		if !ok || p.ETag != trimETag(want.ETag) {
			s.writeError(w, r, errInvalidPart)
			return
		}
		if i < len(req.Parts)-1 && p.Size < minPartSize {
			s.writeError(w, r, errEntityTooSmall)
			return
		}
		totalSize += p.Size
		parts = append(parts, p)
	}

	obj := store.Object{
		Bucket:       bucket,
		Key:          key,
		BatchID:      upload.BatchID,
		Size:         totalSize,
		ETag:         multipartETag(parts),
		ContentType:  upload.ContentType,
		StorageClass: upload.StorageClass,
		UserMetadata: upload.UserMetadata,
		LastModified: time.Now().UTC(),
		Parts:        parts,
		Encrypted:    upload.Encrypted,
		Tags:         upload.Tags,
	}
	versionHeader := stampVersion(&obj, b.Versioning)
	// Conditional completion, same semantics as conditional PUT (design §10).
	var cond *store.PutCondition
	if im, inm := r.Header.Get("If-Match"), r.Header.Get("If-None-Match"); im != "" || inm != "" {
		cond = &store.PutCondition{IfMatch: trimETag(im), IfNoneMatch: trimETag(inm)}
	}
	if err := s.store.PutObject(ctx, obj, cond); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	setVersionHeader(w.Header(), versionHeader)
	// Bookkeeping only: parts not referenced by the completed object simply
	// expire with their stamps (design §7).
	if err := s.store.DeleteMultipartUpload(ctx, upload.UploadID); err != nil {
		s.log.Warn("deleting completed upload", "uploadId", upload.UploadID, "err", err)
	}

	if upload.Encrypted {
		w.Header().Set("x-amz-server-side-encryption", "AES256")
	}
	s.writeCompleteResult(w, r, bucket, key, obj.ETag)
	s.commits.Notify(bucket)
}

// alreadyCompleted reports whether (bucket, key) holds the object produced
// by completing exactly the requested parts — the multipart ETag is derived
// from the request's part ETags alone, so no bookkeeping is needed.
func (s *Server) alreadyCompleted(ctx context.Context, bucket, key string, req *completeMultipartUploadRequest) (string, bool) {
	h := md5.New()
	for _, p := range req.Parts {
		b, err := hex.DecodeString(trimETag(p.ETag))
		if err != nil {
			return "", false
		}
		h.Write(b)
	}
	expected := fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(req.Parts))
	obj, err := s.store.GetObject(ctx, bucket, key)
	if err != nil || obj.ETag != expected {
		return "", false
	}
	return expected, true
}

func (s *Server) writeCompleteResult(w http.ResponseWriter, r *http.Request, bucket, key, etag string) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3Xmlns,
		Location: fmt.Sprintf("%s://%s/%s/%s", scheme, r.Host, bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + etag + `"`,
	})
}

func (s *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	upload, err := s.store.GetMultipartUpload(ctx, bucket, key, r.URL.Query().Get("uploadId"))
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// Already-uploaded part bytes expire with their stamps — abandoned-part
	// GC is automatic on Swarm (design §7).
	if err := s.store.DeleteMultipartUpload(ctx, upload.UploadID); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key string) {
	ctx := r.Context()
	q := r.URL.Query()
	upload, err := s.store.GetMultipartUpload(ctx, bucket, key, q.Get("uploadId"))
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	marker := 0
	if m := q.Get("part-number-marker"); m != "" {
		if marker, err = strconv.Atoi(m); err != nil || marker < 0 {
			s.writeError(w, r, errInvalidArgument.withMessage("invalid part-number-marker"))
			return
		}
	}
	maxParts := 1000
	if m := q.Get("max-parts"); m != "" {
		n, err := strconv.Atoi(m)
		if err != nil || n < 0 {
			s.writeError(w, r, errInvalidArgument.withMessage("invalid max-parts"))
			return
		}
		maxParts = min(n, 1000)
	}

	parts, err := s.store.ListParts(ctx, upload.UploadID, marker, maxParts+1)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	truncated := len(parts) > maxParts
	if truncated {
		parts = parts[:maxParts]
	}

	owner := xmlOwner{ID: "s3warm", DisplayName: "s3warm"}
	resp := listPartsResult{
		Xmlns:            s3Xmlns,
		Bucket:           bucket,
		Key:              key,
		UploadID:         upload.UploadID,
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		IsTruncated:      truncated,
		Initiator:        owner,
		Owner:            owner,
		StorageClass:     upload.StorageClass,
	}
	for _, p := range parts {
		resp.Parts = append(resp.Parts, xmlPart{
			PartNumber:   p.PartNumber,
			LastModified: xmlTime(p.LastModified),
			ETag:         `"` + p.ETag + `"`,
			Size:         p.Size,
		})
	}
	if truncated && len(parts) > 0 {
		resp.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}
	writeXML(w, http.StatusOK, resp)
}

func (s *Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()
	prefix := r.URL.Query().Get("prefix")
	uploads, err := s.store.ListMultipartUploads(ctx, bucket, prefix)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	owner := xmlOwner{ID: "s3warm", DisplayName: "s3warm"}
	resp := listMultipartUploadsResult{
		Xmlns:      s3Xmlns,
		Bucket:     bucket,
		Prefix:     prefix,
		MaxUploads: 1000,
	}
	for _, u := range uploads {
		resp.Uploads = append(resp.Uploads, xmlUpload{
			Key:          u.Key,
			UploadID:     u.UploadID,
			Initiator:    owner,
			Owner:        owner,
			StorageClass: u.StorageClass,
			Initiated:    xmlTime(u.Initiated),
		})
	}
	writeXML(w, http.StatusOK, resp)
}

// multipartETag is S3's multipart ETag algebra: the MD5 of the concatenated
// binary part MD5s, suffixed with the part count.
func multipartETag(parts []store.Part) string {
	h := md5.New()
	for _, p := range parts {
		b, _ := hex.DecodeString(p.ETag) //nolint:errcheck // we wrote these
		h.Write(b)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(parts))
}

func parsePartNumber(r *http.Request) (int, *apiError) {
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > maxPartNumber {
		e := errInvalidArgument.withMessage(
			fmt.Sprintf("part number must be an integer between 1 and %d", maxPartNumber))
		return 0, &e
	}
	return n, nil
}

// parseCopySource extracts (bucket, key, versionId) from x-amz-copy-source.
func parseCopySource(r *http.Request) (string, string, string, *apiError) {
	src := r.Header.Get("x-amz-copy-source")
	src, versionID, _ := strings.Cut(src, "?versionId=")
	unescaped, err := url.PathUnescape(src)
	if err != nil {
		e := errInvalidArgument.withMessage("invalid x-amz-copy-source")
		return "", "", "", &e
	}
	srcBucket, srcKey, ok := strings.Cut(strings.TrimPrefix(unescaped, "/"), "/")
	if !ok || srcKey == "" {
		e := errInvalidArgument.withMessage("x-amz-copy-source must be of the form bucket/key")
		return "", "", "", &e
	}
	return srcBucket, srcKey, versionID, nil
}

// getCopySource resolves a copy source, honoring versionId and treating a
// delete-marker latest as absent.
func (s *Server) getCopySource(r *http.Request) (*store.Object, *apiError) {
	srcBucket, srcKey, versionID, apiErr := parseCopySource(r)
	if apiErr != nil {
		return nil, apiErr
	}
	ctx := r.Context()
	if versionID != "" {
		obj, err := s.store.GetObjectVersion(ctx, srcBucket, srcKey, versionID)
		if err != nil {
			if errors.Is(err, store.ErrObjectNotFound) {
				return nil, &errNoSuchVersion
			}
			e := storeError(err)
			return nil, &e
		}
		if obj.DeleteMarker {
			return nil, &errNoSuchKey
		}
		return obj, nil
	}
	obj, err := s.store.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		e := storeError(err)
		return nil, &e
	}
	if obj.DeleteMarker {
		return nil, &errNoSuchKey
	}
	return obj, nil
}

// uploadOptions builds Bee upload options for a known-length stream.
func (s *Server) uploadOptions(batch, storageClass string, encrypt bool, contentLength int64) bee.UploadOptions {
	return bee.UploadOptions{
		BatchID:         batch,
		Encrypt:         encrypt,
		RedundancyLevel: s.redundancyFor(storageClass),
		Deferred:        s.cfg.Ack != "network",
		ContentLength:   contentLength,
	}
}
