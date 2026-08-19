package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"

	"github.com/petfold/s3warm/internal/auth"
	"github.com/petfold/s3warm/internal/store"
)

// scanPage is the index page size used while assembling one list response.
const scanPage = 1000

// scanResult is one page of delimiter-aware listing.
type scanResult struct {
	contents  []store.Object
	prefixes  []string
	truncated bool
	// resumeAfter is the position (last emitted key or rolled-up prefix) a
	// follow-up listing should continue strictly after. Emission uses a
	// strict "item > after" rule, which makes resuming inside a rolled-up
	// prefix work without extra state: keys under an already-emitted prefix
	// derive the same prefix again and are filtered by the comparison.
	resumeAfter string
}

// scanObjects assembles up to maxKeys items (objects + rolled-up common
// prefixes, S3 counting rules) starting strictly after `after`.
func (s *Server) scanObjects(ctx context.Context, bucket, prefix, delimiter, after string, maxKeys int) (*scanResult, error) {
	res := &scanResult{}
	if maxKeys == 0 {
		return res, nil
	}
	count := 0
	cursor := after
	for {
		batch, err := s.store.ListObjects(ctx, bucket, prefix, cursor, scanPage)
		if err != nil {
			return nil, err
		}
		for _, o := range batch {
			cursor = o.Key
			if delimiter != "" {
				if i := strings.Index(o.Key[len(prefix):], delimiter); i >= 0 {
					cp := o.Key[:len(prefix)+i+len(delimiter)]
					if cp <= after {
						continue // rolled up on a previous page
					}
					if n := len(res.prefixes); n > 0 && res.prefixes[n-1] == cp {
						continue // already rolled up in this response
					}
					if count == maxKeys {
						res.truncated = true
						return res, nil
					}
					res.prefixes = append(res.prefixes, cp)
					res.resumeAfter = cp
					count++
					continue
				}
			}
			if count == maxKeys {
				res.truncated = true
				return res, nil
			}
			res.contents = append(res.contents, o)
			res.resumeAfter = o.Key
			count++
		}
		if len(batch) < scanPage {
			return res, nil
		}
	}
}

// handleListObjects serves both ListObjectsV2 (version==2) and the legacy
// ListObjects (version==1); they share the scan and differ only in
// pagination parameters and response fields.
func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string, version int) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		s.writeError(w, r, errInvalidArgument.withMessage("invalid encoding-type"))
		return
	}
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		n, err := strconv.Atoi(mk)
		if err != nil || n < 0 {
			s.writeError(w, r, errInvalidArgument.withMessage("invalid max-keys"))
			return
		}
		maxKeys = min(n, 1000)
	}

	var after, marker, startAfter, token string
	if version == 2 {
		startAfter = q.Get("start-after")
		after = startAfter
		token = q.Get("continuation-token")
		if token != "" {
			dec, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				s.writeError(w, r, errInvalidArgument.withMessage("invalid continuation token"))
				return
			}
			after = string(dec)
		}
	} else {
		marker = q.Get("marker")
		after = marker
	}

	res, err := s.scanObjects(ctx, bucket, prefix, delimiter, after, maxKeys)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	enc := func(v string) string {
		if encoding == "url" {
			return auth.EncodePath(v)
		}
		return v
	}
	contents := make([]xmlObject, len(res.contents))
	for i, o := range res.contents {
		contents[i] = xmlObject{
			Key:          enc(o.Key),
			LastModified: xmlTime(o.LastModified),
			ETag:         `"` + o.ETag + `"`,
			Size:         o.Size,
			StorageClass: o.StorageClass,
		}
	}
	prefixes := make([]xmlCommonPrefix, len(res.prefixes))
	for i, p := range res.prefixes {
		prefixes[i] = xmlCommonPrefix{Prefix: enc(p)}
	}

	if version == 2 {
		resp := listBucketResultV2{
			Xmlns:             s3Xmlns,
			Name:              bucket,
			Prefix:            enc(prefix),
			StartAfter:        enc(startAfter),
			ContinuationToken: token,
			KeyCount:          len(contents) + len(prefixes),
			MaxKeys:           maxKeys,
			Delimiter:         enc(delimiter),
			EncodingType:      encoding,
			IsTruncated:       res.truncated,
			Contents:          contents,
			CommonPrefixes:    prefixes,
		}
		if res.truncated {
			resp.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(res.resumeAfter))
		}
		writeXML(w, http.StatusOK, resp)
		return
	}

	resp := listBucketResultV1{
		Xmlns:          s3Xmlns,
		Name:           bucket,
		Prefix:         enc(prefix),
		Marker:         enc(marker),
		MaxKeys:        maxKeys,
		Delimiter:      enc(delimiter),
		EncodingType:   encoding,
		IsTruncated:    res.truncated,
		Contents:       contents,
		CommonPrefixes: prefixes,
	}
	if res.truncated {
		resp.NextMarker = enc(res.resumeAfter)
	}
	writeXML(w, http.StatusOK, resp)
}

// handleListObjectVersions serves ListObjectVersions with unversioned
// semantics, as S3 does for never-versioned buckets: every object is one
// Version with VersionId "null" and IsLatest true. Real versioning is
// phase 3 (design §11).
func (s *Server) handleListObjectVersions(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	encoding := q.Get("encoding-type")
	if encoding != "" && encoding != "url" {
		s.writeError(w, r, errInvalidArgument.withMessage("invalid encoding-type"))
		return
	}
	maxKeys := 1000
	if mk := q.Get("max-keys"); mk != "" {
		n, err := strconv.Atoi(mk)
		if err != nil || n < 0 {
			s.writeError(w, r, errInvalidArgument.withMessage("invalid max-keys"))
			return
		}
		maxKeys = min(n, 1000)
	}
	keyMarker := q.Get("key-marker")
	// version-id-marker is accepted and ignored: one version per key.

	res, err := s.scanObjects(ctx, bucket, prefix, delimiter, keyMarker, maxKeys)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	enc := func(v string) string {
		if encoding == "url" {
			return auth.EncodePath(v)
		}
		return v
	}
	versions := make([]xmlVersion, len(res.contents))
	for i, o := range res.contents {
		versions[i] = xmlVersion{
			Key:          enc(o.Key),
			VersionID:    "null",
			IsLatest:     true,
			LastModified: xmlTime(o.LastModified),
			ETag:         `"` + o.ETag + `"`,
			Size:         o.Size,
			StorageClass: o.StorageClass,
			Owner:        xmlOwner{ID: "s3warm", DisplayName: "s3warm"},
		}
	}
	prefixes := make([]xmlCommonPrefix, len(res.prefixes))
	for i, p := range res.prefixes {
		prefixes[i] = xmlCommonPrefix{Prefix: enc(p)}
	}

	resp := listVersionsResult{
		Xmlns:           s3Xmlns,
		Name:            bucket,
		Prefix:          enc(prefix),
		KeyMarker:       enc(keyMarker),
		VersionIDMarker: q.Get("version-id-marker"),
		MaxKeys:         maxKeys,
		Delimiter:       enc(delimiter),
		EncodingType:    encoding,
		IsTruncated:     res.truncated,
		Versions:        versions,
		CommonPrefixes:  prefixes,
	}
	if res.truncated {
		resp.NextKeyMarker = enc(res.resumeAfter)
		resp.NextVersionIDMarker = "null"
	}
	writeXML(w, http.StatusOK, resp)
}
