package api

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/petfold/s3warm/internal/store"
)

// Tagging (roadmap phase 3): bucket and per-version object tag sets, the
// x-amz-tagging header on writes, and copy directives. Tag sets are stored
// as opaque JSON on the index rows.

type xmlTagEntry struct {
	Key   string `xml:"Key" json:"k"`
	Value string `xml:"Value" json:"v"`
}

type taggingDocument struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  struct {
		Tags []xmlTagEntry `xml:"Tag"`
	} `xml:"TagSet"`
}

var (
	errNoSuchTagSet = apiError{"NoSuchTagSet", http.StatusNotFound, "The TagSet does not exist"}
	errInvalidTag   = apiError{"InvalidTag", http.StatusBadRequest, "The TagSet contains an invalid Tag"}
)

const (
	maxObjectTags = 10
	maxBucketTags = 50
)

func validateTags(tags []xmlTagEntry, max int) *apiError {
	if len(tags) > max {
		e := errInvalidTag.withMessage("Tag sets cannot be greater than " + strconv.Itoa(max) + " tags")
		return &e
	}
	seen := map[string]bool{}
	for _, t := range tags {
		if t.Key == "" || len(t.Key) > 128 {
			e := errInvalidTag.withMessage("The TagKey you have provided is invalid")
			return &e
		}
		if len(t.Value) > 256 {
			e := errInvalidTag.withMessage("The TagValue you have provided is invalid")
			return &e
		}
		if seen[t.Key] {
			e := errInvalidTag.withMessage("Cannot provide multiple Tags with the same key")
			return &e
		}
		seen[t.Key] = true
	}
	return nil
}

func tagsToJSON(tags []xmlTagEntry) (string, *apiError) {
	if len(tags) == 0 {
		return "", nil
	}
	// Canonical order: S3 returns tag sets sorted by key.
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })
	b, err := json.Marshal(tags)
	if err != nil {
		e := errInternal.withMessage(err.Error())
		return "", &e
	}
	return string(b), nil
}

func tagsFromJSON(s string) []xmlTagEntry {
	if s == "" {
		return nil
	}
	var tags []xmlTagEntry
	json.Unmarshal([]byte(s), &tags) //nolint:errcheck // written by us
	return tags
}

// parseTaggingHeader parses x-amz-tagging's query-string form
// ("k1=v1&k2=v2"), preserving order.
func parseTaggingHeader(header string) ([]xmlTagEntry, *apiError) {
	if header == "" {
		return nil, nil
	}
	var tags []xmlTagEntry
	for _, pair := range strings.Split(header, "&") {
		k, v, _ := strings.Cut(pair, "=")
		key, err1 := url.QueryUnescape(k)
		value, err2 := url.QueryUnescape(v)
		if err1 != nil || err2 != nil {
			e := errInvalidTag.withMessage("malformed x-amz-tagging header")
			return nil, &e
		}
		tags = append(tags, xmlTagEntry{Key: key, Value: value})
	}
	if apiErr := validateTags(tags, maxObjectTags); apiErr != nil {
		return nil, apiErr
	}
	return tags, nil
}

// ---- bucket tagging ----

func (s *Server) handleGetBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if b.Tags == "" {
		s.writeError(w, r, errNoSuchTagSet)
		return
	}
	doc := taggingDocument{Xmlns: s3Xmlns}
	doc.TagSet.Tags = tagsFromJSON(b.Tags)
	writeXML(w, http.StatusOK, doc)
}

func (s *Server) handlePutBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	tags, apiErr := s.readTaggingBody(r, maxBucketTags)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	tagsJSON, apiErr := tagsToJSON(tags)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if err := s.store.SetBucketTagging(r.Context(), bucket, tagsJSON); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDeleteBucketTagging(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.SetBucketTagging(r.Context(), bucket, ""); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- object tagging ----

// taggedObject resolves the version a tagging call addresses.
func (s *Server) taggedObject(r *http.Request, bucket, key string) (*store.Object, string, *apiError) {
	ctx := r.Context()
	versionID := r.URL.Query().Get("versionId")
	var obj *store.Object
	var err error
	if versionID != "" {
		obj, err = s.store.GetObjectVersion(ctx, bucket, key, versionID)
		if errors.Is(err, store.ErrObjectNotFound) {
			return nil, "", &errNoSuchVersion
		}
	} else {
		obj, err = s.store.GetObject(ctx, bucket, key)
	}
	if err != nil {
		e := storeError(err)
		return nil, "", &e
	}
	if obj.DeleteMarker {
		return nil, "", &errNoSuchKey
	}
	return obj, versionID, nil
}

func (s *Server) handleGetObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, _, apiErr := s.taggedObject(r, bucket, key)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if obj.VersionID != nullVersion {
		setVersionHeader(w.Header(), obj.VersionID)
	}
	doc := taggingDocument{Xmlns: s3Xmlns}
	doc.TagSet.Tags = tagsFromJSON(obj.Tags)
	writeXML(w, http.StatusOK, doc)
}

func (s *Server) handlePutObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, versionID, apiErr := s.taggedObject(r, bucket, key)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	tags, apiErr := s.readTaggingBody(r, maxObjectTags)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	tagsJSON, apiErr := tagsToJSON(tags)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if err := s.store.SetObjectTags(r.Context(), bucket, key, versionID, tagsJSON); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if obj.VersionID != nullVersion {
		setVersionHeader(w.Header(), obj.VersionID)
	}
	w.WriteHeader(http.StatusOK)
	s.commits.Notify(bucket)
}

func (s *Server) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, versionID, apiErr := s.taggedObject(r, bucket, key)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	if err := s.store.SetObjectTags(r.Context(), bucket, key, versionID, ""); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if obj.VersionID != nullVersion {
		setVersionHeader(w.Header(), obj.VersionID)
	}
	w.WriteHeader(http.StatusNoContent)
	s.commits.Notify(bucket)
}

func (s *Server) readTaggingBody(r *http.Request, max int) ([]xmlTagEntry, *apiError) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		e := errInternal.withMessage(err.Error())
		return nil, &e
	}
	var doc taggingDocument
	if err := xml.Unmarshal(body, &doc); err != nil {
		e := errMalformedXML
		return nil, &e
	}
	if apiErr := validateTags(doc.TagSet.Tags, max); apiErr != nil {
		return nil, apiErr
	}
	return doc.TagSet.Tags, nil
}
