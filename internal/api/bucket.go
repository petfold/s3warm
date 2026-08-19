package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/store"
)

var bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func validBucketName(name string) bool {
	if !bucketNameRE.MatchString(name) {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	// Bucket names must not look like IP addresses.
	return net.ParseIP(name) == nil
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.ListBuckets(r.Context())
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	resp := listAllMyBucketsResult{
		Xmlns: s3Xmlns,
		Owner: xmlOwner{ID: "s3warm", DisplayName: "s3warm"},
	}
	for _, b := range buckets {
		resp.Buckets.Bucket = append(resp.Buckets.Bucket, xmlBucket{
			Name:         b.Name,
			CreationDate: xmlTime(b.CreatedAt),
		})
	}
	writeXML(w, http.StatusOK, resp)
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if !validBucketName(bucket) {
		s.writeError(w, r, errInvalidBucketName)
		return
	}
	// An optional CreateBucketConfiguration body (LocationConstraint) is
	// accepted and ignored: there is one "region" here.
	io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10)) //nolint:errcheck

	err := s.store.CreateBucket(r.Context(), store.Bucket{
		Name:      bucket,
		CreatedAt: time.Now().UTC(),
		BatchID:   r.Header.Get("x-swarm-postage-batch-id"),
	})
	if err != nil {
		// In us-east-1, re-creating a bucket you own succeeds, as on AWS.
		if !(errors.Is(err, store.ErrBucketExists) && s.cfg.Region == "us-east-1") {
			s.writeError(w, r, storeError(err))
			return
		}
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// The commit chain's head (design §5): capture x-swarm-bucket-root to
	// browse the bucket via bzz:// or to snapshot this exact state.
	if b.HeadRoot != "" {
		w.Header().Set("x-swarm-bucket-root", b.HeadRoot)
		w.Header().Set("x-swarm-commit-seq", strconv.FormatInt(b.CommitSeq, 10))
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.DeleteBucket(r.Context(), bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.GetBucket(r.Context(), bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// S3 reports us-east-1 as an empty LocationConstraint.
	loc := s.cfg.Region
	if loc == "us-east-1" {
		loc = ""
	}
	writeXML(w, http.StatusOK, locationConstraint{Xmlns: s3Xmlns, Value: loc})
}

func (s *Server) handleGetBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if b.Encryption == "" {
		s.writeError(w, r, errNoSSEConfig)
		return
	}
	writeXML(w, http.StatusOK, sseConfiguration{
		Xmlns: s3Xmlns,
		Rules: []sseRule{{Apply: sseDefault{SSEAlgorithm: b.Encryption}}},
	})
}

func (s *Server) handlePutBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var cfg sseConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil || len(cfg.Rules) == 0 {
		s.writeError(w, r, errMalformedXML)
		return
	}
	if alg := cfg.Rules[0].Apply.SSEAlgorithm; alg != "AES256" {
		s.writeError(w, r, errNotImplemented.withMessage(
			"only SSE-S3 (AES256) is supported, got "+alg))
		return
	}
	if err := s.store.SetBucketEncryption(r.Context(), bucket, "AES256"); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucketEncryption(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.SetBucketEncryption(r.Context(), bucket, ""); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// Never-versioned buckets return the empty configuration, as on S3.
	writeXML(w, http.StatusOK, versioningConfiguration{Xmlns: s3Xmlns, Status: b.Versioning})
}
