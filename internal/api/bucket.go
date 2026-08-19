package api

import (
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Solar-Punk-Ltd/s3warm/internal/store"
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
		s.writeError(w, r, storeError(err))
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.GetBucket(r.Context(), bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
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

func (s *Server) handleGetBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := s.store.GetBucket(r.Context(), bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// Versioning is planned for phase 3; an empty configuration means
	// "never enabled", which is accurate today.
	writeXML(w, http.StatusOK, versioningConfiguration{Xmlns: s3Xmlns})
}
