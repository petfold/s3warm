// Package api implements the S3 REST front end: routing (method + path shape
// + query subresource, the S3 dialect), request authentication, XML codecs
// and the bucket/object handlers. See docs/DESIGN.md.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/auth"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/config"
	"github.com/petfold/s3warm/internal/metrics"
	"github.com/petfold/s3warm/internal/stamp"
	"github.com/petfold/s3warm/internal/store"
)

type Server struct {
	cfg      *config.Config
	store    store.Store
	bee      *bee.Client
	stamps   *stamp.Manager
	verifier *auth.Verifier
	log      *slog.Logger
}

func New(cfg *config.Config, st store.Store, beeClient *bee.Client, stamps *stamp.Manager, logger *slog.Logger) *Server {
	creds := auth.StaticCredentials{}
	if cfg.AccessKey != "" {
		creds[cfg.AccessKey] = cfg.SecretKey
	}
	if logger == nil {
		logger = slog.Default()
	}
	if stamps == nil {
		stamps = stamp.NewManager(beeClient, logger)
	}
	return &Server{
		cfg:    cfg,
		store:  st,
		bee:    beeClient,
		stamps: stamps,
		verifier: &auth.Verifier{
			Creds:          creds,
			AllowAnonymous: cfg.AccessKey == "",
		},
		log: logger,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	sw.Header().Set("x-amz-request-id", newRequestID())
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		metrics.RequestsTotal.WithLabelValues(r.Method, strconv.Itoa(sw.status)).Inc()
		metrics.RequestDuration.WithLabelValues(r.Method).Observe(elapsed.Seconds())
		s.log.Info("s3",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"dur", elapsed.Round(time.Millisecond).String())
	}()

	// Operational endpoints live under an underscore prefix, which is not a
	// valid bucket name, so they can never shadow a bucket.
	switch r.URL.Path {
	case "/_s3warm/health":
		sw.WriteHeader(http.StatusOK)
		io.WriteString(sw, "ok\n") //nolint:errcheck
		return
	case "/_s3warm/ready":
		if err := s.bee.Health(r.Context()); err != nil {
			http.Error(sw, "bee unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		sw.WriteHeader(http.StatusOK)
		io.WriteString(sw, "ready\n") //nolint:errcheck
		return
	case "/_s3warm/metrics":
		metrics.Handler().ServeHTTP(sw, r)
		return
	}

	if _, err := s.verifier.Verify(r); err != nil {
		s.writeError(sw, r, authError(err))
		return
	}

	bucket, key := s.resolveTarget(r)
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			s.handleListBuckets(sw, r)
			return
		}
		s.writeError(sw, r, errMethodNotAllowed)
	case key == "":
		s.dispatchBucket(sw, r, bucket)
	default:
		s.dispatchObject(sw, r, bucket, key)
	}
}

// resolveTarget extracts (bucket, key) using virtual-host-style addressing
// when a base domain is configured and matches, path-style otherwise.
func (s *Server) resolveTarget(r *http.Request) (bucket, key string) {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if d := s.cfg.Domain; d != "" && host != d && strings.HasSuffix(host, "."+d) {
		return strings.TrimSuffix(host, "."+d), strings.TrimPrefix(r.URL.Path, "/")
	}
	bucket, key, _ = strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	return bucket, key
}

func (s *Server) dispatchBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodGet:
		switch {
		case q.Has("location"):
			s.handleGetBucketLocation(w, r, bucket)
		case q.Has("versioning"):
			s.handleGetBucketVersioning(w, r, bucket)
		case q.Has("versions"):
			s.handleListObjectVersions(w, r, bucket)
		case anySubresource(q, "uploads", "acl", "policy", "cors", "tagging",
			"lifecycle", "encryption", "website", "object-lock", "replication",
			"logging", "notification", "requestPayment", "accelerate",
			"intelligent-tiering", "inventory", "metrics", "analytics", "ownershipControls",
			"publicAccessBlock", "policyStatus"):
			s.notImplemented(w, r, "bucket subresource")
		case q.Get("list-type") == "2":
			s.handleListObjects(w, r, bucket, 2)
		case q.Has("list-type"):
			s.writeError(w, r, errInvalidArgument.withMessage("unsupported list-type"))
		default:
			s.handleListObjects(w, r, bucket, 1)
		}
	case http.MethodPut:
		if len(q) > 0 {
			s.notImplemented(w, r, "bucket subresource")
			return
		}
		s.handleCreateBucket(w, r, bucket)
	case http.MethodHead:
		s.handleHeadBucket(w, r, bucket)
	case http.MethodDelete:
		if len(q) > 0 {
			s.notImplemented(w, r, "bucket subresource")
			return
		}
		s.handleDeleteBucket(w, r, bucket)
	case http.MethodPost:
		if q.Has("delete") {
			s.handleDeleteObjects(w, r, bucket)
			return
		}
		s.notImplemented(w, r, "bucket POST")
	default:
		s.writeError(w, r, errMethodNotAllowed)
	}
}

func (s *Server) dispatchObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodGet:
		if q.Has("uploadId") {
			s.notImplemented(w, r, "ListParts")
			return
		}
		if anySubresource(q, "acl", "tagging", "attributes", "legal-hold", "retention", "torrent") {
			s.notImplemented(w, r, "object subresource")
			return
		}
		s.handleGetObject(w, r, bucket, key, true)
	case http.MethodHead:
		s.handleGetObject(w, r, bucket, key, false)
	case http.MethodPut:
		if q.Has("uploadId") {
			s.notImplemented(w, r, "UploadPart")
			return
		}
		if anySubresource(q, "acl", "tagging", "legal-hold", "retention") {
			s.notImplemented(w, r, "object subresource")
			return
		}
		if r.Header.Get("x-amz-copy-source") != "" {
			s.handleCopyObject(w, r, bucket, key)
			return
		}
		s.handlePutObject(w, r, bucket, key)
	case http.MethodDelete:
		if q.Has("uploadId") {
			s.notImplemented(w, r, "AbortMultipartUpload")
			return
		}
		if q.Has("tagging") {
			s.notImplemented(w, r, "object subresource")
			return
		}
		s.handleDeleteObject(w, r, bucket, key)
	case http.MethodPost:
		if q.Has("uploads") {
			s.notImplemented(w, r, "CreateMultipartUpload")
			return
		}
		if q.Has("uploadId") {
			s.notImplemented(w, r, "CompleteMultipartUpload")
			return
		}
		s.notImplemented(w, r, "object POST")
	default:
		s.writeError(w, r, errMethodNotAllowed)
	}
}

func (s *Server) notImplemented(w http.ResponseWriter, r *http.Request, what string) {
	s.writeError(w, r, errNotImplemented.withMessage(
		"not implemented yet: "+what+" (see docs/API-COMPATIBILITY.md)"))
}

func anySubresource(q url.Values, names ...string) bool {
	for _, n := range names {
		if q.Has(n) {
			return true
		}
	}
	return false
}

func newRequestID() string {
	var b [8]byte
	rand.Read(b[:]) //nolint:errcheck // crypto/rand.Read never fails
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// statusWriter records the status code for request logging.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
