// Package api implements the S3 REST front end: routing (method + path shape
// + query subresource, the S3 dialect), request authentication, XML codecs
// and the bucket/object handlers. See docs/DESIGN.md.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/petfold/s3warm/internal/auth"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/config"
	"github.com/petfold/s3warm/internal/manifest"
	"github.com/petfold/s3warm/internal/metrics"
	"github.com/petfold/s3warm/internal/stamp"
	"github.com/petfold/s3warm/internal/store"
)

type Server struct {
	cfg      *config.Config
	store    store.Store
	bee      *bee.Client
	stamps   *stamp.Manager
	commits  *manifest.Committer // nil = commit chain disabled
	verifier *auth.Verifier
	log      *slog.Logger

	// actMu guards actPub, the cached ACT publisher key (the Bee node's
	// compressed public key, design §8).
	actMu  sync.Mutex
	actPub string
}

func New(cfg *config.Config, st store.Store, beeClient *bee.Client, stamps *stamp.Manager, commits *manifest.Committer, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	creds := auth.StaticCredentials{}
	if cfg.AccessKey != "" {
		// The flag pair is the root credential: unrestricted by tenancy.
		creds[cfg.AccessKey] = auth.Credential{Secret: cfg.SecretKey}
	}
	if cfg.CredentialsFile != "" {
		if err := auth.LoadCredentialsFile(cfg.CredentialsFile, creds); err != nil {
			logger.Error("credentials file", "err", err)
		}
	}
	if stamps == nil {
		stamps = stamp.NewManager(beeClient, logger)
	}
	return &Server{
		cfg:     cfg,
		store:   st,
		bee:     beeClient,
		stamps:  stamps,
		commits: commits,
		verifier: &auth.Verifier{
			Creds:          creds,
			AllowAnonymous: len(creds) == 0,
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

	// CORS preflights are unsigned, so they are answered before auth.
	if r.Method == http.MethodOptions {
		s.handlePreflight(sw, r)
		return
	}

	bucket, key := s.resolveTarget(r)
	// CORS decoration precedes auth so browsers can read even error
	// responses; a non-match adds nothing and never blocks.
	s.decorateCORS(sw, r, bucket)

	identity, err := s.verifier.Verify(r)
	if err != nil {
		s.writeError(sw, r, authError(err))
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), identityCtxKey{}, identity))
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
	// Tenancy (design §8 layer 2): everything but bucket creation requires
	// the caller's tenant to own the bucket. Creation has its own conflict
	// semantics (BucketAlreadyExists) handled in handleCreateBucket.
	creating := r.Method == http.MethodPut && len(q) == 0
	if !creating {
		if apiErr := s.authorizeBucket(r, bucket); apiErr != nil {
			s.writeError(w, r, *apiErr)
			return
		}
	}
	switch r.Method {
	case http.MethodGet:
		switch {
		case q.Has("location"):
			s.handleGetBucketLocation(w, r, bucket)
		case q.Has("versioning"):
			s.handleGetBucketVersioning(w, r, bucket)
		case q.Has("versions"):
			s.handleListObjectVersions(w, r, bucket)
		case q.Has("uploads"):
			s.handleListMultipartUploads(w, r, bucket)
		case q.Has("encryption"):
			s.handleGetBucketEncryption(w, r, bucket)
		case q.Has("x-swarm-snapshot"):
			s.handleListSnapshots(w, r, bucket)
		case q.Has("cors"):
			s.handleGetBucketCors(w, r, bucket)
		case q.Has("tagging"):
			s.handleGetBucketTagging(w, r, bucket)
		case q.Has("x-swarm-grants"):
			s.handleGetGrants(w, r, bucket)
		case anySubresource(q, "acl", "policy",
			"lifecycle", "website", "object-lock", "replication",
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
		if q.Has("versioning") {
			s.handlePutBucketVersioning(w, r, bucket)
			return
		}
		if q.Has("encryption") {
			s.handlePutBucketEncryption(w, r, bucket)
			return
		}
		if q.Has("cors") {
			s.handlePutBucketCors(w, r, bucket)
			return
		}
		if q.Has("tagging") {
			s.handlePutBucketTagging(w, r, bucket)
			return
		}
		if q.Has("x-swarm-snapshot") {
			s.handleCreateSnapshot(w, r, bucket)
			return
		}
		if q.Has("x-swarm-grants") {
			s.handlePutGrants(w, r, bucket)
			return
		}
		if len(q) > 0 {
			s.notImplemented(w, r, "bucket subresource")
			return
		}
		s.handleCreateBucket(w, r, bucket)
	case http.MethodHead:
		s.handleHeadBucket(w, r, bucket)
	case http.MethodDelete:
		if q.Has("encryption") {
			s.handleDeleteBucketEncryption(w, r, bucket)
			return
		}
		if q.Has("cors") {
			s.handleDeleteBucketCors(w, r, bucket)
			return
		}
		if q.Has("tagging") {
			s.handleDeleteBucketTagging(w, r, bucket)
			return
		}
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
		if q.Has("x-swarm-restore") {
			s.handleRestoreBucket(w, r, bucket)
			return
		}
		s.notImplemented(w, r, "bucket POST")
	default:
		s.writeError(w, r, errMethodNotAllowed)
	}
}

func (s *Server) dispatchObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	if apiErr := s.authorizeBucket(r, bucket); apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if q.Has("uploadId") {
			s.handleListParts(w, r, bucket, key)
			return
		}
		if q.Has("attributes") {
			s.handleGetObjectAttributes(w, r, bucket, key)
			return
		}
		if q.Has("tagging") {
			s.handleGetObjectTagging(w, r, bucket, key)
			return
		}
		if anySubresource(q, "acl", "legal-hold", "retention", "torrent") {
			s.notImplemented(w, r, "object subresource")
			return
		}
		s.handleGetObject(w, r, bucket, key, true)
	case http.MethodHead:
		s.handleGetObject(w, r, bucket, key, false)
	case http.MethodPut:
		if q.Has("uploadId") {
			if r.Header.Get("x-amz-copy-source") != "" {
				s.handleUploadPartCopy(w, r, bucket, key)
			} else {
				s.handleUploadPart(w, r, bucket, key)
			}
			return
		}
		if q.Has("tagging") {
			s.handlePutObjectTagging(w, r, bucket, key)
			return
		}
		if anySubresource(q, "acl", "legal-hold", "retention") {
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
			s.handleAbortMultipartUpload(w, r, bucket, key)
			return
		}
		if q.Has("tagging") {
			s.handleDeleteObjectTagging(w, r, bucket, key)
			return
		}
		s.handleDeleteObject(w, r, bucket, key)
	case http.MethodPost:
		if q.Has("uploads") {
			s.handleCreateMultipartUpload(w, r, bucket, key)
			return
		}
		if q.Has("uploadId") {
			s.handleCompleteMultipartUpload(w, r, bucket, key)
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

type identityCtxKey struct{}

// identityFrom returns the authenticated identity stashed by ServeHTTP.
func identityFrom(ctx context.Context) *auth.Identity {
	id, _ := ctx.Value(identityCtxKey{}).(*auth.Identity)
	return id
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
