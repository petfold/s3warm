package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/petfold/s3warm/internal/manifest"
	"github.com/petfold/s3warm/internal/store"
)

// Snapshot & rollback extension endpoints (design §5). Not S3 verbs — these
// are s3warm's own surface for the commit chain:
//
//	PUT  /{bucket}?x-swarm-snapshot=<label>   create a snapshot (forces a commit)
//	GET  /{bucket}?x-swarm-snapshot           list snapshots
//	POST /{bucket}?x-swarm-restore=<label|root>  atomic whole-bucket rollback

var snapshotLabelRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

type snapshotJSON struct {
	Bucket    string    `json:"bucket"`
	Label     string    `json:"label"`
	Root      string    `json:"root"`
	Seq       int64     `json:"seq"`
	CreatedAt time.Time `json:"createdAt"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func (s *Server) handleCreateSnapshot(w http.ResponseWriter, r *http.Request, bucket string) {
	label := r.URL.Query().Get("x-swarm-snapshot")
	if !snapshotLabelRE.MatchString(label) {
		s.writeError(w, r, errInvalidArgument.withMessage(
			"snapshot label must match [A-Za-z0-9._-]{1,64}"))
		return
	}
	if s.commits == nil {
		s.writeError(w, r, errNotImplemented.withMessage("commits are disabled (-commit off)"))
		return
	}
	ctx := r.Context()
	root, seq, err := s.commits.CommitNow(ctx, bucket)
	if err != nil {
		s.writeError(w, r, commitError(err))
		return
	}
	snap := store.Snapshot{
		Bucket:    bucket,
		Label:     label,
		Root:      root,
		Seq:       seq,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.store.PutSnapshot(ctx, snap); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	// Best-effort pin: the snapshot should survive the Bee node's local GC.
	if err := s.bee.Pin(ctx, root); err != nil {
		s.log.Warn("pinning snapshot", "root", root, "err", err)
	}
	w.Header().Set("x-swarm-bucket-root", root)
	writeJSON(w, http.StatusOK, snapshotJSON(snap))
}

func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request, bucket string) {
	snaps, err := s.store.ListSnapshots(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	out := make([]snapshotJSON, len(snaps))
	for i, sn := range snaps {
		out[i] = snapshotJSON(sn)
	}
	writeJSON(w, http.StatusOK, map[string]any{"bucket": bucket, "snapshots": out})
}

var rootHexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (s *Server) handleRestoreBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ctx := r.Context()
	if _, err := s.store.GetBucket(ctx, bucket); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	target := r.URL.Query().Get("x-swarm-restore")
	root := ""
	if snap, err := s.store.GetSnapshot(ctx, bucket, target); err == nil {
		root = snap.Root
	} else if rootHexRE.MatchString(target) {
		root = target
	} else {
		s.writeError(w, r, errInvalidArgument.withMessage(
			"x-swarm-restore must be a snapshot label or a 64-hex commit root"))
		return
	}

	ls := manifest.NewLoadSaver(s.bee, "", true)
	commit, err := manifest.GetCommit(ctx, ls, root)
	if err != nil {
		s.writeError(w, r, beeError(err))
		return
	}
	if commit.Bucket != bucket {
		s.writeError(w, r, errInvalidRequest.withMessage(
			"commit "+root+" belongs to bucket "+commit.Bucket))
		return
	}
	// The rollback itself: the index swap is atomic, and the head points at
	// the restored root (design §5).
	if err := s.store.RestoreBucket(ctx, bucket, commit.Objects, root, commit.Seq); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	s.log.Info("bucket restored", "bucket", bucket, "root", root, "seq", commit.Seq, "objects", len(commit.Objects))
	w.Header().Set("x-swarm-bucket-root", root)
	writeJSON(w, http.StatusOK, map[string]any{
		"bucket": bucket, "root": root, "seq": commit.Seq, "objects": len(commit.Objects),
	})
}

// commitError maps commit failures: store errors keep their mapping,
// everything else surfaces as a Bee/upstream problem.
func commitError(err error) apiError {
	if errors.Is(err, manifest.ErrACTBucket) {
		return errInvalidRequest.withMessage(err.Error())
	}
	if e := storeError(err); e.Code != errInternal.Code {
		return e
	}
	return beeError(err)
}
