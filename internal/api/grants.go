package api

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

// ACT-backed grants and key-based tenancy (design §8 layer 2).
//
// Tenancy is gateway-enforced: every credential carries a tenant label, and a
// tenant key only sees buckets that tenant owns (root keys are unrestricted).
//
// Grants are Swarm-enforced: a bucket created with `x-swarm-act: true` has
// all its content uploaded under Swarm's Access Control Trie with the Bee
// node's key as publisher. The grants API maps onto Bee's grantee endpoints:
//
//	GET /{bucket}?x-swarm-grants   grants state: publisher, history, grantees
//	PUT /{bucket}?x-swarm-grants   {"add": [pubkey...], "revoke": [pubkey...]}
//
// A grantee reads the bucket's objects from any Bee node with their own key —
// no s3warm in the path (the payoff stated in design §8). Revocation is
// forward-only: content already fetched stays readable.

// authorizeBucket enforces tenancy: a tenant key may only touch buckets its
// tenant owns. Root identities (flag pair, anonymous dev mode) pass. A
// missing bucket passes too — the handler's own NoSuchBucket/404 semantics
// stay authoritative, and bucket names are not secrets.
func (s *Server) authorizeBucket(r *http.Request, bucket string) *apiError {
	id := identityFrom(r.Context())
	if id.Root() {
		return nil
	}
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		return nil
	}
	if b.Owner != id.Tenant {
		e := errAccessDenied
		return &e
	}
	return nil
}

// publisherKey returns the Bee node's compressed public key — the ACT
// publisher for everything this gateway uploads — cached after first use.
func (s *Server) publisherKey(r *http.Request) (string, error) {
	s.actMu.Lock()
	defer s.actMu.Unlock()
	if s.actPub != "" {
		return s.actPub, nil
	}
	pub, err := s.bee.PublisherKey(r.Context())
	if err != nil {
		return "", err
	}
	s.actPub = pub
	return pub, nil
}

// createActHistory starts a bucket's ACT history by uploading a small
// ACT-protected marker. Doing this at bucket creation (not lazily on the
// first object) keeps concurrent first writes from racing two histories
// into existence.
func (s *Server) createActHistory(r *http.Request, bucket, batch string) (string, *apiError) {
	marker := "s3warm act bucket " + bucket
	_, history, err := s.bee.UploadBytes(r.Context(), strings.NewReader(marker), bee.UploadOptions{
		BatchID:       batch,
		Deferred:      s.cfg.Ack != "network",
		ContentLength: int64(len(marker)),
		Act:           true,
	})
	if err != nil {
		e := beeError(err)
		return "", &e
	}
	if history == "" {
		e := errNotImplemented.withMessage(
			"the Bee node did not return an ACT history address — ACT requires Bee >= 2.2")
		return "", &e
	}
	return history, nil
}

// granteeKeyRE: a compressed secp256k1 public key in hex (33 bytes).
var granteeKeyRE = regexp.MustCompile(`^0[23][0-9a-fA-F]{64}$`)

type grantsRequest struct {
	Add    []string `json:"add"`
	Revoke []string `json:"revoke"`
}

type grantsResponse struct {
	Bucket      string   `json:"bucket"`
	Publisher   string   `json:"publisher"`
	HistoryRef  string   `json:"historyRef"`
	GranteesRef string   `json:"granteesRef,omitempty"`
	Grantees    []string `json:"grantees"`
}

// actBucket loads the bucket and requires it to be ACT-protected.
func (s *Server) actBucket(r *http.Request, bucket string) (*store.Bucket, *apiError) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		e := storeError(err)
		return nil, &e
	}
	if !b.ACT {
		e := errInvalidRequest.withMessage(
			"bucket is not ACT-protected; create it with the x-swarm-act: true header to use grants")
		return nil, &e
	}
	return b, nil
}

func (s *Server) handleGetGrants(w http.ResponseWriter, r *http.Request, bucket string) {
	b, apiErr := s.actBucket(r, bucket)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	pub, err := s.publisherKey(r)
	if err != nil {
		s.writeError(w, r, beeError(err))
		return
	}
	grantees := []string{}
	if b.ActGrantees != "" {
		grantees, err = s.bee.Grantees(r.Context(), b.ActGrantees)
		if err != nil {
			s.writeError(w, r, beeError(err))
			return
		}
	}
	writeJSON(w, http.StatusOK, grantsResponse{
		Bucket:      bucket,
		Publisher:   pub,
		HistoryRef:  b.ActHistory,
		GranteesRef: b.ActGrantees,
		Grantees:    grantees,
	})
}

func (s *Server) handlePutGrants(w http.ResponseWriter, r *http.Request, bucket string) {
	b, apiErr := s.actBucket(r, bucket)
	if apiErr != nil {
		s.writeError(w, r, *apiErr)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var req grantsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, r, errInvalidRequest.withMessage(
			`grants body must be JSON: {"add": [pubkey...], "revoke": [pubkey...]}`))
		return
	}
	if len(req.Add) == 0 && len(req.Revoke) == 0 {
		s.writeError(w, r, errInvalidRequest.withMessage("grants request adds or revokes nothing"))
		return
	}
	for _, k := range append(append([]string{}, req.Add...), req.Revoke...) {
		if !granteeKeyRE.MatchString(k) {
			s.writeError(w, r, errInvalidArgument.withMessage(
				"grantee must be a compressed secp256k1 public key (66 hex chars, 02/03 prefix): "+k))
			return
		}
	}

	ctx := r.Context()
	batch := s.resolveBatch(r, b)
	if batch == "" {
		s.writeError(w, r, errSwarmPostage.withMessage("grant mutations need a postage batch: set x-swarm-postage-batch-id or -batch-id"))
		return
	}
	if err := s.stamps.Check(ctx, batch); err != nil {
		s.writeError(w, r, errSwarmPostage.withMessage(err.Error()))
		return
	}

	var res *bee.GranteesResult
	if b.ActGrantees == "" {
		if len(req.Revoke) > 0 {
			s.writeError(w, r, errInvalidRequest.withMessage("bucket has no grantee list to revoke from"))
			return
		}
		// First grant: create the grantee list on the bucket's existing ACT
		// history so already-uploaded objects become readable by the grantees.
		res, err = s.bee.CreateGrantees(ctx, batch, b.ActHistory, req.Add)
	} else {
		res, err = s.bee.PatchGrantees(ctx, b.ActGrantees, b.ActHistory, batch, req.Add, req.Revoke)
	}
	if err != nil {
		s.writeError(w, r, beeError(err))
		return
	}
	if err := s.store.SetBucketACT(ctx, bucket, res.History, res.Ref); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}

	pub, _ := s.publisherKey(r)
	grantees, err := s.bee.Grantees(ctx, res.Ref)
	if err != nil {
		grantees = nil // the mutation itself succeeded; the echo is best-effort
	}
	writeJSON(w, http.StatusOK, grantsResponse{
		Bucket:      bucket,
		Publisher:   pub,
		HistoryRef:  res.History,
		GranteesRef: res.Ref,
		Grantees:    grantees,
	})
}
