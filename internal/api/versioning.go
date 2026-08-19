package api

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"time"

	"github.com/petfold/s3warm/internal/store"
)

// Versioning (design §11). Content addressing makes the storage side free —
// every overwrite already mints a new Swarm reference and the old bytes stay
// retrievable while stamped — so versioning is purely index semantics:
// version rows, delete markers, latest-resolution.

// nullVersion is the version id of writes into never-versioned or suspended
// buckets, as on S3.
const nullVersion = "null"

// newVersionID mints an opaque, time-prefixed (hence roughly sortable)
// version id: 6 bytes of unix-millis, 10 random.
func newVersionID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixMilli())<<16)
	rand.Read(b[6:]) //nolint:errcheck // crypto/rand.Read never fails
	return hex.EncodeToString(b[:])
}

// stampVersion assigns version identity to a write according to the
// bucket's versioning mode and returns the id to expose in headers
// ("" = no header: the bucket was never versioned).
func stampVersion(o *store.Object, mode string) string {
	o.VSeq = time.Now().UnixNano()
	o.IsLatest = true
	o.VersionID = nullVersion
	if mode == "Enabled" {
		o.VersionID = newVersionID()
		return o.VersionID
	}
	// Suspended and never-versioned writes return no version id, as on S3.
	return ""
}

// setVersionHeader exposes an object's version id on responses, but only
// once versioning has touched the bucket or the object.
func setVersionHeader(h http.Header, versionID string) {
	if versionID != "" {
		h.Set("x-amz-version-id", versionID)
	}
}

func (s *Server) handlePutBucketVersioning(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var cfg versioningConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil {
		s.writeError(w, r, errMalformedXML)
		return
	}
	if cfg.Status != "Enabled" && cfg.Status != "Suspended" {
		s.writeError(w, r, errMalformedXML)
		return
	}
	if err := s.store.SetBucketVersioning(r.Context(), bucket, cfg.Status); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}
