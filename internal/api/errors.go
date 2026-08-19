package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"

	"github.com/petfold/s3warm/internal/auth"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

// apiError is an S3 error: a code string, an HTTP status and a default
// message, rendered in the standard S3 XML error envelope.
type apiError struct {
	Code    string
	Status  int
	Message string
}

func (e apiError) withMessage(msg string) apiError {
	e.Message = msg
	return e
}

var (
	errAccessDenied       = apiError{"AccessDenied", http.StatusForbidden, "Access Denied"}
	errInvalidRequest     = apiError{"InvalidRequest", http.StatusBadRequest, "Invalid Request"}
	errInvalidArgument    = apiError{"InvalidArgument", http.StatusBadRequest, "Invalid Argument"}
	errMalformedXML       = apiError{"MalformedXML", http.StatusBadRequest, "The XML you provided was not well-formed or did not validate against our published schema"}
	errInvalidBucketName  = apiError{"InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid"}
	errNoSuchBucket       = apiError{"NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist"}
	errNoSuchKey          = apiError{"NoSuchKey", http.StatusNotFound, "The specified key does not exist"}
	errBucketAlreadyOwned = apiError{"BucketAlreadyOwnedByYou", http.StatusConflict, "Your previous request to create the named bucket succeeded and you already own it"}
	errBucketNotEmpty     = apiError{"BucketNotEmpty", http.StatusConflict, "The bucket you tried to delete is not empty"}
	errMethodNotAllowed   = apiError{"MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed against this resource"}
	errNotImplemented     = apiError{"NotImplemented", http.StatusNotImplemented, "A header or query you provided implies functionality that is not implemented"}
	errInternal           = apiError{"InternalError", http.StatusInternalServerError, "We encountered an internal error. Please try again"}
	errServiceUnavailable = apiError{"ServiceUnavailable", http.StatusServiceUnavailable, "Service is unable to handle request"}
	errBadDigest          = apiError{"BadDigest", http.StatusBadRequest, "The Content-MD5 you specified did not match what we received"}
	errSHA256Mismatch     = apiError{"XAmzContentSHA256Mismatch", http.StatusBadRequest, "The provided x-amz-content-sha256 header does not match what was computed"}
	errPreconditionFailed = apiError{"PreconditionFailed", http.StatusPreconditionFailed, "At least one of the preconditions you specified did not hold"}
	errInvalidRange       = apiError{"InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable"}
	// errSwarmPostage is an s3warm extension (design §9): postage batch
	// problems are 402 so SDKs fail fast instead of retry-looping.
	errSwarmPostage = apiError{"SwarmPostageError", http.StatusPaymentRequired, "The postage batch is missing, expired or out of capacity"}
)

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, e apiError) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	if r.Method == http.MethodHead {
		return
	}
	io.WriteString(w, xml.Header)           //nolint:errcheck
	xml.NewEncoder(w).Encode(errorResponse{ //nolint:errcheck
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		RequestID: w.Header().Get("x-amz-request-id"),
	})
}

// authError maps an auth failure onto its S3 error.
func authError(err error) apiError {
	var ae *auth.Error
	if !errors.As(err, &ae) {
		return errAccessDenied
	}
	status := http.StatusBadRequest
	switch ae.Code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "RequestTimeTooSkewed":
		status = http.StatusForbidden
	case "NotImplemented":
		status = http.StatusNotImplemented
	}
	return apiError{Code: ae.Code, Status: status, Message: ae.Message}
}

// storeError maps metadata index failures onto S3 errors.
func storeError(err error) apiError {
	switch {
	case errors.Is(err, store.ErrBucketNotFound):
		return errNoSuchBucket
	case errors.Is(err, store.ErrObjectNotFound):
		return errNoSuchKey
	case errors.Is(err, store.ErrBucketExists):
		return errBucketAlreadyOwned
	case errors.Is(err, store.ErrBucketNotEmpty):
		return errBucketNotEmpty
	case errors.Is(err, store.ErrPreconditionFailed):
		return errPreconditionFailed
	default:
		return errInternal.withMessage(err.Error())
	}
}

// beeError maps Bee upstream failures onto S3 errors (design §13).
func beeError(err error) apiError {
	var se *bee.StatusError
	if !errors.As(err, &se) {
		return errServiceUnavailable.withMessage("bee node unreachable: " + err.Error())
	}
	switch se.StatusCode {
	case http.StatusPaymentRequired:
		return errSwarmPostage.withMessage(se.Message)
	case http.StatusNotFound:
		// The index knows the key but Swarm no longer serves the bytes: the
		// honest mapping for expired postage.
		return errNoSuchKey.withMessage("object data not retrievable from swarm (postage batch may have expired): " + se.Message)
	case http.StatusRequestedRangeNotSatisfiable:
		return errInvalidRange
	}
	if se.StatusCode >= 400 && se.StatusCode < 500 {
		return errInvalidRequest.withMessage("bee: " + se.Message)
	}
	return errServiceUnavailable.withMessage("bee: " + se.Message)
}
