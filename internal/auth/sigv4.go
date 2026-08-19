// Package auth implements AWS Signature Version 4 verification for the S3
// dialect (docs/DESIGN.md §8): header-based auth now; presigned URLs and
// streaming (aws-chunked) signatures are planned.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	algorithm   = "AWS4-HMAC-SHA256"
	iso8601     = "20060102T150405Z"
	defaultSkew = 15 * time.Minute
)

// Error is an authentication failure carrying an S3 error code.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type Identity struct {
	AccessKey string
	Anonymous bool
}

type CredentialsProvider interface {
	Lookup(accessKey string) (secret string, ok bool)
}

type StaticCredentials map[string]string

func (s StaticCredentials) Lookup(accessKey string) (string, bool) {
	secret, ok := s[accessKey]
	return secret, ok
}

type Verifier struct {
	Creds          CredentialsProvider
	AllowAnonymous bool
	// MaxSkew is the allowed clock skew (default 15 minutes).
	MaxSkew time.Duration
	// Now is overridable for tests.
	Now func() time.Time
}

// Verify authenticates r. It never reads the request body: the payload hash
// is taken from x-amz-content-sha256 as signed; body integrity against that
// hash is the handlers' job (they hash while streaming).
func (v *Verifier) Verify(r *http.Request) (*Identity, error) {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			return nil, &Error{"NotImplemented", "presigned URLs are not implemented yet"}
		}
		if v.AllowAnonymous {
			return &Identity{Anonymous: true}, nil
		}
		return nil, &Error{"AccessDenied", "anonymous access is disabled"}
	}
	if !strings.HasPrefix(authz, algorithm+" ") {
		return nil, &Error{"AuthorizationHeaderMalformed", "unsupported authorization scheme"}
	}

	var credential, signedHeaders, signature string
	for _, part := range strings.Split(authz[len(algorithm)+1:], ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, &Error{"AuthorizationHeaderMalformed", "malformed authorization component"}
		}
		switch k {
		case "Credential":
			credential = val
		case "SignedHeaders":
			signedHeaders = val
		case "Signature":
			signature = val
		}
	}
	if credential == "" || signedHeaders == "" || signature == "" {
		return nil, &Error{"AuthorizationHeaderMalformed", "authorization header is missing Credential, SignedHeaders or Signature"}
	}

	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[4] != "aws4_request" {
		return nil, &Error{"AuthorizationHeaderMalformed", "malformed credential scope"}
	}
	accessKey, scopeDate, scopeRegion, scopeService := scope[0], scope[1], scope[2], scope[3]
	if scopeService != "s3" {
		return nil, &Error{"AuthorizationHeaderMalformed", "credential scope service must be s3"}
	}
	// Any region label is accepted; the scope's own region feeds key derivation
	// (design §8) so clients need no special region configuration.
	secret, ok := v.Creds.Lookup(accessKey)
	if !ok {
		return nil, &Error{"InvalidAccessKeyId", "the access key id you provided does not exist in our records"}
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return nil, &Error{"InvalidRequest", "missing required header: x-amz-content-sha256"}
	}
	if strings.HasPrefix(payloadHash, "STREAMING-") {
		return nil, &Error{"NotImplemented", "streaming (aws-chunked) payload signing is not implemented yet"}
	}

	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		if d := r.Header.Get("Date"); d != "" {
			if t, err := http.ParseTime(d); err == nil {
				amzDate = t.UTC().Format(iso8601)
			}
		}
	}
	if amzDate == "" {
		return nil, &Error{"InvalidRequest", "missing x-amz-date or Date header"}
	}
	t, err := time.Parse(iso8601, amzDate)
	if err != nil {
		return nil, &Error{"AuthorizationHeaderMalformed", "malformed x-amz-date"}
	}
	if !strings.HasPrefix(amzDate, scopeDate) {
		return nil, &Error{"AuthorizationHeaderMalformed", "date in credential scope does not match request date"}
	}
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	skew := v.MaxSkew
	if skew == 0 {
		skew = defaultSkew
	}
	if d := now.Sub(t); d > skew || d < -skew {
		return nil, &Error{"RequestTimeTooSkewed", "the difference between the request time and the server's time is too large"}
	}

	canonReq := canonicalRequest(r, signedHeaders, payloadHash)
	crSum := sha256.Sum256([]byte(canonReq))
	stringToSign := strings.Join([]string{
		algorithm,
		amzDate,
		strings.Join([]string{scopeDate, scopeRegion, "s3", "aws4_request"}, "/"),
		hex.EncodeToString(crSum[:]),
	}, "\n")

	key := signingKey(secret, scopeDate, scopeRegion, "s3")
	want := hex.EncodeToString(hmacSHA256(key, stringToSign))
	if !hmac.Equal([]byte(want), []byte(strings.ToLower(signature))) {
		return nil, &Error{"SignatureDoesNotMatch", "the request signature we calculated does not match the signature you provided"}
	}
	return &Identity{AccessKey: accessKey}, nil
}

func canonicalRequest(r *http.Request, signedHeaders, payloadHash string) string {
	names := strings.Split(signedHeaders, ";")
	lowered := make([]string, len(names))
	var headers strings.Builder
	for i, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		lowered[i] = name
		var value string
		switch name {
		case "host":
			value = r.Host
		case "content-length":
			value = strings.Join(r.Header.Values(name), ",")
			if value == "" && r.ContentLength >= 0 {
				value = strconv.FormatInt(r.ContentLength, 10)
			}
		default:
			value = strings.Join(r.Header.Values(name), ",")
		}
		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(trimAll(value))
		headers.WriteByte('\n')
	}

	return strings.Join([]string{
		r.Method,
		EncodePath(r.URL.Path),
		canonicalQuery(r.URL.Query()),
		headers.String(),
		strings.Join(lowered, ";"),
		payloadHash,
	}, "\n")
}

func canonicalQuery(q url.Values) string {
	type pair struct{ k, v string }
	pairs := make([]pair, 0, len(q))
	for k, vs := range q {
		ek := escape(k, false)
		for _, v := range vs {
			pairs = append(pairs, pair{ek, escape(v, false)})
		}
	}
	// Sorted by encoded name, then encoded value.
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

// trimAll trims a header value and collapses sequential inner whitespace,
// per the SigV4 canonicalization rules.
func trimAll(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// EncodePath percent-encodes a URL path the way SigV4 for S3 expects: every
// byte except unreserved characters is encoded, '/' is preserved and hex
// digits are uppercase. S3 uses single encoding (unlike other AWS services).
func EncodePath(p string) string {
	return escape(p, true)
}

func escape(s string, keepSlash bool) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~',
			keepSlash && c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func signingKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}
