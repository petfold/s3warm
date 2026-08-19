package api

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// CORS (design roadmap, phase 2): per-bucket rules with S3's matching
// semantics — one '*' wildcard allowed in origin patterns, the method taken
// from Access-Control-Request-Method when present, headers echoed on
// preflight when every requested header is allowed.

type corsRule struct {
	AllowedOrigins []string `xml:"AllowedOrigin" json:"origins"`
	AllowedMethods []string `xml:"AllowedMethod" json:"methods"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty" json:"headers,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty" json:"expose,omitempty"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty" json:"maxAge,omitempty"`
}

type corsConfiguration struct {
	XMLName xml.Name   `xml:"CORSConfiguration"`
	Xmlns   string     `xml:"xmlns,attr,omitempty"`
	Rules   []corsRule `xml:"CORSRule"`
}

var errNoCORSConfig = apiError{"NoSuchCORSConfiguration", http.StatusNotFound, "The CORS configuration does not exist"}

func (s *Server) handleGetBucketCors(w http.ResponseWriter, r *http.Request, bucket string) {
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	if b.CORS == "" {
		s.writeError(w, r, errNoCORSConfig)
		return
	}
	var rules []corsRule
	if err := json.Unmarshal([]byte(b.CORS), &rules); err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	writeXML(w, http.StatusOK, corsConfiguration{Xmlns: s3Xmlns, Rules: rules})
}

func (s *Server) handlePutBucketCors(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	var cfg corsConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil || len(cfg.Rules) == 0 || len(cfg.Rules) > 100 {
		s.writeError(w, r, errMalformedXML)
		return
	}
	for _, rule := range cfg.Rules {
		if len(rule.AllowedOrigins) == 0 || len(rule.AllowedMethods) == 0 {
			s.writeError(w, r, errMalformedXML)
			return
		}
		for _, o := range rule.AllowedOrigins {
			if strings.Count(o, "*") > 1 {
				s.writeError(w, r, errInvalidRequest.withMessage(
					"AllowedOrigin "+o+" can not have more than one wildcard"))
				return
			}
		}
	}
	rules, err := json.Marshal(cfg.Rules)
	if err != nil {
		s.writeError(w, r, errInternal.withMessage(err.Error()))
		return
	}
	if err := s.store.SetBucketCORS(r.Context(), bucket, string(rules)); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteBucketCors(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.SetBucketCORS(r.Context(), bucket, ""); err != nil {
		s.writeError(w, r, storeError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// bucketCORSRules loads a bucket's rules; nil when absent or unparseable.
func (s *Server) bucketCORSRules(r *http.Request, bucket string) []corsRule {
	if bucket == "" {
		return nil
	}
	b, err := s.store.GetBucket(r.Context(), bucket)
	if err != nil || b.CORS == "" {
		return nil
	}
	var rules []corsRule
	if json.Unmarshal([]byte(b.CORS), &rules) != nil {
		return nil
	}
	return rules
}

// corsMatch finds the first rule allowing (origin, method) and returns it
// with the matched origin pattern.
func corsMatch(rules []corsRule, origin, method string) (*corsRule, string) {
	for i := range rules {
		rule := &rules[i]
		allowed := false
		for _, m := range rule.AllowedMethods {
			if strings.EqualFold(m, method) {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		for _, pattern := range rule.AllowedOrigins {
			if originMatches(pattern, origin) {
				return rule, pattern
			}
		}
	}
	return nil, ""
}

// originMatches implements S3's origin patterns: exact match, or a single
// '*' matching any (possibly empty) infix.
func originMatches(pattern, origin string) bool {
	if pattern == "*" {
		return true
	}
	if i := strings.IndexByte(pattern, '*'); i >= 0 {
		prefix, suffix := pattern[:i], pattern[i+1:]
		return len(origin) >= len(prefix)+len(suffix) &&
			strings.HasPrefix(origin, prefix) && strings.HasSuffix(origin, suffix)
	}
	return pattern == origin
}

// applyCORS decorates a response for a matched rule. The pattern decides
// whether the wildcard itself or the echoed origin is returned.
func applyCORS(h http.Header, rule *corsRule, pattern, origin string) {
	allowOrigin := origin
	if pattern == "*" {
		allowOrigin = "*"
	}
	h.Set("Access-Control-Allow-Origin", allowOrigin)
	h.Set("Access-Control-Allow-Methods", strings.Join(rule.AllowedMethods, ", "))
	if len(rule.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(rule.ExposeHeaders, ", "))
	}
	if allowOrigin != "*" {
		h.Set("Access-Control-Allow-Credentials", "true")
		h.Add("Vary", "Origin")
	}
}

// decorateCORS applies CORS headers to an actual (non-preflight) request
// carrying an Origin; a non-match adds nothing and never blocks.
func (s *Server) decorateCORS(w http.ResponseWriter, r *http.Request, bucket string) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	rules := s.bucketCORSRules(r, bucket)
	if rules == nil {
		return
	}
	method := r.Header.Get("Access-Control-Request-Method")
	if method == "" {
		method = r.Method
	}
	if rule, pattern := corsMatch(rules, origin, method); rule != nil {
		applyCORS(w.Header(), rule, pattern, origin)
	}
}

// handlePreflight answers OPTIONS requests: 400 without Origin and a
// request method (AWS's no-information answer), 200 with CORS headers on a
// match, 403 otherwise.
func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	method := r.Header.Get("Access-Control-Request-Method")
	if origin == "" || method == "" {
		s.writeError(w, r, apiError{"BadRequest", http.StatusBadRequest,
			"Insufficient information. Origin request header needed."})
		return
	}
	bucket, _ := s.resolveTarget(r)
	rules := s.bucketCORSRules(r, bucket)
	rule, pattern := corsMatch(rules, origin, method)
	if rule == nil {
		s.writeError(w, r, apiError{"AccessForbidden", http.StatusForbidden,
			"CORSResponse: This CORS request is not allowed. Check your policy and try again."})
		return
	}
	// Every requested header must be allowed for the preflight to succeed.
	if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
		for _, name := range strings.Split(reqHeaders, ",") {
			if !headerAllowed(rule.AllowedHeaders, strings.TrimSpace(name)) {
				s.writeError(w, r, apiError{"AccessForbidden", http.StatusForbidden,
					"CORSResponse: This CORS request is not allowed. Check your policy and try again."})
				return
			}
		}
		w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
	}
	applyCORS(w.Header(), rule, pattern, origin)
	if rule.MaxAgeSeconds > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAgeSeconds))
	}
	w.WriteHeader(http.StatusOK)
}

func headerAllowed(allowed []string, name string) bool {
	for _, a := range allowed {
		if a == "*" || strings.EqualFold(a, name) {
			return true
		}
		if i := strings.IndexByte(a, '*'); i >= 0 {
			la, ln := strings.ToLower(a), strings.ToLower(name)
			prefix, suffix := la[:i], la[i+1:]
			if len(ln) >= len(prefix)+len(suffix) &&
				strings.HasPrefix(ln, prefix) && strings.HasSuffix(ln, suffix) {
				return true
			}
		}
	}
	return false
}
