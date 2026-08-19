package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Test vectors from the AWS documentation, "Authenticating Requests: Using
// the Authorization Header (AWS Signature Version 4)" — the canonical S3
// examples with access key AKIAIOSFODNN7EXAMPLE.
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html

const (
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	emptySHA256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func newTestVerifier() *Verifier {
	return &Verifier{
		Creds: StaticCredentials{testAccessKey: testSecretKey},
		Now:   func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) },
	}
}

func authHeader(signedHeaders, signature string) string {
	return "AWS4-HMAC-SHA256 Credential=" + testAccessKey + "/20130524/us-east-1/s3/aws4_request," +
		"SignedHeaders=" + signedHeaders + ",Signature=" + signature
}

func TestVerifyAWSVectorGetObject(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", "20130524T000000Z")
	r.Header.Set("Authorization", authHeader(
		"host;range;x-amz-content-sha256;x-amz-date",
		"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))

	id, err := newTestVerifier().Verify(r)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.AccessKey != testAccessKey {
		t.Fatalf("access key = %q", id.AccessKey)
	}
}

func TestVerifyAWSVectorPutObject(t *testing.T) {
	// PUT with an encodable character in the key ($ -> %24) exercises the
	// S3 single-encoding path rules.
	r := httptest.NewRequest(http.MethodPut, "http://examplebucket.s3.amazonaws.com/test%24file.text",
		strings.NewReader("Welcome to Amazon S3."))
	r.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	r.Header.Set("x-amz-date", "20130524T000000Z")
	r.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Set("x-amz-content-sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	r.Header.Set("Authorization", authHeader(
		"date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
		"98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"))

	if _, err := newTestVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyAWSVectorGetLifecycle(t *testing.T) {
	// Value-less subresource query (?lifecycle) canonicalizes as "lifecycle=".
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/?lifecycle", nil)
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", "20130524T000000Z")
	r.Header.Set("Authorization", authHeader(
		"host;x-amz-content-sha256;x-amz-date",
		"fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"))

	if _, err := newTestVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyAWSVectorListObjects(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil)
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", "20130524T000000Z")
	r.Header.Set("Authorization", authHeader(
		"host;x-amz-content-sha256;x-amz-date",
		"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"))

	if _, err := newTestVerifier().Verify(r); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifyRejections(t *testing.T) {
	base := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
		r.Header.Set("Range", "bytes=0-9")
		r.Header.Set("x-amz-content-sha256", emptySHA256)
		r.Header.Set("x-amz-date", "20130524T000000Z")
		r.Header.Set("Authorization", authHeader(
			"host;range;x-amz-content-sha256;x-amz-date",
			"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))
		return r
	}

	tests := []struct {
		name     string
		mutate   func(r *http.Request)
		now      time.Time
		wantCode string
	}{
		{
			name: "tampered signature",
			mutate: func(r *http.Request) {
				r.Header.Set("Authorization", authHeader(
					"host;range;x-amz-content-sha256;x-amz-date",
					"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb40"))
			},
			wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "tampered path",
			mutate: func(r *http.Request) {
				r.URL.Path = "/other.txt"
			},
			wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "unknown access key",
			mutate: func(r *http.Request) {
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"),
					testAccessKey, "AKIAUNKNOWNUNKNOWN00", 1))
			},
			wantCode: "InvalidAccessKeyId",
		},
		{
			name:     "clock skew",
			mutate:   func(r *http.Request) {},
			now:      time.Date(2013, 5, 24, 1, 0, 0, 0, time.UTC),
			wantCode: "RequestTimeTooSkewed",
		},
		{
			name: "anonymous rejected",
			mutate: func(r *http.Request) {
				r.Header.Del("Authorization")
			},
			wantCode: "AccessDenied",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := newTestVerifier()
			if !tc.now.IsZero() {
				v.Now = func() time.Time { return tc.now }
			}
			r := base()
			tc.mutate(r)
			_, err := v.Verify(r)
			var ae *Error
			if !errors.As(err, &ae) {
				t.Fatalf("expected *auth.Error, got %v", err)
			}
			if ae.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q (%v)", ae.Code, tc.wantCode, err)
			}
		})
	}
}

func TestVerifyAnonymousAllowed(t *testing.T) {
	v := newTestVerifier()
	v.AllowAnonymous = true
	r := httptest.NewRequest(http.MethodGet, "http://localhost:8333/demo", nil)
	id, err := v.Verify(r)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !id.Anonymous {
		t.Fatal("expected anonymous identity")
	}
}

// The presigned-URL vector from the AWS documentation, "Authenticating
// Requests: Using Query Parameters (AWS Signature Version 4)".
const presignedVectorURL = "http://examplebucket.s3.amazonaws.com/test.txt" +
	"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
	"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
	"&X-Amz-Date=20130524T000000Z" +
	"&X-Amz-Expires=86400" +
	"&X-Amz-SignedHeaders=host" +
	"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"

func TestVerifyAWSVectorPresigned(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, presignedVectorURL, nil)
	id, err := newTestVerifier().Verify(r)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if id.AccessKey != testAccessKey {
		t.Fatalf("access key = %q", id.AccessKey)
	}
}

func TestVerifyPresignedRejections(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(u string) string
		now      time.Time
		wantCode string
	}{
		{
			name:     "expired",
			mutate:   func(u string) string { return u },
			now:      time.Date(2013, 5, 25, 0, 0, 1, 0, time.UTC), // 1s past X-Amz-Expires
			wantCode: "AccessDenied",
		},
		{
			name: "tampered signature",
			mutate: func(u string) string {
				return strings.Replace(u, "f604d404", "f604d405", 1)
			},
			wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "tampered path",
			mutate: func(u string) string {
				return strings.Replace(u, "/test.txt", "/other.txt", 1)
			},
			wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "expires beyond seven days",
			mutate: func(u string) string {
				return strings.Replace(u, "X-Amz-Expires=86400", "X-Amz-Expires=604801", 1)
			},
			wantCode: "AccessDenied",
		},
		{
			name: "missing expires",
			mutate: func(u string) string {
				return strings.Replace(u, "&X-Amz-Expires=86400", "", 1)
			},
			wantCode: "AccessDenied",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := newTestVerifier()
			if !tc.now.IsZero() {
				v.Now = func() time.Time { return tc.now }
			}
			r := httptest.NewRequest(http.MethodGet, tc.mutate(presignedVectorURL), nil)
			_, err := v.Verify(r)
			var ae *Error
			if !errors.As(err, &ae) || ae.Code != tc.wantCode {
				t.Fatalf("Verify = %v, want code %q", err, tc.wantCode)
			}
		})
	}
}

func TestEncodePath(t *testing.T) {
	for in, want := range map[string]string{
		"/test$file.text": "/test%24file.text",
		"/a b/c":          "/a%20b/c",
		"/simple/key.txt": "/simple/key.txt",
		"/über":           "/%C3%BCber",
	} {
		if got := EncodePath(in); got != want {
			t.Errorf("EncodePath(%q) = %q, want %q", in, got, want)
		}
	}
}
