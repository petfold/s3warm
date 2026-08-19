package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

const (
	testScope   = "20130524/us-east-1/s3/aws4_request"
	testAmzDate = "20130524T000000Z"
	testSeedSig = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
)

func testStreamContext() *StreamContext {
	return &StreamContext{
		signingKey: signingKey(testSecretKey, "20130524", "us-east-1", "s3"),
		scope:      testScope,
		amzDate:    testAmzDate,
		prevSig:    testSeedSig,
	}
}

type trailerPair struct{ name, value string }

// buildSignedStream produces a valid aws-chunked body for the given chunks
// and trailers, using the same primitives the verifier uses.
func buildSignedStream(chunks [][]byte, trailers []trailerPair, signTrailer bool) string {
	sc := testStreamContext()
	var b strings.Builder
	for _, ch := range chunks {
		sum := sha256.Sum256(ch)
		sig := sc.chunkSignature(hex.EncodeToString(sum[:]))
		sc.prevSig = sig
		fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(ch), sig)
		b.Write(ch)
		b.WriteString("\r\n")
	}
	finalSig := sc.chunkSignature(emptySHA256)
	sc.prevSig = finalSig
	fmt.Fprintf(&b, "0;chunk-signature=%s\r\n", finalSig)

	var canonical strings.Builder
	for _, tp := range trailers {
		fmt.Fprintf(&b, "%s:%s\r\n", tp.name, tp.value)
		fmt.Fprintf(&canonical, "%s:%s\n", tp.name, tp.value)
	}
	if signTrailer {
		sum := sha256.Sum256([]byte(canonical.String()))
		fmt.Fprintf(&b, "x-amz-trailer-signature:%s\r\n", sc.trailerSignature(hex.EncodeToString(sum[:])))
	}
	b.WriteString("\r\n")
	return b.String()
}

func TestChunkedSignedRoundTrip(t *testing.T) {
	chunks := [][]byte{
		[]byte(strings.Repeat("a", 1024)),
		[]byte("final little chunk"),
	}
	stream := buildSignedStream(chunks, nil, false)

	cr := NewChunkedReader(strings.NewReader(stream), testStreamContext())
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := strings.Repeat("a", 1024) + "final little chunk"
	if string(got) != want {
		t.Fatalf("decoded %d bytes, want %d", len(got), len(want))
	}
}

func TestChunkedSignedTrailer(t *testing.T) {
	sc := testStreamContext()
	sc.trailerSigned = true
	stream := buildSignedStream([][]byte{[]byte("data")},
		[]trailerPair{{"x-amz-checksum-crc32", "AAAAAA=="}}, true)

	cr := NewChunkedReader(strings.NewReader(stream), sc)
	if _, err := io.ReadAll(cr); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if got := cr.Trailer().Get("x-amz-checksum-crc32"); got != "AAAAAA==" {
		t.Fatalf("trailer = %q", got)
	}
}

func TestChunkedTampered(t *testing.T) {
	stream := buildSignedStream([][]byte{[]byte("hello world")}, nil, false)

	tampered := strings.Replace(stream, "hello world", "hello wOrld", 1)
	cr := NewChunkedReader(strings.NewReader(tampered), testStreamContext())
	_, err := io.ReadAll(cr)
	if !errors.Is(err, ErrChunkSignature) {
		t.Fatalf("tampered data: err = %v, want ErrChunkSignature", err)
	}

	// Wrong seed signature (as if the Authorization signature differed).
	sc := testStreamContext()
	sc.prevSig = strings.Repeat("0", 64)
	cr = NewChunkedReader(strings.NewReader(stream), sc)
	if _, err := io.ReadAll(cr); !errors.Is(err, ErrChunkSignature) {
		t.Fatalf("wrong seed: err = %v, want ErrChunkSignature", err)
	}
}

func TestChunkedUnsignedTrailer(t *testing.T) {
	var b strings.Builder
	b.WriteString("b\r\nhello world\r\n")
	b.WriteString("0\r\n")
	b.WriteString("x-amz-checksum-sha256:qUiQTy8PR5uPgZdpSzAYSw0u0cHNKh7A+4XSmaGSpEc=\r\n")
	b.WriteString("\r\n")

	cr := NewChunkedReader(strings.NewReader(b.String()), nil)
	got, err := io.ReadAll(cr)
	if err != nil || string(got) != "hello world" {
		t.Fatalf("decoded %q, %v", got, err)
	}
	if cr.Trailer().Get("x-amz-checksum-sha256") == "" {
		t.Fatal("missing trailer")
	}
}
