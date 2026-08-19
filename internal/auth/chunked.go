package auth

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// emptySHA256 is the hex SHA-256 of the empty string, used in every chunk's
// string-to-sign and for the final zero-length chunk.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// ErrChunkSignature is wrapped by chunk/trailer signature failures so the
// API layer can answer SignatureDoesNotMatch rather than a generic error.
var ErrChunkSignature = errors.New("chunk signature does not match")

// StreamContext carries what is needed to verify an aws-chunked body's
// signature chain. Verify populates it on Identity for
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD payloads; the seed signature is the
// request's own signature.
type StreamContext struct {
	signingKey    []byte
	scope         string // date/region/s3/aws4_request
	amzDate       string
	prevSig       string
	trailerSigned bool
}

func (sc *StreamContext) chunkSignature(payloadSHA string) string {
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD", sc.amzDate, sc.scope, sc.prevSig, emptySHA256, payloadSHA,
	}, "\n")
	return hex.EncodeToString(hmacSHA256(sc.signingKey, sts))
}

func (sc *StreamContext) trailerSignature(trailerSHA string) string {
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256-TRAILER", sc.amzDate, sc.scope, sc.prevSig, trailerSHA,
	}, "\n")
	return hex.EncodeToString(hmacSHA256(sc.signingKey, sts))
}

// ChunkedReader decodes aws-chunked framing, verifying the chunk signature
// chain when a StreamContext is present (nil = the unsigned-trailer
// variant). Trailer headers are available via Trailer after EOF.
type ChunkedReader struct {
	br     *bufio.Reader
	stream *StreamContext

	remaining int64
	chunkSig  string
	chunkHash hash.Hash

	trailer http.Header
	done    bool
	err     error
}

func NewChunkedReader(r io.Reader, stream *StreamContext) *ChunkedReader {
	return &ChunkedReader{br: bufio.NewReader(r), stream: stream}
}

// Err reports the decode/verification failure, if any — useful when the
// consumer saw only a generic read error.
func (c *ChunkedReader) Err() error { return c.err }

// Trailer returns the trailing headers; valid after Read returned io.EOF.
func (c *ChunkedReader) Trailer() http.Header { return c.trailer }

func (c *ChunkedReader) Read(p []byte) (int, error) {
	for {
		if c.err != nil {
			return 0, c.err
		}
		if c.done {
			return 0, io.EOF
		}
		if c.remaining == 0 {
			if err := c.advance(); err != nil {
				c.err = err
				return 0, err
			}
			continue
		}
		if int64(len(p)) > c.remaining {
			p = p[:c.remaining]
		}
		n, err := c.br.Read(p)
		if n > 0 {
			c.remaining -= int64(n)
			if c.chunkHash != nil {
				c.chunkHash.Write(p[:n])
			}
			if c.remaining == 0 {
				if err := c.finishChunk(); err != nil {
					c.err = err
				}
			}
			return n, nil
		}
		if err != nil {
			c.err = fmt.Errorf("aws-chunked: truncated chunk data: %w", err)
			return 0, c.err
		}
	}
}

// advance parses the next chunk header; a zero-size chunk ends the stream
// and is followed by optional trailers.
func (c *ChunkedReader) advance() error {
	line, err := c.readLine()
	if err != nil {
		return fmt.Errorf("aws-chunked: reading chunk header: %w", err)
	}
	sizeStr, sig, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil || size < 0 {
		return fmt.Errorf("aws-chunked: malformed chunk size %q", sizeStr)
	}
	c.chunkSig = strings.TrimPrefix(sig, "chunk-signature=")
	if c.stream != nil && c.chunkSig == "" {
		return fmt.Errorf("aws-chunked: missing chunk signature: %w", ErrChunkSignature)
	}

	if size == 0 {
		if c.stream != nil {
			if err := c.verifyChunk(emptySHA256); err != nil {
				return err
			}
		}
		return c.readTrailer()
	}
	c.remaining = size
	if c.stream != nil {
		c.chunkHash = sha256.New()
	}
	return nil
}

// finishChunk consumes the chunk's trailing CRLF and verifies its signature.
func (c *ChunkedReader) finishChunk() error {
	if err := c.expectCRLF(); err != nil {
		return err
	}
	if c.stream == nil {
		return nil
	}
	sum := c.chunkHash.Sum(nil)
	c.chunkHash = nil
	return c.verifyChunk(hex.EncodeToString(sum))
}

func (c *ChunkedReader) verifyChunk(payloadSHA string) error {
	want := c.stream.chunkSignature(payloadSHA)
	if !hmac.Equal([]byte(want), []byte(strings.ToLower(c.chunkSig))) {
		return ErrChunkSignature
	}
	c.stream.prevSig = want
	return nil
}

// readTrailer parses trailing headers up to the final blank line (or EOF,
// which some clients send instead) and verifies the trailer signature when
// the payload type requires one.
func (c *ChunkedReader) readTrailer() error {
	c.trailer = http.Header{}
	var canonical strings.Builder
	var trailerSig string
	for {
		line, err := c.readLine()
		if errors.Is(err, io.EOF) || line == "" {
			break
		}
		if err != nil {
			return fmt.Errorf("aws-chunked: reading trailer: %w", err)
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("aws-chunked: malformed trailer line %q", line)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if name == "x-amz-trailer-signature" {
			trailerSig = value
			continue
		}
		c.trailer.Set(name, value)
		fmt.Fprintf(&canonical, "%s:%s\n", name, value)
	}

	if c.stream != nil && c.stream.trailerSigned {
		sum := sha256.Sum256([]byte(canonical.String()))
		want := c.stream.trailerSignature(hex.EncodeToString(sum[:]))
		if !hmac.Equal([]byte(want), []byte(strings.ToLower(trailerSig))) {
			return fmt.Errorf("trailer: %w", ErrChunkSignature)
		}
	}
	c.done = true
	return nil
}

func (c *ChunkedReader) readLine() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		return strings.TrimRight(line, "\r\n"), err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (c *ChunkedReader) expectCRLF() error {
	line, err := c.readLine()
	if err != nil {
		return fmt.Errorf("aws-chunked: expected chunk terminator: %w", err)
	}
	if line != "" {
		return fmt.Errorf("aws-chunked: unexpected data after chunk: %q", line)
	}
	return nil
}
