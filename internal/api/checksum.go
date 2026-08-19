package api

import (
	"crypto/sha1"
	"crypto/sha256"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strings"
)

// crc64nvmeTable is the NVME polynomial in reversed form — AWS's CRC64NVME.
var crc64nvmeTable = crc64.MakeTable(0x9a6c9329ac4bc9b5)

var checksumAlgorithms = map[string]func() hash.Hash{
	"crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"crc64nvme": func() hash.Hash { return crc64.New(crc64nvmeTable) },
	"sha1":      sha1.New,
	"sha256":    sha256.New,
}

// checksumRequest describes the flexible checksum a write asked for: the
// algorithm (lowercase), an expected base64 value when given up front, and
// whether the value arrives in the aws-chunked trailer instead.
type checksumRequest struct {
	alg       string
	expected  string
	inTrailer bool
}

// parseChecksumRequest inspects x-amz-checksum-<alg>, x-amz-trailer and
// x-amz-sdk-checksum-algorithm, in that precedence.
func parseChecksumRequest(h http.Header) (*checksumRequest, *apiError) {
	for alg := range checksumAlgorithms {
		if v := h.Get("x-amz-checksum-" + alg); v != "" {
			return &checksumRequest{alg: alg, expected: v}, nil
		}
	}
	if t := strings.ToLower(h.Get("x-amz-trailer")); t != "" {
		alg, ok := strings.CutPrefix(t, "x-amz-checksum-")
		if !ok || checksumAlgorithms[alg] == nil {
			e := errInvalidRequest.withMessage("unsupported x-amz-trailer: " + t)
			return nil, &e
		}
		return &checksumRequest{alg: alg, inTrailer: true}, nil
	}
	if a := strings.ToLower(h.Get("x-amz-sdk-checksum-algorithm")); a != "" {
		if checksumAlgorithms[a] == nil {
			e := errInvalidRequest.withMessage("unsupported checksum algorithm: " + a)
			return nil, &e
		}
		return &checksumRequest{alg: a}, nil
	}
	return nil, nil
}
