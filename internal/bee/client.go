// Package bee is a thin client for the subset of the Bee HTTP API the gateway
// uses (docs/DESIGN.md §2, appendix A).
package bee

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Client struct {
	base string
	hc   *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		base: strings.TrimRight(baseURL, "/"),
		// No global timeout: transfers are streamed and arbitrarily large.
		// Callers bound requests with contexts.
		hc: &http.Client{},
	}
}

// StatusError is a non-2xx response from Bee.
type StatusError struct {
	StatusCode int
	Message    string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("bee: %d %s", e.StatusCode, e.Message)
}

type UploadOptions struct {
	BatchID         string
	Encrypt         bool
	RedundancyLevel int
	Deferred        bool
	// ContentLength of the body when known, -1 otherwise (chunked upload).
	ContentLength int64
}

// UploadBytes streams body to POST /bytes and returns the Swarm reference.
func (c *Client) UploadBytes(ctx context.Context, body io.Reader, opts UploadOptions) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/bytes", body)
	if err != nil {
		return "", err
	}
	if opts.ContentLength >= 0 {
		req.ContentLength = opts.ContentLength
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if opts.BatchID != "" {
		req.Header.Set("swarm-postage-batch-id", opts.BatchID)
	}
	req.Header.Set("swarm-deferred-upload", strconv.FormatBool(opts.Deferred))
	if opts.Encrypt {
		req.Header.Set("swarm-encrypt", "true")
	}
	if opts.RedundancyLevel > 0 {
		req.Header.Set("swarm-redundancy-level", strconv.Itoa(opts.RedundancyLevel))
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", newStatusError(resp)
	}
	var out struct {
		Reference string `json:"reference"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("bee: decoding upload response: %w", err)
	}
	if out.Reference == "" {
		return "", fmt.Errorf("bee: upload response contained no reference")
	}
	return out.Reference, nil
}

// DownloadBytes issues GET /bytes/{ref}, passing through an optional Range
// header. The caller owns resp.Body. Status is 200 or 206.
func (c *Client) DownloadBytes(ctx context.Context, ref, rangeHeader string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/bytes/"+ref, nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		return nil, newStatusError(resp)
	}
	return resp, nil
}

// Stamp is a postage batch as reported by GET /stamps/{id}.
type Stamp struct {
	BatchID          string  `json:"batchID"`
	Exists           bool    `json:"exists"`
	Usable           bool    `json:"usable"`
	Utilization      uint64  `json:"utilization"`
	UtilizationRatio float64 `json:"utilizationRatio"`
	Depth            uint8   `json:"depth"`
	BucketDepth      uint8   `json:"bucketDepth"`
	Amount           string  `json:"amount"`
	BatchTTL         int64   `json:"batchTTL"` // seconds; negative = unknown/unbounded
	ImmutableFlag    bool    `json:"immutableFlag"`
	Label            string  `json:"label"`
}

// Stamp fetches one postage batch's state from the node.
func (c *Client) Stamp(ctx context.Context, batchID string) (*Stamp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/stamps/"+batchID, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, newStatusError(resp)
	}
	var out Stamp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("bee: decoding stamp response: %w", err)
	}
	return &out, nil
}

func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return newStatusError(resp)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // draining for connection reuse
	return nil
}

func newStatusError(resp *http.Response) *StatusError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	msg := strings.TrimSpace(string(body))
	// Bee errors are usually {"code":..,"message":".."}.
	var je struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &je); err == nil && je.Message != "" {
		msg = je.Message
	}
	if msg == "" {
		msg = resp.Status
	}
	return &StatusError{StatusCode: resp.StatusCode, Message: msg}
}
