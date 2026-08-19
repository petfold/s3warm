// Package bee is a thin client for the subset of the Bee HTTP API the gateway
// uses (docs/DESIGN.md §2, appendix A).
package bee

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/petfold/s3warm/internal/metrics"
)

// do executes a request with per-operation metrics (code 0 = transport error).
func (c *Client) do(op string, req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := c.hc.Do(req)
	code := 0
	if resp != nil {
		code = resp.StatusCode
	}
	metrics.BeeRequestsTotal.WithLabelValues(op, strconv.Itoa(code)).Inc()
	metrics.BeeRequestDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	return resp, err
}

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

	resp, err := c.do("bytes_upload", req)
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

// DownloadOptions tune a read: an optional Range and the erasure-coding
// fetch strategy/fallback (design §17).
type DownloadOptions struct {
	Range        string
	Strategy     string // swarm-redundancy-strategy (0-4); empty = node default
	FallbackMode string // swarm-redundancy-fallback-mode (true/false)
}

// DownloadBytes issues GET /bytes/{ref}. The caller owns resp.Body.
// Status is 200 or 206.
func (c *Client) DownloadBytes(ctx context.Context, ref string, o DownloadOptions) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/bytes/"+ref, nil)
	if err != nil {
		return nil, err
	}
	if o.Range != "" {
		req.Header.Set("Range", o.Range)
	}
	if o.Strategy != "" {
		req.Header.Set("swarm-redundancy-strategy", o.Strategy)
	}
	if o.FallbackMode != "" {
		req.Header.Set("swarm-redundancy-fallback-mode", o.FallbackMode)
	}
	resp, err := c.do("bytes_download", req)
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
	resp, err := c.do("stamp", req)
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

// bigIntJSON tolerates Bee's big integers arriving as strings or numbers.
type bigIntJSON big.Int

func (b *bigIntJSON) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if _, ok := (*big.Int)(b).SetString(s, 10); !ok {
		return fmt.Errorf("bee: malformed big integer %q", s)
	}
	return nil
}

// ChequebookBalance returns the chequebook's total and available balances
// in PLUR (1 xBZZ = 1e16 PLUR).
func (c *Client) ChequebookBalance(ctx context.Context) (total, available *big.Int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/chequebook/balance", nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.do("chequebook_balance", req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, newStatusError(resp)
	}
	var out struct {
		TotalBalance     bigIntJSON `json:"totalBalance"`
		AvailableBalance bigIntJSON `json:"availableBalance"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, fmt.Errorf("bee: decoding chequebook balance: %w", err)
	}
	return (*big.Int)(&out.TotalBalance), (*big.Int)(&out.AvailableBalance), nil
}

// WalletBalance returns the node wallet's xBZZ (PLUR) and native xDAI (wei)
// balances.
func (c *Client) WalletBalance(ctx context.Context) (bzz, native *big.Int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/wallet", nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.do("wallet", req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, newStatusError(resp)
	}
	var out struct {
		BZZBalance         bigIntJSON `json:"bzzBalance"`
		NativeTokenBalance bigIntJSON `json:"nativeTokenBalance"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, fmt.Errorf("bee: decoding wallet balance: %w", err)
	}
	return (*big.Int)(&out.BZZBalance), (*big.Int)(&out.NativeTokenBalance), nil
}

// ChequebookDeposit moves amount PLUR of xBZZ from the node wallet into the
// chequebook (an on-chain transaction; the wallet pays gas in xDAI).
func (c *Client) ChequebookDeposit(ctx context.Context, amount *big.Int) error {
	url := c.base + "/chequebook/deposit?amount=" + amount.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.do("chequebook_deposit", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return newStatusError(resp)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // draining for connection reuse
	return nil
}

// UploadSOC uploads a signed single-owner chunk (feed checkpoint updates,
// design §5). data is the wrapped content-addressed chunk's binary form.
func (c *Client) UploadSOC(ctx context.Context, owner, id, signature string, data []byte, batchID string) error {
	url := fmt.Sprintf("%s/soc/%s/%s?sig=%s", c.base, owner, id, signature)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("swarm-postage-batch-id", batchID)
	resp, err := c.do("soc_upload", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return newStatusError(resp)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // draining for connection reuse
	return nil
}

// Pin pins a reference on the node so it survives local garbage collection
// (bucket head roots and snapshots, design §5).
func (c *Client) Pin(ctx context.Context, ref string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/pins/"+ref, nil)
	if err != nil {
		return err
	}
	resp, err := c.do("pin", req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return newStatusError(resp)
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck // draining for connection reuse
	return nil
}

func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.do("health", req)
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
