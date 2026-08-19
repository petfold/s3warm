// Package stamp manages postage batches (design §9): it caches batch state
// from Bee, validates a batch synchronously before an upload is acked (the
// ack-policy rule in §6 — semantic failures must surface on the PUT, never
// later), estimates remaining TTL for the x-swarm-batch-ttl header, and
// warns as batches approach capacity or expiry.
package stamp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/metrics"
)

const (
	// DefaultRefreshInterval bounds both cache staleness for lookups and the
	// background warning sweep.
	DefaultRefreshInterval = 5 * time.Minute
	warnUtilizationRatio   = 0.8
	warnTTL                = 30 * 24 * time.Hour
)

// BatchError is a positively-diagnosed problem with a batch: the client
// could have been told synchronously, so the PUT must not be acked.
type BatchError struct {
	BatchID string
	Reason  string
}

func (e *BatchError) Error() string {
	return "postage batch " + e.BatchID + ": " + e.Reason
}

// Info is a cached batch state plus the time it was observed, so remaining
// TTL can be estimated between refreshes.
type Info struct {
	bee.Stamp
	FetchedAt time.Time
}

// TTLRemaining estimates seconds of batch life left as of now.
func (i *Info) TTLRemaining() time.Duration {
	if i.BatchTTL < 0 {
		// Bee reports a negative TTL when it cannot compute one; treat as
		// unbounded rather than expired.
		return time.Duration(1<<62 - 1)
	}
	return time.Duration(i.BatchTTL)*time.Second - time.Since(i.FetchedAt)
}

// Ratio is the batch's fill ratio in [0, 1].
func (i *Info) Ratio() float64 {
	if i.UtilizationRatio > 0 {
		return i.UtilizationRatio
	}
	if i.Depth <= i.BucketDepth {
		return 0
	}
	return float64(i.Utilization) / float64(uint64(1)<<(i.Depth-i.BucketDepth))
}

type Manager struct {
	bee      *bee.Client
	log      *slog.Logger
	interval time.Duration

	mu    sync.RWMutex
	cache map[string]*Info
}

func NewManager(beeClient *bee.Client, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		bee:      beeClient,
		log:      logger,
		interval: DefaultRefreshInterval,
		cache:    make(map[string]*Info),
	}
}

// Get returns batch state, from cache when fresh. On a fetch failure a stale
// entry beats nothing.
func (m *Manager) Get(ctx context.Context, batchID string) (*Info, error) {
	m.mu.RLock()
	info := m.cache[batchID]
	m.mu.RUnlock()
	if info != nil && time.Since(info.FetchedAt) < m.interval {
		return info, nil
	}
	fresh, err := m.fetch(ctx, batchID)
	if err != nil {
		if info != nil {
			return info, nil
		}
		return nil, err
	}
	return fresh, nil
}

// Check validates a batch before an upload. It returns a *BatchError only
// for positively-diagnosed problems; when Bee cannot be asked (transient
// failure) it returns nil and lets the upload attempt decide.
func (m *Manager) Check(ctx context.Context, batchID string) error {
	info, err := m.Get(ctx, batchID)
	if err != nil {
		var se *bee.StatusError
		if errors.As(err, &se) && se.StatusCode == http.StatusNotFound {
			return &BatchError{batchID, "batch not found on the node"}
		}
		return nil
	}
	switch {
	case !info.Exists:
		return &BatchError{batchID, "batch does not exist"}
	case !info.Usable:
		return &BatchError{batchID, "batch is not usable (yet)"}
	case info.ImmutableFlag && info.Ratio() >= 1:
		return &BatchError{batchID, "immutable batch is out of capacity"}
	case info.TTLRemaining() <= 0:
		return &BatchError{batchID, "batch has expired"}
	}
	return nil
}

// Run refreshes every batch the manager has seen and logs threshold
// warnings, until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.sweep(ctx)
		}
	}
}

func (m *Manager) sweep(ctx context.Context) {
	m.mu.RLock()
	ids := make([]string, 0, len(m.cache))
	for id := range m.cache {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	for _, id := range ids {
		info, err := m.fetch(ctx, id)
		if err != nil {
			m.log.Warn("postage batch refresh failed", "batch", id, "err", err)
			continue
		}
		if r := info.Ratio(); r >= warnUtilizationRatio {
			m.log.Warn("postage batch approaching capacity", "batch", id,
				"utilization", r, "immutable", info.ImmutableFlag)
		}
		if ttl := info.TTLRemaining(); ttl < warnTTL {
			m.log.Warn("postage batch expiring soon", "batch", id,
				"ttl", ttl.Round(time.Minute).String())
		}
	}
}

func (m *Manager) fetch(ctx context.Context, batchID string) (*Info, error) {
	st, err := m.bee.Stamp(ctx, batchID)
	if err != nil {
		return nil, err
	}
	info := &Info{Stamp: *st, FetchedAt: time.Now()}
	m.mu.Lock()
	m.cache[batchID] = info
	m.mu.Unlock()
	metrics.StampTTLSeconds.WithLabelValues(batchID).Set(info.TTLRemaining().Seconds())
	metrics.StampUtilizationRatio.WithLabelValues(batchID).Set(info.Ratio())
	return info, nil
}
