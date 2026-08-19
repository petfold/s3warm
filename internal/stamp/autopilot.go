package stamp

import (
	"context"
	"log/slog"
	"math/big"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/metrics"
)

// DefaultAutopilotInterval is how often the autopilot evaluates batches.
// Batch state moves slowly (TTLs are weeks); an hour keeps reaction time
// far below the thresholds while never hammering the chain.
const DefaultAutopilotInterval = time.Hour

// gnosisBlockTime converts between amount-per-chunk and TTL:
// ttl = amount / price * blockTime.
const gnosisBlockTime = 5 * time.Second

// Autopilot keeps the batches the gateway actually uses alive (design §9,
// opt-in — it spends funds): when a batch's TTL sinks below min it tops it
// up to target, and when utilization approaches capacity it dilutes
// (depth+1, doubling capacity). Both are on-chain transactions paid by the
// node wallet, so every action is guarded by wallet balances and logged.
//
// One action per batch per cycle: topup and dilute interact (dilution
// halves TTL), and the effect of a transaction only shows in batch state
// once mined — acting once and re-evaluating next cycle keeps decisions
// grounded in observed state instead of predictions.
type Autopilot struct {
	mgr       *Manager
	bee       *bee.Client
	log       *slog.Logger
	minTTL    time.Duration
	targetTTL time.Duration
	diluteAt  float64
	interval  time.Duration
}

// NewAutopilot builds the keeper. enabled=false returns nil (Run is
// nil-safe): the autopilot spends real xBZZ, so it is strictly opt-in.
func NewAutopilot(mgr *Manager, beeClient *bee.Client, enabled bool, minTTLDays, targetTTLDays float64, diluteAt float64, logger *slog.Logger) *Autopilot {
	if !enabled {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Autopilot{
		mgr:       mgr,
		bee:       beeClient,
		log:       logger,
		minTTL:    time.Duration(minTTLDays * 24 * float64(time.Hour)),
		targetTTL: time.Duration(targetTTLDays * 24 * float64(time.Hour)),
		diluteAt:  diluteAt,
		interval:  DefaultAutopilotInterval,
	}
}

// Run checks once after a short startup grace period, then hourly.
func (a *Autopilot) Run(ctx context.Context) {
	if a == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Minute):
	}
	a.pass(ctx)
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.pass(ctx)
		}
	}
}

// pass evaluates every batch the gateway uses that this node issued.
func (a *Autopilot) pass(ctx context.Context) {
	own, err := a.bee.ListStamps(ctx)
	if err != nil {
		a.log.Warn("stamp autopilot: listing node batches failed", "err", err)
		return
	}
	mine := make(map[string]bool, len(own))
	for _, st := range own {
		mine[st.BatchID] = true
	}

	a.mgr.mu.RLock()
	ids := make([]string, 0, len(a.mgr.cache))
	for id := range a.mgr.cache {
		ids = append(ids, id)
	}
	a.mgr.mu.RUnlock()

	for _, id := range ids {
		if !mine[id] {
			// Foreign batches (bought elsewhere, handed to the gateway) can
			// only be managed by their issuing node.
			continue
		}
		info, err := a.mgr.Get(ctx, id)
		if err != nil || !info.Exists || !info.Usable {
			continue
		}
		a.evaluate(ctx, id, info)
	}
}

func (a *Autopilot) evaluate(ctx context.Context, id string, info *Info) {
	// Capacity first: dilution halves TTL, so a needed topup is recomputed
	// on post-dilution state next cycle.
	if info.Ratio() >= a.diluteAt {
		if info.ImmutableFlag {
			a.log.Warn("stamp autopilot: immutable batch approaching capacity — cannot dilute; create a new batch and update the gateway/bucket default",
				"batch", id, "utilization", info.Ratio())
		} else {
			newDepth := info.Depth + 1
			if err := a.bee.DiluteBatch(ctx, id, newDepth); err != nil {
				a.log.Warn("stamp autopilot: dilute failed", "batch", id, "err", err)
				return
			}
			metrics.StampDilutesTotal.Inc()
			a.log.Info("stamp autopilot: diluted batch (capacity doubled, TTL halves as the balance spreads)",
				"batch", id, "utilization", info.Ratio(), "new_depth", newDepth)
			return // one action per cycle
		}
	}

	ttl := info.TTLRemaining()
	if ttl >= a.minTTL {
		return
	}
	price, err := a.bee.ChainState(ctx)
	if err != nil || price.Sign() <= 0 {
		a.log.Warn("stamp autopilot: cannot read storage price", "err", err)
		return
	}
	// amount (PLUR/chunk) buying the missing TTL: blocks * price.
	missing := a.targetTTL - ttl
	blocks := new(big.Int).SetInt64(int64(missing/gnosisBlockTime) + 1)
	amount := new(big.Int).Mul(blocks, price)
	// Total wallet cost is per-chunk amount across the whole batch.
	cost := new(big.Int).Lsh(amount, uint(info.Depth))

	walletBZZ, native, err := a.bee.WalletBalance(ctx)
	if err != nil {
		a.log.Warn("stamp autopilot: wallet balance check failed", "err", err)
		return
	}
	if native.Sign() <= 0 {
		a.log.Warn("stamp autopilot: batch needs a top-up but the wallet has no xDAI for gas",
			"batch", id, "ttl", ttl.Round(time.Hour).String())
		return
	}
	if walletBZZ.Cmp(cost) < 0 {
		a.log.Warn("stamp autopilot: batch needs a top-up the wallet cannot afford — fund the node wallet",
			"batch", id, "ttl", ttl.Round(time.Hour).String(),
			"cost_bzz", plurToBZZ(cost), "wallet_bzz", plurToBZZ(walletBZZ))
		return
	}

	if err := a.bee.TopupBatch(ctx, id, amount); err != nil {
		a.log.Warn("stamp autopilot: topup failed", "batch", id, "err", err)
		return
	}
	metrics.StampTopupsTotal.Inc()
	a.log.Info("stamp autopilot: topped up batch",
		"batch", id,
		"was_ttl", ttl.Round(time.Hour).String(),
		"target_ttl", a.targetTTL.String(),
		"amount_per_chunk_plur", amount.String(),
		"cost_bzz", plurToBZZ(cost))
}
