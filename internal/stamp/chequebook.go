package stamp

import (
	"context"
	"log/slog"
	"math/big"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/metrics"
)

// DefaultChequebookInterval is how often the keeper checks the chequebook.
const DefaultChequebookInterval = 24 * time.Hour

// plurPerBZZ converts between xBZZ and Bee's on-chain unit (1e16 PLUR/xBZZ).
var plurPerBZZ = big.NewFloat(1e16)

// Chequebook keeps the node's chequebook funded automatically: Bee seeds it
// exactly once at deployment and never refills it, so without this a busy
// gateway can drain it and lose peers. The keeper checks daily and, when the
// available balance falls below min, tops it up to target from the node
// wallet (guarded by wallet xBZZ and gas balances).
type Chequebook struct {
	bee      *bee.Client
	log      *slog.Logger
	min      *big.Int // PLUR
	target   *big.Int // PLUR
	reserve  *big.Int // PLUR: wallet xBZZ never taken — postage has priority
	interval time.Duration
}

// NewChequebook builds a keeper with thresholds in xBZZ. minBZZ <= 0
// returns nil: automatic top-up disabled (Run and check are nil-safe).
func NewChequebook(beeClient *bee.Client, minBZZ, targetBZZ, reserveBZZ float64, logger *slog.Logger) *Chequebook {
	if minBZZ <= 0 {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Chequebook{
		bee:      beeClient,
		log:      logger,
		min:      bzzToPlur(minBZZ),
		target:   bzzToPlur(targetBZZ),
		reserve:  bzzToPlur(reserveBZZ),
		interval: DefaultChequebookInterval,
	}
}

func bzzToPlur(v float64) *big.Int {
	out, _ := new(big.Float).Mul(big.NewFloat(v), plurPerBZZ).Int(nil)
	return out
}

func plurToBZZ(v *big.Int) float64 {
	out, _ := new(big.Float).Quo(new(big.Float).SetInt(v), plurPerBZZ).Float64()
	return out
}

// Run checks once at startup (after a short grace period for the node) and
// then on the daily interval, until ctx is cancelled.
func (c *Chequebook) Run(ctx context.Context) {
	if c == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}
	c.check(ctx)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.check(ctx)
		}
	}
}

func (c *Chequebook) check(ctx context.Context) {
	_, available, err := c.bee.ChequebookBalance(ctx)
	if err != nil {
		c.log.Warn("chequebook balance check failed", "err", err)
		return
	}
	metrics.ChequebookAvailableBZZ.Set(plurToBZZ(available))

	if available.Cmp(c.min) >= 0 {
		c.log.Info("chequebook balance ok", "available_bzz", plurToBZZ(available))
		return
	}

	need := new(big.Int).Sub(c.target, available)
	walletBZZ, native, err := c.bee.WalletBalance(ctx)
	if err != nil {
		c.log.Warn("wallet balance check failed", "err", err)
		return
	}
	metrics.WalletBZZ.Set(plurToBZZ(walletBZZ))

	if native.Sign() <= 0 {
		c.log.Warn("chequebook needs a top-up but the wallet has no xDAI for gas",
			"available_bzz", plurToBZZ(available), "wallet_bzz", plurToBZZ(walletBZZ))
		return
	}
	// Only wallet funds above the reserve may fund bandwidth: the wallet is
	// what pays for postage, and storage outranks bandwidth.
	depositable := new(big.Int).Sub(walletBZZ, c.reserve)
	if need.Cmp(depositable) > 0 {
		need = depositable
	}
	if need.Sign() <= 0 {
		c.log.Warn("chequebook needs a top-up but the wallet is at or below the postage reserve",
			"available_bzz", plurToBZZ(available),
			"wallet_bzz", plurToBZZ(walletBZZ),
			"reserve_bzz", plurToBZZ(c.reserve))
		return
	}

	if err := c.bee.ChequebookDeposit(ctx, need); err != nil {
		c.log.Warn("chequebook deposit failed", "amount_bzz", plurToBZZ(need), "err", err)
		return
	}
	metrics.ChequebookDepositsTotal.Inc()
	c.log.Info("chequebook topped up from wallet",
		"deposited_bzz", plurToBZZ(need),
		"was_available_bzz", plurToBZZ(available),
		"target_bzz", plurToBZZ(c.target))
}
