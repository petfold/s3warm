package manifest

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/soc"

	"github.com/petfold/s3warm/internal/bee"
)

// FeedPublisher anchors commit roots under a sequence feed (design §5:
// feeds as checkpoint anchors). The feed for a bucket is
// (owner = the configured key, topic = keccak256("s3warm/1/"+bucket));
// each update's payload is the 32-byte commit root, resolvable by any Bee
// via GET /feeds/{owner}/{topic}.
type FeedPublisher struct {
	signer crypto.Signer
	owner  []byte
	bee    *bee.Client
	log    *slog.Logger
}

// NewFeedPublisher builds a publisher from a hex-encoded secp256k1 private
// key (the tenant/feed-owner key, design §8).
func NewFeedPublisher(privateKeyHex string, beeClient *bee.Client, logger *slog.Logger) (*FeedPublisher, error) {
	raw, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("feed key must be hex: %w", err)
	}
	key, err := crypto.DecodeSecp256k1PrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("decoding feed key: %w", err)
	}
	signer := crypto.NewDefaultSigner(key)
	owner, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &FeedPublisher{signer: signer, owner: owner.Bytes(), bee: beeClient, log: logger}, nil
}

// Owner returns the feed owner address (hex, no 0x).
func (f *FeedPublisher) Owner() string { return hex.EncodeToString(f.owner) }

// Topic returns a bucket's feed topic (hex).
func (f *FeedPublisher) Topic(bucket string) string {
	t, _ := crypto.LegacyKeccak256([]byte("s3warm/1/" + bucket)) //nolint:errcheck // keccak write cannot fail
	return hex.EncodeToString(t)
}

// Publish anchors a commit root as sequence-feed update index seq-1
// (commits are 1-based and contiguous, feed indices 0-based — Bee's
// sequence lookup requires index 0 to exist).
func (f *FeedPublisher) Publish(ctx context.Context, bucket, rootHex string, seq int64, batch string) error {
	topic, err := crypto.LegacyKeccak256([]byte("s3warm/1/" + bucket))
	if err != nil {
		return err
	}
	index := make([]byte, 8)
	binary.BigEndian.PutUint64(index, uint64(seq-1))
	id, err := crypto.LegacyKeccak256(append(topic, index...))
	if err != nil {
		return err
	}

	payload, err := hex.DecodeString(rootHex)
	if err != nil {
		return err
	}
	inner, err := cac.New(payload)
	if err != nil {
		return err
	}
	sch := soc.New(id, inner)
	if _, err := sch.Sign(f.signer); err != nil {
		return err
	}
	return f.bee.UploadSOC(ctx,
		hex.EncodeToString(sch.OwnerAddress()),
		hex.EncodeToString(id),
		hex.EncodeToString(sch.Signature()),
		inner.Data(), batch)
}
