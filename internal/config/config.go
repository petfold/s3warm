// Package config holds gateway configuration, loaded from flags with
// environment-variable defaults (S3WARM_*).
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// ListenAddr is the address the S3 API listens on.
	ListenAddr string
	// BeeAPI is the base URL of the Bee node HTTP API.
	BeeAPI string
	// BatchID is the default postage batch id used to stamp uploads.
	BatchID string
	// BatchIDFile is read (trimmed) into BatchID when BatchID is empty —
	// lets an init step provision a batch and hand it to the gateway.
	BatchIDFile string
	// Region is the region label reported to S3 clients.
	Region string
	// AccessKey/SecretKey form the single static credential pair.
	// An empty AccessKey enables anonymous mode (development only).
	AccessKey string
	SecretKey string
	// RedundancyLevel is the default erasure-coding level (0-4) applied to
	// uploads with the STANDARD storage class.
	RedundancyLevel int
	// Encrypt uploads on Swarm (maps to SSE; the 64-byte reference stays private).
	Encrypt bool
	// Ack is the PUT ack policy (design §6): what a 200 means.
	//   "node"    — object is in the Bee node's local store, network push
	//               follows asynchronously (default)
	//   "network" — chunks pushed to the network before the ack (direct
	//               upload): strongest, slowest
	Ack string
	// Domain enables virtual-host-style addressing (bucket.<Domain>) when set.
	Domain string
	// DB is the SQLite metadata index path. Empty selects the in-memory
	// index (development only — metadata is lost on restart).
	DB string
	// Commit selects the bucket commit-chain mode (design §5):
	// "async" (default) builds a debounced manifest commit per write batch;
	// "off" disables the portable on-Swarm representation.
	Commit string
	// FeedKey is a hex secp256k1 private key; when set, every commit is
	// anchored under a sequence feed (owner = this key,
	// topic = keccak256("s3warm/1/"+bucket)) as its checkpoint (design §5).
	FeedKey string
	// FetchStrategy is the default erasure-coding fetch strategy for reads
	// (0-4; empty = node default). Per-request override:
	// x-swarm-redundancy-strategy (design §17).
	FetchStrategy string
}

func Load(args []string) (*Config, error) {
	cfg := &Config{
		ListenAddr:      envStr("S3WARM_LISTEN", ":8333"),
		BeeAPI:          envStr("S3WARM_BEE_API", "http://127.0.0.1:1633"),
		BatchID:         envStr("S3WARM_BATCH_ID", ""),
		BatchIDFile:     envStr("S3WARM_BATCH_ID_FILE", ""),
		Region:          envStr("S3WARM_REGION", "us-east-1"),
		AccessKey:       envStr("S3WARM_ACCESS_KEY", ""),
		SecretKey:       envStr("S3WARM_SECRET_KEY", ""),
		RedundancyLevel: envInt("S3WARM_REDUNDANCY", 0),
		Encrypt:         envBool("S3WARM_ENCRYPT", false),
		Ack:             envStr("S3WARM_ACK", "node"),
		Domain:          envStr("S3WARM_DOMAIN", ""),
		DB:              envStr("S3WARM_DB", "s3warm.db"),
		Commit:          envStr("S3WARM_COMMIT", "async"),
		FeedKey:         envStr("S3WARM_FEED_KEY", ""),
		FetchStrategy:   envStr("S3WARM_FETCH_STRATEGY", ""),
	}

	fs := flag.NewFlagSet("s3warm", flag.ContinueOnError)
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "S3 API listen address")
	fs.StringVar(&cfg.BeeAPI, "bee-api", cfg.BeeAPI, "Bee node API endpoint")
	fs.StringVar(&cfg.BatchID, "batch-id", cfg.BatchID, "default postage batch id used for uploads")
	fs.StringVar(&cfg.BatchIDFile, "batch-id-file", cfg.BatchIDFile, "file to read the default batch id from when -batch-id is empty")
	fs.StringVar(&cfg.Region, "region", cfg.Region, "region label reported to S3 clients")
	fs.StringVar(&cfg.AccessKey, "access-key", cfg.AccessKey, "access key id (empty enables anonymous dev mode)")
	fs.StringVar(&cfg.SecretKey, "secret-key", cfg.SecretKey, "secret access key")
	fs.IntVar(&cfg.RedundancyLevel, "redundancy", cfg.RedundancyLevel, "default erasure-coding redundancy level (0-4)")
	fs.BoolVar(&cfg.Encrypt, "encrypt", cfg.Encrypt, "encrypt uploads on Swarm")
	fs.StringVar(&cfg.Ack, "ack", cfg.Ack, "PUT ack policy: node (Bee local store, default) or network (pushed before ack)")
	fs.StringVar(&cfg.Domain, "domain", cfg.Domain, "base domain for virtual-host-style bucket addressing")
	fs.StringVar(&cfg.DB, "db", cfg.DB, "SQLite metadata index path (empty = in-memory, dev only)")
	fs.StringVar(&cfg.Commit, "commit", cfg.Commit, "bucket commit-chain mode: async (default) or off")
	fs.StringVar(&cfg.FeedKey, "feed-key", cfg.FeedKey, "hex secp256k1 key for feed checkpoint anchors (empty = no feed publishing)")
	fs.StringVar(&cfg.FetchStrategy, "fetch-strategy", cfg.FetchStrategy, "default erasure-coding fetch strategy for reads (0-4, empty = node default)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if cfg.BatchID == "" && cfg.BatchIDFile != "" {
		b, err := os.ReadFile(cfg.BatchIDFile)
		if err != nil {
			return nil, fmt.Errorf("reading batch-id-file: %w", err)
		}
		cfg.BatchID = strings.TrimSpace(string(b))
	}
	if cfg.Commit != "async" && cfg.Commit != "off" {
		return nil, fmt.Errorf("commit mode must be async or off, got %q", cfg.Commit)
	}
	if cfg.Ack != "node" && cfg.Ack != "network" {
		return nil, fmt.Errorf("ack policy must be node or network, got %q", cfg.Ack)
	}
	if cfg.RedundancyLevel < 0 || cfg.RedundancyLevel > 4 {
		return nil, fmt.Errorf("redundancy level must be between 0 and 4, got %d", cfg.RedundancyLevel)
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, fmt.Errorf("access-key and secret-key must be set together")
	}
	return cfg, nil
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
