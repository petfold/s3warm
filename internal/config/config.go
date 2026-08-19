// Package config holds gateway configuration, loaded from flags with
// environment-variable defaults (S3WARM_*).
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	// ListenAddr is the address the S3 API listens on.
	ListenAddr string
	// BeeAPI is the base URL of the Bee node HTTP API.
	BeeAPI string
	// BatchID is the default postage batch id used to stamp uploads.
	BatchID string
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
	// Deferred selects deferred (asynchronous) upload to the network:
	// lower PUT latency, durability follows.
	Deferred bool
	// Domain enables virtual-host-style addressing (bucket.<Domain>) when set.
	Domain string
	// DB is the SQLite metadata index path. Empty selects the in-memory
	// index (development only — metadata is lost on restart).
	DB string
}

func Load(args []string) (*Config, error) {
	cfg := &Config{
		ListenAddr:      envStr("S3WARM_LISTEN", ":8333"),
		BeeAPI:          envStr("S3WARM_BEE_API", "http://127.0.0.1:1633"),
		BatchID:         envStr("S3WARM_BATCH_ID", ""),
		Region:          envStr("S3WARM_REGION", "us-east-1"),
		AccessKey:       envStr("S3WARM_ACCESS_KEY", ""),
		SecretKey:       envStr("S3WARM_SECRET_KEY", ""),
		RedundancyLevel: envInt("S3WARM_REDUNDANCY", 0),
		Encrypt:         envBool("S3WARM_ENCRYPT", false),
		Deferred:        envBool("S3WARM_DEFERRED", true),
		Domain:          envStr("S3WARM_DOMAIN", ""),
		DB:              envStr("S3WARM_DB", "s3warm.db"),
	}

	fs := flag.NewFlagSet("s3warm", flag.ContinueOnError)
	fs.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "S3 API listen address")
	fs.StringVar(&cfg.BeeAPI, "bee-api", cfg.BeeAPI, "Bee node API endpoint")
	fs.StringVar(&cfg.BatchID, "batch-id", cfg.BatchID, "default postage batch id used for uploads")
	fs.StringVar(&cfg.Region, "region", cfg.Region, "region label reported to S3 clients")
	fs.StringVar(&cfg.AccessKey, "access-key", cfg.AccessKey, "access key id (empty enables anonymous dev mode)")
	fs.StringVar(&cfg.SecretKey, "secret-key", cfg.SecretKey, "secret access key")
	fs.IntVar(&cfg.RedundancyLevel, "redundancy", cfg.RedundancyLevel, "default erasure-coding redundancy level (0-4)")
	fs.BoolVar(&cfg.Encrypt, "encrypt", cfg.Encrypt, "encrypt uploads on Swarm")
	fs.BoolVar(&cfg.Deferred, "deferred", cfg.Deferred, "use deferred (asynchronous) uploads to the network")
	fs.StringVar(&cfg.Domain, "domain", cfg.Domain, "base domain for virtual-host-style bucket addressing")
	fs.StringVar(&cfg.DB, "db", cfg.DB, "SQLite metadata index path (empty = in-memory, dev only)")
	if err := fs.Parse(args); err != nil {
		return nil, err
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
