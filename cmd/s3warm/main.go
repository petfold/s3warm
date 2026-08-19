// Command s3warm runs the S3-compatible API gateway for Swarm.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/petfold/s3warm/internal/api"
	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/config"
	"github.com/petfold/s3warm/internal/stamp"
	"github.com/petfold/s3warm/internal/store"
)

func main() {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		slog.Error("configuration", "err", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if cfg.AccessKey == "" {
		logger.Warn("no credentials configured — running in ANONYMOUS mode; do not expose this listener publicly")
	}
	if cfg.BatchID == "" {
		logger.Warn("no default postage batch configured; writes will fail unless a bucket or request provides one (x-swarm-postage-batch-id)")
	}

	var st store.Store
	if cfg.DB == "" {
		logger.Warn("using in-memory index — metadata is lost on restart")
		st = store.NewMemory()
	} else {
		sq, err := store.OpenSQLite(cfg.DB)
		if err != nil {
			logger.Error("opening index", "db", cfg.DB, "err", err)
			os.Exit(1)
		}
		defer sq.Close()
		logger.Info("metadata index", "db", cfg.DB)
		st = sq
	}

	beeClient := bee.New(cfg.BeeAPI)
	stamps := stamp.NewManager(beeClient, logger)
	if cfg.BatchID != "" {
		checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := stamps.Check(checkCtx, cfg.BatchID); err != nil {
			logger.Warn("default postage batch", "err", err)
		}
		cancel()
	}

	handler := api.New(cfg, st, beeClient, stamps, logger)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go stamps.Run(ctx)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx) //nolint:errcheck
	}()

	logger.Info("s3warm listening", "addr", cfg.ListenAddr, "bee", cfg.BeeAPI, "region", cfg.Region)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server", "err", err)
		os.Exit(1)
	}
	logger.Info("shut down cleanly")
}
