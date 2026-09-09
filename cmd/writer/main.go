package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BABTUNA/minicdc/internal/config"
	"github.com/BABTUNA/minicdc/internal/writer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	w, err := writer.New(ctx, cfg)
	if err != nil {
		slog.Error("writer init failed", "err", err)
		os.Exit(1)
	}
	defer w.Close()

	slog.Info("writer up", "dest", cfg.DestDSN, "topic", cfg.Topic)
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("writer failed", "err", err)
		os.Exit(1)
	}
	slog.Info("writer shut down")
}
