package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BABTUNA/minicdc/internal/config"
	"github.com/BABTUNA/minicdc/internal/reader"
	"github.com/BABTUNA/minicdc/internal/sink"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	pub := sink.NewKafkaPublisher(cfg.KafkaBroker, cfg.Topic)
	defer pub.Close()

	r, err := reader.New(ctx, cfg, pub)
	if err != nil {
		slog.Error("reader init failed", "err", err)
		os.Exit(1)
	}
	defer r.Close(context.Background())

	slog.Info("reader up", "source", cfg.SourceDSN, "topic", cfg.Topic)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("reader failed", "err", err)
		os.Exit(1)
	}
	slog.Info("reader shut down")
}
