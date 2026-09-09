package writer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/minicdc/internal/config"
	"github.com/BABTUNA/minicdc/internal/events"
)

type Writer struct {
	consumer *kafka.Reader
	pool     *pgxpool.Pool
	ddl      *ddlManager
}

func New(ctx context.Context, cfg config.Config) (*Writer, error) {
	pool, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to destination: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping destination: %w", err)
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaBroker},
		GroupID:     "minicdc-writer",
		Topic:       cfg.Topic,
		StartOffset: kafka.FirstOffset,
	})

	return &Writer{consumer: consumer, pool: pool, ddl: newDDLManager()}, nil
}

func (w *Writer) Close() error {
	w.pool.Close()
	return w.consumer.Close()
}

// Run is phase 1's deliberately dumb apply loop: one message, one insert, one
// offset commit. Phase 2 replaces the middle with buffer/flush/merge; the
// invariant that the offset commits only AFTER the destination write succeeds
// is already load-bearing and survives every later phase.
func (w *Writer) Run(ctx context.Context) error {
	for {
		msg, err := w.consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		evt, err := events.Decode(msg.Value)
		if err != nil {
			return fmt.Errorf("offset %d: %w", msg.Offset, err)
		}
		if evt.Op != events.OpCreate {
			return fmt.Errorf("offset %d: op %q not supported in phase 1", msg.Offset, evt.Op)
		}

		if err := w.ddl.ensureTable(ctx, w.pool, evt); err != nil {
			return err
		}
		if err := applyInsert(ctx, w.pool, evt); err != nil {
			return err
		}

		if err := w.consumer.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit offset %d: %w", msg.Offset, err)
		}
		slog.Info("applied", "table", evt.Table, "pk", evt.PK, "offset", msg.Offset)
	}
}
