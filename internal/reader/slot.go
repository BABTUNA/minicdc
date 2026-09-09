package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// ensureSlot creates the logical replication slot if it does not exist yet.
// The slot is Postgres-side memory of our position: WAL below the slot's
// confirmed position may be recycled, everything above is retained for us.
func ensureSlot(ctx context.Context, conn *pgconn.PgConn, slot string) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err == nil {
		slog.Info("created replication slot", "slot", slot)
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
		slog.Info("replication slot exists, resuming", "slot", slot)
		return nil
	}
	return fmt.Errorf("create replication slot %q: %w", slot, err)
}
