package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// SlotState reports whether the slot was created fresh (so a backfill should
// run against its exported snapshot) or already existed (resume streaming, the
// snapshot is long gone).
type SlotState struct {
	Fresh        bool
	SnapshotName string // valid only when Fresh; consumed before StartReplication
}

// ensureSlot creates the logical replication slot if it does not exist yet.
// On fresh creation it uses EXPORT_SNAPSHOT so a backfill can read a database
// view consistent with the exact WAL position streaming will start from.
//
// The exported snapshot lives only while this replication connection stays
// idle after the command, so the caller must backfill (over other
// connections) and only then StartReplication on this one.
func ensureSlot(ctx context.Context, conn *pgconn.PgConn, slot string) (SlotState, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{SnapshotAction: "EXPORT_SNAPSHOT"})
	if err == nil {
		slog.Info("created replication slot", "slot", slot, "consistent_point", res.ConsistentPoint, "snapshot", res.SnapshotName)
		return SlotState{Fresh: true, SnapshotName: res.SnapshotName}, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
		slog.Info("replication slot exists, resuming", "slot", slot)
		return SlotState{Fresh: false}, nil
	}
	return SlotState{}, fmt.Errorf("create replication slot %q: %w", slot, err)
}
