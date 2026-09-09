package writer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/minicdc/internal/events"
)

// flushTable applies one table's batch inside a single destination
// transaction: dedupe to final states, load them into a temp staging table,
// merge staging into the target. Idempotent by construction: replaying the
// same batch re-asserts the same final states.
func flushTable(ctx context.Context, pool *pgxpool.Pool, ddl *ddlManager, table string, evts []events.ChangeEvent) error {
	rep := representative(evts)
	if err := ddl.ensureTable(ctx, pool, rep); err != nil {
		return err
	}

	deduped := dedupe(evts)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin flush tx: %w", table, err)
	}
	defer tx.Rollback(ctx)

	cols := orderedColumns(rep)
	if err := createStaging(ctx, tx, rep, cols); err != nil {
		return err
	}
	if err := loadStaging(ctx, tx, rep, cols, deduped); err != nil {
		return err
	}
	if err := mergeStaging(ctx, tx, rep, cols); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: commit flush tx: %w", table, err)
	}
	return nil
}

// dedupe collapses a batch to one event per PK: the destination cares about
// final states, not history. Safe because a row's events share a Kafka
// partition, so "later in the slice" really is later.
//
// Collapsing must FOLD, not just keep the last event: if an earlier event in
// the batch carried a TOASTed value and the last event has it marked
// unchanged, the value would otherwise be lost (the earlier event never
// reaches the destination on its own).
func dedupe(evts []events.ChangeEvent) []events.ChangeEvent {
	state := map[string]events.ChangeEvent{}
	var order []string
	for _, evt := range evts {
		key := pkKey(evt)
		prev, seen := state[key]
		if !seen {
			order = append(order, key)
		}
		state[key] = fold(prev, seen, evt)
	}
	out := make([]events.ChangeEvent, 0, len(order))
	for _, key := range order {
		out = append(out, state[key])
	}
	return out
}

func fold(prev events.ChangeEvent, seen bool, next events.ChangeEvent) events.ChangeEvent {
	if !seen || next.Op == events.OpDelete || prev.Op == events.OpDelete || prev.After == nil {
		return next
	}
	if len(next.Unchanged) == 0 {
		return next
	}
	// Fill next's unchanged columns from what the batch already knows.
	after := make(map[string]any, len(next.After)+len(next.Unchanged))
	for k, v := range next.After {
		after[k] = v
	}
	var stillUnknown []string
	for _, col := range next.Unchanged {
		if v, ok := prev.After[col]; ok {
			after[col] = v
		} else {
			stillUnknown = append(stillUnknown, col)
		}
	}
	next.After = after
	next.Unchanged = stillUnknown
	return next
}

func pkKey(evt events.ChangeEvent) string {
	b, err := marshalCanonical(evt.PK)
	if err != nil {
		// PK maps are built from decoded scalars; this cannot fail in practice.
		panic(fmt.Sprintf("marshal pk for %s: %v", evt.Table, err))
	}
	return string(b)
}

// representative picks the event with the fullest Types map, used for DDL and
// staging shape. Delete events carry full Types too, so any event works; this
// just guards against mixed batches.
func representative(evts []events.ChangeEvent) events.ChangeEvent {
	rep := evts[0]
	for _, e := range evts[1:] {
		if len(e.Types) > len(rep.Types) {
			rep = e
		}
	}
	return rep
}
