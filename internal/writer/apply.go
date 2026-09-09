package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/minicdc/internal/events"
)

// applyInsert is phase 1's whole apply path: a plain INSERT. A replayed event
// (writer died after insert, before offset commit) therefore hits a PK
// conflict; DO NOTHING makes the replay converge instead of crash-looping.
// True idempotent merge semantics arrive with updates in phase 2.
func applyInsert(ctx context.Context, pool *pgxpool.Pool, evt events.ChangeEvent) error {
	cols := orderedColumns(evt)

	quoted := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, col := range cols {
		quoted[i] = quoteIdent(col)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = toParam(evt.After[col])
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		quoteTable(evt.Table), strings.Join(quoted, ", "), strings.Join(placeholders, ", "),
	)
	if _, err := pool.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("insert into %s: %w", evt.Table, err)
	}
	return nil
}

// toParam converts a JSON-decoded value into something pgx can bind against
// any destination column type. Numbers and timestamps travel as text and let
// Postgres cast on input, exactly like a psql literal would.
func toParam(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case string:
		return val
	case json.Number:
		return val.String()
	case float64:
		// Shouldn't appear (events.Decode uses json.Number) but be safe.
		return fmt.Sprintf("%v", val)
	case int64:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
