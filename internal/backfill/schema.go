package backfill

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

type column struct {
	name    string
	typeOID uint32
	isPK    bool
}

// loadColumns returns a table's columns in the pipeline's canonical order: PK
// columns first, the rest alphabetical. Matching the writer's ordering keeps
// DDL and staging consistent, though the writer creates tables from event
// data regardless.
func loadColumns(ctx context.Context, tx pgx.Tx, table string) ([]column, error) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("table %q is not schema-qualified", table)
	}

	rows, err := tx.Query(ctx, `
		SELECT a.attname, a.atttypid,
		       COALESCE(i.indisprimary, false) AS is_pk
		FROM pg_attribute a
		LEFT JOIN pg_index i ON i.indrelid = a.attrelid
		                     AND a.attnum = ANY(i.indkey) AND i.indisprimary
		WHERE a.attrelid = $1::regclass
		  AND a.attnum > 0 AND NOT a.attisdropped`, table)
	if err != nil {
		return nil, fmt.Errorf("load columns for %s: %w", table, err)
	}
	defer rows.Close()

	var pks, rest []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.typeOID, &c.isPK); err != nil {
			return nil, err
		}
		if c.isPK {
			pks = append(pks, c)
		} else {
			rest = append(rest, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(pks, func(i, j int) bool { return pks[i].name < pks[j].name })
	sort.Slice(rest, func(i, j int) bool { return rest[i].name < rest[j].name })
	return append(pks, rest...), nil
}

func listTables(ctx context.Context, tx pgx.Tx, publication string) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1`, publication)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteTable(t string) string {
	parts := strings.SplitN(t, ".", 2)
	if len(parts) == 2 {
		return quoteIdent(parts[0]) + "." + quoteIdent(parts[1])
	}
	return quoteIdent(t)
}
