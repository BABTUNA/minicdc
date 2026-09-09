package verify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Checksum summarizes an entire table as two values. Equal counts and digests
// on both sides means the tables hold identical data.
type Checksum struct {
	Count  int64
	Digest string
}

// tableShape is read from the SOURCE and reused verbatim against the
// destination, so both digests hash the same columns in the same order even
// though the two tables were created with different column ordering.
type tableShape struct {
	Table   string // schema-qualified
	PKCols  []string
	AllCols []string // PK first, rest alphabetical: the pipeline's canonical order
}

func loadShape(ctx context.Context, pool *pgxpool.Pool, table string) (tableShape, error) {
	shape := tableShape{Table: table}

	pkRows, err := pool.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`, table)
	if err != nil {
		return shape, fmt.Errorf("%s: load pk columns: %w", table, err)
	}
	defer pkRows.Close()
	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err != nil {
			return shape, err
		}
		shape.PKCols = append(shape.PKCols, col)
	}
	if len(shape.PKCols) == 0 {
		return shape, fmt.Errorf("%s: no primary key", table)
	}

	parts := strings.SplitN(table, ".", 2)
	colRows, err := pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2`, parts[0], parts[1])
	if err != nil {
		return shape, fmt.Errorf("%s: load columns: %w", table, err)
	}
	defer colRows.Close()

	pkSet := map[string]bool{}
	for _, c := range shape.PKCols {
		pkSet[c] = true
	}
	var rest []string
	for colRows.Next() {
		var col string
		if err := colRows.Scan(&col); err != nil {
			return shape, err
		}
		if !pkSet[col] {
			rest = append(rest, col)
		}
	}
	sort.Strings(rest)
	shape.AllCols = append(append([]string{}, shape.PKCols...), rest...)
	return shape, nil
}

// checksum runs the same digest query on whichever side it is pointed at:
// md5 per row over an explicit ROW(cols...) in canonical order, aggregated in
// PK order. The session is pinned to UTC so timestamptz renders identically
// on both sides.
func checksum(ctx context.Context, pool *pgxpool.Pool, shape tableShape) (Checksum, error) {
	quoted := make([]string, len(shape.AllCols))
	for i, c := range shape.AllCols {
		quoted[i] = quoteIdent(c)
	}
	orderBy := make([]string, len(shape.PKCols))
	for i, c := range shape.PKCols {
		orderBy[i] = quoteIdent(c)
	}

	query := fmt.Sprintf(
		`SELECT count(*), COALESCE(md5(string_agg(md5(ROW(%s)::text), '' ORDER BY %s)), '') FROM %s`,
		strings.Join(quoted, ", "), strings.Join(orderBy, ", "), quoteTable(shape.Table),
	)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Checksum{}, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
		return Checksum{}, err
	}

	var cs Checksum
	if err := conn.QueryRow(ctx, query).Scan(&cs.Count, &cs.Digest); err != nil {
		return Checksum{}, fmt.Errorf("%s: checksum: %w", shape.Table, err)
	}
	return cs, nil
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
