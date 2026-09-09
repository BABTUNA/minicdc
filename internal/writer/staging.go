package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/BABTUNA/minicdc/internal/events"
)

const stagingName = "_minicdc_staging"

// marshalCanonical exists for pkKey: encoding/json sorts map keys, so equal
// PK maps always produce equal strings.
func marshalCanonical(m map[string]any) ([]byte, error) {
	return json.Marshal(m)
}

// createStaging makes a session-temp copy of the target's shape plus two
// bookkeeping columns, dropped automatically when the transaction ends.
func createStaging(ctx context.Context, tx pgx.Tx, rep events.ChangeEvent, cols []string) error {
	defs := make([]string, 0, len(cols)+2)
	for _, col := range cols {
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(col), destType(rep.Types[col])))
	}
	defs = append(defs, "__op text NOT NULL", "__unchanged text[]")

	stmt := fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP", stagingName, strings.Join(defs, ", "))
	if _, err := tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("%s: create staging: %w", rep.Table, err)
	}
	return nil
}

// loadStaging bulk-inserts the deduped batch. Values travel as text with an
// explicit cast per column ($n::timestamptz etc.), the same conversion a psql
// literal gets; correct over clever, COPY binary is a later optimization.
func loadStaging(ctx context.Context, tx pgx.Tx, rep events.ChangeEvent, cols []string, batch []events.ChangeEvent) error {
	colList := make([]string, 0, len(cols)+2)
	for _, col := range cols {
		colList = append(colList, quoteIdent(col))
	}
	colList = append(colList, "__op", "__unchanged")
	paramsPerRow := len(cols) + 2

	// Chunk to stay far below the 65535 bind-parameter protocol limit.
	maxRows := 200
	for start := 0; start < len(batch); start += maxRows {
		chunk := batch[start:min(start+maxRows, len(batch))]

		var placeholders []string
		var args []any
		for i, evt := range chunk {
			row := make([]string, 0, paramsPerRow)
			for j, col := range cols {
				row = append(row, fmt.Sprintf("$%d::%s", i*paramsPerRow+j+1, destType(rep.Types[col])))
				args = append(args, toParam(columnValue(evt, col)))
			}
			row = append(row,
				fmt.Sprintf("$%d::text", i*paramsPerRow+len(cols)+1),
				fmt.Sprintf("$%d::text[]", i*paramsPerRow+len(cols)+2),
			)
			args = append(args, string(evt.Op), evt.Unchanged)
			placeholders = append(placeholders, "("+strings.Join(row, ", ")+")")
		}

		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			stagingName, strings.Join(colList, ", "), strings.Join(placeholders, ", "))
		if _, err := tx.Exec(ctx, stmt, args...); err != nil {
			return fmt.Errorf("%s: load staging: %w", rep.Table, err)
		}
	}
	return nil
}

// columnValue: deletes only carry the PK; everything else reads from After,
// where an unchanged TOAST column is simply absent (stays NULL in staging and
// the merge preserves the target's value).
func columnValue(evt events.ChangeEvent, col string) any {
	if v, ok := evt.PK[col]; ok {
		return v
	}
	if evt.After == nil {
		return nil
	}
	return evt.After[col]
}

// mergeStaging drains staging into the target in one MERGE:
// delete the 'd' rows, overwrite matches (preserving unchanged TOAST columns),
// insert the rest.
func mergeStaging(ctx context.Context, tx pgx.Tx, rep events.ChangeEvent, cols []string) error {
	var pkCols, nonPK []string
	for _, col := range cols {
		if _, ok := rep.PK[col]; ok {
			pkCols = append(pkCols, col)
		} else {
			nonPK = append(nonPK, col)
		}
	}

	on := make([]string, len(pkCols))
	for i, col := range pkCols {
		on[i] = fmt.Sprintf("t.%s = s.%s", quoteIdent(col), quoteIdent(col))
	}

	updateClause := ""
	if len(nonPK) > 0 {
		sets := make([]string, len(nonPK))
		for i, col := range nonPK {
			q := quoteIdent(col)
			sets[i] = fmt.Sprintf("%s = CASE WHEN %s = ANY(s.__unchanged) THEN t.%s ELSE s.%s END",
				q, quoteLiteral(col), q, q)
		}
		updateClause = "WHEN MATCHED THEN UPDATE SET " + strings.Join(sets, ", ")
	} else {
		updateClause = "WHEN MATCHED THEN DO NOTHING"
	}

	allQuoted := make([]string, len(cols))
	fromStaging := make([]string, len(cols))
	for i, col := range cols {
		allQuoted[i] = quoteIdent(col)
		fromStaging[i] = "s." + quoteIdent(col)
	}

	stmt := fmt.Sprintf(`
MERGE INTO %s AS t
USING %s AS s ON %s
WHEN MATCHED AND s.__op = 'd' THEN DELETE
%s
WHEN NOT MATCHED AND s.__op <> 'd' THEN
  INSERT (%s) VALUES (%s)`,
		quoteTable(rep.Table), stagingName, strings.Join(on, " AND "),
		updateClause,
		strings.Join(allQuoted, ", "), strings.Join(fromStaging, ", "))

	if _, err := tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("%s: merge: %w", rep.Table, err)
	}
	return nil
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// toParam converts a JSON-decoded value into something pgx can bind against
// the staging cast ($n::type). Numbers and timestamps travel as text and let
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
