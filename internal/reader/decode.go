package reader

import (
	"fmt"
	"strconv"

	"github.com/jackc/pglogrepl"

	"github.com/BABTUNA/minicdc/internal/events"
)

// buildChangeEvent turns a pgoutput insert tuple into our envelope.
// LSN and CommitTS are stamped later, by the Commit message of the enclosing
// transaction; pgoutput only reveals them at commit time.
func buildChangeEvent(rel Relation, tuple *pglogrepl.TupleData) (events.ChangeEvent, error) {
	if len(tuple.Columns) != len(rel.Columns) {
		return events.ChangeEvent{}, fmt.Errorf("%s: tuple has %d columns, relation has %d", rel.Table, len(tuple.Columns), len(rel.Columns))
	}

	evt := events.ChangeEvent{
		Op:    events.OpCreate,
		Table: rel.Table,
		PK:    map[string]any{},
		After: map[string]any{},
		Types: map[string]string{},
	}

	for i, col := range rel.Columns {
		evt.Types[col.Name] = col.TypeName

		val, err := decodeTupleColumn(tuple.Columns[i], col)
		if err != nil {
			return events.ChangeEvent{}, fmt.Errorf("%s.%s: %w", rel.Table, col.Name, err)
		}
		evt.After[col.Name] = val
		if col.IsKey {
			evt.PK[col.Name] = val
		}
	}

	if len(evt.PK) == 0 {
		return events.ChangeEvent{}, fmt.Errorf("%s: no primary key columns in relation; refusing keyless rows", rel.Table)
	}
	return evt, nil
}

func decodeTupleColumn(tc *pglogrepl.TupleDataColumn, col Column) (any, error) {
	switch tc.DataType {
	case pglogrepl.TupleDataTypeNull:
		return nil, nil
	case pglogrepl.TupleDataTypeText:
		return textToValue(string(tc.Data), col.TypeOID)
	case pglogrepl.TupleDataTypeToast:
		// Unchanged TOAST value: cannot happen on inserts, and phase 1 only
		// handles inserts. Phase 2 (updates) must deal with this properly.
		return nil, fmt.Errorf("unchanged TOAST value; not expected for inserts")
	default:
		return nil, fmt.Errorf("unsupported tuple data type %q", tc.DataType)
	}
}

// textToValue converts pgoutput's text representation into a JSON-friendly Go
// value. Integers and bools become typed; everything else (numeric, timestamps,
// text, enums) stays a string, which Postgres happily casts back on insert.
func textToValue(s string, typeOID uint32) (any, error) {
	switch typeOID {
	case 20, 21, 23: // int8, int2, int4
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse int %q: %w", s, err)
		}
		return n, nil
	case 16: // bool
		return s == "t", nil
	default:
		return s, nil
	}
}
