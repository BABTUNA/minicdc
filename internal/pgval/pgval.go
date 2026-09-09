// Package pgval converts Postgres text-format values into JSON-friendly Go
// values, keyed by type OID. Both the stream decoder and the backfill scanner
// MUST use these same functions: if they disagree (int64 9999 vs string
// "9999"), one row produces two dedupe keys and the writer's MERGE fails with
// "cannot affect row a second time".
package pgval

import (
	"fmt"
	"strconv"
)

// TextToValue converts pgoutput/text representation into a typed Go value.
// Integers and bools become typed; everything else (numeric, timestamps,
// text, enums) stays a string, which Postgres happily casts back on insert.
func TextToValue(s string, typeOID uint32) (any, error) {
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

// TypeNameForOID maps built-in Postgres type OIDs to type names the writer
// can use in destination DDL. Anything unknown (notably enums, which get
// dynamic OIDs) lands as text: correct as data, lossy as DDL.
func TypeNameForOID(oid uint32) string {
	switch oid {
	case 16:
		return "boolean"
	case 20:
		return "bigint"
	case 21:
		return "smallint"
	case 23:
		return "integer"
	case 25:
		return "text"
	case 700:
		return "real"
	case 701:
		return "double precision"
	case 1042:
		return "text" // bpchar
	case 1043:
		return "text" // varchar; length limit not carried over
	case 1082:
		return "date"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamptz"
	case 1700:
		return "numeric"
	case 2950:
		return "uuid"
	case 3802:
		return "jsonb"
	default:
		return "text"
	}
}
