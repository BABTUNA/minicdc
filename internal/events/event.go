package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// Op mirrors Debezium's single-letter convention.
type Op string

const (
	OpCreate Op = "c"
	OpUpdate Op = "u"
	OpDelete Op = "d"
	OpRead   Op = "r" // snapshot read (backfill); applied as an upsert
)

// ChangeEvent is the envelope every change travels in between the reader and
// the writer. Frozen after phase 1: later phases may add fields but never
// change the meaning of existing ones.
type ChangeEvent struct {
	Op       Op             `json:"op"`
	Table    string         `json:"table"` // schema-qualified, e.g. "public.animals"
	LSN      uint64         `json:"lsn"`
	CommitTS time.Time      `json:"commit_ts"`
	PK       map[string]any `json:"pk"`
	After    map[string]any `json:"after"` // full row; nil for deletes
	// Unchanged lists TOAST columns the source did not retransmit because the
	// update left them untouched. They are absent from After; the writer must
	// preserve the destination's existing value, never write NULL.
	Unchanged []string `json:"unchanged,omitempty"`
	// Types maps column name to the source Postgres type name (e.g. "int8",
	// "timestamptz"). The writer uses it to create destination tables without
	// ever touching the source database.
	Types map[string]string `json:"types,omitempty"`
}

// Key is the Kafka message key: table + PK. Every event for a given row lands
// in the same partition, so per-row ordering is preserved.
func (e ChangeEvent) Key() ([]byte, error) {
	pk, err := json.Marshal(e.PK)
	if err != nil {
		return nil, fmt.Errorf("marshal pk: %w", err)
	}
	return []byte(e.Table + ":" + string(pk)), nil
}

func (e ChangeEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// Decode uses json.Number for numeric values so int8 columns above 2^53
// survive the round trip exactly (float64 would silently round them).
func Decode(b []byte) (ChangeEvent, error) {
	var e ChangeEvent
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&e); err != nil {
		return ChangeEvent{}, fmt.Errorf("unmarshal change event: %w", err)
	}
	if e.Op == "" || e.Table == "" {
		return ChangeEvent{}, fmt.Errorf("change event missing op or table: %s", b)
	}
	return e, nil
}
