package events

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	// A bigint beyond 2^53: float64 decoding would corrupt it, json.Number must not.
	in := ChangeEvent{
		Op:       OpCreate,
		Table:    "public.animals",
		LSN:      24605072,
		CommitTS: time.Date(2026, 9, 8, 18, 4, 11, 0, time.UTC),
		PK:       map[string]any{"animal_id": json.Number("9007199254740993")},
		After:    map[string]any{"animal_id": json.Number("9007199254740993"), "name": "Testo"},
		Types:    map[string]string{"animal_id": "bigint", "name": "text"},
	}

	b, err := in.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in: %+v\nout: %+v", in, out)
	}
}

func TestKeyIsStablePerRow(t *testing.T) {
	e := ChangeEvent{Op: OpCreate, Table: "public.animals", PK: map[string]any{"animal_id": 42}}
	k1, err := e.Key()
	if err != nil {
		t.Fatal(err)
	}
	e.After = map[string]any{"name": "changed"}
	e.Op = OpUpdate
	k2, err := e.Key()
	if err != nil {
		t.Fatal(err)
	}
	if string(k1) != string(k2) {
		t.Fatalf("key changed between events for same row: %s vs %s", k1, k2)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte(`{"table":""}`)); err == nil {
		t.Fatal("expected error for event missing op/table")
	}
	if _, err := Decode([]byte(`not json`)); err == nil {
		t.Fatal("expected error for non-JSON")
	}
}
