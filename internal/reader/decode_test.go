package reader

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func testRelation() Relation {
	return Relation{
		ID:    1,
		Table: "public.animals",
		Columns: []Column{
			{Name: "animal_id", TypeOID: 23, TypeName: "integer", IsKey: true},
			{Name: "name", TypeOID: 25, TypeName: "text"},
			{Name: "alive", TypeOID: 16, TypeName: "boolean"},
			{Name: "notes", TypeOID: 25, TypeName: "text"},
		},
	}
}

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

func TestBuildChangeEvent(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"),
		textCol("Testo"),
		textCol("t"),
		{DataType: pglogrepl.TupleDataTypeNull},
	}}

	evt, err := buildChangeEvent(testRelation(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	if got := evt.PK["animal_id"]; got != int64(42) {
		t.Errorf("pk animal_id = %v (%T), want int64 42", got, got)
	}
	if got := evt.After["alive"]; got != true {
		t.Errorf("alive = %v, want true", got)
	}
	if got := evt.After["notes"]; got != nil {
		t.Errorf("notes = %v, want nil", got)
	}
	if evt.Types["animal_id"] != "integer" {
		t.Errorf("types[animal_id] = %q", evt.Types["animal_id"])
	}
}

func TestBuildChangeEventColumnCountMismatch(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("42")}}
	if _, err := buildChangeEvent(testRelation(), tuple); err == nil {
		t.Fatal("expected error for column count mismatch")
	}
}

func TestBuildChangeEventRefusesKeylessRelation(t *testing.T) {
	rel := Relation{ID: 2, Table: "public.nopk", Columns: []Column{{Name: "x", TypeOID: 25, TypeName: "text"}}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("v")}}
	if _, err := buildChangeEvent(rel, tuple); err == nil {
		t.Fatal("expected error for relation without key columns")
	}
}

func TestRelCacheMissFailsLoudly(t *testing.T) {
	if _, err := NewRelCache().Get(999); err == nil {
		t.Fatal("expected error for unknown relation ID")
	}
}
