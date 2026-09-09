package reader

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

// Relation is our cached view of a table's schema, built from pgoutput
// Relation messages. Postgres sends one before the first row change of each
// table on a connection; row messages then reference it by relation ID only.
type Relation struct {
	ID      uint32
	Table   string // schema-qualified
	Columns []Column
}

type Column struct {
	Name     string
	TypeOID  uint32
	TypeName string // normalized via typeNameForOID
	IsKey    bool   // part of the replica identity (primary key)
}

type RelCache struct {
	rels map[uint32]Relation
}

func NewRelCache() *RelCache {
	return &RelCache{rels: make(map[uint32]Relation)}
}

func (c *RelCache) Store(msg *pglogrepl.RelationMessageV2) {
	rel := Relation{
		ID:    msg.RelationID,
		Table: msg.Namespace + "." + msg.RelationName,
	}
	for _, col := range msg.Columns {
		rel.Columns = append(rel.Columns, Column{
			Name:     col.Name,
			TypeOID:  col.DataType,
			TypeName: typeNameForOID(col.DataType),
			IsKey:    col.Flags&1 != 0,
		})
	}
	c.rels[msg.RelationID] = rel
}

// Get fails loudly on a miss: a tuple referencing an uncached relation means
// we broke protocol handling, and guessing would corrupt data downstream.
func (c *RelCache) Get(relID uint32) (Relation, error) {
	rel, ok := c.rels[relID]
	if !ok {
		return Relation{}, fmt.Errorf("received tuple for unknown relation ID %d (Relation message not seen)", relID)
	}
	return rel, nil
}

// typeNameForOID maps built-in Postgres type OIDs to type names the writer can
// use in destination DDL. Anything unknown (notably enums, which get dynamic
// OIDs) lands as text: correct as data, lossy as DDL, revisited in phase 5.
func typeNameForOID(oid uint32) string {
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
