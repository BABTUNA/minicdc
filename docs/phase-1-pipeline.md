# Phase 1: The Flowing Skeleton

Goal: `INSERT` a row on the source database, see it appear in the destination seconds later, through our reader, Redpanda, and our writer. Inserts only. No merges, no recovery, no backfill.

Done when:

```bash
psql $SOURCE -c "INSERT INTO animals (animal_id, name, species, ...) VALUES (9999, 'Testo', 'lion', ...);"
psql $DEST   -c "SELECT name FROM animals WHERE animal_id = 9999;"   # -> Testo
```

## Components

| Piece | What it is |
| --- | --- |
| `deploy/docker-compose.yml` | `source` (Terra-style Postgres 16, seeded, `wal_level=logical`, `artie` role, `dbz_publication`), `redpanda` (topic `cdc.events`), `dest` (empty Postgres 16) |
| `cmd/reader` | WAL to Kafka |
| `cmd/writer` | Kafka to destination |
| `internal/events` | the shared `ChangeEvent` envelope |

## Function tree: reader

```text
main()                                                    cmd/reader/main.go
├── config.Load()                                         internal/config/config.go
├── reader.New(cfg)                                       internal/reader/reader.go
│   ├── pgconn.Connect(dsn + "replication=database")      (pglogrepl requirement)
│   ├── ensureSlot("minicdc")                             internal/reader/slot.go
│   │   └── CREATE_REPLICATION_SLOT ... LOGICAL pgoutput  (skip if exists)
│   └── sink.NewPublisher("cdc.events")                   internal/sink/kafka.go
└── reader.Run(ctx)                                       internal/reader/reader.go
    ├── pglogrepl.StartReplication(slot, startLSN,
    │       plugin args: proto_version=2,
    │       publication_names=dbz_publication)
    └── for { conn.ReceiveMessage() }
        ├── PrimaryKeepalive
        │   └── sendStandbyUpdate(ackedLSN)               internal/reader/reader.go
        └── XLogData
            └── decode.Message(walData)                   internal/reader/decode.go
                ├── RelationMessage
                │   └── relCache.Store(relID, cols)       internal/reader/relcache.go
                ├── InsertMessage
                │   └── buildChangeEvent(rel, tuple)      internal/reader/decode.go
                │       └── append to txnBuffer
                └── CommitMessage
                    ├── stamp commitTS + commitLSN on txnBuffer
                    ├── publisher.Publish(txnBuffer)      internal/sink/kafka.go
                    └── on Kafka ack: ackedLSN = commitLSN
```

Key invariant already in phase 1: `ackedLSN` (what we report to Postgres via standby updates) only advances after Kafka confirms the publish. Postgres retains WAL until we ack, so a dead reader loses nothing.

## Function tree: writer

```text
main()                                                    cmd/writer/main.go
├── config.Load()                                         internal/config/config.go
├── writer.New(cfg)                                       internal/writer/writer.go
│   ├── kafka.NewReader(group "minicdc-writer")           internal/writer/consumer.go
│   └── pgxpool.New(destDSN)
└── writer.Run(ctx)
    └── for { consumer.FetchMessage() }
        ├── events.Decode(msg.Value)                      internal/events/event.go
        ├── ensureTable(evt)                              internal/writer/ddl.go
        │   └── CREATE TABLE IF NOT EXISTS (mapped types) internal/writer/typemap.go
        ├── applyInsert(evt)                              internal/writer/apply.go
        │   └── INSERT INTO tbl (...) VALUES (...)
        └── consumer.CommitMessages(msg)                  only after apply succeeds
```

Phase 2 replaces `applyInsert` with buffer/flush/merge. Everything else in this tree survives.

## Core data shapes

### 1. ChangeEvent (ours, frozen after this phase)

Kafka message value. Message key is `table + PK json` so one row's events always share a partition and stay ordered.

```json
{
  "op": "c",
  "table": "public.animals",
  "lsn": 24605072,
  "commit_ts": "2026-09-08T18:04:11.902Z",
  "pk": {"animal_id": 9999},
  "after": {"animal_id": 9999, "name": "Testo", "species": "lion", "...": "..."},
  "types": {"animal_id": "integer", "name": "text", "species": "text"}
}
```

`types` maps columns to source Postgres type names so the writer can create destination tables without ever touching the source database. Enums degrade to `text` (dynamic OIDs), revisited in phase 5.

`op` is `c`/`u`/`d` (Debezium's convention). `after` is the full row, null for deletes (phase 2). `lsn` and `commit_ts` come from the CommitMessage, not the row.

### 2. pgoutput messages (Postgres to us; binary on the wire, shown decoded)

The reader handles three types in phase 1:

```json
// Relation: "here's what table 16388 looks like", arrives once, we cache it
{
  "relation_id": 16388,
  "table": "public.animals",
  "columns": [
    {"name": "animal_id", "type_oid": 23, "is_pk": true},
    {"name": "name",      "type_oid": 25, "is_pk": false},
    {"name": "species",   "type_oid": 24754, "is_pk": false}
  ]
}

// Insert: "a row for 16388", no names, no types, just positions
{
  "relation_id": 16388,
  "tuple": ["9999", "Testo", "lion"]
}

// Commit: "that transaction is real, here's where and when"
{
  "commit_lsn": 24605072,
  "commit_ts": "2026-09-08T18:04:11.902Z"
}
```

```text
Relation ──▶ cache          (must arrive first; unknown relation_id later = fail loudly)
Insert   ──▶ cache ⋈ tuple ──▶ ChangeEvent ──▶ txnBuffer
Commit   ──▶ stamp buffer ──▶ publish ──▶ ackedLSN advances
```

### 3. LSN (log sequence number)

One `uint64`, a byte-offset into the WAL: `24605072` (printed `0/1776A10`).

```text
0 ──────────────────────────────────────────────▶ WAL grows forever
                    ▲                    ▲
                ackedLSN            commit_lsn of newest txn
          "safe to discard below"   "we are this far behind"
```

The same number is used for: where to resume replication, what to ack, and the phase 4 snapshot boundary. Monotonic per server.

### 4. Standby status update (us to Postgres, every 5s or on request)

```json
{"write": 24605080, "flush": 24605080, "apply": 24605080}
```

```text
rule: these fields only ever carry ackedLSN,
      and ackedLSN only advances after a Kafka ack.
      one optimistic report here = the one way this system loses data
```

Postgres uses it to decide what WAL it may recycle, and it's where a restarted reader resumes.

### 5. Type mapping (source col types to dest DDL, `typemap.go`)

```text
integer, bigint, smallint  ──▶ same
text, numeric, boolean     ──▶ same
timestamptz, date, uuid    ──▶ same
enum (dynamic OID)         ──▶ text     data survives, type identity lost, phase 5
anything unknown           ──▶ text     never reject a value, degrade it
```

## Explicitly out of scope

Updates/deletes, pre-seeded rows (dest starts with only new inserts), crashes mid-batch, schema changes, TOAST. Scheduled for phases 2 to 5.

## Fallback

If pgoutput binary decoding burns too long: switch the slot to the `wal2json` plugin (JSON output, trivial parse) and note the tradeoff honestly in the README. pgoutput first, it is what production readers use.
