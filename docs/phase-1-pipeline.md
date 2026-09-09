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

### 2. pgoutput messages (Postgres to us, binary)

The reader decodes three types in phase 1:

| Message | Carries | We do |
| --- | --- | --- |
| Relation | relID, table name, column names, type OIDs, PK flags | cache it; needed to decode any row from that table |
| Insert | relID + tuple (column values as text/binary) | look up relID in cache, zip cols with values, build ChangeEvent |
| Commit | commit LSN + commit timestamp | stamp buffered events, publish, then advance ack |

Trap: Postgres sends Relation once per connection before the first row of that table. A tuple with an unknown relID is a bug, fail loudly.

### 3. LSN (log sequence number)

A `uint64` byte-position in the WAL, printed as `0/1776A10`. It is the single currency of progress on the read side: where to start replication, what to ack, what phase 4 will use as the snapshot boundary. Monotonic per server.

### 4. Standby status update (us to Postgres)

Tiny periodic message: "I have durably processed up to LSN X." Postgres uses it to decide what WAL it may recycle. Sent on keepalive request or timer. Reporting an LSN we have not truly secured is the classic way CDC loses data, so it only ever carries `ackedLSN`.

### 5. Type mapping (source col types to dest DDL)

Phase 1 keeps a small map in `typemap.go`: ints, text, numeric, timestamptz, bool pass through; enums land as `text`; anything unknown lands as `text` with a logged warning. Revisit in phase 5.

## Explicitly out of scope

Updates/deletes, pre-seeded rows (dest starts with only new inserts), crashes mid-batch, schema changes, TOAST. Scheduled for phases 2 to 5.

## Fallback

If pgoutput binary decoding burns too long: switch the slot to the `wal2json` plugin (JSON output, trivial parse) and note the tradeoff honestly in the README. pgoutput first, it is what production readers use.
