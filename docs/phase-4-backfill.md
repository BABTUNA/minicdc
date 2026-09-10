# Phase 4: Backfill to Live

(Phase 3, recovery, was absorbed into phase 2: its crash scripts and resume paths were required by that phase's done-when.)

Goal: a fresh pipeline pointed at an already-populated source copies the existing rows, then transitions to live streaming with no gap and no overlap. Today `crash-test.sh` truncates the seed to dodge this; this phase makes the seed replicate.

Done when:

```bash
./scripts/backfill-test.sh   # fresh SEEDED stack (1430+ observations), start pipeline,
                             # backfill runs, live load writes during and after it
# final line: VERIFY: all 3 tables MATCH
```

## The core idea

The whole problem is picking one instant and knowing exactly which changes are before it (copy them) and after it (stream them). Postgres hands us that instant atomically at slot creation:

```json
// CREATE_REPLICATION_SLOT bartie LOGICAL pgoutput EXPORT_SNAPSHOT returns:
{
  "slot_name": "bartie",
  "consistent_point": "0/1776A10",
  "snapshot_name": "00000003-00000002-1"
}
```

`snapshot_name` is a frozen view of the database exactly at `consistent_point`, usable from other connections. The pairing is the guarantee:

```text
WAL:  ────────────●──────────────────────────▶
             consistent_point
snapshot sees:  every commit <= here     → backfill copies these
slot streams:   every commit  > here     → CDC delivers these
nothing is in both sets, nothing is in neither
```

## Function tree (reader delta)

```text
reader.New                                                internal/reader/reader.go
└── ensureSlot(slot)                                      internal/reader/slot.go
    ├── slot created fresh: with EXPORT_SNAPSHOT
    │   └── keep {consistent_point, snapshot_name}        → backfill will run
    └── slot already exists: no snapshot                  → skip backfill, resume

reader.Run                                                internal/reader/reader.go
├── if fresh slot:
│   └── backfill.Run(cfg, snapshotName, pub)              internal/backfill/backfill.go
│       ├── listTables(publication)                       pg_publication_tables
│       └── per table (normal SQL connection):
│           ├── BEGIN ISOLATION LEVEL REPEATABLE READ
│           ├── SET TRANSACTION SNAPSHOT '<snapshot_name>'
│           ├── loadColumns(tbl)                          pg_attribute: names, type OIDs, pk
│           ├── SELECT every column ::text FROM tbl
│           ├── row -> op "r" ChangeEvent                 reuses reader's textToValue by OID
│           └── pub.Publish(chunks of 500)                internal/sink/kafka.go
└── pglogrepl.StartReplication(slot, 0, ...)              only AFTER backfill finishes
```

The writer changes not at all: an "r" event is an upsert like any other, and it flows through the same buffer, fold, staging, and MERGE.

## Core data shapes

### 1. The "r" (snapshot read) event, Debezium's convention

```json
{
  "op": "r",
  "table": "public.animals",
  "lsn": 0,
  "commit_ts": "2026-09-10T09:00:00Z",
  "pk": {"animal_id": 1},
  "after": {"animal_id": 1, "name": "Mosi", "species": "blue_wildebeest", "...": "..."},
  "types": {"animal_id": "integer", "name": "text", "...": "..."}
}
```

`lsn` is 0: a snapshot row has no WAL position of its own. `commit_ts` is the backfill start time, marking these rows in latency measurements as "not stream latency."

### 2. Why order survives

```text
partition for row 42:  [r: snapshot state] [u: later change] [u: ...]
```

All backfill events publish before `StartReplication` sends the first stream event, and a row's events share a partition. So every row replays as: state at the snapshot, then its changes after the snapshot, in order. If a row changed between snapshot and stream start, the stream delivers that change; the merge overwrites the snapshot state. Idempotent as always.

### 3. Value canonicalization (subtle, breaks MERGE if wrong)

Backfill reads columns as text and converts with the same OID-based `textToValue` the stream decoder uses, so `animal_id` is int64 `9999` from both paths. If backfill shipped `"9999"` (string) instead, dedupe would see two different PKs for one row, put both in staging, and MERGE fails with "cannot affect row a second time."

## Gotchas known going in

- The exported snapshot is only valid while the replication connection sits idle after `CREATE_REPLICATION_SLOT`. Strict order: create slot → backfill over other connections → then StartReplication. Any other command on the replication connection first kills the snapshot.
- **Crash mid-backfill is not resumable in this phase.** The slot exists but its snapshot died with the connection. The honest answer: `scripts/reset.sh` and redo. The production answer is watermark-tracked, restartable backfills running concurrently with streaming, which is exactly Artie's "Database Backfill Without Downtime" post; say so in the interview rather than pretending.
- The slot retains WAL from `consistent_point` while backfill runs. Long backfill on a busy source = WAL growth on the source. Fine at our scale, named in the README as the known production concern.
- `crash-test.sh` keeps its seed TRUNCATE: it isolates streaming correctness. `backfill-test.sh` is the seeded scenario. Both must pass.

## Components

| Piece | What changes |
| --- | --- |
| `internal/reader/slot.go` | ensureSlot reports fresh-vs-existing, carries snapshot name + consistent point |
| `internal/backfill` | new package: snapshot-pinned table scans emitting "r" events |
| `internal/writer` | nothing |
| `scripts/backfill-test.sh` | the done-when: seeded stack, live load during backfill, verify |
| `scripts/reset.sh` | full reset (compose down -v, up) for the mid-backfill-crash case |

## Explicitly out of scope

Restartable/concurrent backfill (production concern, discussed not built), parallel table scans (CTID ranges, Artie's post), schema changes during backfill (phase 5 territory).
