# Phase 2: It's Correct

Goal: full insert/update/delete replication that is idempotent and provably survives being killed. `applyInsert` dies; buffer → staging → merge replaces it.

Done when:

```bash
./scripts/crash-test.sh   # mixed I/U/D load, kill -9 writer mid-stream, restart,
                          # then kill -9 reader, restart
# final line: VERIFY: all tables MATCH (0 lost, 0 duplicated)
```

## Components

| Piece | What changes |
| --- | --- |
| `cmd/cdcctl` | new binary, `verify` subcommand: the judge for everything else |
| `internal/reader` | decodes Update and Delete messages, including the TOAST trap |
| `internal/events` | envelope gains `"unchanged"` (TOAST columns to preserve) |
| `internal/writer` | per-event insert replaced by buffered flush: dedupe, staging COPY, merge |
| `scripts/` | repeatable load generator + crash tests |

## Function tree: cdcctl verify

```text
main()                                                    cmd/cdcctl/main.go
└── "verify" subcommand
    └── verify.Run(cfg, timeout)                          internal/verify/verify.go
        ├── listTables(source)                            pg_publication_tables
        └── retry until match or timeout:
            ├── checksum(sourcePool, tbl)                 internal/verify/checksum.go
            ├── checksum(destPool, tbl)                   same query, both sides
            └── compare {count, digest} per table
```

Retry exists because a live pipeline has lag: a mismatch right now may just mean "in flight." Verify converging within the timeout is the pass condition.

## Function tree: reader (delta only)

```text
handleWALMessage                                          internal/reader/reader.go
├── UpdateMessageV2
│   └── buildUpdateEvent(rel, oldTuple, newTuple)         internal/reader/decode.go
│       ├── op "u", pk + after from newTuple
│       └── TOAST column marked 'u' (unchanged)
│           └── omit from after, append to evt.Unchanged
└── DeleteMessageV2
    └── buildDeleteEvent(rel, oldTuple)                   internal/reader/decode.go
        └── op "d", pk from oldTuple, after = null
```

## Function tree: writer (replaces the phase 1 loop body)

```text
writer.Run                                                internal/writer/writer.go
└── for { consumer.FetchMessage() }
    ├── events.Decode(msg.Value)                          internal/events/event.go
    ├── buffer.Add(evt, msg)                              internal/writer/buffer.go
    └── if buffer.ShouldFlush()          (count >= 500, or 2s since last flush)
        └── flushAll()                                    internal/writer/flush.go
            └── per table with buffered events:
                ├── dedupe: fold to last state per PK     internal/writer/flush.go
                ├── tx.Begin on dest
                ├── ensureTable(evt)                      internal/writer/ddl.go
                ├── bulk-load batch into temp staging     internal/writer/staging.go
                ├── MERGE staging into target             internal/writer/staging.go
                │     (delete 'd' rows, upsert the rest, unchanged-aware)
                ├── tx.Commit
                └── consumer.CommitMessages(newest msg)   ONLY after tx.Commit
```

## Core data shapes

### 1. Update and Delete pgoutput messages (decoded)

```json
// Update: new row state; old tuple only present if replica identity cols changed
{
  "relation_id": 16388,
  "new_tuple": ["9999", "Testo Renamed", "lion", {"toast": "unchanged"}]
}

// Delete: with default replica identity (PK), only key columns are present
{
  "relation_id": 16388,
  "old_tuple": ["9999", null, null, null]
}
```

The TOAST trap: a large value (e.g. a long `observations.notes`) that was NOT touched by the update arrives as marker `'u'`, not as data. Treating it as NULL silently erases it downstream. This is the bug Artie's "Why TOAST Columns Break Postgres CDC" post is about.

### 2. ChangeEvent for update and delete

```json
// update: notes was TOASTed and untouched -> listed in "unchanged", absent from "after"
{
  "op": "u",
  "table": "public.observations",
  "lsn": 24610000,
  "commit_ts": "2026-09-09T10:00:00Z",
  "pk": {"observation_id": 77},
  "after": {"observation_id": 77, "animal_id": 42, "watering_hole_id": 3,
            "observed_at": "2026-09-09 09:59:58+02"},
  "unchanged": ["notes"],
  "types": {"observation_id": "bigint", "animal_id": "integer", "...": "..."}
}

// delete: after is null, pk is all we have and all we need
{
  "op": "d",
  "table": "public.animals",
  "lsn": 24611000,
  "commit_ts": "2026-09-09T10:00:02Z",
  "pk": {"animal_id": 9999},
  "after": null
}
```

### 3. The flush: buffer to destination

```text
buffer (per table):
  [u pk=77] [c pk=101] [u pk=77] [d pk=101]      arrival order, per-row order guaranteed
       │ dedupe: FOLD to last state per PK        (same partition = ordered, so safe)
       │   fold, not "keep last": if an earlier event carried a TOASTed value
       │   and the last has it "unchanged", the value is copied forward,
       │   otherwise it would be lost (the earlier event is discarded)
       ▼
  {77: [u ...final state], 101: [d]}
       │ bulk-load into staging (batched INSERT with $n::type casts;
       │  binary COPY is a later optimization)
       ▼
staging row = target columns + two extras:
  __op         'c' | 'u' | 'd'
  __unchanged  text[] of TOAST columns to preserve
```

### 4. The merge SQL (per table, inside one transaction)

```sql
-- deletes first
DELETE FROM animals t USING staging s
  WHERE t.animal_id = s.animal_id AND s.__op = 'd';

-- then upserts; unchanged-aware per column
INSERT INTO animals AS t (animal_id, name, notes, ...)
  SELECT animal_id, name, notes, ... FROM staging WHERE __op <> 'd'
ON CONFLICT (animal_id) DO UPDATE SET
  name  = EXCLUDED.name,
  notes = CASE WHEN 'notes' = ANY(s_unchanged) THEN t.notes ELSE EXCLUDED.notes END,
  ...;
```

Idempotency: replaying the same batch re-runs "delete these PKs, set those PKs to state X." Same end state every time. That retires phase 1's `ON CONFLICT DO NOTHING` crutch, which would have skipped newer states.

### 5. Offset commit timeline (the crash-safety contract)

```text
fetch ─▶ buffer ─▶ ...more fetches... ─▶ staging+merge tx COMMIT ─▶ Kafka offset commit
                                              ▲                          ▲
      crash anywhere left of here: batch replays, merge converges, nothing lost
      crash between the two commits: batch replays, merge converges, nothing duplicated
```

### 6. Verify checksum (same query both sides)

```sql
SELECT count(*),
       md5(string_agg(md5(t::text), '' ORDER BY animal_id)) AS digest
FROM animals t;
```

```text
per table: {count, digest} source  vs  {count, digest} dest
equal on both -> MATCH; retry until --timeout, report per table
```

Count catches loss/duplication; the ordered digest catches wrong values (a TOAST bug shows up here even when counts agree).

## Crash tests (scripts/)

```text
load.sh          loop of random INSERT / UPDATE (incl. notes-untouched updates) / DELETE
crash-test.sh    load.sh &  ->  kill -9 writer  ->  restart writer  ->
                 kill -9 reader ->  restart reader ->  stop load ->
                 cdcctl verify --timeout 60s     -> exit 0 iff MATCH
```

## Gotchas found while building

- kafka-go's `Writer` defaults `BatchTimeout` to 1 second, so a synchronous per-transaction publish crawls at ~1 txn/s. The first crash-test run "failed" purely from this lag: dest converging at a trickle past the verify timeout. 10ms fixed it. Lesson worth retelling: the pipeline was correct but unusably slow, and only an end-to-end test caught it.
- A kill -9'd writer never leaves its consumer group; the broker evicts it only after the session timeout, and the restarted writer waits out that rebalance. Session timeout lowered to 10s to keep recovery snappy.

## Gotchas known going in

- A PK-changing UPDATE ships the old key in `old_tuple`. Handled as delete(old PK) + insert(new PK); rare, but silently wrong otherwise.
- Dedupe is only safe because a row's events share a Kafka partition. Cross-row order across tables is not preserved, and does not need to be: verify checks state, not history.
- FK ordering: deletes/upserts across tables in one flush can violate destination FKs. Destination tables are created WITHOUT foreign keys (warehouse-style); noted in README.
- The flush transaction and the Kafka offset commit are two systems, not atomic. The gap is safe only because the merge is idempotent; say this sentence in the interview.

## Explicitly out of scope

Backfill of pre-seeded rows (phase 4), schema changes mid-stream (phase 5), benchmark numbers (phase 5).
