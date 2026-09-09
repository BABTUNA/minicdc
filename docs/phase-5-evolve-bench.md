# Phase 5: Schema Evolution and Benchmarks

Bonus phase, past the cut line. Two independent pieces, either cuttable:

- **5a Schema evolution:** a column added on the source appears downstream.
- **5b Benchmark:** measure end-to-end latency and throughput under load, report it like Artie does.

Done when:

```bash
./scripts/evolve-test.sh     # ADD COLUMN on source, touch a row, verify MATCH incl. new column
./scripts/bench.sh           # sustained write load, writes data/latency_<date>.csv + a summary line
```

---

## 5a: Schema evolution

### The idea

A bare `ADD COLUMN` emits no WAL event; the next row change on that table does, and it arrives with a new Relation message listing the extra column. So the reader already learns about the column for free. The gap is downstream: the destination table has no such column, and the INSERT/MERGE would fail. The writer must reconcile the destination table to the event's columns before applying.

```text
source: ALTER TABLE animals ADD COLUMN weight_kg numeric;  (no WAL yet)
source: UPDATE animals SET weight_kg = 5 WHERE ...;         (WAL: new Relation + row)
reader: Relation now has weight_kg -> event carries it in after + types
writer: dest animals has no weight_kg -> ALTER TABLE ADD, then apply
```

Scope: **additive only** (ADD COLUMN). Renames, type changes, and drops are called out as out of scope with reasons, which is itself an interview talking point.

### Function tree (writer delta)

```text
ensureTable(evt)                                          internal/writer/ddl.go
├── table unknown -> CREATE TABLE (today's path)
└── table known -> reconcileColumns(evt)                  internal/writer/ddl.go  (new)
    ├── introspect dest columns (cached)
    ├── event has a column dest lacks?
    │   └── ALTER TABLE ADD COLUMN <name> <mapped type>
    └── dest has a column the event lacks -> ignore (drop is out of scope)
```

### Data shape: the event after ADD COLUMN

```json
{
  "op": "u",
  "table": "public.animals",
  "pk": {"animal_id": 1},
  "after": {"animal_id": 1, "name": "Mosi", "weight_kg": "5.00", "...": "..."},
  "types": {"animal_id": "integer", "weight_kg": "numeric", "...": "..."}
}
```

The reader needs no change: a new Relation message repopulates the cache, and `buildChangeEvent` already emits whatever columns the relation has. All the work is the writer noticing `weight_kg` is new and running one ALTER before the merge.

### Gotchas

- ADD COLUMN with no row change is invisible by design (Terra's README says the same). The demo must touch a row after the ALTER. Fine: that is how CDC actually behaves.
- New column is nullable with no default downstream, matching an additive source change. A source default backfills existing source rows (those changes stream through); we do not replay history.
- Reconcile must be idempotent and cached: check dest once per new column, not per event, or every row pays an introspection round-trip.
- RENAME COLUMN looks like drop-old + add-new over CDC and would silently lose the column's data downstream. Out of scope, named in README as the hard case (it needs DDL replication, not column reconciliation).

---

## 5b: Benchmark

### The idea

Borrow Artie's own method (from artie-labs/benchmarks): stamp each row with its source commit time, then at the destination latency = load_time - commit_time. We already carry `commit_ts` on every event and can stamp a `__loaded_at` when the writer commits, so no schema games with a filler column are needed.

```text
source write ──▶ event.commit_ts ──▶ ... pipeline ... ──▶ writer stamps __loaded_at
latency per row = __loaded_at - commit_ts
```

### Function tree

```text
scripts/bench.sh
├── reset.sh                                              fresh seeded stack
├── start reader + writer
├── pgbench-style write loop (reuse load.sh at volume)    scripts/load.sh
├── sample: SELECT lag metrics from dest                  internal/... or SQL
└── writeCSV data/latency_<date>.csv + print summary

writer flush                                              internal/writer/flush.go
└── stamp __loaded_at = now() on staging rows             (new bookkeeping column)
```

### Data shape: latency CSV (mirrors benchmarks/data/*.csv)

```text
minute,               row_count, avg_latency_s, max_latency_s, min_latency_s
2026-09-10 18:16:00,  4200,      1.8,           4,             1
2026-09-10 18:17:00,  5100,      2.1,           5,             1
```

Summary line for the README:

```text
BENCH: 60s, 12,400 rows, avg 1.9s / p95 4s / max 6s end-to-end latency, 207 rows/s
```

### Measurement query (dest side)

```sql
SELECT date_trunc('minute', __loaded_at) AS minute,
       count(*),
       avg (extract(epoch from __loaded_at - commit_ts)),
       max (extract(epoch from __loaded_at - commit_ts)),
       min (extract(epoch from __loaded_at - commit_ts))
FROM observations               -- carries __loaded_at + a commit_ts column for bench
GROUP BY 1 ORDER BY 1;
```

### Honest framing for the interview

- These are laptop numbers (colima VM, single Redpanda broker, Postgres to Postgres), not Artie's 700B-rows-a-year production. Report them as "the engine sustains X locally," never as a comparison to Artie or DMS.
- Latency is dominated by our flush interval (2s) by design: a knob trading latency for merge cost, which is exactly Artie's "Multi-Step Merge" tradeoff. Mention that the number is a policy choice, not a ceiling.
- No claim of exactly-once throughput records; the point is that the numbers are real, measured the way Artie measures, and reproducible with one script.

### Gotchas

- Bench needs `commit_ts` persisted on the destination row to compute latency; that means a dedicated bench table (or two extra columns) rather than measuring the safari tables, whose schema should stay clean. Keep bench isolated.
- Clock: source and dest are the same machine, so `commit_ts` (source) and `__loaded_at` (dest) share a clock. On separate hosts this would need clock-skew handling; note it, do not build it.

---

## Components

| Piece | What changes |
| --- | --- |
| `internal/writer/ddl.go` | reconcileColumns: ALTER TABLE ADD for new columns, cached |
| `internal/writer/flush.go` | stamp `__loaded_at` for the bench path |
| `scripts/evolve-test.sh` | ADD COLUMN, touch row, verify includes new column |
| `scripts/bench.sh` | load + latency CSV + summary line |
| `README.md` | results table, the flush-interval tradeoff, honest-numbers framing |

## Explicitly out of scope

RENAME / DROP / type-change DDL (needs DDL replication, not reconciliation), cross-host clock skew, parallel-scan throughput records.
