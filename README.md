# minicdc

A small change data capture engine: it watches a Postgres database's write-ahead log and keeps a live copy of its tables in a second database, without losing or duplicating a change. One source, one destination, built from first principles as a study of how systems like [Artie](https://artie.com) work.

```text
source Postgres ──WAL──▶ reader ──▶ Redpanda ──▶ writer ──▶ destination Postgres
```

The source fixture is Artie's own [terra](https://github.com/artie-labs/terra) demo dataset, run unmodified.

## Run it

```bash
cd deploy
./fetch-terra.sh          # clones the terra source fixture
docker compose up -d      # source pg + redpanda + dest pg

go run ./cmd/reader &     # WAL -> Kafka
go run ./cmd/writer &     # Kafka -> destination
```

Then watch a change flow through:

```bash
psql postgres://postgres:minicdc@localhost:5410/terra \
  -c "INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status) VALUES (9999, 'Testo', 'lion', 1, 'adult');"

psql postgres://postgres:minicdc@localhost:5411/warehouse \
  -c "SELECT name FROM public.animals WHERE animal_id = 9999;"
```

## Guarantees

- **No loss, no duplication under crashes.** The reader acks a WAL position to Postgres only after Kafka confirms the events; the writer commits a Kafka offset only after the destination transaction commits. A `kill -9` of either side re-delivers a batch, and the writer's merge re-asserts the same final state, so replays are harmless. Proven, not asserted: `scripts/crash-test.sh` kills both processes mid-load and requires `cdcctl verify` to report an exact match.
- **Backfill to live, gapless.** A fresh pipeline copies existing rows from a snapshot exported at slot creation, then streams from that exact WAL position. `scripts/backfill-test.sh` seeds the source, writes concurrently during backfill, and verifies.
- **Correct on the hard cases.** Unchanged TOAST columns are preserved rather than nulled; primary-key-changing updates become delete+insert; multi-row transactions stay ordered per row via table+PK Kafka keys.

## Measured latency

Streaming latency under sustained mixed load, measured Artie's way (source commit time vs destination apply time, both stamped on every row):

```
avg 1.1s / p95 2s / max 2s end-to-end
```

Laptop numbers (a colima VM, single Redpanda broker, Postgres to Postgres), not a production comparison. Latency is set by the writer's 2s flush interval, a deliberate latency-versus-merge-cost knob (Artie's "multi-step merge" tradeoff), not a ceiling. Reproduce with `scripts/bench.sh`.

## How it's built

Five phases, each with a function trace and the data shapes flowing through it, in [docs/](docs/): the flowing skeleton, correctness via staging merge, recovery, backfill, and schema evolution plus benchmarking. The design keeps the writer ignorant of the source: every change (insert, update, delete, and snapshot read) is the same self-describing JSON event, so backfill and streaming share one apply path.
