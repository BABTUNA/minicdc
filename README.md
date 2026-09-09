# minicdc

A small change data capture engine: it watches a Postgres database's write-ahead log and keeps a live copy of its tables in a second database, without losing or duplicating a change. One source, one destination, built from first principles as a study of how systems like [Artie](https://artie.com) work.

```text
source Postgres ──WAL──▶ reader ──▶ Redpanda ──▶ writer ──▶ destination Postgres
```

The source fixture is Artie's own [terra](https://github.com/artie-labs/terra) demo dataset, run unmodified.

## Run it

Everything below runs from the repo root and needs only Docker and Go.

```bash
./deploy/fetch-terra.sh                             # one-time: clone the terra source fixture
docker compose -f deploy/docker-compose.yml up -d   # source pg + redpanda + dest pg
go build ./...                                      # build the binaries into bin/
./bin/reader &                                       # WAL -> Kafka
./bin/writer &                                       # Kafka -> destination
```

Insert a row on the source:

```bash
docker exec minicdc-source psql -U postgres -d terra -c "INSERT INTO animals (animal_id, name, species, home_watering_hole_id, status) VALUES (9999, 'Testo', 'lion', 1, 'adult');"
```

See it arrive in the destination a second later:

```bash
docker exec minicdc-dest psql -U postgres -d warehouse -c "SELECT animal_id, name, __minicdc_commit_ts, __minicdc_updated_at FROM public.animals WHERE animal_id = 9999;"
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

## Commands

Three demo scripts, each self-contained (they reset the stack, so run one at a time):

```bash
./scripts/crash-test.sh 60      # load, kill -9 writer and reader mid-stream, restart, verify MATCH
./scripts/backfill-test.sh 80   # seed 1430 rows, replicate them while writing live, verify
./scripts/bench.sh 400          # sustained load, then report end-to-end latency + write a CSV
```

The system those scripts drive:

| Command | What it does |
| --- | --- |
| `go build ./...` | build the three binaries into `bin/` |
| `./bin/reader` | source side: read the WAL, publish change events to Kafka (runs until killed) |
| `./bin/writer` | destination side: consume events, apply via staging + merge (runs until killed) |
| `./bin/cdcctl verify [--timeout 30s]` | checksum every table on both databases, print MATCH or MISMATCH; the independent judge |
| `./bin/cdcctl latency [--table T] [--csv PATH]` | report avg/p95/max end-to-end latency from the destination's timestamp columns |

Setup and stack control:

| Command | What it does |
| --- | --- |
| `./deploy/fetch-terra.sh` | one-time: clone the terra source fixture |
| `./scripts/reset.sh` | destroy volumes and bring the stack back up freshly seeded (clean slate) |
| `./scripts/load.sh [N]` | generate N iterations of mixed insert/update/delete traffic (the workload the demos use) |
| `docker exec -it minicdc-source psql -U postgres -d terra` | open a shell on the source database |
| `docker exec -it minicdc-dest psql -U postgres -d warehouse` | open a shell on the destination database |

The scripts are three complete demos; the binaries are the system they drive (run them by hand only for the manual walkthrough above); `cdcctl` inspects results; `reset.sh` gets you back to zero.

## How it's built

Five phases, each with a function trace and the data shapes flowing through it, in [docs/](docs/): the flowing skeleton, correctness via staging merge, recovery, backfill, and schema evolution plus benchmarking. The design keeps the writer ignorant of the source: every change (insert, update, delete, and snapshot read) is the same self-describing JSON event, so backfill and streaming share one apply path.
