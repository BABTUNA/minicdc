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

## Design notes

- The reader acks a WAL position to Postgres only after Kafka has confirmed the events; the writer commits a Kafka offset only after the destination write succeeds. Every component can be killed at any moment and resumes without loss.
- Events travel in a small JSON envelope keyed by table+PK, so each row's changes stay ordered within one Kafka partition.
- Build phases and function traces live in [docs/](docs/).

Status: phase 1 (inserts flowing end to end). Updates/deletes, crash-recovery verification, backfill, schema evolution, and benchmarks are the next phases, in that order.
