# Kest

Kest is a single-node, lean lakehouse. It is storage-first, uses open table formats(iceberg), and keeps compute portable. The architecture is intentionally minimal and designed for low operational burden, deterministic pipelines, and future scale-up without data migration.

## Canonical architecture

The canonical source of truth is [docs/architecture.md](docs/architecture.md). All other docs must align with it.

## V1 stack (fixed)

- MinIO (object storage)
- Apache Iceberg (table format)
- PostgreSQL (Iceberg catalog + Airflow metadata DB)
- dlt (ingestion)
- DuckDB (transform + serving)
- Airflow LocalExecutor (orchestration)
- Metabase (BI)
- Soda Core (data quality)

## Non-goals

- No Spark, Kafka, Trino, Flink, Kubernetes, or distributed execution.
- No extra services unless explicitly justified.
- No business logic inside Airflow DAGs.

## Lifecycle constraints

- Bronze is immutable and append-only.
- dlt handles extract/load/normalize only; business transforms happen in DuckDB.
- Metabase reads curated gold tables only via DuckDB.
- Storage and compute are decoupled; compute is disposable.

## Docs layout

- [docs/architecture.md](docs/architecture.md): canonical architecture and lifecycle
- Phase 1 and Phase 2 docs will reference architecture.md as the source of truth.

## Local run (minimal vertical slice)

1. Copy environment template:

```bash
cp .env.example .env
```

2. Build and start services:

```bash
docker compose up -d --build
```

3. Open Airflow and trigger the DAG:

- Airflow UI: http://localhost:8080
- DAG: `kest_example_pipeline`

4. Metabase connection (optional):

- URL: http://localhost:3000
- Database: DuckDB
- Path: /metabase/duckdb/kest.duckdb
- Use gold views only (for example, `gold_daily_metrics`).

## Test run (step by step)

1. Prepare environment:

```bash
cp .env.example .env
```

2. Start the stack:

```bash
docker compose up -d --build
```

3. Verify containers are up:

```bash
docker compose ps
```

4. Open Airflow UI and enable the DAG:

- http://localhost:8080
- Username: `admin`
- Password: `admin`
- Toggle on `kest_example_pipeline`

5. Trigger a manual run:

- In Airflow, click the play icon for `kest_example_pipeline`.

6. Watch task logs in Airflow (each task should succeed):

- `run_dlt`
- `run_bronze_validation`
- `run_duckdb_silver`
- `run_silver_checks`
- `run_duckdb_gold`
- `run_gold_checks`

7. Verify artifacts in MinIO:

- http://localhost:9001
- Buckets -> `lakehouse`
- Check:
	- `bronze/example/example_events/`
	- `silver/domain=example/entity=events/`
	- `gold/domain=example/entity=daily_metrics/`

8. Verify DuckDB outputs locally:

```bash
docker compose exec airflow-webserver python - <<'PY'
import duckdb

con = duckdb.connect('/opt/kest/duckdb/kest.duckdb')
print(con.execute('SELECT * FROM gold_daily_metrics').fetchall())
PY
```

9. Optional: connect Metabase to DuckDB:

- URL: http://localhost:3000
- Database: DuckDB
- Path: /metabase/duckdb/kest.duckdb
- Query `gold_daily_metrics` only

## Minimal lifecycle

The example DAG runs the full lifecycle:

1. Extract and load to bronze via dlt.
2. Bronze validation via Soda.
3. Silver transform via DuckDB (Iceberg on MinIO).
4. Silver quality checks via Soda.
5. Gold transform via DuckDB (Iceberg on MinIO).
6. Gold quality checks via Soda.

Artifacts written:
- Bronze: s3://lakehouse/bronze/example/example_events/
- Silver: s3://lakehouse/silver/domain=example/entity=events/
- Gold: s3://lakehouse/gold/domain=example/entity=daily_metrics/
