# Operations

Canonical reference: [docs/architecture.md](architecture.md).

## Recovery and idempotency

- Retry only transient failures (network, temporary source outage).
- A batch can be replayed if the previous run failed or was partial.
- Completion signal: batch_id present in bronze plus quality gate passed.
- Reconciliation: compare record counts and checksums between bronze and silver.
- Replay must not overwrite bronze; silver/gold are recomputed deterministically.

## Backup priorities

1. PostgreSQL (first priority): Iceberg catalog and Airflow metadata.
2. MinIO (second priority): Iceberg data and bronze objects.
3. DuckDB (stateless): runtime only, no long-term state.

## Postgres backup policy

- Daily full backup.
- WAL enabled with retention for point-in-time recovery.
- Store backups on a separate volume.

## MinIO snapshot policy

- Daily snapshots of lakehouse bucket.
- Optional replication if a second disk is available.

## Monitoring

- Disk usage > 85% alert.
- Memory pressure alert.
- Pipeline failure alert.
- Stale data alert.

## Runbook defaults

- DuckDB uses explicit memory_limit and temp_directory for spill.
- Airflow restarts must not re-run completed batches unless requested.
