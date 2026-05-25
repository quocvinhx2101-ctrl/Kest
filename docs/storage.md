# Storage

Canonical reference: [docs/architecture.md](architecture.md).

## Storage stack

- MinIO: S3-compatible object storage for all data.
- Apache Iceberg: table format for silver and gold layers.
- PyIceberg: official Apache Iceberg Python library for DDL, writes, and catalog management.
- PostgreSQL: Iceberg catalog (SqlCatalog) and Airflow metadata.
- DuckDB: read-only compute engine for transforms and serving views.

## Write/read separation

- **Writes**: PyIceberg handles all Iceberg table operations (create, append, overwrite) with proper partition specs, schema management, and manifest handling.
- **Reads**: DuckDB reads Iceberg tables via `iceberg_scan()` for serving views and transforms. DuckDB also reads bronze Parquet files via `read_parquet()`.
- **Catalog**: PyIceberg SqlCatalog backed by PostgreSQL. Single source of truth for all Iceberg metadata.

## Buckets and layout

MinIO bucket: lakehouse

Layout:
- bronze/
- silver/
- gold/
- checkpoints/
- quality/
- tmp/

## Bronze

- Append-only, immutable, never overwrite.
- Raw formats allowed: jsonl, parquet, csv, html.
- Partition: source=<source>/date=YYYY-MM-DD/.
- Written by dlt (filesystem destination). Not Iceberg — raw files by design.

## Silver/Gold

- Parquet only, managed by Iceberg.
- Written by PyIceberg with proper partition specs.
- Silver partitioned by `days(event_time)`.
- Gold partitioned by `months(event_date)`.
- Read by DuckDB via `iceberg_scan()`.

## Object immutability rules

- Bronze objects are immutable.
- Silver/gold tables are mutable via Iceberg transactions only (PyIceberg).
- No direct object overwrites outside Iceberg.

## Retention

- Bronze retention defined per source, default 90 days.
- Silver/gold retention defined per domain, default 365 days.
- Deletions must be executed through Iceberg table policies.
- Snapshot expiration managed by `iceberg/housekeeping.py`.

## Iceberg features in use

- Partition specs (day/month transforms).
- SqlCatalog on PostgreSQL for metadata.
- Snapshot-based writes (overwrite, append).
- Schema evolution support via PyIceberg.
- Snapshot expiration for housekeeping.
