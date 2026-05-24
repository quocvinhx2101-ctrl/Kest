# Storage

Canonical reference: [docs/architecture.md](architecture.md).

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

## Silver/Gold

- Parquet only.
- Iceberg tables stored on MinIO.
- Partition: domain=<domain>/year=YYYY/month=MM/day=DD/.

## Object immutability rules

- Bronze objects are immutable.
- Silver/gold tables are mutable via Iceberg transactions only.
- No direct object overwrites outside Iceberg.

## Retention

- Bronze retention defined per source, default 90 days.
- Silver/gold retention defined per domain, default 365 days.
- Deletions must be executed through Iceberg table policies.
