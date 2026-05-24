# BI

Canonical reference: [docs/architecture.md](architecture.md).

## Serving path

Metabase -> DuckDB -> Iceberg (gold only)

## Access policy

- Metabase reads curated gold tables only.
- No access to bronze or silver.
- No direct access to raw objects in MinIO.

## Recommended marts

- daily_metrics
- user_activity
- retention
- growth
- event_summary
