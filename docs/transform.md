# Transform

Canonical reference: [docs/architecture.md](architecture.md).

## DuckDB responsibilities

- Silver transforms: cleanup, dedup, type casting.
- Gold transforms: marts, KPIs, aggregates, semantic tables.
- Analytical serving for Metabase.

## SQL style

Allowed:
- CREATE TABLE AS
- INSERT OVERWRITE
- MERGE INTO

Avoid:
- Pandas-based transforms
- Non-deterministic logic

## Extensions

Required:
- httpfs
- iceberg
- postgres

## Runtime separation

- ETL runtime for pipeline transforms.
- Analytics runtime for BI queries.
- Do not share the same process.

## Resource settings

- Set memory limits explicitly.
- Set temp directory for spill.
- Example:
  - memory_limit = 8GB
  - threads = 4
  - temp_directory = /tmp/duckdb

## Determinism

- Transforms must be repeatable for a given batch_id.
- Use record_hash and source_id for dedup.
