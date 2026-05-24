# Ingestion

Canonical reference: [docs/architecture.md](architecture.md).

## dlt responsibilities

dlt handles:
- extract
- load
- normalize
- checkpointing
- schema evolution support

dlt must NOT perform business transformations.

## Output contract

Bronze output must include:
- _ingested_at
- _source
- _batch_id
- _raw_payload

## Pipeline structure

Each pipeline contains:
- source.py
- resources.py
- schema.yml
- quality.sql
- transform.sql
- config.toml

## Incremental loading

- Prefer updated_at or created_at cursors.
- If no cursor exists, use a high-watermark timestamp.
- Checkpoints must be stored under lakehouse/checkpoints.

## dlt -> bronze boundary

- Bronze contains normalized raw data only.
- Business logic starts in DuckDB transforms.
- No enrichment, aggregation, or domain rules in dlt.
