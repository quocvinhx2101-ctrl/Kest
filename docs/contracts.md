# Global Contracts

This document defines cross-cutting contracts for Kest. All pipelines, tables, and docs must comply. The canonical architecture is [docs/architecture.md](architecture.md).

## Naming conventions

- Table name: <domain>_<entity> (snake_case, ASCII only).
- Schema names: bronze, silver, gold.
- Iceberg table path: lakehouse/<layer>/<domain>/<entity>/.
- Datasets, sources, and pipelines: lower_snake_case.

## Timestamp conventions

- All timestamps are UTC.
- Required fields:
  - event_time: when the event happened in the source system.
  - ingested_at: when the record entered bronze.
  - processed_at: when the record entered silver or gold.
- Use ISO 8601 for serialized timestamps.

## Partition strategy

- Bronze: source=<source>/date=YYYY-MM-DD/.
- Silver/Gold: domain=<domain>/year=YYYY/month=MM/day=DD/.
- Partition keys must be present as columns in tables.

## Metadata columns (required)

Bronze must include:
- _ingested_at
- _source
- _batch_id
- _raw_payload

Silver and gold must include:
- event_time
- ingested_at
- processed_at
- source_id
- record_hash
- batch_id

## Record IDs

- source_id: stable source identifier if available.
- record_hash: deterministic hash of selected source fields used for dedup.
- Do not reuse record_hash across unrelated entities.

## Batch IDs

- batch_id is generated once per ingestion run.
- batch_id must be propagated from bronze to silver/gold.
- A batch is complete only if all expected records are loaded and validated.

## Schema evolution policy

- Bronze allows additive columns and type widening only.
- Silver/gold require explicit migration steps and backfill plans.
- Breaking changes must be staged and documented.

## Idempotency guarantees

- Ingestion replays must not create duplicates in silver/gold.
- Transforms must be deterministic and keyed on source_id + record_hash.
- Replay of a batch must be safe and produce identical outputs.

## Replay semantics

- Replay is defined as reprocessing a known batch_id from bronze.
- Replays are allowed for bronze and silver; gold can be recomputed deterministically.
- Replay must not delete or overwrite bronze data.
