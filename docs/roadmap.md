# Roadmap

Canonical reference: [docs/architecture.md](architecture.md).

## Phase 2+ upgrades

- Analytics scale: DuckDB -> Trino without data migration.
- Streaming: add Redpanda or Kafka without changing storage.
- Metadata: add OpenMetadata or DataHub only if operationally justified.

## Non-goals

- No distributed execution in V1.
- No complex orchestration beyond Airflow LocalExecutor.
- No heavy service sprawl.
