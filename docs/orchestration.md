# Orchestration

Canonical reference: [docs/architecture.md](architecture.md).

## Airflow role

- Airflow orchestrates only.
- No business logic inside DAGs.
- Use tasks to trigger dlt and DuckDB jobs.

## Executor

- LocalExecutor only.
- No Celery, no Kubernetes, no distributed execution.

## Concurrency

- parallelism = 2
- dag_concurrency = 2
- max_active_runs_per_dag = 1

## DAG pattern

1. extract
2. bronze validation
3. silver transform
4. quality checks
5. gold publish

## Failure handling

- Retries for transient failures only.
- Hard fail on schema breaking changes.
- Replay a batch from bronze on failure.
