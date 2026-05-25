# DuckDB Runtime

DuckDB serves as the compute and serving layer for Kest.

- **Compute**: SQL transforms on bronze data (via `read_parquet()`) and silver data (via PyIceberg Arrow tables).
- **Serving**: Read-only views on Iceberg tables via `iceberg_scan()` for Metabase and ad-hoc queries.

DuckDB does NOT write Iceberg tables directly. All Iceberg writes go through PyIceberg (see `iceberg/` and `scripts/run_silver.py`, `scripts/run_gold.py`).
