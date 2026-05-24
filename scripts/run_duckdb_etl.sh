#!/usr/bin/env bash
set -euo pipefail

echo "Running DuckDB silver transforms..."
python /opt/kest/scripts/run_duckdb_etl.py --stage silver

echo "Running DuckDB gold transforms..."
python /opt/kest/scripts/run_duckdb_etl.py --stage gold
