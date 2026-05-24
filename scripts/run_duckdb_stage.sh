#!/usr/bin/env bash
set -euo pipefail

stage=${1:-}
if [ -z "$stage" ]; then
  echo "Usage: run_duckdb_stage.sh <silver|gold>"
  exit 1
fi

echo "Running DuckDB stage: $stage"
python /opt/kest/scripts/run_duckdb_etl.py --stage "$stage"
