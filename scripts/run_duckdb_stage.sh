#!/usr/bin/env bash
set -euo pipefail

stage=${1:-}
if [ -z "$stage" ]; then
  echo "Usage: run_duckdb_stage.sh <silver|gold|serving>"
  exit 1
fi

if [ "$stage" = "silver" ]; then
  echo "Running silver transform via PyIceberg..."
  python /opt/kest/scripts/run_silver.py
elif [ "$stage" = "gold" ]; then
  echo "Running gold transform via PyIceberg..."
  python /opt/kest/scripts/run_gold.py
elif [ "$stage" = "serving" ]; then
  echo "Refreshing DuckDB serving views..."
  python /opt/kest/scripts/run_duckdb_etl.py --stage serving
else
  echo "Unknown stage: $stage"
  exit 1
fi
