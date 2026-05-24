#!/usr/bin/env bash
set -euo pipefail

layer=${1:-all}

echo "Running Soda checks (${layer})..."

if [ "$layer" = "bronze" ]; then
	python /opt/kest/scripts/run_duckdb_etl.py --stage bronze_views
	soda scan -d kest_duckdb /opt/kest/soda/checks/bronze.yml
elif [ "$layer" = "silver" ]; then
	soda scan -d kest_duckdb /opt/kest/soda/checks/silver.yml
elif [ "$layer" = "gold" ]; then
	soda scan -d kest_duckdb /opt/kest/soda/checks/gold.yml
else
	python /opt/kest/scripts/run_duckdb_etl.py --stage bronze_views
	soda scan -d kest_duckdb /opt/kest/soda/checks/bronze.yml /opt/kest/soda/checks/silver.yml /opt/kest/soda/checks/gold.yml
fi
