#!/usr/bin/env bash
set -euo pipefail

cd /opt/kest/dlt

echo "Bootstrapping MinIO bucket..."
python /opt/kest/scripts/bootstrap_minio.py

echo "Running dlt pipeline..."
python /opt/kest/dlt/pipelines/example/run.py
