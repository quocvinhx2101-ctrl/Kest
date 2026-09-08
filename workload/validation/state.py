import argparse
import json

from pyiceberg.catalog import load_catalog

from workload.core.config import Settings
from workload.validation.postgres import check_postgres
from workload.validation.storage import (
    check_cdc,
    check_future_layers_empty,
    check_history,
)

PHASES = ("setup", "history", "cdc")


def verify(phase):
    settings = Settings.from_env()
    check_postgres(settings, require_slot=phase == "cdc")
    check_future_layers_empty(settings)

    result = {
        "bucket": settings.s3_bucket,
        "phase": phase,
        "postgres_tables": 10,
        "silver_objects": 0,
        "gold_objects": 0,
    }
    if phase == "history":
        total, parquet_count = check_history(settings)
        result.update(
            bronze_gib=round(total / 1024**3, 3),
            parquet_objects=parquet_count,
        )
    if phase == "cdc":
        result.update(
            cdc_slot_active=False,
            landing_objects=check_cdc(settings),
        )

    namespaces = load_catalog("kest").list_namespaces()
    if namespaces:
        raise AssertionError(f"Iceberg namespaces must remain empty: {namespaces}")
    result["iceberg_namespaces"] = 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Validate a CyberMarket lifecycle phase"
    )
    parser.add_argument("--phase", choices=PHASES, default="setup")
    args = parser.parse_args()
    verify(args.phase)


if __name__ == "__main__":
    main()
