import argparse
from datetime import datetime, timedelta, timezone

from catalog import get_catalog


def expire_snapshots(table_id: str, older_than_days: int = 7) -> None:
    catalog = get_catalog()
    table = catalog.load_table(table_id)
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    table.manage_snapshots().expire_snapshots_older_than(cutoff_ms).commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="e.g. silver.example_events")
    parser.add_argument("--older-than-days", type=int, default=7)
    args = parser.parse_args()
    expire_snapshots(args.table, args.older_than_days)


if __name__ == "__main__":
    main()
