import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "iceberg"))
sys.path.insert(0, "/opt/kest/iceberg")

import duckdb

from catalog import get_catalog
from tables import ensure_tables


def run_gold() -> None:
    catalog = get_catalog()
    ensure_tables(catalog)

    silver_table = catalog.load_table("silver.example_events")
    gold_table = catalog.load_table("gold.daily_metrics")

    silver_arrow = silver_table.scan().to_arrow()

    conn = duckdb.connect()
    conn.register("silver_events", silver_arrow)

    gold_arrow = conn.execute(
        """
        SELECT
            date_trunc('day', event_time)::DATE AS event_date,
            count(*)::BIGINT AS event_count,
            min(ingested_at) AS ingested_at,
            max(processed_at) AS processed_at
        FROM silver_events
        GROUP BY 1
        """
    ).arrow()

    conn.close()

    gold_table.overwrite(gold_arrow)
    print(f"Gold: wrote {gold_arrow.num_rows} rows via PyIceberg")


if __name__ == "__main__":
    run_gold()
