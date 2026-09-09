import psycopg

from workload.cybermarket.schema import (
    DIMENSION_TABLES,
    EXPECTED_COLUMNS,
    FACT_TABLES,
)
from workload.lakehouse.catalog import catalog, load_manifest, record_count
from workload.pipelines.batch import GOLD_QUERIES

GOLD_COLUMNS = {
    "daily_market_metrics": [
        "metric_date",
        "platform_key",
        "transaction_count",
        "unique_buyers",
        "unique_vendors",
        "gross_merchandise_value",
        "cross_border_transactions",
        "high_risk_transactions",
        "payment_failed_events",
    ],
    "vendor_risk_summary": [
        "seller_key",
        "transaction_count",
        "unique_buyers",
        "gross_merchandise_value",
        "high_risk_transactions",
        "average_fraud_probability",
        "last_transaction_at",
    ],
    "buyer_360": [
        "buyer_key",
        "transaction_count",
        "session_count",
        "completed_checkouts",
        "lifetime_value",
        "average_session_seconds",
        "last_purchase_at",
        "auth_level",
        "buyer_risk_profile",
    ],
    "product_performance": [
        "product_category",
        "subcategory",
        "listing_age",
        "seller_key",
        "transaction_count",
        "units_sold",
        "gross_revenue",
        "product_availability",
    ],
}


def _postgres_counts(settings):
    with (
        psycopg.connect(**settings.pg_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        result = {}
        for table in EXPECTED_COLUMNS:
            cursor.execute(f'SELECT count(*) FROM "{table}"')
            result[table] = cursor.fetchone()[0]
        return result


def check_batch(settings):
    iceberg_catalog = catalog(settings)
    namespaces = set(iceberg_catalog.list_namespaces())
    required_namespaces = {
        (settings.silver_namespace,),
        (settings.gold_namespace,),
    }
    if not required_namespaces <= namespaces:
        raise AssertionError(
            f"Missing Iceberg namespaces: {required_namespaces - namespaces}"
        )
    staging = [namespace for namespace in namespaces if "_stage_" in namespace[-1]]
    if staging:
        raise AssertionError(f"Staging namespaces remain: {staging}")

    silver_tables = set(iceberg_catalog.list_tables((settings.silver_namespace,)))
    expected_silver = {(settings.silver_namespace, table) for table in EXPECTED_COLUMNS}
    if silver_tables != expected_silver:
        raise AssertionError(f"Silver tables differ: {sorted(silver_tables)}")

    gold_tables = set(iceberg_catalog.list_tables((settings.gold_namespace,)))
    expected_gold = {(settings.gold_namespace, table) for table in GOLD_QUERIES}
    if gold_tables != expected_gold:
        raise AssertionError(f"Gold tables differ: {sorted(gold_tables)}")

    manifest, manifest_sha = load_manifest(settings)
    postgres_counts = _postgres_counts(settings)
    silver_counts = {}
    batch_ids = set()
    for table in EXPECTED_COLUMNS:
        iceberg_table = iceberg_catalog.load_table((settings.silver_namespace, table))
        if iceberg_table.schema().column_names != EXPECTED_COLUMNS[table]:
            raise AssertionError(f"Silver {table} columns differ")
        expected = postgres_counts[table]
        if table in FACT_TABLES:
            expected += manifest["rows"][table]
        if table in DIMENSION_TABLES and expected != postgres_counts[table]:
            raise AssertionError(f"Silver {table} dimension count is invalid")
        actual = record_count(iceberg_table)
        if actual != expected:
            raise AssertionError(f"Silver {table}: expected {expected}, got {actual}")
        if iceberg_table.properties.get("kest.bronze-manifest-sha256") != manifest_sha:
            raise AssertionError(f"Silver {table} points to another bronze manifest")
        batch_ids.add(iceberg_table.properties.get("kest.batch-id"))
        silver_counts[table] = actual

    gold_data = {}
    gold_counts = {}
    for table, columns in GOLD_COLUMNS.items():
        iceberg_table = iceberg_catalog.load_table((settings.gold_namespace, table))
        if iceberg_table.schema().column_names != columns:
            raise AssertionError(f"Gold {table} columns differ")
        batch_ids.add(iceberg_table.properties.get("kest.batch-id"))
        data = iceberg_table.scan().to_arrow()
        if not data.num_rows:
            raise AssertionError(f"Gold {table} is empty")
        gold_data[table] = data
        gold_counts[table] = data.num_rows

    if len(batch_ids) != 1 or None in batch_ids:
        raise AssertionError(f"Iceberg tables do not share one batch ID: {batch_ids}")
    if gold_counts["vendor_risk_summary"] != postgres_counts["vendors"]:
        raise AssertionError(
            "Gold vendor cardinality differs from the canonical domain"
        )
    if gold_counts["buyer_360"] != postgres_counts["buyers"]:
        raise AssertionError("Gold buyer cardinality differs from the canonical domain")
    if gold_counts["product_performance"] != postgres_counts["products"]:
        raise AssertionError(
            "Gold product cardinality differs from the canonical domain"
        )

    transaction_count = silver_counts["transactions"]
    for table in ("daily_market_metrics", "vendor_risk_summary", "buyer_360"):
        total = sum(gold_data[table]["transaction_count"].to_pylist())
        if total != transaction_count:
            raise AssertionError(f"Gold {table} transaction total differs: {total}")
    units = sum(gold_data["product_performance"]["units_sold"].to_pylist())
    if units != silver_counts["transaction_products"]:
        raise AssertionError(
            "Gold product units differ from Silver transaction products"
        )

    return {
        "batch_id": batch_ids.pop(),
        "gold_rows": gold_counts,
        "silver_rows": silver_counts,
    }
