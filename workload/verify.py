import gzip
import io
import json

import psycopg
from pyarrow import parquet
from pyiceberg.catalog import load_catalog

from workload.config import Settings
from workload.storage import list_objects, s3_client

EXPECTED_COLUMNS = {
    "markets": [
        "PlatCode",
        "PlatName",
        "PlatformType",
        "AgeDays",
        "OperStatus",
        "RepScore",
        "ConfidenceLevel",
        "SizeCat",
        "DayTxnVol",
        "ActiveUsersMo",
        "SellerCount",
        "AcqCount",
        "ItemListings",
        "lastUpdated",
        "RefreshHrs",
        "platform_compliance",
    ],
    "vendors": [
        "SellerKey",
        "DaysActive",
        "PerformanceRating",
        "TotalTxns",
        "CompletedTxns",
        "DisputedEvents",
        "VerTier",
        "LastActiveDt",
        "AccessLevel",
        "InvestigationFlag",
        "LE_Interest",
        "ComplianceRisk",
        "RegStandeff",
        "vendor_compliance_ratings",
    ],
    "buyers": [
        "AcqCode",
        "ProfileAge",
        "PurchaseCount",
        "AuthLevel",
        "buyer_risk_profile",
    ],
    "products": [
        "ProdCat",
        "Subcategory",
        "ListingAge",
        "SellerPointer",
        "product_availability",
    ],
    "transactions": [
        "EventCode",
        "RecordTag",
        "EventTimestamp",
        "PlatformKey",
        "VendorLink",
        "AcqLink",
        "OriginRegion",
        "DestRegion",
        "CrossBorder",
        "RouteComplex",
        "Transaction_Velocity",
        "Border_cross_border_pre",
        "GeoDistScore",
        "transaction_financials",
    ],
    "transaction_products": [
        "EventLink",
        "ProdCat",
        "Subcategory",
        "ListingAge",
        "SellerPointer",
        "PriceAmt",
        "QtySold",
    ],
    "BuyerSessionAnalytics": [
        "BSA_id",
        "acq_ref",
        "session_start_time",
        "session_duration_seconds",
        "pages_viewed_count",
        "products_viewed_count",
        "cart_additions_count",
        "cart_removals_count",
        "search_queries_count",
        "checkout_initiated",
        "checkout_completed",
        "bounce_indicator",
        "referral_source",
        "device_category",
        "geo_region",
        "avg_time_per_page_seconds",
        "click_through_rate",
        "scroll_depth_pct",
        "error_encounters_count",
        "session_value_estimate",
    ],
    "PaymentProcessingEvents": [
        "PPE_id",
        "transaction_ref",
        "event_timestamp",
        "payment_method_type",
        "processing_stage",
        "amount_requested",
        "amount_processed",
        "currency_code",
        "processor_name",
        "authorization_code",
        "processing_fee",
        "processing_fee_pct",
        "fraud_check_passed",
        "fraud_score",
        "avs_response_code",
        "cvv_verification_passed",
        "three_ds_authenticated",
        "decline_reason",
        "retry_count",
        "processing_time_ms",
    ],
    "risk_analytics": [
        "TxnLink",
        "RiskIndicatorCount",
        "FraudProb",
        "ML_Risk",
        "LinkedEvents",
        "ChainLength",
        "wallet_risk_assessment",
    ],
    "RiskModelPredictions": [
        "RMP_id",
        "txn_link_ref",
        "prediction_timestamp",
        "model_name",
        "model_version",
        "fraud_probability",
        "risk_category_predicted",
        "confidence_score",
        "top_risk_factor",
        "risk_factors_count",
        "feature_importance_velocity",
        "feature_importance_amount",
        "feature_importance_device",
        "feature_importance_behavior",
        "recommendation_action",
        "actual_outcome",
        "prediction_latency_ms",
        "ensemble_agreement_rate",
        "manual_review_triggered",
        "model_drift_indicator",
    ],
}


def check_postgres(settings):
    with (
        psycopg.connect(**settings.pg_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = {row[0] for row in cursor.fetchall()}
        if tables != set(EXPECTED_COLUMNS):
            raise AssertionError(f"Unexpected PostgreSQL tables: {sorted(tables)}")
        for table, expected in EXPECTED_COLUMNS.items():
            cursor.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """,
                (table,),
            )
            actual = [row[0] for row in cursor.fetchall()]
            if actual != expected:
                raise AssertionError(f"{table} columns differ: {actual}")
            cursor.execute(
                "SELECT relreplident FROM pg_class WHERE oid = %s::regclass",
                (f'public."{table}"',),
            )
            if cursor.fetchone()[0] != "f":
                raise AssertionError(f"{table} does not use REPLICA IDENTITY FULL")
        cursor.execute(
            """
            SELECT slot_name, plugin, active, wal_status
            FROM pg_replication_slots WHERE slot_name = %s
        """,
            (settings.cdc_slot,),
        )
        slot = cursor.fetchone()
        if slot != (settings.cdc_slot, "wal2json", False, "reserved"):
            raise AssertionError(f"Unexpected CDC slot state: {slot}")
        cursor.execute("SHOW max_slot_wal_keep_size")
        if cursor.fetchone()[0] != "1GB":
            raise AssertionError("max_slot_wal_keep_size is not 1GB")


def check_object_storage(settings):
    client = s3_client(settings)
    bronze = list(
        list_objects(client, settings.s3_bucket, settings.bronze_prefix + "/")
    )
    parquet_objects = [item for item in bronze if item["Key"].endswith(".parquet")]
    total = sum(item["Size"] for item in parquet_objects)
    if not settings.bronze_target_bytes <= total <= settings.bronze_target_bytes * 1.05:
        raise AssertionError(
            f"Bronze Parquet size is {total}, expected about {settings.bronze_target_bytes}"
        )
    by_table = {}
    for item in parquet_objects:
        table = item["Key"][len(settings.bronze_prefix) + 1 :].split("/", 1)[0]
        by_table.setdefault(table, item)
    if set(by_table) != set(EXPECTED_COLUMNS):
        raise AssertionError(f"Bronze tables differ: {sorted(by_table)}")
    for table, item in by_table.items():
        body = client.get_object(Bucket=settings.s3_bucket, Key=item["Key"])[
            "Body"
        ].read()
        columns = parquet.ParquetFile(io.BytesIO(body)).schema_arrow.names
        if columns != EXPECTED_COLUMNS[table]:
            raise AssertionError(f"Bronze {table} columns differ: {columns}")

    manifest_key = f"{settings.bronze_prefix}/_manifest.json"
    manifest = json.loads(
        client.get_object(Bucket=settings.s3_bucket, Key=manifest_key)["Body"].read()
    )
    if manifest["total_parquet_bytes"] != total:
        raise AssertionError("Bronze manifest byte count differs from stored Parquet")
    rows = manifest["rows"]
    if set(rows) != set(EXPECTED_COLUMNS):
        raise AssertionError(f"Bronze manifest tables differ: {sorted(rows)}")
    if rows["transaction_products"] != 2 * rows["transactions"]:
        raise AssertionError(
            "Bronze history must contain exactly two items per transaction"
        )
    minimum_rows = {
        "markets": 10_000,
        "vendors": 100_000,
        "buyers": 1_000_000,
        "products": 1_000_000,
        "transactions": 1_000_000,
        "risk_analytics": 1_000_000,
    }
    for table, minimum in minimum_rows.items():
        if rows[table] < minimum:
            raise AssertionError(
                f"Bronze {table} has too few rows for its foreign keys"
            )

    landing = list(
        list_objects(client, settings.s3_bucket, settings.landing_prefix + "/")
    )
    raw_objects = [item for item in landing if item["Key"].endswith(".jsonl.gz")]
    if not raw_objects:
        raise AssertionError("No raw CDC landing objects found")
    body = client.get_object(Bucket=settings.s3_bucket, Key=raw_objects[-1]["Key"])[
        "Body"
    ].read()
    event = json.loads(gzip.decompress(body).splitlines()[0])
    if set(event) != {"schema_version", "event_id", "ingested_at", "source", "change"}:
        raise AssertionError(f"Unexpected raw envelope: {event.keys()}")
    for prefix in ("silver/", "gold/"):
        if list(list_objects(client, settings.s3_bucket, prefix)):
            raise AssertionError(f"{prefix} must remain empty")
    return total, len(parquet_objects), len(raw_objects)


def main():
    settings = Settings()
    if settings.s3_bucket != "mini-cybet":
        raise AssertionError(f"Expected bucket mini-cybet, got {settings.s3_bucket}")
    check_postgres(settings)
    total, parquet_count, raw_count = check_object_storage(settings)
    namespaces = load_catalog("kest").list_namespaces()
    if namespaces:
        raise AssertionError(f"Iceberg namespaces must remain empty: {namespaces}")
    print(
        json.dumps(
            {
                "bucket": settings.s3_bucket,
                "bronze_gib": round(total / 1024**3, 3),
                "parquet_objects": parquet_count,
                "landing_objects": raw_count,
                "silver_objects": 0,
                "gold_objects": 0,
                "iceberg_namespaces": 0,
                "cdc_slot_active": False,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
