import argparse
import json
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from workload.core.config import Settings
from workload.core.storage import list_objects, s3_client
from workload.cybermarket.ids import (
    BUYER_COUNT,
    HISTORY_REFERENCE_COUNT,
    MARKET_COUNT,
    PRODUCT_COUNT,
    PRODUCTS_PER_VENDOR,
    VENDOR_COUNT,
)

MIB = 1024**2
FILE_TARGET_BYTES = 128 * MIB
TABLE_WEIGHTS = {
    "markets": 0.005,
    "vendors": 0.02,
    "buyers": 0.02,
    "products": 0.08,
    "transactions": 0.25,
    "transaction_products": 0.25,
    "BuyerSessionAnalytics": 0.12,
    "PaymentProcessingEvents": 0.12,
    "risk_analytics": 0.055,
    "RiskModelPredictions": 0.08,
}
FIXED_TABLE_ROWS = {
    "markets": MARKET_COUNT,
    "vendors": VENDOR_COUNT,
    "buyers": BUYER_COUNT,
    "products": PRODUCT_COUNT,
}


SELECT_LISTS = {
    "markets": """
        printf('PLAT-%03d', i) AS "PlatCode",
        printf('CyberMarket %d %s', i, md5(i::varchar)) AS "PlatName",
        CASE i % 3 WHEN 0 THEN 'marketplace' ELSE 'specialized' END AS "PlatformType",
        (365 + i % 8000)::bigint AS "AgeDays", 'active' AS "OperStatus",
        (60 + i % 40)::varchar AS "RepScore", 'high' AS "ConfidenceLevel",
        CASE i % 3 WHEN 0 THEN 'large' ELSE 'medium' END AS "SizeCat",
        (1000 + i % 50000)::real AS "DayTxnVol", (10000 + i % 900000)::varchar AS "ActiveUsersMo",
        (100 + i % 50000)::bigint AS "SellerCount", (1000 + i % 500000)::bigint AS "AcqCount",
        (1000 + i % 1000000)::bigint AS "ItemListings",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "lastUpdated",
        24::bigint AS "RefreshHrs",
        json_object('status', 'compliant', 'audit', md5('market:' || i::varchar)) AS platform_compliance
    """,
    "vendors": """
        printf('SELLER-%06d', i) AS "SellerKey", (30 + i % 3000)::bigint AS "DaysActive",
        (3 + (i % 20) / 10.0)::real AS "PerformanceRating", (i % 50000)::varchar AS "TotalTxns",
        (i % 49000)::bigint AS "CompletedTxns", (i % 25)::bigint AS "DisputedEvents",
        CASE i % 10 WHEN 0 THEN 'enhanced' ELSE 'standard' END AS "VerTier",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "LastActiveDt",
        'active' AS "AccessLevel", CASE i % 97 WHEN 0 THEN 'review' ELSE 'none' END AS "InvestigationFlag",
        CASE i % 53 WHEN 0 THEN 'medium' ELSE 'low' END AS "LE_Interest", 'low' AS "ComplianceRisk",
        'verified' AS "RegStandeff",
        json_object('rating_id', md5('vendor:' || i::varchar), 'reviewed', i % 2 = 0) AS vendor_compliance_ratings
    """,
    "buyers": """
        printf('BUYER-%07d', i) AS "AcqCode", (i % 2500)::bigint AS "ProfileAge",
        (i % 500)::bigint AS "PurchaseCount", CASE i % 3 WHEN 0 THEN 'mfa' ELSE 'standard' END AS "AuthLevel",
        json_object('fingerprint', md5('buyer:' || i::varchar), 'score', (i % 1000) / 1000.0) AS buyer_risk_profile
    """,
    "products": """
        printf('CAT-%03d', i % 20) AS "ProdCat", printf('SUB-%05d', i % 100) AS "Subcategory",
        (i % 4)::bigint AS "ListingAge", printf('SELLER-%06d', 1 + (i - 1) // 4) AS "SellerPointer",
        json_object('stock', i % 500, 'sku_hash', md5('product:' || i::varchar)) AS product_availability
    """,
    "transactions": f"""
        printf('EVT-HIST-%014d', i) AS "EventCode", 'purchase' AS "RecordTag",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "EventTimestamp",
        printf('PLAT-%03d', 1 + (i - 1) % {MARKET_COUNT}) AS "PlatformKey",
        printf('SELLER-%06d', 1 + (i - 1) % {VENDOR_COUNT}) AS "VendorLink",
        printf('BUYER-%07d', 1 + (i - 1) % {BUYER_COUNT}) AS "AcqLink",
        CASE i % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS "OriginRegion",
        CASE (i // 3) % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS "DestRegion",
        ((i % 4) != ((i // 3) % 4))::bigint AS "CrossBorder",
        CASE WHEN (i % 4) != ((i // 3) % 4) THEN 'multi-hop' ELSE 'direct' END AS "RouteComplex",
        CASE i % 5 WHEN 0 THEN 'burst' ELSE 'normal' END AS "Transaction_Velocity",
        CASE WHEN (i % 4) != ((i // 3) % 4) THEN 'cross-border' ELSE 'domestic' END AS "Border_cross_border_pre",
        ((i * 37) % 10000 / 100.0)::varchar AS "GeoDistScore",
        json_object('amount', 5 + (i % 250000) / 100.0, 'currency', 'USD', 'status', 'settled',
                    'trace', md5('txn:' || i::varchar)) AS transaction_financials
    """,
    "transaction_products": f"""
        printf('EVT-HIST-%014d', 1 + (i - 1) // 2) AS "EventLink",
        printf('CAT-%03d', ((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 + (i - 1) % 2) % 20) AS "ProdCat",
        printf('SUB-%05d', ((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 + (i - 1) % 2) % 100) AS "Subcategory",
        (((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 + (i - 1) % 2) % {PRODUCTS_PER_VENDOR})::bigint AS "ListingAge",
        printf('SELLER-%06d', 1 + ((i - 1) // 2) % {VENDOR_COUNT}) AS "SellerPointer",
        ((5 + ((1 + (i - 1) // 2) % 250000) / 100.0) * CASE (i - 1) % 2 WHEN 0 THEN 0.45 ELSE 0.55 END)::real AS "PriceAmt",
        1::bigint AS "QtySold"
    """,
    "BuyerSessionAnalytics": f"""
        printf('BSA-HIST-%014d', i) AS "BSA_id", printf('BUYER-%07d', 1 + (i - 1) % {BUYER_COUNT}) AS acq_ref,
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS session_start_time,
        (10 + i % 1800)::integer AS session_duration_seconds, (1 + i % 30)::integer AS pages_viewed_count,
        (i % 15)::integer AS products_viewed_count, (i % 5)::integer AS cart_additions_count,
        (i % 3)::integer AS cart_removals_count, (i % 8)::integer AS search_queries_count,
        (i % 3 = 0) AS checkout_initiated, (i % 5 = 0) AS checkout_completed, (i % 7 = 0) AS bounce_indicator,
        CASE i % 4 WHEN 0 THEN 'direct' WHEN 1 THEN 'search' WHEN 2 THEN 'affiliate' ELSE 'social' END AS referral_source,
        CASE i % 3 WHEN 0 THEN 'desktop' WHEN 1 THEN 'mobile' ELSE 'tablet' END AS device_category,
        CASE i % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS geo_region,
        (5 + i % 120)::real AS avg_time_per_page_seconds, ((i % 1000) / 1000.0)::real AS click_through_rate,
        (i % 101)::real AS scroll_depth_pct, (i % 3)::integer AS error_encounters_count,
        ((i * 13) % 100000 / 100.0)::real AS session_value_estimate
    """,
    "PaymentProcessingEvents": f"""
        printf('PPE-HIST-%014d', i) AS "PPE_id",
        printf('EVT-HIST-%014d', 1 + (i - 1) % {HISTORY_REFERENCE_COUNT}) AS transaction_ref,
        TIMESTAMP '2024-01-01' + ((1 + (i - 1) % {HISTORY_REFERENCE_COUNT}) % 31536000) * INTERVAL 1 SECOND
            + (10 + i % 900) * INTERVAL 1 MILLISECOND AS event_timestamp,
        CASE i % 3 WHEN 0 THEN 'card' WHEN 1 THEN 'wallet' ELSE 'bank_transfer' END AS payment_method_type,
        'settled' AS processing_stage, (5 + i % 250000 / 100.0)::real AS amount_requested,
        (5 + i % 250000 / 100.0)::real AS amount_processed, 'USD' AS currency_code,
        CASE i % 2 WHEN 0 THEN 'nova-pay' ELSE 'orbit-pay' END AS processor_name,
        upper(substr(md5('auth:' || i::varchar), 1, 12)) AS authorization_code,
        ((5 + i % 250000 / 100.0) * 0.021)::real AS processing_fee, 2.1::real AS processing_fee_pct,
        (i % 100 != 0) AS fraud_check_passed, ((i * 17) % 1000 / 1000.0)::real AS fraud_score,
        'Y' AS avs_response_code, (i % 50 != 0) AS cvv_verification_passed,
        (i % 4 != 0) AS three_ds_authenticated, CASE i % 100 WHEN 0 THEN 'risk_decline' ELSE NULL END AS decline_reason,
        (i % 3)::integer AS retry_count, (25 + i % 900)::integer AS processing_time_ms
    """,
    "risk_analytics": """
        printf('EVT-HIST-%014d', i) AS "TxnLink", (i % 6)::bigint AS "RiskIndicatorCount",
        ((i * 17) % 1000 / 1000.0)::real AS "FraudProb",
        CASE WHEN i % 1000 >= 700 THEN 'high' WHEN i % 1000 >= 300 THEN 'medium' ELSE 'low' END AS "ML_Risk",
        (i % 9)::bigint AS "LinkedEvents", (1 + i % 5)::bigint AS "ChainLength",
        json_object('wallet_hash', md5('wallet:' || i::varchar), 'age_days', i % 2500) AS wallet_risk_assessment
    """,
    "RiskModelPredictions": f"""
        printf('RMP-HIST-%014d', i) AS "RMP_id",
        printf('EVT-HIST-%014d', 1 + (i - 1) % {HISTORY_REFERENCE_COUNT}) AS txn_link_ref,
        TIMESTAMP '2024-01-01' + ((1 + (i - 1) % {HISTORY_REFERENCE_COUNT}) % 31536000) * INTERVAL 1 SECOND
            + (1 + i % 300) * INTERVAL 1 SECOND AS prediction_timestamp,
        'cyber-risk-lite' AS model_name, '1.0.0' AS model_version,
        ((i * 17) % 1000 / 1000.0)::real AS fraud_probability,
        CASE WHEN i % 1000 >= 700 THEN 'high' WHEN i % 1000 >= 300 THEN 'medium' ELSE 'low' END AS risk_category_predicted,
        (0.6 + i % 400 / 1000.0)::real AS confidence_score,
        CASE i % 4 WHEN 0 THEN 'velocity' WHEN 1 THEN 'amount' WHEN 2 THEN 'device' ELSE 'behavior' END AS top_risk_factor,
        (1 + i % 8)::integer AS risk_factors_count, ((i * 3) % 1000 / 1000.0)::real AS feature_importance_velocity,
        ((i * 5) % 1000 / 1000.0)::real AS feature_importance_amount,
        ((i * 7) % 1000 / 1000.0)::real AS feature_importance_device,
        ((i * 11) % 1000 / 1000.0)::real AS feature_importance_behavior,
        CASE WHEN i % 1000 >= 700 THEN 'manual_review' ELSE 'approve' END AS recommendation_action,
        CASE i % 3 WHEN 0 THEN 'fraud' ELSE 'legitimate' END AS actual_outcome,
        (2 + i % 95)::integer AS prediction_latency_ms, (0.6 + i % 400 / 1000.0)::real AS ensemble_agreement_rate,
        (i % 1000 >= 700) AS manual_review_triggered, (i % 200 / 1000.0)::real AS model_drift_indicator
    """,
}


def existing_parts(client, settings):
    result = defaultdict(list)
    prefix = settings.bronze_prefix + "/"
    for item in list_objects(client, settings.s3_bucket, prefix):
        if not item["Key"].endswith(".parquet"):
            continue
        relative = item["Key"][len(prefix) :]
        table, _, _ = relative.partition("/")
        head = client.head_object(Bucket=settings.s3_bucket, Key=item["Key"])
        metadata = head["Metadata"]
        if "row-count" not in metadata or "row-start" not in metadata:
            raise RuntimeError(
                f"Cannot resume object without row metadata: {item['Key']}"
            )
        result[table].append(
            {
                "key": item["Key"],
                "size": item["Size"],
                "row_start": int(metadata["row-start"]),
                "row_count": int(metadata["row-count"]),
            }
        )
    return result


def write_parquet(connection, table, start, rows, output):
    select_list = SELECT_LISTS[table]
    connection.execute(f"""
        COPY (
            SELECT {select_list}
            FROM range({start}, {start + rows}) AS generated(i)
        ) TO '{output.as_posix()}' (
            FORMAT parquet,
            COMPRESSION zstd,
            COMPRESSION_LEVEL 1,
            ROW_GROUP_SIZE 100000
        )
    """)


def generate_table(
    connection, client, settings, table, target, parts, target_rows=None
):
    total = sum(part["size"] for part in parts)
    row_count = sum(part["row_count"] for part in parts)
    start = max((part["row_start"] + part["row_count"] for part in parts), default=1)
    part_number = len(parts)
    bytes_per_row = None
    with tempfile.TemporaryDirectory(prefix="kest-history-") as temp_dir:
        while row_count < target_rows if target_rows is not None else total < target:
            remaining = target - total
            rows = (
                100000
                if bytes_per_row is None
                else max(1000, int(min(FILE_TARGET_BYTES, remaining) / bytes_per_row))
            )
            if target_rows is not None:
                rows = min(rows, target_rows - row_count)
            output = Path(temp_dir) / f"part-{part_number:05d}.parquet"
            write_parquet(connection, table, start, rows, output)
            size = output.stat().st_size
            if size > remaining * 1.05 and remaining < FILE_TARGET_BYTES:
                adjusted = max(1000, int(rows * remaining / size * 0.99))
                output.unlink()
                rows = adjusted
                write_parquet(connection, table, start, rows, output)
                size = output.stat().st_size
            bytes_per_row = size / rows
            key = f"{settings.bronze_prefix}/{table}/part-{part_number:05d}.parquet"
            client.upload_file(
                str(output),
                settings.s3_bucket,
                key,
                ExtraArgs={
                    "ContentType": "application/vnd.apache.parquet",
                    "Metadata": {"row-start": str(start), "row-count": str(rows)},
                },
            )
            output.unlink()
            total += size
            row_count += rows
            start += rows
            part_number += 1
            print(f"{table}: {total / MIB:,.1f} MiB / {target / MIB:,.1f} MiB")
    return total, row_count


def main():
    parser = argparse.ArgumentParser(
        description="Create the ~5 GiB historical bronze Parquet state"
    )
    parser.parse_args()
    settings = Settings.from_env()
    client = s3_client(settings)
    parts = existing_parts(client, settings)
    unknown = set(parts) - set(TABLE_WEIGHTS)
    if unknown:
        raise RuntimeError(f"Unexpected bronze table prefixes: {sorted(unknown)}")

    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '1GB'")
    totals = {}
    row_counts = {}
    try:
        tables = list(TABLE_WEIGHTS)
        for index, table in enumerate(tables):
            if index == len(tables) - 1:
                target = settings.bronze_target_bytes - sum(totals.values())
            else:
                target = int(settings.bronze_target_bytes * TABLE_WEIGHTS[table])
            target_rows = FIXED_TABLE_ROWS.get(table)
            if table == "transaction_products":
                target_rows = 2 * row_counts["transactions"]
            totals[table], row_counts[table] = generate_table(
                connection,
                client,
                settings,
                table,
                target,
                parts.get(table, []),
                target_rows,
            )
    finally:
        connection.close()

    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "format": "parquet",
        "compression": "zstd",
        "tables": totals,
        "rows": row_counts,
        "total_parquet_bytes": sum(totals.values()),
        "requested_bytes": settings.bronze_target_bytes,
    }
    key = f"{settings.bronze_prefix}/_manifest.json"
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2, sort_keys=True).encode(),
        ContentType="application/json",
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
