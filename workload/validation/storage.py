import gzip
import io
import json
import re
from datetime import datetime, timedelta

from pyarrow import parquet

from workload.core.storage import list_objects, s3_client
from workload.cybermarket.ids import (
    BUYER_COUNT,
    BUYER_PATTERN,
    HISTORY_REFERENCE_COUNT,
    MARKET_COUNT,
    MARKET_PATTERN,
    PRODUCT_COUNT,
    PRODUCTS_PER_VENDOR,
    VENDOR_COUNT,
    VENDOR_PATTERN,
    buyer_id,
    market_id,
    product_key,
    vendor_id,
)
from workload.cybermarket.schema import EXPECTED_COLUMNS, TABLES

HISTORY_STARTED_AT = datetime(2024, 1, 1)
SAMPLE_SIZE = 256


def check_future_layers_empty(settings):
    client = s3_client(settings)
    for prefix in ("silver/", "gold/"):
        if list(list_objects(client, settings.s3_bucket, prefix)):
            raise AssertionError(f"{prefix} must remain empty")


def _history_index(value, prefix):
    match = re.fullmatch(rf"{prefix}-HIST-(\d{{14}})", value)
    if not match:
        raise AssertionError(f"Invalid historical identifier: {value}")
    return int(match.group(1))


def _read_parquet(client, settings, item, full_row_group=False):
    body = client.get_object(Bucket=settings.s3_bucket, Key=item["Key"])["Body"].read()
    file = parquet.ParquetFile(io.BytesIO(body))
    rows = file.read_row_group(0)
    if not full_row_group:
        rows = rows.slice(0, SAMPLE_SIZE)
    return file.schema_arrow.names, rows.to_pylist()


def _check_dimension_samples(samples):
    expected = {
        "markets": {market_id(i) for i in range(1, MARKET_COUNT + 1)},
        "vendors": {vendor_id(i) for i in range(1, VENDOR_COUNT + 1)},
        "buyers": {buyer_id(i) for i in range(1, BUYER_COUNT + 1)},
    }
    keys = {"markets": "PlatCode", "vendors": "SellerKey", "buyers": "AcqCode"}
    patterns = {
        "markets": MARKET_PATTERN,
        "vendors": VENDOR_PATTERN,
        "buyers": BUYER_PATTERN,
    }
    for table, key in keys.items():
        values = {row[key] for row in samples[table]}
        if not all(patterns[table].fullmatch(value) for value in values):
            raise AssertionError(f"Historical {table} uses a non-canonical ID format")
        if values != expected[table]:
            raise AssertionError(
                f"Historical {table} differs from its canonical ID domain"
            )

    products = {product_key(i) for i in range(1, PRODUCT_COUNT + 1)}
    product_keys = set()
    for row in samples["products"]:
        key = (
            row["ProdCat"],
            row["Subcategory"],
            row["ListingAge"],
            row["SellerPointer"],
        )
        if key not in products:
            raise AssertionError(
                "Historical products use a non-canonical composite key"
            )
        product_keys.add(key)
    if product_keys != products:
        raise AssertionError(
            "Historical products differ from the canonical product domain"
        )


def _check_fact_samples(samples, manifest):
    platform_ids = {market_id(i) for i in range(1, MARKET_COUNT + 1)}
    vendor_ids = {vendor_id(i) for i in range(1, VENDOR_COUNT + 1)}
    buyer_ids = {buyer_id(i) for i in range(1, BUYER_COUNT + 1)}
    product_ids = {product_key(i) for i in range(1, PRODUCT_COUNT + 1)}
    items_by_event = {}

    for row in samples["transactions"]:
        event_index = _history_index(row["EventCode"], "EVT")
        if not 1 <= event_index <= manifest["rows"]["transactions"]:
            raise AssertionError(
                "Historical transaction ID is outside the manifest domain"
            )
        if row["PlatformKey"] not in platform_ids:
            raise AssertionError("Historical transaction references an unknown market")
        if row["VendorLink"] not in vendor_ids:
            raise AssertionError("Historical transaction references an unknown vendor")
        if row["AcqLink"] not in buyer_ids:
            raise AssertionError("Historical transaction references an unknown buyer")
        crosses_border = row["OriginRegion"] != row["DestRegion"]
        if row["CrossBorder"] != int(crosses_border):
            raise AssertionError(
                "Historical transaction has a contradictory border flag"
            )
        expected_route = "multi-hop" if crosses_border else "direct"
        if row["RouteComplex"] != expected_route:
            raise AssertionError("Historical transaction has a contradictory route")
        expected_border = "cross-border" if crosses_border else "domestic"
        if row["Border_cross_border_pre"] != expected_border:
            raise AssertionError(
                "Historical transaction has a contradictory border label"
            )

    for row in samples["transaction_products"]:
        event_index = _history_index(row["EventLink"], "EVT")
        if not 1 <= event_index <= manifest["rows"]["transactions"]:
            raise AssertionError("Historical item references an unknown transaction")
        key = (
            row["ProdCat"],
            row["Subcategory"],
            row["ListingAge"],
            row["SellerPointer"],
        )
        if key not in product_ids:
            raise AssertionError("Historical item references an unknown product")
        vendor_index = 1 + (event_index - 1) % VENDOR_COUNT
        expected_vendor = vendor_id(vendor_index)
        if row["SellerPointer"] != expected_vendor:
            raise AssertionError("Historical item and transaction vendors differ")
        expected_products = {
            product_key((vendor_index - 1) * PRODUCTS_PER_VENDOR + line)
            for line in (1, 2)
        }
        if key not in expected_products:
            raise AssertionError(
                "Historical transaction uses an unexpected vendor product"
            )
        items_by_event.setdefault(row["EventLink"], set()).add(key)
        expected_amount = 5 + event_index % 250000 / 100.0
        first_product = product_key((vendor_index - 1) * PRODUCTS_PER_VENDOR + 1)
        expected_price = expected_amount * (0.45 if key == first_product else 0.55)
        if row["QtySold"] != 1 or abs(row["PriceAmt"] - expected_price) > 0.01:
            raise AssertionError("Historical item amount contradicts its transaction")

    for event, keys in items_by_event.items():
        event_index = _history_index(event, "EVT")
        vendor_index = 1 + (event_index - 1) % VENDOR_COUNT
        expected = {
            product_key((vendor_index - 1) * PRODUCTS_PER_VENDOR + line)
            for line in (1, 2)
        }
        if keys != expected:
            raise AssertionError("Historical transaction does not contain two products")

    for row in samples["BuyerSessionAnalytics"]:
        _history_index(row["BSA_id"], "BSA")
        if row["acq_ref"] not in buyer_ids:
            raise AssertionError("Historical session references an unknown buyer")

    for row in samples["PaymentProcessingEvents"]:
        _history_index(row["PPE_id"], "PPE")
        transaction_index = _history_index(row["transaction_ref"], "EVT")
        if not 1 <= transaction_index <= manifest["rows"]["transactions"]:
            raise AssertionError("Historical payment references an unknown transaction")
        transaction_time = HISTORY_STARTED_AT + timedelta(
            seconds=transaction_index % 31_536_000
        )
        if row["event_timestamp"] < transaction_time:
            raise AssertionError("Historical payment predates its transaction")

    for row in samples["risk_analytics"]:
        transaction_index = _history_index(row["TxnLink"], "EVT")
        if not 1 <= transaction_index <= manifest["rows"]["transactions"]:
            raise AssertionError(
                "Historical risk row references an unknown transaction"
            )

    for row in samples["RiskModelPredictions"]:
        _history_index(row["RMP_id"], "RMP")
        transaction_index = _history_index(row["txn_link_ref"], "EVT")
        if not 1 <= transaction_index <= manifest["rows"]["risk_analytics"]:
            raise AssertionError("Historical prediction references an unknown risk row")
        transaction_time = HISTORY_STARTED_AT + timedelta(
            seconds=transaction_index % 31_536_000
        )
        if row["prediction_timestamp"] < transaction_time:
            raise AssertionError("Historical prediction predates its transaction")


def check_history(settings):
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
    for item in sorted(parquet_objects, key=lambda value: value["Key"]):
        table = item["Key"][len(settings.bronze_prefix) + 1 :].split("/", 1)[0]
        by_table.setdefault(table, item)
    if set(by_table) != TABLES:
        raise AssertionError(f"Bronze tables differ: {sorted(by_table)}")

    samples = {}
    for table, item in by_table.items():
        columns, samples[table] = _read_parquet(
            client,
            settings,
            item,
            full_row_group=table in {"markets", "vendors", "buyers", "products"},
        )
        if columns != EXPECTED_COLUMNS[table]:
            raise AssertionError(f"Bronze {table} columns differ: {columns}")

    manifest_key = f"{settings.bronze_prefix}/_manifest.json"
    manifest = json.loads(
        client.get_object(Bucket=settings.s3_bucket, Key=manifest_key)["Body"].read()
    )
    if manifest["total_parquet_bytes"] != total:
        raise AssertionError("Bronze manifest byte count differs from stored Parquet")
    rows = manifest["rows"]
    if set(rows) != TABLES:
        raise AssertionError(f"Bronze manifest tables differ: {sorted(rows)}")
    expected_dimensions = {
        "markets": MARKET_COUNT,
        "vendors": VENDOR_COUNT,
        "buyers": BUYER_COUNT,
        "products": PRODUCT_COUNT,
    }
    for table, expected_count in expected_dimensions.items():
        if rows[table] != expected_count:
            raise AssertionError(
                f"Historical {table} has {rows[table]} rows; expected {expected_count}"
            )
    if rows["transaction_products"] != 2 * rows["transactions"]:
        raise AssertionError(
            "Bronze history must contain exactly two items per transaction"
        )
    if rows["transactions"] < HISTORY_REFERENCE_COUNT:
        raise AssertionError("Historical transaction reference domain is incomplete")
    if rows["risk_analytics"] < HISTORY_REFERENCE_COUNT:
        raise AssertionError("Historical risk reference domain is incomplete")
    if rows["risk_analytics"] > rows["transactions"]:
        raise AssertionError("Historical risk rows exceed the transaction domain")

    _check_dimension_samples(samples)
    _check_fact_samples(samples, manifest)
    return total, len(parquet_objects)


def check_cdc(settings):
    client = s3_client(settings)
    prefix = settings.landing_prefix + "/"
    objects = [
        item
        for item in list_objects(client, settings.s3_bucket, prefix)
        if item["Key"].endswith(".jsonl.gz")
    ]
    if not objects:
        raise AssertionError("CDC phase requires raw landing objects")

    for item in objects:
        relative = item["Key"][len(prefix) :]
        table = relative.split("/", 1)[0]
        if table not in TABLES:
            raise AssertionError(f"Unexpected landing table: {table}")
        body = client.get_object(Bucket=settings.s3_bucket, Key=item["Key"])[
            "Body"
        ].read()
        lines = gzip.decompress(body).splitlines()
        if not lines:
            raise AssertionError(f"Empty raw landing object: {item['Key']}")
        event = json.loads(lines[0])
        if set(event) != {
            "schema_version",
            "event_id",
            "ingested_at",
            "source",
            "change",
        }:
            raise AssertionError(f"Unexpected raw envelope: {event.keys()}")
        if not re.fullmatch(r"[0-9a-f]{64}", event["event_id"]):
            raise AssertionError("Raw event_id is not a stable SHA-256 identifier")
        if event["change"].get("table") != table:
            raise AssertionError("Raw landing path and change table differ")
        source = event["source"]
        if not source.get("lsn") or not source.get("xid"):
            raise AssertionError("Raw source metadata is missing LSN or XID")
    return len(objects)
