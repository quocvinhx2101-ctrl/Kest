import gzip
import json
from collections import Counter

import psycopg

from workload.cdc import LandingWriter
from workload.cdc import run as run_cdc
from workload.config import Settings
from workload.generate import run as run_generator
from workload.storage import list_objects, s3_client

TABLES = {
    "markets",
    "vendors",
    "buyers",
    "products",
    "transactions",
    "transaction_products",
    "BuyerSessionAnalytics",
    "PaymentProcessingEvents",
    "risk_analytics",
    "RiskModelPredictions",
}


def row_counts(connection):
    with connection.cursor() as cursor:
        result = {}
        for table in TABLES:
            cursor.execute(f'SELECT count(*) FROM "{table}"')
            result[table] = cursor.fetchone()[0]
        return result


def main():
    settings = Settings()
    client = s3_client(settings)
    prefix = settings.landing_prefix + "/"
    before_keys = {
        item["Key"] for item in list_objects(client, settings.s3_bucket, prefix)
    }

    writer = LandingWriter(settings)
    try:
        writer.ensure_slot()
    finally:
        writer.close()

    with psycopg.connect(**settings.pg_kwargs()) as connection:
        before_rows = row_counts(connection)

    counts = run_generator(duration=1.0)
    expected_events = Counter(
        {
            "buyer_session": 8,
            "purchase": 6,
            "payment_update": 3,
            "risk_prediction": 2,
            "transaction_status_update": 1,
        }
    )
    if counts != expected_events:
        raise AssertionError(f"Unexpected one-second event mix: {counts}")

    landed = run_cdc(follow=False)
    if landed != 56:
        raise AssertionError(f"Expected 56 row changes, landed {landed}")

    with psycopg.connect(**settings.pg_kwargs()) as connection:
        after_rows = row_counts(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (settings.cdc_slot,),
            )
            slot = cursor.fetchone()
            if slot != (False,):
                raise AssertionError(
                    f"CDC slot should be inactive after the test: {slot}"
                )

    expected_deltas = {
        "BuyerSessionAnalytics": 8,
        "transactions": 6,
        "transaction_products": 12,
        "PaymentProcessingEvents": 6,
        "risk_analytics": 6,
        "RiskModelPredictions": 2,
    }
    for table, delta in expected_deltas.items():
        actual = after_rows[table] - before_rows[table]
        if actual != delta:
            raise AssertionError(f"{table}: expected +{delta}, got +{actual}")

    new_objects = [
        item
        for item in list_objects(client, settings.s3_bucket, prefix)
        if item["Key"] not in before_keys
    ]
    if not new_objects:
        raise AssertionError("CDC produced no new landing objects")
    raw_events = []
    for item in new_objects:
        body = client.get_object(Bucket=settings.s3_bucket, Key=item["Key"])[
            "Body"
        ].read()
        raw_events.extend(
            json.loads(line) for line in gzip.decompress(body).splitlines()
        )
    if len(raw_events) != 56:
        raise AssertionError(f"Expected 56 raw events, read {len(raw_events)}")
    if len({event["event_id"] for event in raw_events}) != 56:
        raise AssertionError("Raw CDC event IDs are not unique")
    if any(event["change"].get("table") not in TABLES for event in raw_events):
        raise AssertionError("Landing contains a change outside the selected 10 tables")

    print(
        json.dumps(
            {
                "business_events": sum(counts.values()),
                "landing_objects": len(new_objects),
                "raw_changes": len(raw_events),
                "slot_active": False,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
