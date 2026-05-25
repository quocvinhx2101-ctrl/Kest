from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import (
    DateType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

SILVER_EVENTS_SCHEMA = Schema(
    NestedField(1, "source_id", StringType(), required=True),
    NestedField(2, "record_hash", StringType(), required=True),
    NestedField(3, "batch_id", StringType(), required=False),
    NestedField(4, "event_time", TimestampType(), required=True),
    NestedField(5, "ingested_at", TimestampType(), required=True),
    NestedField(6, "processed_at", TimestampType(), required=True),
    NestedField(7, "value", IntegerType(), required=False),
)

SILVER_EVENTS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=4, field_id=1000, transform=DayTransform(), name="event_day",
    ),
)

GOLD_METRICS_SCHEMA = Schema(
    NestedField(1, "event_date", DateType(), required=True),
    NestedField(2, "event_count", LongType(), required=True),
    NestedField(3, "ingested_at", TimestampType(), required=False),
    NestedField(4, "processed_at", TimestampType(), required=False),
)

GOLD_METRICS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=MonthTransform(), name="event_month",
    ),
)


def ensure_tables(catalog: SqlCatalog) -> None:
    existing_ns = [ns[0] for ns in catalog.list_namespaces()]

    if "silver" not in existing_ns:
        catalog.create_namespace("silver")
    if ("silver", "example_events") not in catalog.list_tables("silver"):
        catalog.create_table(
            identifier="silver.example_events",
            schema=SILVER_EVENTS_SCHEMA,
            partition_spec=SILVER_EVENTS_PARTITION,
            location="s3://lakehouse/silver/domain=example/entity=events",
        )

    if "gold" not in existing_ns:
        catalog.create_namespace("gold")
    if ("gold", "daily_metrics") not in catalog.list_tables("gold"):
        catalog.create_table(
            identifier="gold.daily_metrics",
            schema=GOLD_METRICS_SCHEMA,
            partition_spec=GOLD_METRICS_PARTITION,
            location="s3://lakehouse/gold/domain=example/entity=daily_metrics",
        )
