import psycopg

from workload.cybermarket.ids import (
    BUYER_COUNT,
    LIVE_ID_PATTERNS,
    MARKET_COUNT,
    PRODUCT_COUNT,
    VENDOR_COUNT,
    buyer_id,
    market_id,
    product_key,
    vendor_id,
)
from workload.cybermarket.schema import EXPECTED_COLUMNS, EXPECTED_TYPES


def _values(cursor, table, columns):
    selection = ", ".join(f'"{column}"' for column in columns)
    cursor.execute(f'SELECT {selection} FROM "{table}"')
    return set(cursor.fetchall())


def _check_schema(cursor):
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    tables = {row[0] for row in cursor.fetchall()}
    if tables != set(EXPECTED_COLUMNS):
        raise AssertionError(f"Unexpected PostgreSQL tables: {sorted(tables)}")

    for table, expected_columns in EXPECTED_COLUMNS.items():
        cursor.execute(
            """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        actual = cursor.fetchall()
        expected = list(zip(expected_columns, EXPECTED_TYPES[table], strict=True))
        if actual != expected:
            raise AssertionError(f"{table} schema differs: {actual}")
        cursor.execute(
            "SELECT relreplident FROM pg_class WHERE oid = %s::regclass",
            (f'public."{table}"',),
        )
        if cursor.fetchone()[0] != "f":
            raise AssertionError(f"{table} does not use REPLICA IDENTITY FULL")


def _check_entity_universe(cursor):
    expected = {
        "markets": {(market_id(i),) for i in range(1, MARKET_COUNT + 1)},
        "vendors": {(vendor_id(i),) for i in range(1, VENDOR_COUNT + 1)},
        "buyers": {(buyer_id(i),) for i in range(1, BUYER_COUNT + 1)},
        "products": {product_key(i) for i in range(1, PRODUCT_COUNT + 1)},
    }
    selections = {
        "markets": ("PlatCode",),
        "vendors": ("SellerKey",),
        "buyers": ("AcqCode",),
        "products": ("ProdCat", "Subcategory", "ListingAge", "SellerPointer"),
    }
    for table, columns in selections.items():
        actual = _values(cursor, table, columns)
        if actual != expected[table]:
            raise AssertionError(
                f"{table} does not match the canonical entity universe"
            )


def _check_current_facts(cursor):
    identities = {
        "transactions": ("EventCode", "EVT"),
        "BuyerSessionAnalytics": ("BSA_id", "BSA"),
        "PaymentProcessingEvents": ("PPE_id", "PPE"),
        "RiskModelPredictions": ("RMP_id", "RMP"),
    }
    for table, (column, prefix) in identities.items():
        cursor.execute(f'SELECT "{column}" FROM "{table}"')
        invalid = [
            value
            for (value,) in cursor.fetchall()
            if not LIVE_ID_PATTERNS[prefix].fullmatch(value)
        ]
        if invalid:
            raise AssertionError(f"{table} contains non-canonical current IDs")

    assertions = {
        "transaction product vendor differs from transaction vendor": """
            SELECT count(*) FROM transaction_products item
            JOIN transactions txn ON txn."EventCode" = item."EventLink"
            WHERE item."SellerPointer" != txn."VendorLink"
        """,
        "transaction has other than two product rows": """
            SELECT count(*) FROM (
                SELECT txn."EventCode" FROM transactions txn
                LEFT JOIN transaction_products item ON item."EventLink" = txn."EventCode"
                GROUP BY txn."EventCode" HAVING count(item.*) != 2
            ) invalid
        """,
        "payment predates transaction": """
            SELECT count(*) FROM "PaymentProcessingEvents" payment
            JOIN transactions txn ON txn."EventCode" = payment.transaction_ref
            WHERE payment.event_timestamp < txn."EventTimestamp"
        """,
        "prediction predates transaction": """
            SELECT count(*) FROM "RiskModelPredictions" prediction
            JOIN transactions txn ON txn."EventCode" = prediction.txn_link_ref
            WHERE prediction.prediction_timestamp < txn."EventTimestamp"
        """,
    }
    for message, query in assertions.items():
        cursor.execute(query)
        if cursor.fetchone()[0]:
            raise AssertionError(f"PostgreSQL {message}")


def _check_slot(cursor, slot_name, require_slot):
    cursor.execute(
        """
        SELECT slot_name, plugin, active, wal_status
        FROM pg_replication_slots WHERE slot_name = %s
        """,
        (slot_name,),
    )
    slot = cursor.fetchone()
    if require_slot and slot is None:
        raise AssertionError("CDC phase requires a replication slot")
    if slot is not None and (slot[1:] != ("wal2json", False, "reserved")):
        raise AssertionError(f"Unexpected CDC slot state: {slot}")


def check_postgres(settings, require_slot=False):
    with (
        psycopg.connect(**settings.pg_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        _check_schema(cursor)
        _check_entity_universe(cursor)
        _check_current_facts(cursor)
        _check_slot(cursor, settings.cdc_slot, require_slot)
        cursor.execute("SHOW max_slot_wal_keep_size")
        if cursor.fetchone()[0] != "1GB":
            raise AssertionError("max_slot_wal_keep_size is not 1GB")
