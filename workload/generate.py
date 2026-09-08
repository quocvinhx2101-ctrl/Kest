import argparse
import json
import random
import signal
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.types.json import Jsonb

from workload.config import Settings

EVENT_FRAME = (
    "purchase",
    "buyer_session",
    "payment_update",
    "buyer_session",
    "risk_prediction",
    "purchase",
    "buyer_session",
    "transaction_status_update",
    "buyer_session",
    "purchase",
    "payment_update",
    "buyer_session",
    "purchase",
    "buyer_session",
    "risk_prediction",
    "buyer_session",
    "purchase",
    "payment_update",
    "buyer_session",
    "purchase",
)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Generator:
    def __init__(self, settings):
        if settings.event_rate != 20:
            raise ValueError("CyberMarket's fixed workload rate must be 20 events/sec")
        self.settings = settings
        self.random = random.Random(settings.random_seed)
        self.connection = psycopg.connect(**settings.pg_kwargs(), autocommit=True)
        self.recent = deque(maxlen=5000)
        with self.connection.cursor() as cursor:
            cursor.execute("""
                SELECT t."EventCode", p."PPE_id"
                FROM transactions t
                JOIN "PaymentProcessingEvents" p ON p.transaction_ref = t."EventCode"
                ORDER BY t."EventTimestamp" DESC LIMIT 1000
            """)
            self.recent.extend(cursor.fetchall())

    def close(self):
        self.connection.close()

    def skewed_id(self, prefix, size, width):
        index = 1 + int((self.random.random() ** 3) * size)
        return f"{prefix}-{index:0{width}d}"

    def buyer_session(self):
        started = utc_now()
        duration = self.random.randint(8, 1800)
        pages = self.random.randint(1, 35)
        products = min(pages, self.random.randint(0, 14))
        additions = min(products, self.random.randint(0, 4))
        checkout = additions > 0 and self.random.random() < 0.55
        completed = checkout and self.random.random() < 0.68
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO "BuyerSessionAnalytics" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    f"BSA-{uuid.uuid4()}",
                    self.skewed_id("BUYER", 10000, 7),
                    started,
                    duration,
                    pages,
                    products,
                    additions,
                    max(0, additions - (1 if completed else 0)),
                    self.random.randint(0, 8),
                    checkout,
                    completed,
                    pages == 1,
                    self.random.choice(("direct", "search", "affiliate", "social")),
                    self.random.choice(("mobile", "desktop", "tablet")),
                    self.random.choice(("NA", "EU", "APAC", "LATAM")),
                    duration / pages,
                    products / pages,
                    self.random.uniform(10, 100),
                    self.random.randint(0, 2),
                    round(products * self.random.uniform(8, 120), 2),
                ),
            )

    def purchase(self):
        now = utc_now()
        event_id = f"EVT-{uuid.uuid4()}"
        payment_id = f"PPE-{uuid.uuid4()}"
        vendor = self.skewed_id("SELLER", 1000, 6)
        buyer = self.skewed_id("BUYER", 10000, 7)
        platform = f"PLAT-{self.random.randint(1, 10):03d}"
        origin = self.random.choice(("NA", "EU", "APAC", "LATAM"))
        destination = self.random.choice(("NA", "EU", "APAC", "LATAM"))
        amount = round(self.random.lognormvariate(4.2, 0.9), 2)
        fraud_probability = min(0.99, self.random.betavariate(1.2, 8))
        financials = {
            "amount": amount,
            "currency": "USD",
            "status": "authorized",
            "updated_at": now.isoformat(timespec="milliseconds") + "Z",
        }
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT "ProdCat", "Subcategory", "ListingAge", "SellerPointer"
                FROM products WHERE "SellerPointer" = %s
                ORDER BY "ListingAge" LIMIT 2
            """,
                (vendor,),
            )
            products = cursor.fetchall()
            if len(products) != 2:
                raise RuntimeError(f"Seed products missing for {vendor}")
            cursor.execute(
                """
                INSERT INTO transactions VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    event_id,
                    "purchase",
                    now,
                    platform,
                    vendor,
                    buyer,
                    origin,
                    destination,
                    int(origin != destination),
                    "multi-hop" if origin != destination else "direct",
                    self.random.choice(("normal", "elevated", "burst")),
                    "cross-border" if origin != destination else "domestic",
                    str(round(self.random.uniform(0, 100), 3)),
                    Jsonb(financials),
                ),
            )
            for line, product in enumerate(products, start=1):
                cursor.execute(
                    """
                    INSERT INTO transaction_products VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (event_id, *product, amount * (0.45 if line == 1 else 0.55), 1),
                )
            cursor.execute(
                """
                INSERT INTO "PaymentProcessingEvents" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    payment_id,
                    event_id,
                    now + timedelta(milliseconds=10),
                    self.random.choice(("card", "wallet", "bank_transfer")),
                    "authorized",
                    amount,
                    amount,
                    "USD",
                    self.random.choice(("nova-pay", "orbit-pay")),
                    uuid.uuid4().hex[:12].upper(),
                    round(amount * 0.021, 2),
                    2.1,
                    fraud_probability < 0.8,
                    fraud_probability,
                    "Y",
                    True,
                    self.random.random() < 0.75,
                    None,
                    0,
                    self.random.randint(25, 900),
                ),
            )
            cursor.execute(
                """
                INSERT INTO risk_analytics VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    event_id,
                    self.random.randint(0, 5),
                    fraud_probability,
                    "high"
                    if fraud_probability >= 0.7
                    else "medium"
                    if fraud_probability >= 0.3
                    else "low",
                    self.random.randint(0, 8),
                    self.random.randint(1, 5),
                    Jsonb(
                        {
                            "wallet_age_days": self.random.randint(1, 2500),
                            "score": fraud_probability,
                        }
                    ),
                ),
            )
            cursor.execute(
                """
                UPDATE buyers SET
                    "PurchaseCount" = "PurchaseCount" + 1,
                    buyer_risk_profile = buyer_risk_profile || %s
                WHERE "AcqCode" = %s
            """,
                (Jsonb({"last_purchase_at": now.isoformat() + "Z"}), buyer),
            )
            cursor.execute(
                """
                UPDATE vendors SET
                    "TotalTxns" = (COALESCE(NULLIF("TotalTxns", ''), '0')::bigint + 1)::text,
                    "CompletedTxns" = "CompletedTxns" + 1,
                    "LastActiveDt" = %s
                WHERE "SellerKey" = %s
            """,
                (now, vendor),
            )
        self.recent.append((event_id, payment_id))

    def choose_recent(self):
        if not self.recent:
            self.purchase()
        return self.random.choice(tuple(self.recent))

    def payment_update(self):
        _, payment_id = self.choose_recent()
        now = utc_now()
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE "PaymentProcessingEvents" SET
                    event_timestamp = %s,
                    processing_stage = 'settled',
                    processing_time_ms = processing_time_ms + %s,
                    retry_count = retry_count + 1
                WHERE "PPE_id" = %s
            """,
                (now, self.random.randint(5, 120), payment_id),
            )

    def risk_prediction(self):
        event_id, _ = self.choose_recent()
        probability = min(0.99, self.random.betavariate(1.4, 7))
        category = (
            "high" if probability >= 0.7 else "medium" if probability >= 0.3 else "low"
        )
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO "RiskModelPredictions" VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    f"RMP-{uuid.uuid4()}",
                    event_id,
                    utc_now(),
                    "cyber-risk-lite",
                    "1.0.0",
                    probability,
                    category,
                    self.random.uniform(0.65, 0.99),
                    self.random.choice(("velocity", "amount", "device", "behavior")),
                    self.random.randint(1, 8),
                    self.random.random(),
                    self.random.random(),
                    self.random.random(),
                    self.random.random(),
                    "manual_review" if category == "high" else "approve",
                    None,
                    self.random.randint(2, 95),
                    self.random.uniform(0.6, 1),
                    category == "high",
                    self.random.uniform(0, 0.2),
                ),
            )

    def transaction_status_update(self):
        event_id, _ = self.choose_recent()
        update = {"status": "completed", "updated_at": utc_now().isoformat() + "Z"}
        with self.connection.transaction(), self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE transactions SET transaction_financials = transaction_financials || %s
                WHERE "EventCode" = %s
            """,
                (Jsonb(update), event_id),
            )

    def emit(self, event):
        getattr(self, event)()


def run(duration=None, ready_event=None):
    settings = Settings()
    generator = Generator(settings)
    counts = Counter()
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    deadline = started + duration if duration else None
    sequence = 0
    if ready_event:
        ready_event.set()
    try:
        while not stopping and (deadline is None or time.monotonic() < deadline):
            event = EVENT_FRAME[sequence % len(EVENT_FRAME)]
            generator.emit(event)
            counts[event] += 1
            sequence += 1
            target = started + sequence / settings.event_rate
            time.sleep(max(0, target - time.monotonic()))
    finally:
        generator.close()
    print(json.dumps(dict(sorted(counts.items())), sort_keys=True))
    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Run the fixed 20 events/sec CyberMarket workload"
    )
    parser.add_argument(
        "--duration", type=float, help="Seconds to run; default runs until stopped"
    )
    args = parser.parse_args()
    run(args.duration)


if __name__ == "__main__":
    main()
