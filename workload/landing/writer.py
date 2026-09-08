import gzip
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone

import psycopg

from workload.core.storage import s3_client

DECODER_OPTIONS = (
    "'format-version', '2', "
    "'include-xids', 'true', "
    "'include-timestamp', 'true', "
    "'include-lsn', 'true', "
    "'include-types', 'true', "
    "'include-transaction', 'false'"
)


class LandingWriter:
    def __init__(self, settings):
        self.settings = settings
        self.s3 = s3_client(settings)
        self.connection = psycopg.connect(**settings.pg_kwargs(), autocommit=True)

    def close(self):
        self.connection.close()

    def ensure_slot(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                (self.settings.cdc_slot,),
            )
            row = cursor.fetchone()
            if row and row[0] != "wal2json":
                raise RuntimeError(
                    f"Slot {self.settings.cdc_slot} uses {row[0]}, expected wal2json"
                )
            if not row:
                cursor.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')",
                    (self.settings.cdc_slot,),
                )
                cursor.fetchone()
                print(f"Created logical replication slot {self.settings.cdc_slot}.")

    def peek(self):
        statement = f"""
            SELECT lsn::text, xid::text, data
            FROM pg_logical_slot_peek_changes(
                %s, NULL, %s, {DECODER_OPTIONS}
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                statement, (self.settings.cdc_slot, self.settings.cdc_batch_size)
            )
            return cursor.fetchall()

    def acknowledge(self, expected_rows):
        statement = f"""
            SELECT lsn::text, xid::text, data
            FROM pg_logical_slot_get_changes(
                %s, NULL, %s, {DECODER_OPTIONS}
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(statement, (self.settings.cdc_slot, len(expected_rows)))
            acknowledged = cursor.fetchall()
        if acknowledged != expected_rows:
            raise RuntimeError("WAL changes acknowledged do not match the landed batch")
        return len(acknowledged)

    def land_batch(self, rows):
        captured_at = datetime.now(timezone.utc)
        groups = defaultdict(list)
        for lsn, xid, raw_data in rows:
            change = json.loads(raw_data)
            table = change.get("table", "_metadata")
            event_id = hashlib.sha256(
                f"{self.settings.pg_database}|{lsn}|{xid}|{raw_data}".encode()
            ).hexdigest()
            groups[table].append(
                {
                    "schema_version": 1,
                    "event_id": event_id,
                    "ingested_at": captured_at.isoformat(),
                    "source": {
                        "connector": "postgresql-wal2json",
                        "service": "postgres-source",
                        "database": self.settings.pg_database,
                        "lsn": lsn,
                        "xid": xid,
                    },
                    "change": change,
                }
            )

        first_lsn = rows[0][0].replace("/", "-")
        last_lsn = rows[-1][0].replace("/", "-")
        date = captured_at.strftime("%Y-%m-%d")
        hour = captured_at.strftime("%H")
        for table, events in groups.items():
            key = (
                f"{self.settings.landing_prefix}/{table}/"
                f"ingest_date={date}/hour={hour}/"
                f"batch-{first_lsn}-{last_lsn}.jsonl.gz"
            )
            payload = gzip.compress(
                b"".join(
                    json.dumps(event, separators=(",", ":"), sort_keys=True).encode()
                    + b"\n"
                    for event in events
                ),
                mtime=0,
            )
            self.s3.put_object(
                Bucket=self.settings.s3_bucket,
                Key=key,
                Body=payload,
                ContentType="application/x-ndjson",
                ContentEncoding="gzip",
                Metadata={
                    "first-lsn": rows[0][0],
                    "last-lsn": rows[-1][0],
                    "event-count": str(len(events)),
                },
            )
            print(
                f"Landed {len(events):4d} events to s3://{self.settings.s3_bucket}/{key}"
            )

        acknowledged = self.acknowledge(rows)
        print(f"Acknowledged {acknowledged} WAL changes through LSN {rows[-1][0]}.")
        return sum(len(events) for events in groups.values())
