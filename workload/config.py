import os
from dataclasses import dataclass


def required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    pg_host: str = required("PGHOST")
    pg_port: int = int(os.getenv("PGPORT", "5432"))
    pg_database: str = required("PGDATABASE")
    pg_user: str = required("PGUSER")
    pg_password: str = required("PGPASSWORD")
    s3_endpoint: str = required("S3_ENDPOINT_URL")
    s3_bucket: str = required("S3_BUCKET")
    aws_access_key_id: str = required("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = required("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    event_rate: int = int(os.getenv("WORKLOAD_EVENT_RATE", "20"))
    random_seed: int = int(os.getenv("WORKLOAD_RANDOM_SEED", "20260909"))
    cdc_slot: str = os.getenv("CDC_SLOT_NAME", "kest_landing")
    cdc_batch_size: int = int(os.getenv("CDC_BATCH_SIZE", "500"))
    cdc_poll_interval: float = float(os.getenv("CDC_POLL_INTERVAL_SECONDS", "1"))
    landing_prefix: str = os.getenv(
        "CDC_LANDING_PREFIX", "landing/postgres-source"
    ).strip("/")
    bronze_prefix: str = os.getenv("BRONZE_PREFIX", "bronze/history").strip("/")
    bronze_target_bytes: int = int(os.getenv("BRONZE_TARGET_BYTES", str(5 * 1024**3)))

    def pg_kwargs(self):
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }
