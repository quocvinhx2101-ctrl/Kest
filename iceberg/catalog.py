import os

from pyiceberg.catalog.sql import SqlCatalog


def get_catalog() -> SqlCatalog:
    pg_user = os.environ.get("POSTGRES_USER", "kest")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "kest_password")
    pg_host = os.environ.get("POSTGRES_HOST", "postgres")
    pg_port = os.environ.get("POSTGRES_PORT", "5432")
    pg_db = os.environ.get("POSTGRES_DB", "kest")

    s3_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    s3_region = os.environ.get("DUCKDB_S3_REGION", "us-east-1")

    return SqlCatalog(
        name="kest",
        **{
            "uri": f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}",
            "warehouse": "s3://lakehouse",
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.region": s3_region,
            "s3.path-style-access": "true",
        },
    )
