import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "iceberg"))
sys.path.insert(0, "/opt/kest/iceberg")

import duckdb

from catalog import get_catalog
from tables import ensure_tables


def _duckdb_s3_config(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(
        f"""
        SET s3_region='{os.environ.get("DUCKDB_S3_REGION", "us-east-1")}';
        SET s3_endpoint='{os.environ.get("DUCKDB_S3_ENDPOINT", "minio:9000")}';
        SET s3_access_key_id='{os.environ.get("DUCKDB_S3_ACCESS_KEY", "minioadmin")}';
        SET s3_secret_access_key='{os.environ.get("DUCKDB_S3_SECRET_KEY", "minioadmin")}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
        """
    )


def run_silver() -> None:
    catalog = get_catalog()
    ensure_tables(catalog)
    table = catalog.load_table("silver.example_events")

    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    bronze_path = f"s3://{bucket}/bronze/example/example_events/*.parquet"

    conn = duckdb.connect()
    _duckdb_s3_config(conn)

    arrow_table = conn.execute(
        f"""
        SELECT
            json_extract_string(_raw_payload, '$.id') AS source_id,
            md5(concat(
                json_extract_string(_raw_payload, '$.id'), '-',
                json_extract_string(_raw_payload, '$.event_time'), '-',
                json_extract_string(_raw_payload, '$.value')
            )) AS record_hash,
            _batch_id AS batch_id,
            cast(json_extract_string(_raw_payload, '$.event_time') AS TIMESTAMP) AS event_time,
            cast(_ingested_at AS TIMESTAMP) AS ingested_at,
            cast(_ingested_at AS TIMESTAMP) AS processed_at,
            cast(json_extract_string(_raw_payload, '$.value') AS INTEGER) AS value
        FROM read_parquet('{bronze_path}')
        """
    ).arrow()

    conn.close()

    table.overwrite(arrow_table)
    print(f"Silver: wrote {arrow_table.num_rows} rows via PyIceberg")


if __name__ == "__main__":
    run_silver()
