import os

import boto3
from botocore.client import Config


def main() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    buckets = [item["Name"] for item in client.list_buckets().get("Buckets", [])]
    if bucket not in buckets:
        client.create_bucket(Bucket=bucket)


if __name__ == "__main__":
    main()
