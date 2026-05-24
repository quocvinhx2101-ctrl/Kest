INSTALL httpfs;
LOAD httpfs;

SET s3_region='${DUCKDB_S3_REGION}';
SET s3_endpoint='${DUCKDB_S3_ENDPOINT}';
SET s3_access_key_id='${DUCKDB_S3_ACCESS_KEY}';
SET s3_secret_access_key='${DUCKDB_S3_SECRET_KEY}';
SET s3_url_style='path';
SET s3_use_ssl=false;

SET memory_limit='8GB';
SET threads=4;
SET temp_directory='/tmp/duckdb';