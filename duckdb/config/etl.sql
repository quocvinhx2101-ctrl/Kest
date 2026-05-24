INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
INSTALL postgres;
LOAD postgres;

SET s3_region='${DUCKDB_S3_REGION}';
SET s3_endpoint='${DUCKDB_S3_ENDPOINT}';
SET s3_access_key_id='${DUCKDB_S3_ACCESS_KEY}';
SET s3_secret_access_key='${DUCKDB_S3_SECRET_KEY}';
SET s3_url_style='path';
SET s3_use_ssl=false;

CREATE CATALOG IF NOT EXISTS kest_catalog
USING 'iceberg'
WITH (
	type='jdbc',
	uri='jdbc:postgresql://postgres:5432/${POSTGRES_DB}',
	jdbc_user='${POSTGRES_USER}',
	jdbc_password='${POSTGRES_PASSWORD}',
	warehouse='s3://lakehouse'
);

USE kest_catalog;

SET memory_limit='8GB';
SET threads=4;
SET temp_directory='/tmp/duckdb';
