CREATE OR REPLACE VIEW bronze_example_events AS
SELECT *
FROM read_parquet('s3://lakehouse/bronze/example/example_events/*.parquet');

CREATE OR REPLACE VIEW silver_example_events AS
SELECT *
FROM iceberg_scan('s3://lakehouse/silver/domain=example/entity=events');
