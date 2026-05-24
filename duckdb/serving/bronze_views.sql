CREATE OR REPLACE VIEW bronze_example_events AS
SELECT *
FROM read_parquet('s3://lakehouse/bronze/example/example_events/*.parquet');
