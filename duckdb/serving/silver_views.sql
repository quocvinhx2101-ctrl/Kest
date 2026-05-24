CREATE OR REPLACE VIEW bronze_example_events AS
SELECT *
FROM read_parquet('s3://lakehouse/bronze/example/example_events/*.parquet');

CREATE OR REPLACE VIEW silver_example_events AS
SELECT *
FROM kest_catalog.silver.example_events;
