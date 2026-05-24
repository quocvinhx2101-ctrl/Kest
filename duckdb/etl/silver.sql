USE kest_catalog;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.example_events
USING iceberg
LOCATION 's3://lakehouse/silver/domain=example/entity=events/';

INSERT OVERWRITE silver.example_events
SELECT
	json_extract_string(_raw_payload, '$.id') AS source_id,
	md5(
		concat(
			json_extract_string(_raw_payload, '$.id'),
			'-',
			json_extract_string(_raw_payload, '$.event_time'),
			'-',
			json_extract_string(_raw_payload, '$.value')
		)
	) AS record_hash,
	_batch_id AS batch_id,
	cast(json_extract_string(_raw_payload, '$.event_time') AS TIMESTAMP) AS event_time,
	cast(_ingested_at AS TIMESTAMP) AS ingested_at,
	cast(_ingested_at AS TIMESTAMP) AS processed_at,
	cast(json_extract_string(_raw_payload, '$.value') AS INTEGER) AS value
FROM read_parquet('s3://lakehouse/bronze/example/example_events/*.parquet');
