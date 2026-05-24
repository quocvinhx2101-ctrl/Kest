USE kest_catalog;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.daily_metrics
USING iceberg
LOCATION 's3://lakehouse/gold/domain=example/entity=daily_metrics/';

INSERT OVERWRITE gold.daily_metrics
SELECT
	date_trunc('day', event_time) AS event_date,
	count(*) AS event_count,
	min(ingested_at) AS ingested_at,
	max(processed_at) AS processed_at
FROM silver.example_events
GROUP BY 1;
