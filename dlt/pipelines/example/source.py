from datetime import datetime, timezone

import dlt

@dlt.source
def example_source():
    @dlt.resource(name="example_events", write_disposition="append")
    def events():
        ingested_at = datetime.now(timezone.utc).isoformat()
        load_package = dlt.current.load_package()
        batch_id = getattr(load_package, "load_id", None) or getattr(load_package, "package_id", None)
        payload = {
            "id": "evt_1",
            "event_time": "2026-05-25T00:00:00Z",
            "value": 1,
        }
        yield {
            "_ingested_at": ingested_at,
            "_source": "example",
            "_batch_id": batch_id,
            "_raw_payload": payload,
        }

    return events()
