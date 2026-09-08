# CyberMarket workload

The workload is opt-in and runs in disposable tool containers. The seven local
infrastructure services remain the only long-lived default services.

## Source layout

```text
workload/
  core/           # environment settings and S3 client
  cybermarket/    # schema bootstrap, SQL and transactional event writes
  landing/        # upload-before-ack raw CDC writer
  pipelines/      # long-running CDC, live generator and bronze history jobs
  validation/     # one-second smoke test and full state verification
```

Long-running entry points live in `pipelines/`; domain code does not own process
signals. Configuration is read explicitly at startup, so importing a module
never requires environment secrets.

## Object layout

```text
s3://mini-cybet/
  landing/postgres-source/<table>/ingest_date=YYYY-MM-DD/hour=HH/*.jsonl.gz
  bronze/history/<table>/*.parquet
  bronze/history/_manifest.json
  iceberg/                         # Lakekeeper warehouse root; currently empty
```

`silver/` and `gold/` have no objects or Iceberg namespaces yet. Raw landing
records are immutable gzip-compressed JSON Lines. Each envelope contains a stable
event ID, ingestion time, source LSN/XID and the complete `wal2json` change.
The CDC consumer uploads every table batch before advancing its PostgreSQL slot.
Retries are therefore at-least-once; downstream transforms should deduplicate on
`event_id` or source LSN.

## Commands

```sh
make workload-setup   # exact schema plus causal idempotent dimension seed
make history          # resumable generation of approximately 5 GiB Parquet
make cdc-test         # one second: 20 events, land WAL, validate, then exit
make cdc              # foreground CDC; start this before live generation
make generate         # foreground fixed-rate 20 events/sec generator
make cdc-drain        # land pending changes and exit
make workload-check   # validate database, bucket, catalog and stopped tools
```

Run `make cdc` and `make generate` in separate terminals for a live demo. Stop
either with Ctrl-C. The CDC replication slot persists while the process is off,
but PostgreSQL caps retained slot WAL at 1 GiB. Drain promptly after generating
events; an overrun invalidates the slot and requires deliberate recreation.

The one-second event frame always contains 8 buyer sessions, 6 purchases,
3 payment updates, 2 risk predictions and 1 transaction status update. Each
purchase is one database transaction with exactly the six operations specified
in the workload contract, including two product lines.
