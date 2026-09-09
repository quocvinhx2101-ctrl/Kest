# CyberMarket workload

The workload is opt-in and runs in disposable tool containers. The seven local
infrastructure services remain the only long-lived default services.

## Source layout

```text
workload/
  core/           # environment settings and S3 client
  cybermarket/    # schema bootstrap, SQL and transactional event writes
  landing/        # upload-before-ack raw CDC writer
  lakehouse/      # Lakekeeper catalog and Iceberg helpers
  pipelines/      # CDC, live generator, bronze history and finite batch jobs
  validation/     # smoke tests plus lifecycle and batch verification
```

Long-running entry points live in `pipelines/`; domain code does not own process
signals. Configuration is read explicitly at startup, so importing a module
never requires environment secrets.

## Data model

PostgreSQL is the current operational state. Parquet is analytical history from
the same CyberMarket universe, and raw CDC contains the new PostgreSQL mutations
that continue it. The shared entity domains are 10 `PLAT-NNN` markets, 1,000
`SELLER-NNNNNN` vendors, 10,000 `BUYER-NNNNNNN` buyers and 4,000 composite
product keys. Historical fact IDs use `HIST`; new operational facts use `LIVE`,
which prevents key collisions while preserving one identifier convention.

The 5 GiB target is concentrated in event tables. Historical dimension files
contain only the canonical entities, so PostgreSQL does not need millions of
operational dimension rows. Mixed casing, quoted identifiers, composite keys,
JSONB and numeric-looking text remain intentional parts of the schema.

## Object layout

```text
s3://mini-cybet/
  landing/postgres-source/<table>/ingest_date=YYYY-MM-DD/hour=HH/*.jsonl.gz
  bronze/history/<table>/*.parquet
  bronze/history/_manifest.json
  iceberg/                         # Lakekeeper-managed Silver/Gold Iceberg data
```

Raw landing records are immutable gzip-compressed JSON Lines. Each envelope
contains a stable event ID, ingestion time, source LSN/XID and the complete
`wal2json` change. The CDC consumer uploads every table batch before advancing
its PostgreSQL slot. Retries are therefore at-least-once; downstream incremental
transforms should deduplicate on `event_id` or source LSN.

## Batch lakehouse

The finite batch builds all tables in staging namespaces and publishes them only
after row-count checks pass. Silver keeps the ten source-shaped tables and their
awkward names/types. Dimension tables use the latest PostgreSQL snapshot; fact
tables combine bronze history with the current PostgreSQL facts. JSONB values are
stored as JSON strings because Iceberg has no native JSON type and remain
queryable with DuckDB JSON functions.

Gold contains four small query models:

| Table | Grain and purpose |
| --- | --- |
| `daily_market_metrics` | Date and platform; volume, GMV, cross-border, payment and risk metrics |
| `vendor_risk_summary` | Vendor; transaction, buyer, GMV and fraud-risk metrics |
| `buyer_360` | Buyer; purchase, session, checkout and lifetime-value metrics |
| `product_performance` | Composite product key; transactions, units and revenue |

The batch reads PostgreSQL as the authoritative current snapshot. Raw CDC remains
the immutable audit/replay input for the later incremental or streaming phase.

## Commands

```sh
make workload-setup   # exact schema plus causal idempotent dimension seed
make history          # resumable generation of approximately 5 GiB Parquet
make cdc-test         # one second: 20 events, land WAL, validate, then exit
make cdc              # foreground CDC; start this before live generation
make generate         # foreground fixed-rate 20 events/sec generator
make cdc-drain        # land pending changes and exit
make batch            # build and publish Silver/Gold Iceberg tables directly
make batch-check      # validate Iceberg schemas, counts and Gold rollups
make batch-airflow    # trigger the same two-step batch through Airflow
make airflow-dag-check # list parsed DAGs and import errors
make workload-check           # State A: schema and canonical operational seed
make workload-check-history   # State B: State A plus Parquet and manifest
make workload-check-cdc       # State C: State A plus slot and raw landing
```

The checks are lifecycle-aware: setup does not require history or CDC, history
does not require a replication slot, CDC does not require Parquet, and
`batch-check` requires the complete published lakehouse.

Run `make cdc` and `make generate` in separate terminals for a live demo. Stop
either with Ctrl-C. The CDC replication slot persists while the process is off,
but PostgreSQL caps retained slot WAL at 1 GiB. Drain promptly after generating
events; an overrun invalidates the slot and requires deliberate recreation.

The one-second event frame always contains 8 buyer sessions, 6 purchases,
3 payment updates, 2 risk predictions and 1 transaction status update. Each
purchase is one database transaction with exactly the six operations specified
in the workload contract, including two product lines.

The `cybermarket_batch` DAG has no schedule and permits one active run. Airflow
therefore orchestrates batch work only when explicitly triggered. Lakekeeper is
the Iceberg REST catalog and MinIO owns the table data. RisingWave remains an
unused capability; this batch adds no source, stream or materialized view.
