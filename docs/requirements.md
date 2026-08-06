# Kest Product Requirements

- Status: Draft for review
- Version: 0.1
- Last updated: 2026-08-06

## 1. Purpose

This document defines what Kest must provide, who it serves. and how success is measured. It is the upstream product requirement for the architecture, data
contracts, implementation roadmap, and acceptance tests.

The documents have the following responsibilities:

- `requirements.md`: why the product exists and what outcomes it must guarantee.
- `architecture.md`: how the approved requirements are implemented.
- `contracts.md`: machine-enforceable data and platform invariants.
- Architecture Decision Records (ADRs): why significant technical decisions were made.

Every production component must trace back to at least one requirement in this
document. A technology must not be added only to make the stack appear complete.

The terms **MUST**, **SHOULD**, and **MAY** indicate mandatory, recommended, and
optional behavior. Priorities are:

- **P0**: required for the first credible release.
- **P1**: required for a useful team platform after the P0 foundation is stable.
- **P2**: deferred until real workloads justify the cost.

## 2. Product thesis

Kest is a lean, AI-ready lakehouse data platform that makes governed,
reproducible, discoverable, and trustworthy data available to both people and
software agents.

Kest is designed primarily for small data teams and single-node deployments. It
must improve the daily work of Data Engineers (DE), Data Analysts (DA), and Data
Scientists (DS), while exposing machine-readable context that lets AI systems
use data safely and accurately.

AI-ready does not mean that Kest trains or serves models. It means that a human
or AI consumer can determine, without guessing:

- what a dataset means;
- who owns it;
- whether it is fresh and trustworthy;
- which business definitions apply;
- which fields are sensitive;
- how it was produced;
- which version or snapshot was used;
- and how it may be queried.

## 3. Problem statement

Small teams commonly face two bad choices:

1. Use a single analytics database that is easy to start with but tightly couples
   storage, compute, metadata, and serving.
2. Assemble an enterprise-scale data platform whose operational cost is larger
   than the workloads it serves.

AI consumers make the problem harder. A schema alone is insufficient for a
reliable agent: the agent also needs semantics, quality state, ownership,
lineage, policy, and stable query interfaces.

Kest must provide the smallest coherent platform that offers those guarantees
without attempting to reproduce every capability of an enterprise platform.

## 4. Product outcomes

Kest succeeds when:

- DE can add and operate a data pipeline without modifying the platform core.
- DA can discover and query certified data products using stable SQL names and
  shared metric definitions.
- DS can access a declared dataset snapshot and reproduce an earlier analysis.
- AI agents can retrieve bounded, machine-readable data context and query only
  authorized, published data products.
- A failed or repeated pipeline run does not silently corrupt or duplicate data.
- Storage survives replacement of the compute or serving engine.
- A fresh deployment can prove the full lifecycle through automated acceptance
  tests rather than screenshots or manual claims.

## 5. Target users and jobs to be done

### 5.1 Data Engineer

The DE needs to:

- define sources, schemas, incremental cursors, and data contracts;
- run pipelines locally before deployment;
- inspect run state and failure causes;
- replay a batch or backfill a time range safely;
- evolve schemas explicitly;
- publish a validated data product;
- replace a compute component without migrating the underlying data.

### 5.2 Data Analyst

The DA needs to:

- find the correct certified dataset without knowing its physical location;
- understand grain, dimensions, metrics, freshness, and caveats;
- query through SQL and connect a BI tool;
- distinguish draft, failed, stale, deprecated, and certified data products;
- receive consistent metric results across supported query clients.

### 5.3 Data Scientist

The DS needs to:

- access curated and, when authorized, lower-layer datasets through open formats;
- load data efficiently into Arrow-compatible tools and notebooks;
- record the dataset snapshot used by an experiment;
- reproduce the same input dataset later;
- avoid depending on a BI-specific serving model.

Model training, experiment tracking, feature serving, and model deployment are
outside the initial Kest scope.

### 5.4 AI consumer

An AI consumer may be a text-to-SQL system such as Kur, a data assistant, or an
automated analysis workflow. It needs to:

- discover only datasets visible to its identity;
- retrieve compact schema, semantic, quality, lineage, and policy context;
- resolve business terms and metrics to canonical definitions;
- identify the stable query name of a data product;
- reference the snapshot and context version used for a response;
- execute through a governed, auditable, read-only interface;
- fail explicitly when context is ambiguous, stale, unauthorized, or incomplete.

The AI application owns conversation flow, generation, approval UX, and answer
presentation. Kest owns the data context and data-access guarantees.

### 5.5 Platform operator

The operator needs to:

- deploy the platform reproducibly on one machine;
- observe service health, pipeline state, data freshness, and storage usage;
- back up and restore all durable state;
- rotate credentials without rebuilding data;
- upgrade components with a documented rollback path.

## 6. Scope

### 6.1 P0 scope

- Batch ingestion from APIs, files, and relational databases.
- Immutable raw data with complete load and batch metadata.
- Deterministic SQL-oriented transforms into curated Iceberg tables.
- One canonical table catalog and one canonical identity for each data product.
- Data contracts and automated quality gates.
- Safe replay and bounded backfill.
- Snapshot-aware access for SQL, Arrow, and AI consumers.
- Machine-readable data product metadata and business semantics.
- A read-only serving path for published data products.
- Single-node deployment, health checks, backup, restore, and smoke tests.

### 6.2 P1 scope

- Lineage and impact metadata across ingestion, transforms, and published products.
- A notebook-ready developer experience.
- BI connectivity through the shared serving interface.
- Fine-grained access policies and auditable service identities.
- A reusable pipeline/data-product template and CI validation workflow.
- Kur integration as the reference AI consumer.

### 6.3 Explicit non-goals for P0 and P1

- Distributed compute.
- Kubernetes or a service mesh.
- Streaming and sub-minute real-time processing.
- Change data capture infrastructure.
- Multi-region or active-active deployment.
- A feature store or online feature serving.
- Vector search or a general-purpose vector database.
- Model training, model registry, experiment tracking, or model serving.
- A custom notebook product, BI product, or data marketplace.
- Supporting multiple tools with overlapping responsibility in the same plane.
- Enterprise multi-tenancy.

These capabilities may be reconsidered only when a documented workload cannot
meet its requirements with the existing platform.

## 7. Architectural constraints

These are product constraints, not final component selections.

### ARC-001 — Storage/compute separation (P0)

Durable analytical data MUST use an open table and file format in object
storage. Replacing the transform or serving engine MUST NOT require rewriting
the authoritative datasets.

### ARC-002 — One source of metadata truth (P0)

Kest MUST have one canonical catalog for namespaces, tables, current metadata
pointers, and data-product identity. Search indexes and AI context stores may
cache this metadata but MUST NOT become independent sources of truth.

### ARC-003 — Replaceable adapters (P0)

Ingestion, compute, quality, orchestration, catalog, and serving integrations
MUST have explicit boundaries. A platform contract MUST not depend on an
engine-specific hidden state when an open representation is available.

### ARC-004 — Orchestration is control flow (P0)

The orchestrator MUST schedule, coordinate, retry, and record work. Business
transform logic MUST remain in versioned transformation code outside workflow
definitions.

### ARC-005 — Single-node first (P0)

The reference deployment MUST operate on one Linux host. Distributed services
MUST NOT be introduced until a measured workload exceeds the single-node
envelope.

### ARC-006 — Automatable interfaces (P0)

Every lifecycle operation required for acceptance testing MUST be available
through a CLI, API, or version-controlled declaration. A UI may wrap these
interfaces but MUST NOT be the only way to perform an operation.

## 8. Functional requirements

### 8.1 Ingestion and movement

#### ING-001 — Source isolation (P0)

A new source MUST be addable as an isolated pipeline package without editing
the ingestion runtime or other source pipelines.

#### ING-002 — Load identity (P0)

Every ingestion attempt MUST receive a stable `batch_id`. All records and load
metadata produced by that attempt MUST be traceable to that identifier.

#### ING-003 — Complete-load marker (P0)

Consumers MUST be able to distinguish a complete batch from a partial or failed
batch. Downstream transforms MUST NOT process a batch before its completion is
recorded.

#### ING-004 — Incremental state (P0)

Incremental cursors and checkpoints MUST be durable, inspectable, and scoped to
the source. Updating operational state MUST NOT mutate previously landed raw
records.

#### ING-005 — Raw fidelity (P0)

The raw layer MUST preserve the source payload or a lossless representation of
it together with ingestion time, source identity, and batch identity.

#### ING-006 — Schema drift (P0)

Additive compatible changes MAY be accepted automatically when permitted by the
data contract. Breaking changes MUST stop publication and produce an actionable
error.

#### ING-007 — File/layout declaration (P0)

The output file format and object layout MUST be declared explicitly. A reader
MUST NOT assume a format or path that the writer does not guarantee.

#### ING-008 — Batch reconciliation (P1)

The platform SHOULD record source count, landed count, rejected count, and a
reconciliation result for each batch when the source exposes sufficient
information.

### 8.2 Storage and table lifecycle

#### STO-001 — Immutable raw records (P0)

Successfully landed raw records MUST be append-only. Corrections MUST be
represented as new batches or explicit tombstone/correction events.

#### STO-002 — Transactional curated tables (P0)

Silver and gold publication MUST use atomic table-format commits. Readers MUST
observe either the previous valid snapshot or the new valid snapshot, never a
partial publish.

#### STO-003 — Snapshot identity (P0)

Every published curated dataset MUST expose its current snapshot identifier and
commit timestamp. A consumer MUST be able to record these values.

#### STO-004 — Time travel and reproducibility (P1)

Within the configured retention window, an authorized consumer SHOULD be able
to read a curated table by snapshot or timestamp.

#### STO-005 — Partition ownership (P0)

Partition strategy MUST be declared with the data product and managed through
the table format. Consumers MUST NOT construct object paths from partition
assumptions.

#### STO-006 — Retention safety (P1)

Snapshot expiration, orphan-file removal, and raw retention MUST be separate,
observable operations. Cleanup MUST preserve snapshots still protected by the
declared reproducibility window.

#### STO-007 — Catalog recoverability (P0)

Catalog metadata and object storage MUST have coordinated backup and restore
procedures. A restore test MUST prove that published tables remain resolvable.

### 8.3 Transform and publication

#### TRN-001 — Deterministic transform (P0)

Given the same input snapshots, transform version, and parameters, a transform
MUST produce logically identical output.

#### TRN-002 — Idempotent replay (P0)

Reprocessing an already processed batch MUST NOT create duplicate logical
records or double-count aggregate results.

#### TRN-003 — Explicit input selection (P0)

A transform run MUST record its input batches or snapshots and its transform
version.

#### TRN-004 — Layer responsibilities (P0)

- Raw/bronze preserves source fidelity and operational metadata.
- Silver applies typing, normalization, validation, and deduplication.
- Gold defines consumer-facing entities, metrics, and aggregates.

Business aggregation MUST NOT occur in ingestion code.

#### TRN-005 — Publish after validation (P0)

A new data-product version MUST become visible as certified only after all
mandatory transform and quality checks succeed.

#### TRN-006 — Backfill boundary (P1)

A DE SHOULD be able to backfill a declared time or batch range without a full
table rebuild when the table strategy supports it.

#### TRN-007 — Schema migration (P1)

Breaking curated-schema changes MUST include a migration plan, compatibility
window, affected consumers, and rollback procedure.

### 8.4 Orchestration and execution control

#### ORC-001 — Observable run state (P0)

Each task and pipeline run MUST expose start time, end time, status, attempt,
input identity, output identity, and a useful failure reason.

#### ORC-002 — Retry classification (P0)

Only failures classified as transient SHOULD be retried automatically. Contract,
schema, policy, and deterministic transform failures MUST fail without blind
retry.

#### ORC-003 — Concurrency control (P0)

The platform MUST prevent concurrent runs from corrupting shared state or
publishing conflicting snapshots. Concurrency limits MUST be enforced in
configuration, not only documented.

#### ORC-004 — Resume and replay (P0)

An operator MUST be able to resume from a safe boundary or replay a named batch
without manually editing storage objects or metadata tables.

#### ORC-005 — Housekeeping workflows (P1)

Retention, compaction where applicable, orphan cleanup, and restore verification
SHOULD be scheduled and auditable platform workflows.

### 8.5 Data quality and contracts

#### DQ-001 — Contract as code (P0)

Every published data product MUST have a version-controlled, machine-readable
contract. At minimum it MUST declare schema, grain, keys, owner, freshness,
sensitivity, compatibility policy, and mandatory checks.

#### DQ-002 — Layered checks (P0)

Quality checks MUST reflect layer responsibility:

- raw: load completeness, parseability, and freshness;
- silver: types, required values, uniqueness, and deduplication;
- gold: metric invariants, referential integrity, and business bounds.

#### DQ-003 — Quality result persistence (P0)

Quality results MUST be stored with dataset, snapshot, check version, execution
time, status, and failure details.

#### DQ-004 — Publication gate (P0)

A failed mandatory check MUST prevent certification/publication. Existing good
snapshots MUST remain queryable unless explicitly revoked.

#### DQ-005 — Quarantine (P1)

Invalid records SHOULD be inspectable in a bounded quarantine area without
being included in certified output.

### 8.6 Catalog, discovery, semantics, and lineage

#### CAT-001 — Stable product identity (P0)

Every data product MUST have a stable identifier independent of its physical
object path and serving engine.

#### CAT-002 — Product manifest (P0)

Each data product MUST expose machine-readable metadata containing at least:

- stable identifier and version;
- name, description, domain, and lifecycle state;
- owner and support contact;
- schema, grain, keys, and partition strategy;
- freshness target and latest successful refresh;
- quality status and contract version;
- sensitivity classification and access policy reference;
- current table snapshot;
- stable serving/query identifier;
- upstream and downstream references when available.

#### CAT-003 — Lifecycle state (P0)

The catalog MUST distinguish at least `draft`, `published`, `certified`, `stale`,
`deprecated`, and `revoked` products.

#### SEM-001 — Metric definition (P0)

A business metric MUST declare a stable name, description, formula, grain,
allowed dimensions, source products, owner, caveats, and version.

#### SEM-002 — Glossary and synonyms (P1)

Business terms SHOULD map human and domain synonyms to canonical entities,
dimensions, and metrics.

#### SEM-003 — Semantic consistency (P1)

Supported SQL, BI, and AI consumers SHOULD resolve a published metric to the
same definition and compatible result.

#### LIN-001 — Run lineage (P0)

The platform MUST record which input batches/snapshots and transform version
produced each published snapshot.

#### LIN-002 — Impact discovery (P1)

Before a breaking data-product change, an operator SHOULD be able to identify
known downstream products and registered consumers.

### 8.7 Serving and consumption

#### SRV-001 — Published-products boundary (P0)

The default shared query interface MUST expose only published or certified data
products. Raw and intermediate access requires an explicit privileged role.

#### SRV-002 — Read-only consumer access (P0)

DA, DS, BI, and AI consumer identities MUST be read-only by default. Publication
credentials MUST not be shared with query clients.

#### SRV-003 — Stable query names (P0)

A published product MUST have a stable logical query name that does not expose
its object-storage path.

#### SRV-004 — Engine-independent product contract (P0)

Changing the serving engine MAY change operational configuration but MUST NOT
change data-product identifiers, documented semantics, or authoritative table
history.

#### SRV-005 — Bounded query execution (P1)

The shared query path SHOULD support configurable time, row, memory, and
concurrency limits and return explicit limit errors.

#### SRV-006 — Standard client access (P1)

The reference platform SHOULD support SQL clients, one BI client, and
Arrow-compatible notebook access without copying authoritative datasets into a
proprietary store.

### 8.8 AI readiness

#### AIR-001 — Machine-readable context package (P0)

For an authorized data product, Kest MUST be able to return a bounded context
package containing schema, descriptions, grain, keys, relationships, metrics,
freshness, quality state, sensitivity, and stable query name.

#### AIR-002 — Versioned context (P0)

An AI response MUST be able to record the data-product version, table snapshot,
contract version, and semantic-definition version used to construct it.

#### AIR-003 — Ambiguity signal (P0)

The context interface MUST represent missing, ambiguous, stale, or conflicting
metadata explicitly. It MUST NOT silently choose an arbitrary table or metric.

#### AIR-004 — Policy-filtered discovery (P0)

Search and metadata responses MUST be filtered using the consumer identity.
Hiding data only at query execution time is insufficient.

#### AIR-005 — Safe samples (P1)

If sample values are exposed for grounding, they MUST obey sensitivity policy,
masking, size limits, and audit requirements. Schema discovery MUST not require
sample-row access.

#### AIR-006 — Governed query interface (P1)

Kest SHOULD provide the policy and audit hooks required for an AI application
to validate and execute read-only queries. Conversational approval and SQL
generation remain responsibilities of the consuming application.

#### AIR-007 — Reference Kur integration (P1)

Kur SHOULD be able to discover certified Kest products, retrieve their context,
generate a query using stable names, execute it through the governed serving
path, and report the snapshot used.

### 8.9 Security and identity

#### SEC-001 — Distinct identities (P0)

Ingestion, transformation, publication, orchestration, and consumption MUST use
distinct service identities or credentials.

#### SEC-002 — Least privilege (P0)

Each service MUST receive only the permissions required for its role. The BI or
AI consumer MUST NOT possess object deletion, table commit, or catalog mutation
permissions.

#### SEC-003 — Secret handling (P0)

Secrets MUST not be committed, embedded in images, printed in logs, or exposed
through product metadata. Development defaults MUST be clearly identified as
unsafe outside local environments.

#### SEC-004 — Audit trail (P1)

Publication, contract changes, policy decisions, and shared query executions
SHOULD record actor, action, target, time, and outcome.

#### SEC-005 — Sensitive data classification (P1)

Columns and products SHOULD support explicit sensitivity classifications that
are available to both policy enforcement and AI context generation.

### 8.10 Operations and developer experience

#### OPS-001 — Reproducible deployment (P0)

Images and runtime dependencies MUST be versioned. A clean host MUST be able to
produce the same service topology from version-controlled configuration.

#### OPS-002 — Health model (P0)

Every required service MUST expose a meaningful readiness check. Pipeline work
MUST not begin before its dependencies are ready.

#### OPS-003 — End-to-end smoke test (P0)

The repository MUST include an automated smoke test that deploys or targets a
clean stack, runs the reference lifecycle, and verifies raw data, curated table
snapshots, quality gates, catalog state, and serving access.

#### OPS-004 — Backup and restore (P0)

Durable state MUST be enumerated. Backup and restore commands MUST be automated,
documented, and tested against the reference workload.

#### OPS-005 — Minimum observability (P0)

Operators MUST be able to inspect service readiness, pipeline failures, latest
dataset refresh, quality status, object-storage usage, and catalog availability.

#### OPS-006 — No generated runtime state in Git (P0)

Logs, database files, object-store data, credentials, checkpoints, and UI state
MUST not be committed to source control.

#### DEV-001 — Local validation (P0)

A contributor MUST be able to validate contracts, transforms, DAG/workflow
syntax, configuration, and unit tests without running the full platform.

#### DEV-002 — Change traceability (P1)

CI SHOULD map a failed check to the affected requirement, contract, data
product, or architecture decision where practical.

## 9. Reference workloads

The architecture MUST be evaluated against concrete workloads rather than a
generic capability diagram.

### RW-01 — Batch API to certified metric (P0)

1. Ingest records from a paginated API.
2. Land an immutable raw batch with a completion marker.
3. Produce typed, deduplicated silver records.
4. Produce a daily gold metric.
5. Run mandatory checks.
6. Publish the product and expose it through the shared query interface.
7. Return its machine-readable context package.

### RW-02 — Safe replay (P0)

Replay the same raw batch and prove that logical silver rows and gold metrics do
not change or duplicate.

### RW-03 — Schema change (P0)

Accept an additive source field, then reject a breaking type change with a clear
contract error while preserving the last certified snapshot.

### RW-04 — Failure and resume (P0)

Inject a transform or quality failure, prove that no partial product is
published, fix the cause, and resume or replay from a declared safe boundary.

### RW-05 — Analyst consumption (P1)

Discover a certified product and metric, query it through SQL and the reference
BI client, and receive consistent results.

### RW-06 — Reproducible DS input (P1)

Read a product snapshot into an Arrow-compatible notebook, record its identity,
publish a later snapshot, and reproduce the original input.

### RW-07 — Kur consumption (P1)

Using a restricted AI identity, discover an authorized certified product,
retrieve metric and policy context, generate and validate a read-only query,
execute it, and report the snapshot used.

### RW-08 — Backup and restore (P0)

Back up durable state, recreate the platform on clean state, restore it, and
prove that the reference product, catalog entry, snapshot, and query result are
recoverable.

## 10. Initial service-level objectives

These are initial correctness and operability targets. Performance targets
remain open until the reference hardware and dataset envelope are agreed.

| Objective | Initial target | Priority |
|---|---:|---:|
| Successful batch publication is atomic | 100% | P0 |
| Replay of the same batch changes logical results | 0 times | P0 |
| Mandatory quality failure publishes a bad snapshot | 0 times | P0 |
| Published product has owner, contract, quality state, and snapshot | 100% | P0 |
| Required service has an automated readiness check | 100% | P0 |
| Fresh deployment passes the reference smoke test | 100% of release candidates | P0 |
| Backup release is accepted without a restore test | 0 times | P0 |
| AI context references versioned product and semantic metadata | 100% | P0 |

The following values MUST be decided before calling Kest production-capable:

- reference host CPU, memory, and storage;
- maximum tested dataset and daily ingest volume;
- batch freshness and completion target;
- interactive query latency and concurrency target;
- backup RPO and restore RTO;
- snapshot and raw-data retention periods.

## 11. Release acceptance

### 11.1 P0 release gate

A P0 release is accepted only when:

- RW-01, RW-02, RW-03, RW-04, and RW-08 pass automatically on a clean deployment;
- all P0 requirements are implemented or explicitly waived with a recorded reason;
- the implementation has no undeclared writer/reader format mismatch;
- the implementation has one canonical catalog;
- generated runtime state is absent from Git;
- recovery documentation has been exercised, not only written;
- architecture components and data contracts trace to this document.

### 11.2 P1 release gate

A P1 release additionally requires:

- RW-05, RW-06, and RW-07;
- policy-filtered discovery and read-only consumer identities;
- lineage and impact metadata for the reference products;
- CI validation for a newly added pipeline and data product;
- measured performance against the agreed reference envelope.

## 12. Open product decisions

The following decisions must be resolved before architecture is finalized:

1. What is the reference single-node hardware and maximum tested data envelope?
2. Is Kest responsible for operating a shared SQL serving engine, or only for
   publishing Iceberg and catalog interfaces to consumer-owned engines?
3. Which catalog is canonical for both Kest and Kur: SQL/JDBC, REST, Gravitino,
   or another implementation?
4. What authoring contract should transforms use: SQL files, a transformation
   framework, or a small Kest-native specification?
5. What is the minimum semantic model shared by SQL, BI, notebooks, and Kur?
6. Which lifecycle states are manual approvals versus automated outcomes?
7. What identity model is sufficient for the single-node reference deployment?
8. What data sensitivity classes and sample-data rules are required?
9. Which metadata and lineage interfaces must be open standards in P0 versus P1?
10. Which reference API, relational source, BI client, and notebook form the
    acceptance environment?

## 13. Change policy

A requirement change must state:

- the user or workload that motivates it;
- its priority;
- its acceptance test;
- any affected contract or SLO;
- and the architecture components expected to change.

A new platform component must identify the requirement it satisfies and why an
existing component or simpler approach is insufficient.
