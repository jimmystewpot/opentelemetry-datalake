# Lakehouse Integration Specification for `opentelemetry-datalake`

## Status of this Memo

This document specifies an architectural specification for integrating OpenTelemetry telemetry data sinks with Transactional Lakehouse Formats (Delta Lake, Apache Iceberg, Apache Hudi, and Apache Paimon). It adheres strictly to the key phrase requirements defined in **RFC 2119**.

## 1. Introduction & Terminology

The keywords **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).


## 2. Sink Architecture Design

`opentelemetry-datalake` operates as a transactional streaming/batch sink. To maintain lakehouse metadata integrity and optimize performance via Apache Arrow, the sink engine **MUST** comply with the following transactional behaviors:

1. **ACID Commit Isolation:** Every data write block compiled from incoming OTLP streams **MUST** be committed to the target lakehouse table using an isolated transaction. Partial or uncommitted Parquet files **MUST NOT** be visible to active lakehouse readers.
2. **Idempotency and Retries:** In the event of a network or metadata commit timeout, the system **SHOULD** retry the transaction. To prevent data duplication during distributed pipeline retries, the sink engine **MUST** track batch delivery tokens inside transaction identifiers if supported by the underlying storage protocol (e.g., Iceberg snapshot properties).


## 3. Schema Evolution Modes

The sink engine **MUST** support exactly three modes for managing table schemas: `fixed`, `auto`, and `catalog`.

```yaml
# Configuration validation enum schema
sink:
  schema_mode: "fixed" | "auto" | "catalog"
  log_dropped_fields: true # Only applies to catalog mode

```

### 3.1 `fixed` Schema Mode

* When configured to `fixed`, the sink **MUST** validate incoming Apache Arrow `RecordBatch` payloads against a user-defined schema provided inside the pipeline configuration file.
* If an incoming telemetry payload contains structural anomalies, new attributes, or altered field types that deviate from the explicitly configured layout, the sink engine **MUST NOT** alter the target table schema. Instead, the non-compliant fields **MUST** be dropped, or the entire sub-batch **SHOULD** be routed to a dead-letter quarantine table if a quarantine target is configured.

### 3.2 `auto` Schema Mode (Automatic Schema Evolution)

* When configured to `auto`, control over schema updates is handed entirely to `opentelemetry-datalake`.
* The engine **SHALL** dynamically inspect incoming payload schemas. If a batch contains fields that are absent from the current lakehouse table metadata, the engine **MUST** execute an atomic metadata alteration transaction.
* **Additive Only Constraint:** The schema engine **MUST ONLY** append new columns. It **MUST NOT** delete existing columns, and it **MUST NOT** modify or cast existing column data types (e.g., changing an existing `Int32` column to `Int64` is strictly prohibited). Non-backward-compatible type mismatches **MUST** result in processing failure or batch quarantine.
* Struct arrays (e.g., flattened resource attributes) **MAY** expand dynamically by appending new fields to the struct type definition leaf nodes.

### 3.3 `catalog` Schema Mode (Catalog as Source of Truth)

* When configured to `catalog`, the sink **MUST** fetch the target table schema directly from the catalog (e.g., Iceberg REST, Glue, Unity) during initialization and periodically refresh it.
* The engine **SHALL** map incoming telemetry fields 1:1 by name to the schema retrieved from the catalog.
* **Schema Strictness:** Any fields present in the incoming telemetry payload that do not exist in the catalog-provided schema **MUST** be dropped.
* **Logging:** The sink engine **MUST** log the names of any dropped fields if `log_dropped_fields` is set to `true` (default). This provides visibility into schema drift without interrupting the ingestion pipeline.


## 4. Permission & Catalog Security Model

For `auto` schema evolution to operate reliably without manual intervention, the storage catalog credentials supplied to `opentelemetry-datalake` **MUST** possess specific privileges. The catalog service (e.g., AWS Glue, Iceberg REST Catalog, Hive Metastore, or Unity Catalog) **MUST** enforce and support the following capabilities:

### 4.1 Required Catalog Permissions

* **Metadata Read/Write:** The sink credentials **MUST** have permissions to read current table snapshots (`GetTable`, `GetDatabase`) and write data state transitions.
* **Schema Alter Privileges:** When `schema_mode` is set to `auto`, the target catalog authorization model **MUST** grant the sink service identity the right to append structural attributes (e.g., `ALTER TABLE ... ADD COLUMNS`).
* **Concurrent Lock Management:** The catalog **MUST** safely support optimistic concurrency control (OCC). If multiple `opentelemetry-datalake` worker nodes simultaneously attempt to execute an additive schema update, the catalog provider **MUST** serialize the transactions cleanly, rejecting conflict actions while allowing the safe schema changes to propagate sequentially.



## 5. Physical Layout & Partitioning Logic

Optimizing query execution on open table formats requires uniform file sizing and temporal alignment.

```rust
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionGranularity {
    Hourly,
    Daily,
}

```

### 5.1 Partitioning Constraints

* The default partition granularity for all telemetry types **SHALL** be `hourly`.
* The user **MAY** redefine this configuration parameter to `daily` using the explicit configuration enumeration. No other granularities (e.g., minute-level or monthly) are valid options.
* Virtual paths for partition layouts **MUST** follow standardized ISO-8601 temporal directories derived strictly from the primary timestamp of the telemetry event:
* Hourly Format: `year=YYYY/month=MM/day=DD/hour=HH/`
* Daily Format: `year=YYYY/month=MM/day=DD/`



## 6. Telemetry Table Layout & Ordering Optimization

To leverage Parquet metadata dictionary encoding, min/max statistics, and pushdown filters effectively within table engines like Iceberg and Delta, data **MUST** be pre-sorted in-memory using Apache Arrow before it is flushed to physical storage blocks.

Following the structure utilized by standard OLAP systems (such as the OpenTelemetry ClickHouse templates), the data ordering tuples are defined below.

### 6.1 Traces Table Optimization

Traces contain high cardinality keys but are almost always analyzed within the context of an application domain over time. The sink engine **MUST** sort the Arrow data arrays inside each physical file block by the following multi-key tuple prior to committing:

$$\text{Sort Order} = (\text{ServiceName}, \text{SpanName}, \text{toDateTime(Timestamp)})$$

* **Rationale:** Placing `ServiceName` and `SpanName` first guarantees tight compression of highly repetitive string keys via dictionary encoding, while secondary temporal sorting facilitates lightning-fast range scans during trace investigations.

### 6.2 Gauge Metrics Table Optimization

Metrics queries regularly aggregate numeric values across distinct dimensions. The sink engine **MUST** sort gauge records by the following coordinate arrangement:

$$\text{Sort Order} = (\text{ServiceName}, \text{MetricName}, \text{Attributes}, \text{toUnixTimestamp64Nano(TimeUnix)})$$

* **Rationale:** Grouping by `MetricName` and its corresponding label `Attributes` forces highly repeating metric streams into contiguously indexed storage blocks, maximizing vectorization efficiency during aggregation scans.

### 6.3 Logs Table Optimization

Based on standard high-performance logging templates, log ingestion profiles vary drastically between unstructured text bodies and highly contextualized structured attributes. The sink engine **MUST** layout the logs table according to the following layout sequence:

$$\text{Sort Order} = (\text{ServiceName}, \text{SeverityText}, \text{toDateTime(Timestamp)})$$

* **Rationale:** Isolating `ServiceName` ensures tenant data grouping. Elevating `SeverityText` to the second position allows analytical lakehouse queries to prune files instantly when scanning exclusively for `ERROR` or `CRITICAL` entries during operational incident debugging, while `Timestamp` ensures strict sequence tracking within chronological debugging bounds.


## 7. Batching & Compaction Optimization

To minimize metadata overhead and avoid the "small files problem" common in distributed lakehouse environments, the sink engine **MUST** implement configurable batching logic.

```yaml
# Configuration for commit batching
batching:
  max_batch_size_bytes: 134217728 # Default: 128MB
  max_batch_interval_sec: 60      # Default: 60 seconds
```

### 7.1 Buffer Integration

The sink engine batching logic **SHOULD** be aligned with the underlying [Buffer Specification](buffer.md) to ensure that data is committed to the lakehouse in sync with the pipeline's buffering and flushing cycle.

### 7.2 Format-Specific Batching Heuristics

Batching strategy **SHOULD** be adjusted based on the target lakehouse format's tolerance for frequent commits and its background compaction architecture.

#### 7.2.1 Non-Realtime Formats (Apache Iceberg, Delta Lake)

* These formats typically utilize Copy-on-Write (CoW) or Merge-on-Read with infrequent compaction cycles.
* **Guideline:** Users **SHOULD** prefer larger batch sizes (e.g., 128MB - 512MB) and longer commit intervals (e.g., 5 - 15 minutes).
* **Rationale:** Frequent small commits in Iceberg create a large number of manifest files and snapshots, which significantly degrades query performance and increases catalog pressure until a heavy-weight compaction job is executed.

#### 7.2.2 Realtime Formats (Apache Hudi, Apache Paimon)

* These formats are designed for high-frequency ingestion using Merge-on-Read (MoR) and often support asynchronous, background compaction that runs concurrently with writers.
* **Guideline:** Users **MAY** configure smaller batch sizes and more aggressive intervals (e.g., < 60 seconds) to achieve lower data latency.
* **Rationale:** Hudi and Paimon handle the accumulation of small log files more gracefully by merging them into base files in the background, making them more suitable for near-real-time telemetry requirements.
