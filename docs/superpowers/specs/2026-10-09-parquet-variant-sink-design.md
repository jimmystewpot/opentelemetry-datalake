# Architecture Design Specification: Parquet Streaming Sink with VARIANT Support

- **Document ID**: SPEC-2026-10-09-PARQUET-VARIANT-SINK
- **Status**: Finalized (Updated with OTLP, S3, and Variant constraints)
- **Author**: Principal Software Engineer
- **Date**: 2026-10-09
- **Target Workspace**: `crates/parquet-sink`

---

## 1. Executive Summary

This specification defines the architecture, storage abstractions, data layout, and runtime guarantees for a new high-throughput streaming Parquet sink (`parquet-sink`) within `opentelemetry-datalake`.

The sink streams OpenTelemetry signals (Logs, Metrics, Traces) directly from Arrow `RecordBatch` streams into modern Parquet files residing on cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or S3-compatible systems like RustFS) or local/shared POSIX filesystems.

To maximize read efficiency and query performance across analytical engines (Snowflake, Databricks/Spark, DuckDB, ClickHouse, and StarRocks), the sink adopts the **Apache Parquet / Spark / Iceberg Variant binary format** (`metadata` and `value` payloads).

---

## 2. Problem Statement & Motivation

Currently, semi-structured telemetry data (OTel `attributes`, `resource.attributes`, and log `body`) is converted to JSON-encoded strings. While simple, stringified JSON in columnar storage has severe drawbacks:
1. **Query Scan Degradation**: Downstream query engines must scan and parse full JSON text strings row-by-row using CPU-expensive scalar functions (`get_json_object`), preventing vectorized evaluation.
2. **Storage Inefficiency**: JSON keys and delimiters are repeatedly stored as uncompressed strings, inflating data volume.
3. **No Direct Sub-field Pruning**: Analytic engines cannot skip non-matching records based on nested attribute predicates without parsing the entire payload.

Furthermore, deploying dozens or hundreds of ingestion nodes requires a distributed, lock-free file writing strategy to prevent naming collisions and avoid uncommitted partial-read corruptions.

---

## 3. Goals & Non-Goals

### Goals
* **Direct Streaming I/O**: Stream Parquet data directly to object storage via multipart uploads and local filesystems without intermediate local disk spooling.
* **Modern Parquet Format**: Full support for Parquet 2.0+ features, Data Page V2, column statistics, Bloom filters on ID columns, and selectable compression codecs (`Zstd`, `Snappy`, `Lz4Raw`, `Gzip`, `Uncompressed`).
* **Zero-Allocation VARIANT Encoding**: Fast, thread-local binary encoding for OTel attributes and dynamic payloads conforming to the Apache Parquet Variant specification.
* **Collision-Free Multi-Node Ingestion**: Deterministic, entropy-backed naming (`{timestamp_nano}_{node_id}_{uuidv7}_{sequence:04}.parquet`) guaranteeing zero file collisions across concurrent nodes.
* **Bounded Resource Usage**: LRU-evicted active partition pool, active global memory tracking, and backpressure propagation back to OTLP ingestion.
* **Zero-Panic Compliance**: Strict adherence to `AGENTS.md`—no `.unwrap()`, `.expect()`, or unhandled panics.

### Non-Goals
* **ACID Table Catalog Commit**: This sink writes raw partitioned Parquet files for direct engine scanning or external tables. Catalog management is handled by `crates/storage`.
* **In-Sink Compaction**: Small-file compaction is delegated to asynchronous background compactor jobs (especially critical when memory pressure forces early file rolls).

---

## 4. Architecture & System Flow

```
                      ┌────────────────────────────────────────────────────────┐
                      │                      ParquetSink                       │
                      │                                                        │
PipelineReceiver ────►│  SignalRouter ──► VariantTransformer ──► PartitionMgr  │
 (SignalBatch)        │  (Logs/Metrics/   (Zero-alloc binary     (Vectorized   │
                      │      Traces)          encoding)          slicing & LRU)│
                      └───────────────────────────────────────────────┬────────┘
                                                                      │
                                      ┌───────────────────────────────┴────────┐
                                      ▼                                        ▼
                         ┌──────────────────────────┐             ┌──────────────────────────┐
                         │ Active Partition Writer  │             │ Active Partition Writer  │
                         │ (Partition A)            │             │ (Partition B)            │
                         │  ┌────────────────────┐  │             │  ┌────────────────────┐  │
                         │  │ tokio::spawn_block │  │             │  │ tokio::spawn_block │  │
                         │  │ • Sync ArrowWriter │  │             │  │ • Sync ArrowWriter │  │
                         │  └────────┬───────────┘  │             │  └────────┬───────────┘  │
                         │           │ (Channel)    │             │           │ (Channel)    │
                         │  ┌────────▼───────────┐  │             │  ┌────────▼───────────┐  │
                         │  │ Async Uploader     │  │             │  │ Async Uploader     │  │
                         │  │ • WriteMultipart   │  │             │  │ • WriteMultipart   │  │
                         │  └────────────────────┘  │             │  └────────────────────┘  │
                         └────────────┬─────────────┘             └────────────┬─────────────┘
                                      │                                        │
                                      ▼                                        ▼
                      ┌────────────────────────────────────────────────────────────────────────┐
                      │                      object_store::ObjectStore                         │
                      │          (s3://... | gcs://... | azblob://... | file://...)            │
                      └────────────────────────────────────────────────────────────────────────┘
```

### Component Breakdown

1. **`ParquetSink` (`pipeline_core::pipeline::Sink`)**:
   Main entry point consuming `SignalBatch`es from the async Tokio channel.

2. **`SignalRouter` & The Metrics Schema Explosion**:
   Extracts `RecordBatch` by signal type (`Logs`, `Metrics`, `Traces`). 
   * **Metrics Handling**: Because OTLP Metrics are highly polymorphic (Gauges, Sums, Histograms), flattening them into a strict relational schema results in extreme column sparsity. The router flattens the metadata (name, description, unit) but serializes the polymorphic `DataPoint` (including exemplars and dynamic buckets) directly into the `VARIANT` binary payload, keeping the target Parquet schema clean and queryable.

3. **`VariantTransformer`**:
   Vectorized transformation pass that inspects semi-structured columns. If `variant_encoding` is enabled, transforms these into Arrow `StructArray`s (`metadata: Binary`, `value: Binary`) using pre-allocated reusable scratch buffers.

4. **`PartitionManager`**:
   Evaluates partition keys using Arrow compute temporal kernels (`arrow::compute::kernels::temporal`). Splits heterogeneous batches across target partitions via boolean masks (`arrow::compute::filter`). Enforces the `GlobalMemoryTracker`.

5. **`PartitionWriter` (Decoupled Sync Encoder + Async Uploader)**:
   * **Encoder**: A synchronous `parquet::arrow::ArrowWriter` runs inside a `tokio::task::spawn_blocking` pool to prevent starving the async reactor with heavy ZSTD compression. It pushes bytes via an in-memory channel.
   * **Uploader**: An async task reads the channel and streams to `object_store::WriteMultipart`. 
   * **S3 5MB Coalescing Requirement**: The uploader strictly uses `WriteMultipart` (which buffers into $\ge$ 5MB chunks) rather than raw `put_part()` calls. This prevents HTTP 400 `EntityTooSmall` errors from S3 when the sync writer flushes small Parquet pages.

---

## 5. Parquet Physical Layout & VARIANT Specification

### 5.1 Arrow v59 Compatibility & Parquet Schema

Semi-structured columns are built as an Arrow `StructArray`:
* `DataType::Struct(vec![Field::new("metadata", DataType::Binary, false), Field::new("value", DataType::Binary, false)])`

**Ecosystem Constraint (Arrow v59)**:
* **Fallback Behavior**: The sink explicitly injects Arrow Extension Metadata (`ARROW:extension:name = "variant"`). However, until upstream logic lands, the `parquet` writer will emit a standard Parquet `Struct`. Downstream engines seamlessly read this as a shredded struct, maintaining performance while awaiting full upstream logical type support.

### 5.2 Zero-Allocation Scratch Buffer Strategy & Sorting Constraint
To eliminate heap allocations in the hot ingestion path:
* Worker tasks hold thread-local reusable scratch buffers (`SmallVec<u8, 512>` for `metadata` and `SmallVec<u8, 2048>` for `value`).
* **Lexicographical Sorting**: The Apache Parquet Variant specification strictly mandates that string keys in the `metadata` dictionary must be sorted. After extracting keys (e.g. using `FxIndexSet`), the encoder performs an in-place lexicographical sort before finalizing the `metadata` binary payload. This guarantees $O(\log N)$ binary search capability for downstream query engines.

### 5.3 Selectable Compression & Writer Configuration
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    Zstd { level: Option<i32> },
    Snappy,
    Lz4Raw,
    Gzip,
    Uncompressed,
}
```
* **Data Page Version**: `DataPageVersion::V2`.
* **Dictionary Encoding**: Enabled for string columns with cardinality $< 100,000$.
* **Bloom Filters**: Enabled on `trace_id` and `span_id`.
* **Row Group Size**: Configurable default `64 MB`.

---

## 6. Multi-Node Collision Avoidance & Atomic Visibility

### 6.1 Collision-Free Distributed Naming
All files written use a collision-proof naming template:
`{partition_prefix}/{timestamp_nano}_{node_id}_{uuidv7}_{sequence:04}.parquet`

### 6.2 Atomic Visibility & Orphan Prevention (The Async `Drop` Pitfall)
* **Cloud Object Stores (`s3://`, `gcs://`, `azblob://`)**:
  Uploads use `object_store::WriteMultipart`. The file becomes visible if and only if `complete().await` succeeds.
  * **Orphan Prevention**: Rust does not support asynchronous `Drop`. If a node panics or a task is cancelled, the `MultipartUpload` could leak uncommitted chunks in S3. The `PartitionWriter` uses a custom `Drop` guard that spins up a fire-and-forget `tokio::spawn` task to execute `multipart.abort().await` in the background, guaranteeing S3 hygiene.
* **Local Filesystems (`file://`)**: 
  The sink treats the `LocalFileSystem` instance identically to S3. Native atomic transactions (internal temp files and POSIX renames) are handled automatically by `object_store::local` when `complete().await` is called.

---

## 7. Partitioning, Rolling & Memory Management

### 7.1 Partition Path Routing
Hive-style paths (`signal={signal}/date={YYYY-MM-DD}/hour={HH}/`) evaluated via zero-copy temporal date/hour extraction kernels.

### 7.2 File Rolling Triggers (Data-Driven & Idle Sweep)
A partition writer rolls when:
1. **Size Limit**: Uncompressed buffer size exceeds `max_file_size_bytes` (default: 64 MB).
2. **Time Window**: Wall-clock time since the file was opened exceeds `max_file_interval_sec` (default: 60s).
3. **Record Count**: Exceeds `max_records` (default: 500,000).

* **Idle Sweep Ticker**: The `PartitionManager` spawns a background `tokio::time::interval` ticker that periodically sweeps the active writer pool and forces a flush on expired idle partitions even when no new data arrives.

### 7.3 Bounded Memory & Global Memory Tracker
* **`GlobalMemoryTracker`**: Tracks aggregate buffer sizes across all open writers. If total memory approaches `global_memory_limit_bytes` (default: 1 GB), the tracker forcefully evicts the largest/oldest partition writers.
* **File Rolling Consequence**: Evicting a writer to reclaim memory necessitates closing and completing the Parquet file. Under heavy memory pressure, this forces the generation of smaller files (e.g., 5 MB instead of 64 MB). Downstream background compaction is highly recommended.
* **Channel Backpressure**: If object storage writes stall, writer buffers fill and trip the global memory limit. The upstream OTLP network layer consequently returns HTTP 503 / `UNAVAILABLE`. No data is silently dropped.

---

## 8. Configuration Reference

```toml
[sink.parquet]
type = "parquet"
enabled = true
storage_uri = "s3://otel-datalake-production/telemetry"
node_id = "collector-pod-01"

variant_encoding = true
compression = "zstd"
compression_level = 3

max_file_size_bytes = 67108864   # 64 MB
max_file_interval_sec = 60       # 1 minute
max_records = 500000

max_open_partitions = 16
global_memory_limit_bytes = 1073741824 # 1 GB
partition_pattern = "signal={signal}/date={date}/hour={hour}"

[sink.parquet.storage_options]
aws_region = "us-east-1"
```

---

## 9. Error Handling & Zero-Panic Safety

In strict compliance with `AGENTS.md`:

```rust
#[derive(thiserror::Error, Debug)]
pub enum ParquetSinkError {
    #[error("Object store error for path '{path}': {source}")]
    ObjectStore { path: String, #[source] source: object_store::Error },
    #[error("Parquet writing failure: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow array processing error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    // ... VariantEncoding, Config, Internal
}
```

---

## 10. Observability & Telemetry

Emits Prometheus/OpenTelemetry metrics adhering to `docs/instrumentation.md`:
* `datalake.sink.parquet.records_written_total` (counter, by `signal`, `compression`)
* `datalake.sink.parquet.bytes_written_total` (counter)
* `datalake.sink.parquet.files_committed_total` (counter, by `partition`)
* `datalake.sink.parquet.active_partitions` (gauge)
* `datalake.sink.parquet.upload_duration_seconds` (histogram)

---

## 11. Verification & Testing Plan

1. **Unit Tests**:
   * `test_variant_binary_encoding_sorted_keys`: Validates Arrow `StructArray` creation and strict lexicographical dictionary sorting.
   * `test_collision_free_naming`: Validates UUIDv7 uniqueness and ordering.
   * `test_global_memory_eviction`: Simulates memory pressure and asserts early forced rolling.
2. **Integration Tests**:
   * `test_s3_5mb_coalescing`: Asserts that small flushes are correctly buffered to $\ge$ 5MB before hitting the mock object store.
   * `test_idle_partition_sweep`: Asserts the background ticker closes stale writers without new incoming data.
   * `test_multipart_upload_abort`: Induces failure mid-upload and asserts `tokio::spawn` background abort runs successfully.
