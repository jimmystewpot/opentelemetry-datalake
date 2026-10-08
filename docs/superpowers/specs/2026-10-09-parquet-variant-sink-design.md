# Architecture Design Specification: Parquet Streaming Sink with VARIANT Support

- **Document ID**: SPEC-2026-10-09-PARQUET-VARIANT-SINK
- **Status**: Draft (Updated post-Principal Review)
- **Author**: Principal Software Engineer
- **Date**: 2026-10-09
- **Target Workspace**: `crates/parquet-sink`

---

## 1. Executive Summary

This specification defines the architecture, storage abstractions, data layout, and runtime guarantees for a new high-throughput streaming Parquet sink (`parquet-sink`) within `opentelemetry-datalake`.

The sink streams OpenTelemetry signals (Logs, Metrics, Traces) directly from Arrow `RecordBatch` streams into modern Parquet files residing on cloud object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or S3-compatible systems like RustFS) or local/shared POSIX filesystems.

To maximize read efficiency and query performance across analytical engines (Snowflake, Databricks/Spark, DuckDB, ClickHouse, and StarRocks), the sink implements the **Apache Parquet / Spark / Iceberg Variant binary format** (`group (VARIANT)` with binary `metadata` and `value`), enabling sub-field pruning without runtime JSON string parsing overhead.

---

## 2. Problem Statement & Motivation

Currently, semi-structured telemetry data (OTel `attributes`, `resource.attributes`, and log `body`) is converted to JSON-encoded strings. While simple, stringified JSON in columnar storage has severe drawbacks:
1. **Query Scan Degradation**: Downstream query engines must scan and parse full JSON text strings row-by-row using CPU-expensive scalar functions (`get_json_object`), preventing vectorized evaluation.
2. **Storage Inefficiency**: JSON keys and delimiters are repeatedly stored as uncompressed or redundantly compressed strings, inflating data volume.
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
* **ACID Table Catalog Commit**: This sink writes raw partitioned Parquet files for direct engine scanning or external tables. Catalog management (e.g. Iceberg catalog commits) is handled by `crates/storage`.
* **In-Sink Compaction**: Small-file compaction is delegated to asynchronous background compactor jobs.

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
                         │  • AsyncArrowWriter      │             │  • AsyncArrowWriter      │
                         │  • tokio::spawn_blocking │             │  • tokio::spawn_blocking │
                         │  • MultipartUploadPipe   │             │  • MultipartUploadPipe   │
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
   Main entry point consuming `SignalBatch`es from the async Tokio mpsc channel. Manages rolling timers, cancellation tokens, and graceful draining upon `SIGTERM`.

2. **`SignalRouter`**:
   Extracts `RecordBatch` by signal type (`Logs`, `Metrics`, `Traces`), matching incoming schemas against the pre-compiled Parquet target schemas.

3. **`VariantTransformer`**:
   Vectorized transformation pass that inspects semi-structured columns (`attributes`, `resource_attributes`, `body`). If `variant_encoding` is enabled, transforms these columns into Arrow `StructArray`s (`metadata: Binary`, `value: Binary`) using pre-allocated reusable scratch buffers.

4. **`PartitionManager`**:
   Evaluates partition keys from the record timestamp column using Arrow compute temporal kernels (`arrow::compute::kernels::temporal`). Splits heterogeneous batches across target partitions via boolean masks (`arrow::compute::filter`). Manages an LRU pool of active `AsyncPartitionWriter`s and enforces the `GlobalMemoryTracker`.

5. **`AsyncPartitionWriter`**:
   Wraps an active `parquet::arrow::async_writer::AsyncArrowWriter` streaming bytes into a cloud `MultipartUpload` stream or a hidden atomic local staging file (`.{name}.tmp`). 
   * **Crucial Detail**: Parquet compression and row-group encoding are CPU-intensive. Calls to `writer.write()` and `writer.close()` MUST be wrapped in `tokio::task::spawn_blocking` to avoid stalling the async Tokio reactor thread.

---

## 5. Parquet Physical Layout & VARIANT Specification

### 5.1 Parquet VARIANT Binary Layout & Arrow v59 Compatibility

Semi-structured columns are written as a Parquet Group. In Apache Arrow, this is constructed as a `StructArray`:
* `DataType::Struct(vec![Field::new("metadata", DataType::Binary, false), Field::new("value", DataType::Binary, false)])`

**Ecosystem Constraint (Arrow v59)**: Native, automatic `VARIANT` logical typing is experimental. To ensure Parquet readers (Spark, Snowflake) recognize the group as a `VARIANT`, the `VariantTransformer` explicitly injects Arrow Extension Metadata into the struct's `Field`:
* `ARROW:extension:name = "variant"` (or the appropriate Iceberg/Spark variant tag).
This forces the Arrow-to-Parquet writer to map it to the requested logical schema.

### 5.2 Zero-Allocation Scratch Buffer Strategy
To eliminate heap allocations in the hot ingestion path:
* Each worker task holds thread-local reusable scratch buffers (`SmallVec<u8, 512>` for `metadata` and `SmallVec<u8, 2048>` for `value`).
* Keys and offsets are indexed using a task-local fast hash set (`FxIndexSet<String>`).
* Serialized bytes are appended directly to Arrow's native `BinaryBuilder::append_value(&slice)`.

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
* **Bloom Filters**: Enabled on `trace_id` and `span_id` with target false positive probability $p = 0.01$.
* **Statistics**: `Statistics::Page`.
* **Row Group Size**: Configurable default `64 MB`.

---

## 6. Multi-Node Collision Avoidance & Atomic Visibility

### 6.1 Collision-Free Distributed Naming
All files written to a partition prefix use a collision-proof naming template:
`{partition_prefix}/{timestamp_nano}_{node_id}_{uuidv7}_{sequence:04}.parquet`

### 6.2 Atomic Visibility & Orphan Prevention
* **Cloud Object Stores (`s3://`, `gcs://`, `azblob://`)**:
  Uploads use `object_store::MultipartUpload`. The file becomes visible if and only if `complete().await` succeeds.
  * **Orphan Prevention**: If a node crashes, the `AsyncPartitionWriter` is dropped, or an unrecoverable upload error occurs, the implementation MUST explicitly call `multipart.abort().await`. A `Drop` guard or safe error-handling block ensures hidden, uncommitted chunks do not accumulate in S3 and cause billing leaks.
* **Local Filesystems (`file://`)**: Streams into `.{filename}.tmp` and executes an atomic POSIX `rename()` upon rolling.

---

## 7. Partitioning, Rolling & Memory Management

### 7.1 Partition Path Routing
Hive-style paths (`signal={signal}/date={YYYY-MM-DD}/hour={HH}/`) evaluated via zero-copy temporal date/hour extraction kernels.

### 7.2 File Rolling Triggers (Data-Driven & Idle Sweep)
An active partition writer rolls when:
1. **Size Limit**: Uncompressed buffer size exceeds `max_file_size_bytes` (default: 64 MB).
2. **Time Window**: Wall-clock time since the file was opened exceeds `max_file_interval_sec` (default: 60s).
3. **Record Count**: Exceeds `max_records` (default: 500,000).

* **Idle Sweep Ticker**: Because stream-processing systems only evaluate triggers when new data arrives, idle partitions (e.g., an expired hour) can hang indefinitely. The `PartitionManager` spawns a background `tokio::time::interval` ticker that periodically sweeps the active writer pool and forces a flush on expired idle partitions.

### 7.3 Bounded Memory & Global Memory Tracker
To resolve mathematical OOM risks (e.g., $N$ open partitions $\times$ 128 MB row groups $\gg$ system RAM):
* **`GlobalMemoryTracker`**: Tracks aggregate buffer sizes across all open writers. If total memory exceeds `global_memory_limit_bytes` (default: 1 GB), the tracker forcefully flushes the largest/oldest row groups across the pool, overriding per-writer limits.
* **`max_open_partitions`** (default: 16): If the cardinality of time windows exceeds 16, the coldest writer is cleanly evicted and finalized.
* **Channel Backpressure**: If object storage writes stall, writer buffers fill and trip the global memory limit. The sink stops polling `PipelineReceiver`. The OTLP network layer consequently returns HTTP 503 / `UNAVAILABLE`. No data is silently dropped.

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
* **Drop Safety**: The sink guarantees `abort().await` is invoked on any failed `MultipartUpload` to satisfy cloud hygiene requirements.

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
   * `test_variant_binary_encoding`: Validates Arrow `StructArray` with Extension Metadata maps correctly to Variant.
   * `test_collision_free_naming`: Validates UUIDv7 uniqueness and ordering.
   * `test_global_memory_eviction`: Simulates memory pressure and asserts early forced flushing.
2. **Integration Tests**:
   * `test_idle_partition_sweep`: Asserts the background ticker closes stale writers without new incoming data.
   * `test_multipart_upload_abort`: Induces failure mid-upload and asserts `abort` API was called.
   * `test_spawn_blocking_offload`: Validates Tokio executor remains responsive during heavy ZSTD compression.
