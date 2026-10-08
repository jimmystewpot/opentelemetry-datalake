# Architecture Design Specification: Parquet Streaming Sink with VARIANT Support

- **Document ID**: SPEC-2026-10-09-PARQUET-VARIANT-SINK
- **Status**: Draft (Approved in Brainstorming)
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
* **Bounded Resource Usage**: LRU-evicted active partition pool and backpressure propagation back to OTLP ingestion.
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
   Evaluates partition keys from the record timestamp column using Arrow compute temporal kernels (`arrow::compute::kernels::temporal`). Splits heterogeneous batches across target partitions via boolean masks (`arrow::compute::filter`). Manages an LRU pool of active `AsyncPartitionWriter`s bounded by `max_open_partitions`.

5. **`AsyncPartitionWriter`**:
   Wraps an active `parquet::arrow::async_writer::AsyncArrowWriter` streaming bytes into a cloud `MultipartUpload` stream or a hidden atomic local staging file (`.{name}.tmp`).

---

## 5. Parquet Physical Layout & VARIANT Specification

### 5.1 Parquet VARIANT Binary Layout

Semi-structured columns are written as a Parquet Group annotated with the `VARIANT` logical type:

```text
optional group attributes (VARIANT) {
  required binary metadata;
  required binary value;
}
```

In Apache Arrow, this is represented as:
* `DataType::Struct(vec![Field::new("metadata", DataType::Binary, false), Field::new("value", DataType::Binary, false)])`

#### Binary Encodings
* **`metadata`**:
  * Byte 0: Version (`0x01`).
  * Byte 1..N: Sorted dictionary containing unique string keys present in the record. Offset dictionary enables $O(\log K)$ or direct array indexing for field lookup.
* **`value`**:
  * Header Byte: Upper 3 bits indicate basic type family (Primitive, Object, Array); lower 5 bits indicate physical subtype or size width.
  * Payloads:
    * `Null`, `BooleanTrue`, `BooleanFalse` (0 bytes payload).
    * `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64` (fixed width).
    * `String` (length-prefixed or offset).
    * `Object`: Count of elements, key offset table, value offset table, followed by recursively encoded value entries.

### 5.2 Zero-Allocation Scratch Buffer Strategy

To eliminate heap allocations in the hot ingestion path:
* Each worker task holds thread-local reusable scratch buffers (`SmallVec<u8, 512>` for `metadata` and `SmallVec<u8, 2048>` for `value`).
* Keys and offsets are indexed using a task-local fast hash set (`FxIndexSet<String>`).
* Serialized bytes are appended directly to Arrow's native `BinaryBuilder::append_value(&slice)` without intermediate heap allocations.

### 5.3 Selectable Compression & Writer Configuration

Compression is configurable per sink instance to satisfy heterogeneous reader requirements:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// Modern default: high ratio and fast decompression.
    Zstd { level: Option<i32> },
    /// Maximum legacy compatibility and lowest CPU overhead.
    Snappy,
    /// Fast block compression.
    Lz4Raw,
    /// Standard compatibility.
    Gzip,
    /// Zero CPU compression for high-bandwidth networks.
    Uncompressed,
}
```

#### Physical Writer Properties:
* **Data Page Version**: `DataPageVersion::V2` (byte-level index navigation).
* **Dictionary Encoding**: Enabled for string columns with cardinality $< 100,000$.
* **Bloom Filters**: Enabled on `trace_id` (16 bytes) and `span_id` (8 bytes) with target false positive probability $p = 0.01$.
* **Statistics**: `Statistics::Page` (min/max and null counts written into page headers for instant predicate pushdown).
* **Row Group Size**: Configurable between 64 MB and 128 MB.

---

## 6. Multi-Node Collision Avoidance & Atomic Visibility

### 6.1 Collision-Free Distributed Naming
All files written to a partition prefix use a collision-proof naming template:

```text
{partition_prefix}/{timestamp_nano}_{node_id}_{uuidv7}_{sequence:04}.parquet
```

* **`timestamp_nano`**: 64-bit UTC timestamp in nanoseconds ensuring natural time-ordered object listing.
* **`node_id`**: Configurable worker identifier (e.g. Kubernetes pod name `otel-collector-7f8d6-4g2k` or hostname).
* **`uuidv7`**: 128-bit time-ordered UUID providing cryptographic entropy. Probability of collision between arbitrary nodes at the same nanosecond is $< 2^{-122}$.
* **`sequence`**: Atomic monotonic 16-bit counter per partition inside each node.

### 6.2 Atomic Visibility
* **Cloud Object Stores (`s3://`, `gcs://`, `azblob://`)**:
  Uploads use `object_store::MultipartUpload`. While the file is open, raw data parts are uploaded uncommitted. The file is made visible in the bucket if and only if `complete().await` succeeds upon file roll or shutdown.
* **Local & Shared Filesystems (`file://`)**:
  Writers stream into a hidden temporary file `.{filename}.tmp`. Upon file roll, the sink executes an atomic POSIX rename (`std::fs::rename`) to `{filename}.parquet`.

---

## 7. Partitioning, Rolling & Memory Management

### 7.1 Partition Path Routing
Partitions follow Hive-style path conventions:
```text
{base_path}/signal={signal}/date={YYYY-MM-DD}/hour={HH}/
```
Values are evaluated from the Arrow `timestamp` column using zero-copy temporal date/hour extraction kernels.

### 7.2 File Rolling Triggers
An active partition writer rolls and finalizes a Parquet file when any of the following triggers are met:
1. **Size Limit**: Uncompressed or compressed buffer size exceeds `max_file_size_bytes` (default: 128 MB).
2. **Time Window**: Wall-clock time since the file was opened exceeds `max_file_interval_sec` (default: 60s).
3. **Record Count**: Total records written to the file exceeds `max_records` (default: 500,000).

### 7.3 Bounded Memory & Backpressure
* **`max_open_partitions`** (default: 64): If incoming records belong to more than 64 unique time partitions, the least recently updated writer is evicted from the active pool and cleanly finalized.
* **Bounded Channel & Backpressure**: The sink pulls batches from a bounded `PipelineReceiver` (mpsc channel). If network or storage upload bandwidth is throttled, internal writer buffers fill, halting sink channel polling. The upstream channel fills, and the OTLP receiver returns HTTP 503 / gRPC `UNAVAILABLE` to collectors. No data is lost, and memory usage remains strictly bounded.

---

## 8. Configuration Reference

```toml
[sink.parquet]
type = "parquet"
enabled = true

# Storage Target (Local path or object store URI)
# Examples:
#   "file:///var/data/telemetry"
#   "s3://my-datalake-bucket/telemetry"
#   "gcs://my-datalake-bucket/telemetry"
storage_uri = "s3://otel-datalake-production/telemetry"

# Node identity for collision-free naming (defaults to HOSTNAME or pod name)
node_id = "collector-pod-01"

# Semi-structured encoding
variant_encoding = true

# Parquet compression (zstd, snappy, lz4_raw, gzip, uncompressed)
compression = "zstd"
compression_level = 3

# File Rolling Configuration
max_file_size_bytes = 134217728  # 128 MB
max_file_interval_sec = 60       # 1 minute
max_records = 500000

# Partition Management
max_open_partitions = 64
partition_pattern = "signal={signal}/date={date}/hour={hour}"

# Object Store Credentials and Options (passed to object_store builder)
[sink.parquet.storage_options]
aws_region = "us-east-1"
aws_endpoint = "http://rustfs.storage.internal:9000"
aws_allow_http = "true"
```

---

## 9. Error Handling & Zero-Panic Safety

In compliance with `AGENTS.md`, the implementation forbids `unwrap()`, `expect()`, `panic!()`, and `todo!()` in all production paths.

### Domain Error Model

```rust
#[derive(thiserror::Error, Debug)]
pub enum ParquetSinkError {
    #[error("Object store error for path '{path}': {source}")]
    ObjectStore {
        path: String,
        #[source]
        source: object_store::Error,
    },

    #[error("Parquet writing failure: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow array processing error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Variant encoding error: {0}")]
    VariantEncoding(String),

    #[error("Configuration validation error: {0}")]
    Config(String),

    #[error("Internal pipeline failure: {0}")]
    Internal(String),
}
```

---

## 10. Observability & Telemetry

The sink emits Prometheus/OpenTelemetry metrics adhering to `docs/instrumentation.md`:

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `datalake.sink.parquet.records_written_total` | Counter | `signal`, `compression` | Total telemetry records serialized into Parquet |
| `datalake.sink.parquet.bytes_written_total` | Counter | `signal` | Total raw bytes committed to storage |
| `datalake.sink.parquet.files_committed_total` | Counter | `signal`, `partition` | Total completed Parquet files committed |
| `datalake.sink.parquet.active_partitions` | Gauge | `signal` | Current count of open partition writers |
| `datalake.sink.parquet.upload_duration_seconds` | Histogram | `target` | Latency distribution of file upload completions |
| `datalake.sink.parquet.evictions_total` | Counter | `reason` | Count of LRU partition writer evictions |

---

## 11. Verification & Testing Plan

1. **Unit Tests**:
   * `test_variant_binary_encoding`: Validates that OTel attributes encode into valid `metadata` and `value` binary arrays conforming to the Variant specification.
   * `test_collision_free_naming`: Generates 1,000,000 file names across 50 simulated concurrent threads, asserting zero collisions and strict lexicographical monotonicity.
   * `test_vectorized_partition_routing`: Verifies multi-hour timestamps in a single `RecordBatch` are split into correct sub-batches without data loss.

2. **Integration Tests**:
   * `test_local_filesystem_sink`: Writes simulated Logs, Metrics, and Traces to a temporary directory; reads back using `parquet::arrow::arrow_reader::ParquetRecordBatchReader` and validates schema and values.
   * `test_s3_compatible_streaming`: Spins up a local S3-compatible test server (e.g. RustFS/MinIO); tests multi-part streaming, rolling by size and time, and graceful drain.
   * `test_selectable_compression`: Encodes batches using `Zstd`, `Snappy`, `Lz4Raw`, and `Gzip`, verifying that downstream readers decompress without error.
   * `test_lru_partition_eviction`: Sends telemetry across 128 distinct hourly partitions with `max_open_partitions = 16`, asserting all 128 files are committed cleanly with zero memory leaks or unclosed multipart uploads.

3. **Compatibility & Quality Gates**:
   * `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
   * `cargo fmt --check`
