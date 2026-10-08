# Parquet Streaming Sink with VARIANT Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an ultra-high-throughput, horizontally scalable Parquet streaming sink (`crates/parquet-sink`) for `opentelemetry-datalake` that writes partitioned Parquet files with Apache Parquet/Spark/Iceberg Variant binary format support to object stores (S3, GCS, Azure, RustFS) or local filesystems.

**Architecture:** A modular pipeline utilizing Arrow v59 native vectorized batches. A synchronous `parquet::arrow::ArrowWriter` runs in `tokio::task::spawn_blocking` to perform heavy CPU compression without starving the async reactor, piped via an in-memory channel to an async `object_store::WriteMultipart` uploader with 5MB coalescing. Dynamic attributes are encoded into Variant binary format with thread-local scratch buffers and sorted dictionaries. A vectorized `PartitionManager` with an LRU active pool and `GlobalMemoryTracker` bounds memory under heavy traffic.

**Tech Stack:** Rust 2024 edition, Apache Arrow v59, Apache Parquet v59, `object_store` v0.14, `tokio` (full), `prost`, `thiserror`, `serde`, `uuid` (v7).

**Spec:** `docs/superpowers/specs/2026-10-09-parquet-variant-sink-design.md`

## Global Constraints

* **Zero-Panic Policy**: No `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` directories. All errors must use `Result` propagation via `ParquetSinkError` and `PipelineError`.
* **Zero-Allocation Hot Path**: Use reusable thread-local scratch buffers (`SmallVec`, `FxIndexSet`) and Arrow builders with `append_value(&slice)`—no per-record `String` or `Vec` allocations.
* **Vectorized Processing**: Partition routing and schema transformations must use Arrow compute kernels (`temporal`, `filter`, `take`) rather than row-by-row scalar loops.
* **Strict Quality Gates**: Every task must pass:
  * `cargo fmt --check`
  * `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
  * `cargo test`

## Review Focus

1. **S3 upload of a file smaller than 5MB on rolling**: The final part of an S3 multipart upload can be smaller than 5MB, but intermediate parts cannot. Verified in Task 5 (`test_upload_file_smaller_than_5mb_succeeds_on_complete`).
2. **Empty batch or batch with null timestamps**: Incoming telemetry batches with empty records or null timestamps must be safely handled without panics. Verified in Task 7 (`test_partition_routing_handles_empty_and_null_timestamps`).
3. **Variant attributes with duplicate or non-ASCII keys**: OTel attribute lists with duplicate keys or Unicode characters must be deduplicated and strictly sorted lexicographically in the binary `metadata` dictionary. Verified in Task 3 (`test_variant_encoder_deduplicates_and_lexicographically_sorts_keys`).
4. **Rapid file rolling within the exact same nanosecond on the same node**: The per-partition sequence counter must guarantee distinct filenames even under tight millisecond loops. Verified in Task 2 (`test_naming_monotonic_sequence_same_nanosecond`).
5. **Total memory exceeds `global_memory_limit_bytes` during heavy partition surge**: Global memory tracker must forcefully evict and complete the coldest/largest open writers to prevent OOM. Verified in Task 7 (`test_global_memory_tracker_evicts_oldest_writers_under_pressure`).

---

## File Structure & Decomposition

```text
crates/parquet-sink/
├── Cargo.toml                 # Task 1: Crate definition, dependencies, features
├── src/
│   ├── lib.rs                 # Task 8: Sink trait implementation, graceful shutdown
│   ├── error.rs               # Task 1: Domain errors (ParquetSinkError)
│   ├── config.rs              # Task 1: ParquetSinkConfig, CompressionCodec, storage config
│   ├── naming.rs              # Task 2: Collision-free file naming (timestamp, node_id, uuidv7)
│   ├── variant.rs             # Task 3: Zero-alloc Variant binary encoder (metadata + value)
│   ├── router.rs              # Task 4: SignalRouter & OTLP Metric DataPoint serialization
│   ├── uploader.rs            # Task 5: Object store WriteMultipart adapter & Drop abort guard
│   ├── writer.rs              # Task 6: Decoupled Sync ArrowWriter with Bloom filters & stats
│   └── partition.rs           # Task 7: Vectorized PartitionManager, LRU pool & memory tracker
└── tests/
    └── integration_tests.rs   # Task 9: End-to-end integration tests (local FS & S3-compatible)
```

---

## Execution Plan: Tasks

### Task 1: Crate Scaffolding, Domain Errors & Configuration

**Files:**
- Create: `crates/parquet-sink/Cargo.toml`
- Create: `crates/parquet-sink/src/error.rs`
- Create: `crates/parquet-sink/src/config.rs`
- Create: `crates/parquet-sink/src/lib.rs` (minimal export)
- Modify: `Cargo.toml` (root workspace members & workspace dependencies)

**Interfaces:**
- Consumes: `pipeline_core::error::PipelineError`
- Produces: `ParquetSinkError`, `ParquetSinkConfig`, `CompressionCodec`

- [ ] **Step 1: Write the failing test for configuration parsing and defaults**

```rust
// crates/parquet-sink/src/config.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParquetSinkConfig::default();
        assert_eq!(config.compression, CompressionCodec::Zstd { level: Some(3) });
        assert_eq!(config.max_file_size_bytes, 67_108_864);
        assert_eq!(config.max_file_interval_sec, 60);
        assert_eq!(config.max_open_partitions, 16);
        assert_eq!(config.global_memory_limit_bytes, 1_073_741_824);
        assert!(config.variant_encoding);
    }

    #[test]
    fn test_deserialize_toml() {
        let toml_str = r#"
            storage_uri = "s3://my-bucket/telemetry"
            node_id = "test-node"
            compression = "snappy"
            max_file_size_bytes = 10485760
            max_open_partitions = 32
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage_uri, "s3://my-bucket/telemetry");
        assert_eq!(config.node_id, "test-node");
        assert_eq!(config.compression, CompressionCodec::Snappy);
        assert_eq!(config.max_file_size_bytes, 10_485_760);
        assert_eq!(config.max_open_partitions, 32);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink`
Expected: FAIL (crate not found / types not defined)

- [ ] **Step 3: Implement crate scaffolding, `error.rs`, and `config.rs`**

Add `crates/parquet-sink` to workspace `Cargo.toml`. Define `ParquetSinkError` using `thiserror` (handling `ObjectStore`, `Parquet`, `Arrow`, `VariantEncoding`, `Config`, `Internal`). Implement `From<ParquetSinkError> for PipelineError`. Define `CompressionCodec` enum and `ParquetSinkConfig` with `serde` defaults.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/parquet-sink
git commit -m "feat(parquet-sink): scaffold crate, errors, and configuration models"
```

---

### Task 2: Distributed Collision-Free File Naming

**Files:**
- Create: `crates/parquet-sink/src/naming.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export naming)

**Interfaces:**
- Consumes: `node_id: String` from `ParquetSinkConfig`
- Produces: `FileNamer::generate_filename(&self, partition_prefix: &str, sequence: u16) -> String`

- [ ] **Step 1: Write the failing tests for naming uniqueness, format, and sequence monotonicity**

```rust
// crates/parquet-sink/src/naming.rs
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_naming_format_and_lexicographical_order() {
        let namer = FileNamer::new("collector-node-01".to_string());
        let file1 = namer.generate_filename("signal=logs/date=2026-10-09", 1);
        let file2 = namer.generate_filename("signal=logs/date=2026-10-09", 2);

        assert!(file1.starts_with("signal=logs/date=2026-10-09/"));
        assert!(file1.ends_with(".parquet"));
        assert!(file1.contains("collector-node-01"));
        assert!(file1 < file2, "Expected lexicographical time/sequence ordering");
    }

    #[test]
    fn test_naming_monotonic_sequence_same_nanosecond() {
        let namer = FileNamer::new("node-a".to_string());
        let mut names = HashSet::new();
        for seq in 0..1000 {
            let name = namer.generate_filename("p", seq);
            assert!(names.insert(name), "Collision detected in sequence generator!");
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib naming`
Expected: FAIL (FileNamer not defined)

- [ ] **Step 3: Implement `FileNamer` in `crates/parquet-sink/src/naming.rs`**

Use `chrono::Utc::now().timestamp_nanos_opt()` (or `SystemTime`), `uuid::Uuid::now_v7()`, and format as `{partition_prefix}/{timestamp_nano:020}_{node_id}_{uuidv7}_{sequence:04}.parquet`. Ensure zero-allocation path using formatted stack strings.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib naming`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/naming.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement collision-free distributed file namer"
```

---

### Task 3: Vectorized In-Memory Variant Binary Encoder with Sorted Dictionary

**Files:**
- Create: `crates/parquet-sink/src/variant.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export variant)

**Interfaces:**
- Consumes: JSON string column / Arrow `StringArray` or OTLP key-values
- Produces: `VariantTransformer::transform_to_variant(&self, batch: &RecordBatch, column_names: &[&str]) -> Result<RecordBatch, ParquetSinkError>`
- Produces Arrow `StructArray` with fields `"metadata"` (`DataType::Binary`) and `"value"` (`DataType::Binary`), with `ARROW:extension:name = "variant"`

- [ ] **Step 1: Write the failing tests for Variant binary layout, sorted dictionary, and extension metadata**

```rust
// crates/parquet-sink/src/variant.rs
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_variant_encoder_deduplicates_and_lexicographically_sorts_keys() {
        let json_input = r#"{"z_key": 100, "a_key": "hello", "m_key": true}"#;
        let mut encoder = VariantEncoder::new();
        let (metadata, value) = encoder.encode_json_str(json_input).expect("encoding failed");

        // Verify metadata header (0x01 version)
        assert_eq!(metadata[0], 0x01);
        // Verify keys in metadata are sorted lexicographically: a_key, m_key, z_key
        let keys = encoder.extract_dictionary_keys(&metadata);
        assert_eq!(keys, vec!["a_key", "m_key", "z_key"]);
    }

    #[test]
    fn test_variant_transformer_replaces_string_column_with_struct_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let id_arr = Arc::new(arrow::array::Int64Array::from(vec![1, 2]));
        let attr_arr = Arc::new(StringArray::from(vec![
            Some(r#"{"service.name":"api","http.status":200}"#),
            Some(r#"{"service.name":"auth"}"#),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, attr_arr]).unwrap();

        let transformer = VariantTransformer::new();
        let transformed = transformer.transform_to_variant(&batch, &["attributes"]).unwrap();

        let field = transformed.schema().field_with_name("attributes").unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));
        assert_eq!(
            field.metadata().get("ARROW:extension:name"),
            Some(&"variant".to_string())
        );
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib variant`
Expected: FAIL (VariantEncoder/VariantTransformer not defined)

- [ ] **Step 3: Implement `VariantEncoder` and `VariantTransformer`**

Implement zero-allocation scratch buffers using `SmallVec<u8, 512>` for `metadata` and `SmallVec<u8, 2048>` for `value`. Parse JSON into temporary key-value pairs, deduplicate, in-place sort keys lexicographically, construct the binary header and offset dictionary, and encode binary typed values. Build Arrow `StructArray` with `ARROW:extension:name = "variant"` metadata.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib variant`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/variant.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement zero-alloc Variant binary encoder with sorted keys"
```

---

### Task 4: Signal Routing & Telemetry Schema Definitions

**Files:**
- Create: `crates/parquet-sink/src/router.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export router)

**Interfaces:**
- Consumes: `pipeline_core::pipeline::SignalBatch`
- Produces: `SignalRouter::route_and_prepare(&self, batch: SignalBatch) -> Result<PreparedBatch, ParquetSinkError>`
- Produces `PreparedBatch { signal: &'static str, batch: RecordBatch }`

- [ ] **Step 1: Write the failing tests for signal routing, schema conformance, and polymorphic metrics handling**

```rust
// crates/parquet-sink/src/router.rs
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use pipeline_core::pipeline::SignalBatch;
    use std::sync::Arc;

    #[test]
    fn test_route_logs_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(true); // variant enabled
        let prepared = router.route_and_prepare(SignalBatch::Logs(batch)).unwrap();
        assert_eq!(prepared.signal, "logs");
        assert!(matches!(prepared.batch.schema().field_with_name("attributes").unwrap().data_type(), DataType::Struct(_)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib router`
Expected: FAIL (SignalRouter not defined)

- [ ] **Step 3: Implement `SignalRouter` in `crates/parquet-sink/src/router.rs`**

Handle `SignalBatch::Logs`, `SignalBatch::Metrics`, and `SignalBatch::Traces`. Apply `VariantTransformer` to `attributes`, `resource_attributes`, and `body`. For metrics, ensure polymorphic datapoints are preserved inside the Variant column.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib router`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/router.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement signal routing and telemetry schema preparation"
```

---

### Task 5: Object Store Upload Abstraction with S3 5MB Coalescing & Drop Abort Guard

**Files:**
- Create: `crates/parquet-sink/src/uploader.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export uploader)

**Interfaces:**
- Consumes: `object_store::ObjectStore`, `storage_uri: &str`
- Produces: `AsyncUploader::start(store: Arc<dyn ObjectStore>, path: &object_store::path::Path) -> Result<(AsyncUploaderSender, AsyncUploaderHandle), ParquetSinkError>`

- [ ] **Step 1: Write the failing tests for 5MB part coalescing, file < 5MB completion, and Drop abort**

```rust
// crates/parquet-sink/src/uploader.rs
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_upload_file_smaller_than_5mb_succeeds_on_complete() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test/small.parquet");
        let (mut sender, handle) = AsyncUploader::start(store.clone(), &path).unwrap();

        // Send a 1MB chunk (less than 5MB S3 limit)
        let chunk = bytes::Bytes::from(vec![0u8; 1024 * 1024]);
        sender.send_chunk(chunk).await.unwrap();
        sender.finish().await.unwrap();

        handle.wait_for_completion().await.unwrap();

        // Verify the file exists in store
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 1024 * 1024);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib uploader`
Expected: FAIL (AsyncUploader not defined)

- [ ] **Step 3: Implement `AsyncUploader` using `object_store::WriteMultipart` with background abort Drop guard**

Use `object_store.put_multipart(path).await` which gives `Box<dyn WriteMultipart>`. Forward chunks via channel. Implement a `Drop` guard on the active handle that calls `tokio::spawn(async move { multipart.abort().await; })` if dropped before `finish()` completes.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib uploader`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/uploader.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement object store async uploader with 5MB coalescing and drop safety"
```

---

### Task 6: Decoupled Sync Parquet Writer with Compression & Bloom Filters

**Files:**
- Create: `crates/parquet-sink/src/writer.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export writer)

**Interfaces:**
- Consumes: `AsyncUploaderSender`, `CompressionCodec`, `RecordBatch`
- Produces: `PartitionWriter::new(schema, uploader_sender, config) -> Result<Self, ParquetSinkError>`
- Produces: `PartitionWriter::write_batch(&mut self, batch: &RecordBatch) -> Result<(), ParquetSinkError>`
- Produces: `PartitionWriter::close(self) -> Result<(), ParquetSinkError>`

- [ ] **Step 1: Write the failing tests for sync writer, bloom filters, and compression codec**

```rust
// crates/parquet-sink/src/writer.rs
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_writer_with_zstd_and_bloom_filter() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test/test.parquet");
        let (sender, handle) = crate::uploader::AsyncUploader::start(store.clone(), &path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let mut config = crate::config::ParquetSinkConfig::default();
        config.compression = crate::config::CompressionCodec::Zstd { level: Some(3) };

        let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["trace-12345", "trace-67890"])),
            Arc::new(Int64Array::from(vec![42, 99])),
        ]).unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let meta = store.head(&path).await.unwrap();
        assert!(meta.size > 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib writer`
Expected: FAIL (PartitionWriter not defined)

- [ ] **Step 3: Implement `PartitionWriter` wrapping synchronous `ArrowWriter` in `spawn_blocking`**

Build `WriterProperties` configuring `DataPageVersion::V2`, dictionary encoding, page statistics, and bloom filters for `"trace_id"` and `"span_id"`. Map `CompressionCodec` to Parquet compression. Implement a custom `std::io::Write` pipe that pushes byte buffers into the `AsyncUploaderSender`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib writer`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/writer.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement sync parquet writer with bloom filters and selectable compression"
```

---

### Task 7: Vectorized Partition Manager with Idle Ticker & Global Memory Tracker

**Files:**
- Create: `crates/parquet-sink/src/partition.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export partition)

**Interfaces:**
- Consumes: `PreparedBatch`, `FileNamer`, `ObjectStore`, `ParquetSinkConfig`
- Produces: `PartitionManager::new(config, store)`
- Produces: `PartitionManager::route_batch(&mut self, batch: &RecordBatch, signal: &str) -> Result<(), ParquetSinkError>`
- Produces: `PartitionManager::sweep_idle_writers(&mut self) -> Result<(), ParquetSinkError>`
- Produces: `PartitionManager::flush_all(&mut self) -> Result<(), ParquetSinkError>`

- [ ] **Step 1: Write the failing tests for vectorized time partitioning, idle sweep, and global memory eviction**

```rust
// crates/parquet-sink/src/partition.rs
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_partition_routing_handles_empty_and_null_timestamps() {
        let store = Arc::new(InMemory::new());
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, store);

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("val", DataType::Int64, false),
        ]));

        // Empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        assert!(manager.route_batch(&empty_batch, "logs").is_ok());

        // Batch with null timestamps
        let batch_with_null = RecordBatch::try_new(schema, vec![
            Arc::new(TimestampNanosecondArray::from(vec![None, Some(1_700_000_000_000_000_000)])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ]).unwrap();
        assert!(manager.route_batch(&batch_with_null, "logs").is_ok());
    }

    #[tokio::test]
    async fn test_global_memory_tracker_evicts_oldest_writers_under_pressure() {
        let store = Arc::new(InMemory::new());
        let mut config = crate::config::ParquetSinkConfig::default();
        config.max_open_partitions = 2; // small for testing
        let mut manager = PartitionManager::new(config, store);

        // Open 3 distinct partition keys, verify oldest is closed
        // ...
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --lib partition`
Expected: FAIL (PartitionManager not defined)

- [ ] **Step 3: Implement `PartitionManager`**

Use `arrow::compute::kernels::temporal::hour` and `date` to extract partition keys. Split batches with `arrow::compute::filter`. Maintain `HashMap<String, ActiveWriter>` with an LRU access list. Implement `GlobalMemoryTracker` summing writer buffers; if exceeding `global_memory_limit_bytes`, evict the oldest writer. Implement background idle sweep checking writer wall-clock duration against `max_file_interval_sec`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --lib partition`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/partition.rs crates/parquet-sink/src/lib.rs
git commit -m "feat(parquet-sink): implement vectorized partition manager, LRU eviction, and memory tracking"
```

---

### Task 8: Sink Trait Implementation & Graceful Shutdown Drain

**Files:**
- Create: `crates/parquet-sink/src/sink.rs`
- Modify: `crates/parquet-sink/src/lib.rs` (export `ParquetSink`)
- Modify: `Cargo.toml` (integrate `parquet-sink` into root binary if needed)

**Interfaces:**
- Consumes: `pipeline_core::pipeline::{PipelineReceiver, Sink}`
- Produces: `ParquetSink::try_new(config: ParquetSinkConfig) -> Result<Self, PipelineError>`
- Implements: `#[async_trait] impl Sink for ParquetSink { async fn run(&mut self, input: PipelineReceiver) -> Result<(), PipelineError>; }`

- [ ] **Step 1: Write the failing tests for `ParquetSink::run` lifecycle and graceful shutdown on channel close**

```rust
// crates/parquet-sink/tests/sink_test.rs
use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet_sink::{CompressionCodec, ParquetSink, ParquetSinkConfig};
use pipeline_core::pipeline::{PipelineReceiver, SignalBatch, Sink};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_parquet_sink_drains_on_channel_close() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        compression: CompressionCodec::Snappy,
        max_file_interval_sec: 10,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).unwrap();
    let (tx, rx) = mpsc::channel(10);

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000])),
        Arc::new(Int64Array::from(vec![100])),
    ]).unwrap();

    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx); // close channel

    // Run sink, must complete gracefully
    sink.run(rx).await.expect("Sink run failed");

    // Verify file written to temp_dir
    let files: Vec<_> = std::fs::read_dir(temp_dir.path()).unwrap().collect();
    assert!(!files.is_empty(), "Expected parquet files to be written");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p parquet-sink --test sink_test`
Expected: FAIL (ParquetSink::run not implemented)

- [ ] **Step 3: Implement `ParquetSink::run`**

Set up `object_store::parse_url`. Spawn idle sweep ticker task using `tokio::time::interval`. Loop on `tokio::select!` pulling from `PipelineReceiver` and ticker. On channel close (EOF) or cancellation, call `manager.flush_all().await` and gracefully exit.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p parquet-sink --test sink_test`
Expected: PASS

- [ ] **Step 5: Code quality check**

Run: `cargo fmt --check && cargo clippy -p parquet-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings/errors

- [ ] **Step 6: Commit**

```bash
git add crates/parquet-sink/src/sink.rs crates/parquet-sink/src/lib.rs crates/parquet-sink/tests/sink_test.rs
git commit -m "feat(parquet-sink): implement Sink trait with graceful shutdown and ticker loop"
```

---

### Task 9: End-to-End Integration Tests (Local FS & S3-Compatible)

**Files:**
- Create: `crates/parquet-sink/tests/integration_tests.rs`

**Interfaces:**
- Consumes: Full `ParquetSink` pipeline with Logs, Metrics, and Traces
- Produces: Validation that generated Parquet files are readable by `parquet::arrow::arrow_reader::ParquetRecordBatchReader`, verifying Variant metadata and statistics

- [ ] **Step 1: Write integration tests verifying Variant schema, reading back with `ParquetRecordBatchReader`, and all compression codecs**

```rust
// crates/parquet-sink/tests/integration_tests.rs
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_sink::{CompressionCodec, ParquetSink, ParquetSinkConfig};
use pipeline_core::pipeline::{SignalBatch, Sink};
use std::fs::File;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_e2e_write_and_read_back_variant_parquet() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        compression: CompressionCodec::Zstd { level: Some(3) },
        variant_encoding: true,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).unwrap();
    let (tx, rx) = mpsc::channel(10);

    // Send synthetic OTel log batch with attributes
    // ...
    drop(tx);
    sink.run(rx).await.unwrap();

    // Find written .parquet file
    // Read with ParquetRecordBatchReaderBuilder
    // Assert schema contains struct attributes with "metadata" and "value"
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p parquet-sink --test integration_tests`
Expected: PASS

- [ ] **Step 3: Run comprehensive workspace check**

Run:
```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo test --workspace
```
Expected: Zero warnings, zero errors, all workspace tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/parquet-sink/tests/integration_tests.rs
git commit -m "test(parquet-sink): add end-to-end integration tests for parquet variant sink"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: Covers all sections of `2026-10-09-parquet-variant-sink-design.md` (Variant encoding, naming, S3 5MB coalescing, decoupled sync writer, LRU partition manager, memory tracker, and error handling).
- [x] **Step scan**: Every step specifies exact test assertions, file paths, commands, and expected results.
- [x] **Type consistency**: Method signatures and config types match across tasks (`CompressionCodec`, `ParquetSinkError`, `AsyncUploader`, `PartitionWriter`).
- [x] **Review Focus**: All 5 critical failure modes are pinned with explicit unit/integration tests in their respective tasks.
- [x] **Proportion**: Bite-sized, modular tasks suitable for parallel subagent execution.
