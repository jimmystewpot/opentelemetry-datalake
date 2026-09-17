# Elasticsearch Sink Unified Chunking & Pipeline Resiliency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the Elasticsearch sink ingestion pipeline into a single operational pathway, replace monolithic batch serialization with zero-copy bounded payload chunking, and isolate data-level anomalies so oversized batches are split into valid bulk payloads without killing the sink task.

**Architecture:** 
- `crates/elasticsearch-sink/src/serializer.rs`: Add `serialize_batch_chunks` which produces `Vec<Bytes>` where each chunk is kept strictly under `max_payload_bytes`. Individual rows exceeding `max_payload_bytes` alone are logged and dropped without halting serialization of subsequent rows.
- `crates/elasticsearch-sink/src/lib.rs`: Unify ingestion into a single `BufferState` and `flush_buffer` pathway across both micro-batching and non-batching modes. Add a single-batch fast path bypassing `arrow::compute::concat_batches`, and dispatch each serialized chunk concurrently under the semaphore.

**Tech Stack:** Rust 2021, Apache Arrow 57, `tokio`, `bytes::Bytes`, `tracing`, `wiremock`.

**Spec:** [`docs/superpowers/specs/2026-09-17-elasticsearch-sink-unified-chunking-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-17-elasticsearch-sink-unified-chunking-design.md)

## Global Constraints

- Zero `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` paths — test modules excepted.
- All changes must pass: `cargo fmt --all -- --check`
- All changes must pass: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo test -p elasticsearch-sink`
- All changes must pass: `cargo test -p elasticsearch-sink --features aws`
- Commits must be GPG-signed and include sign-off: `git commit -s -S`
- Prefer `crate::` over `super::` in production paths; `super::` is fine in `#[cfg(test)]` modules.

---

## File Map & Parallel Decoupling

| Task | Target File | Description | Can Parallelize? |
|---|---|---|---|
| **Task 1** | `crates/elasticsearch-sink/src/serializer.rs` | Chunked NDJSON serializer + row-level size isolation | Yes (isolated to `serializer.rs`) |
| **Task 2** | `crates/elasticsearch-sink/src/lib.rs` | Unified ingestion pipeline + chunked dispatch loop | Yes (isolated to `lib.rs`) |
| **Task 3** | Workspace integration | Final end-to-end suite verification and lint gates | Sequential after Tasks 1 & 2 |

---

### Task 1: Serializer Chunking Engine & Row-Level Isolation

**Files:**
- Modify: `crates/elasticsearch-sink/src/serializer.rs`
- Test: `crates/elasticsearch-sink/src/serializer.rs`

**Interfaces:**
- Produces:
  ```rust
  pub fn serialize_batch_chunks(
      batch: &RecordBatch,
      unpack_attributes: bool,
      max_payload_bytes: usize,
  ) -> Result<Vec<Bytes>, ElasticsearchError>;

  pub fn serialize_batch(
      batch: &RecordBatch,
      unpack_attributes: bool,
      max_payload_bytes: usize,
  ) -> Result<Bytes, ElasticsearchError>;
  ```

- [ ] **Step 1: Write failing unit tests in `serializer.rs`**

Add tests to `crates/elasticsearch-sink/src/serializer.rs` in `mod tests`:

```rust
#[test]
fn test_serialize_batch_chunks_single_chunk() {
    let batch = make_test_batch();
    let chunks = serialize_batch_chunks(&batch, true, 10_000_000).expect("should serialize into single chunk");
    assert_eq!(chunks.len(), 1);
    assert!(!chunks[0].is_empty());
}

#[test]
fn test_serialize_batch_chunks_splits_oversized_batch() {
    // Create a batch with 10 rows
    let timestamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000i64; 10]);
    let values = StringArray::from(vec!["test-value-12345"; 10]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(values)]).unwrap();

    // Determine size of 1 row
    let single_row_batch = batch.slice(0, 1);
    let single_chunk = serialize_batch_chunks(&single_row_batch, true, 10_000_000).unwrap();
    let row_len = single_chunk[0].len();

    // Set max_payload_bytes to fit roughly 3 rows per chunk
    let max_payload_bytes = row_len * 3 + 5;
    let chunks = serialize_batch_chunks(&batch, true, max_payload_bytes).expect("should chunk");

    // 10 rows with 3 rows per chunk -> 4 chunks (3, 3, 3, 1)
    assert_eq!(chunks.len(), 4);
    for chunk in &chunks {
        assert!(chunk.len() <= max_payload_bytes);
    }
}

#[test]
fn test_serialize_batch_chunks_drops_single_oversized_row() {
    let timestamps = TimestampNanosecondArray::from(vec![
        1_700_000_000_000_000_000i64,
        1_700_000_000_000_000_001i64,
        1_700_000_000_000_000_002i64,
    ]);
    // Middle row has massive value
    let values = StringArray::from(vec![
        "short",
        "extremely-long-string-exceeding-the-limit-by-far",
        "short",
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(values)]).unwrap();

    // Limit allows normal rows (~70 bytes) but rejects middle row (~110 bytes)
    let max_payload_bytes = 85;
    let chunks = serialize_batch_chunks(&batch, true, max_payload_bytes).expect("should succeed by skipping oversized row");

    // Should have serialized row 0 and row 2, skipping row 1
    assert_eq!(chunks.len(), 2);
    let total_ndjson = String::from_utf8(chunks[0].to_vec()).unwrap();
    assert!(total_ndjson.contains("short"));
    assert!(!total_ndjson.contains("extremely-long-string"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p elasticsearch-sink test_serialize_batch_chunks`
Expected: FAIL with `cannot find function serialize_batch_chunks`

- [ ] **Step 3: Implement `serialize_batch_chunks` in `serializer.rs`**

Replace `serialize_batch` in `crates/elasticsearch-sink/src/serializer.rs` with `serialize_batch_chunks` and a backward-compatibility wrapper:

```rust
/// Serializes an Arrow `RecordBatch` into bounded NDJSON bulk payloads for the Elasticsearch Bulk API.
///
/// Each chunk is kept strictly within `max_payload_bytes`. If adding the next row would exceed
/// `max_payload_bytes`, the current buffer is sealed as a chunk and a new chunk begins.
/// If an individual row exceeds `max_payload_bytes` on its own, it is logged with `tracing::error!`
/// and skipped so it does not terminate the sink or discard adjacent rows.
pub fn serialize_batch_chunks(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Vec<Bytes>, ElasticsearchError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mem_size = batch.get_array_memory_size();
    let chunk_capacity = mem_size.min(max_payload_bytes);
    let mut current_chunk = Vec::with_capacity(chunk_capacity);
    let mut row_buf = Vec::with_capacity(1024);
    let mut chunks = Vec::new();

    let schema = batch.schema();
    let num_rows = batch.num_rows();

    for row in 0..num_rows {
        row_buf.clear();
        row_buf.extend_from_slice(BULK_ACTION);
        row_buf.push(b'{');

        let mut first_field = true;
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let name = field.name();

            let output_name = if name == "timestamp" {
                "@timestamp"
            } else {
                name.as_str()
            };

            if col.is_null(row) {
                continue;
            }

            if !first_field {
                row_buf.push(b',');
            }
            first_field = false;

            row_buf.push(b'"');
            row_buf.extend_from_slice(output_name.as_bytes());
            row_buf.extend_from_slice(b"\":");

            let is_attr_field = name == "attributes" || name == "resource_attributes";

            write_value(
                &mut row_buf,
                col.as_ref(),
                row,
                field.data_type(),
                unpack_attributes && is_attr_field,
            )?;
        }

        row_buf.extend_from_slice(b"}\n");

        // Check single record size limit
        if row_buf.len() > max_payload_bytes {
            tracing::error!(
                row,
                record_bytes = row_buf.len(),
                max_payload_bytes,
                "Dropping individual telemetry record exceeding maximum payload bytes"
            );
            continue;
        }

        // Check if adding this row exceeds current chunk capacity
        if current_chunk.len().saturating_add(row_buf.len()) > max_payload_bytes
            && !current_chunk.is_empty()
        {
            chunks.push(Bytes::from(std::mem::take(&mut current_chunk)));
            current_chunk = Vec::with_capacity(chunk_capacity);
        }

        current_chunk.extend_from_slice(&row_buf);
    }

    if !current_chunk.is_empty() {
        chunks.push(Bytes::from(current_chunk));
    }

    Ok(chunks)
}

/// Serializes an Arrow `RecordBatch` into a single NDJSON bulk payload.
///
/// If the serialized batch exceeds `max_payload_bytes` across multiple chunks, returns
/// `ElasticsearchError::PayloadTooLarge`.
pub fn serialize_batch(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Bytes, ElasticsearchError> {
    let mut chunks = serialize_batch_chunks(batch, unpack_attributes, max_payload_bytes)?;
    match chunks.len() {
        0 => Ok(Bytes::new()),
        1 => Ok(chunks.remove(0)),
        _ => {
            let actual = chunks.iter().map(Bytes::len).sum();
            Err(ElasticsearchError::PayloadTooLarge {
                actual,
                limit: max_payload_bytes,
            })
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p elasticsearch-sink test_serialize_batch`
Expected: PASS (all serializer unit tests pass, including existing and new tests)

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/serializer.rs
git commit -s -S -m "feat(elasticsearch-sink): add chunked NDJSON serialization and row-level payload isolation"
```

---

### Task 2: Unified Ingestion Pipeline & Chunked Dispatch in `lib.rs`

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs`
- Test: `crates/elasticsearch-sink/src/lib.rs`

**Interfaces:**
- Consumes:
  `crate::serializer::serialize_batch_chunks(batch: &RecordBatch, unpack_attributes: bool, max_payload_bytes: usize) -> Result<Vec<Bytes>, ElasticsearchError>`
- Produces:
  Unified `flush_buffer` and `run` execution loop in `ElasticsearchSink`.

- [ ] **Step 1: Write failing integration tests in `lib.rs`**

Add tests to `crates/elasticsearch-sink/src/lib.rs` in `mod tests`:

```rust
#[tokio::test]
async fn test_sink_non_batching_splits_oversized_batch_into_multiple_requests() {
    let server = MockServer::start().await;

    // Server expects 2 distinct POST bulk requests
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "took": 1,
            "errors": false,
            "items": []
        })))
        .expect(2)
        .mount(&server)
        .await;

    let mut config = make_test_config(server.uri(), false);
    config.batching = None; // Non-batching mode
    config.max_payload_bytes = 150; // Small limit to force chunking

    let sink = ElasticsearchSink::try_new(config).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    // Create a batch with 4 rows that will exceed 150 bytes
    let timestamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000i64; 4]);
    let bodies = StringArray::from(vec!["hello-world-data"; 4]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(bodies)]).unwrap();

    let handle = tokio::spawn(async move {
        sink.run(rx).await.unwrap();
    });

    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx);

    handle.await.unwrap();
    server.verify().await;
}

#[tokio::test]
async fn test_sink_batching_splits_oversized_batch_into_multiple_requests() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "took": 1,
            "errors": false,
            "items": []
        })))
        .expect(2)
        .mount(&server)
        .await;

    let mut config = make_test_config(server.uri(), false);
    config.batching = Some(crate::config::ElasticsearchBatchingConfig {
        max_records: 100,
        max_bytes: 10_000_000,
        interval_secs: 10,
    });
    config.max_payload_bytes = 150;

    let sink = ElasticsearchSink::try_new(config).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    let timestamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000i64; 4]);
    let bodies = StringArray::from(vec!["hello-world-data"; 4]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("body", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(bodies)]).unwrap();

    let handle = tokio::spawn(async move {
        sink.run(rx).await.unwrap();
    });

    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx);

    handle.await.unwrap();
    server.verify().await;
}
```

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p elasticsearch-sink test_sink_non_batching_splits_oversized_batch`
Expected: FAIL because non-batching still calls `serialize_batch` and returns `PayloadTooLarge`.

- [ ] **Step 3: Refactor `flush_buffer` and `run` in `crates/elasticsearch-sink/src/lib.rs`**

1. Update `flush_buffer`:
   - Fast path for single batch: `if batches.len() == 1 { batches.pop().unwrap_or_default() } else { concat_batches(...) }`
   - Use `crate::serializer::serialize_batch_chunks` inside `tokio::task::spawn_blocking`.
   - Iterate over `chunks: Vec<Bytes>` and call `Self::dispatch` for each chunk.

2. Unify `run()`:
   - Remove `if batching.is_none() { ... } else { ... }` dual branches.
   - For all incoming messages:
     - Push batch into `buf.batches` and update `buf.bytes` and `buf.records`.
     - Emit standard buffer telemetry.
     - Determine `should_flush`:
       ```rust
       let should_flush = match batching {
           Some(cfg) => buf.bytes >= cfg.max_bytes || buf.records >= cfg.max_records,
           None => true,
       };
       if should_flush {
           if let Err(e) = self.flush_buffer(buf, signal_type, self.data_stream_for(signal_type), &mut join_set).await {
               let _ = Self::drain_join_set(&mut join_set).await;
               return Err(e);
           }
       }
       ```
   - Draining on channel close:
     - Call `self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await?;`
     - Call `Self::drain_join_set(&mut join_set).await?;`

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p elasticsearch-sink`
Expected: PASS (all 135+ tests pass)

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -s -S -m "refactor(elasticsearch-sink): unify ingestion pipeline and dispatch chunked payloads"
```

---

### Task 3: Workspace Integration, Quality Gates & Verification

**Files:**
- Test: Full workspace test suite

- [ ] **Step 1: Check code formatting**

Run: `cargo fmt --all -- --check`
Expected: Clean with 0 diffs.

- [ ] **Step 2: Check clippy default features**

Run: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: 0 warnings, 0 errors.

- [ ] **Step 3: Check clippy AWS feature**

Run: `cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: 0 warnings, 0 errors.

- [ ] **Step 4: Run full workspace test suite**

Run: `cargo test --workspace`
Expected: All tests pass.

- [ ] **Step 5: Run AWS feature test suite**

Run: `cargo test -p elasticsearch-sink --features aws`
Expected: All tests pass.
