# Architecture & Design Specification: Elasticsearch Sink Unified Chunking & Pipeline Resiliency

**Date**: 2026-09-17  
**Status**: Approved  
**Topic**: `elasticsearch-sink-unified-chunking`  
**Scope**: `crates/elasticsearch-sink` (`lib.rs`, `serializer.rs`, `client.rs`, `error.rs`)

---

## 1. Overview & Problem Statement

In PR #52, `elasticsearch-sink` introduced dual ingestion pathways (`if batching.is_none()` vs `if batching.is_some()`), monolithic NDJSON batch serialization, and fatal task termination on payload limit violations. 

This design produced several systemic issues:
1. **Dual-Path Divergence**: Fixes applied to `flush_buffer` (e.g. CPU task offloading, joinset draining, buffer metrics) were not shared with the non-batching branch, creating duplicated code and repetitive edge-case vulnerabilities.
2. **Monolithic Wire Serialization**: `serialize_batch` attempted to write an entire Arrow `RecordBatch` into a single `Bytes` payload and failed with `PayloadTooLarge` if the result exceeded `max_payload_bytes`.
3. **Fatal Sink Task Termination**: When a batch exceeded `max_payload_bytes`, the error bubbled up through `run()`, permanently killing that signal's sink task. Because the batch had already been accepted upstream, telemetry was permanently lost and the receiver subsequently returned `503 UNAVAILABLE`.

This specification replaces monolithic serialization with zero-copy bounded payload chunking, unifies the ingestion pipeline into a single operational path, and isolates data-level serialization errors from crashing the persistent actor loop.

---

## 2. Component Architecture & Data Flow

```
[OTLP Receiver]
       │
       ▼ (mpsc channel: SignalBatch)
┌───────────────────────────────────────────────────────────┐
│ ElasticsearchSink::run                                    │
│                                                           │
│  1. Receive batch (Logs, Metrics, Traces)                 │
│  2. Push to per-signal BufferState                        │
│                                                           │
│  ┌───────────────────────────────┐                        │
│  │ Batching Check                │                        │
│  │  - Disabled: flush immediately│                        │
│  │  - Enabled: flush on limit/   │                        │
│  │             timer tick        │                        │
│  └───────────────┬───────────────┘                        │
│                  ▼                                        │
│  3. flush_buffer(&mut BufferState, signal_type)           │
│     │                                                     │
│     ├─► tokio::task::spawn_blocking                       │
│     │     ├─► Concat (only if batches.len() > 1)          │
│     │     ├─► BatchSorter::sort                           │
│     │     └─► serialize_batch_chunks(max_payload_bytes)   │
│     │           └─► Returns Vec<Bytes> (each <= limit)    │
│     │                                                     │
│     └─► For each chunk in Vec<Bytes>:                     │
│           Self::dispatch(client, semaphore, join_set)     │
└───────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Specifications

### 3.1 Serializer Chunking Engine (`serializer.rs`)

Replace monolithic single-buffer serialization with a row-aware chunking engine:

```rust
pub fn serialize_batch_chunks(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Vec<Bytes>, ElasticsearchError>
```

#### Chunking Algorithm
1. **Empty Batch Handling**: If `batch.num_rows() == 0`, returns `Ok(Vec::new())`.
2. **Buffer Allocation**:
   - Maintains a current chunk buffer: `current_chunk: Vec<u8>`.
   - Maintains a reusable row scratch buffer: `row_buf: Vec<u8>`.
   - Pre-allocates `current_chunk` with `min(batch.get_array_memory_size(), max_payload_bytes)`.
3. **Row Serialization**:
   - For each row $i \in 0..\text{batch.num\_rows()}$:
     - Clears `row_buf`.
     - Appends the static bulk action line: `{"create":{}}\n`.
     - Serializes the JSON document representing row $i$ into `row_buf`.
     - Appends `\n`.
     - **Individual Row Check**: If `row_buf.len() > max_payload_bytes`:
       - Logs a structured error:
         ```rust
         tracing::error!(
             row = i,
             row_bytes = row_buf.len(),
             max_payload_bytes,
             "Dropping individual telemetry record exceeding maximum payload bytes"
         );
         ```
       - Skips this row and continues to row $i + 1$. The sink task is not terminated.
     - **Chunk Boundary Check**:
       - If `current_chunk.len() + row_buf.len() > max_payload_bytes` and `!current_chunk.is_empty()`:
         - Seals `current_chunk` as `Bytes::from(std::mem::take(&mut current_chunk))`.
         - Pushes sealed chunk to `chunks: Vec<Bytes>`.
         - Pre-allocates new `current_chunk` with capacity `max_payload_bytes`.
       - Appends `row_buf` into `current_chunk`.
4. **Final Flush**:
   - If `!current_chunk.is_empty()`, seals and pushes to `chunks`.
5. **Returns**: `Ok(chunks)`.

#### Backward Compatibility Wrapper
```rust
pub fn serialize_batch(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Bytes, ElasticsearchError> {
    let mut chunks = serialize_batch_chunks(batch, unpack_attributes, max_payload_bytes)?;
    if chunks.is_empty() {
        Ok(Bytes::new())
    } else if chunks.len() == 1 {
        Ok(chunks.remove(0))
    } else {
        Err(ElasticsearchError::PayloadTooLarge {
            actual: chunks.iter().map(Bytes::len).sum(),
            limit: max_payload_bytes,
        })
    }
}
```

---

### 3.2 Pipeline Engine Unification (`lib.rs`)

Eliminate the dual-path branch in `ElasticsearchSink::run`.

#### 1. Unified Buffer State
Every signal maintains its own `BufferState` struct:
```rust
struct BufferState {
    batches: Vec<RecordBatch>,
    bytes: usize,
    records: usize,
}
```

#### 2. Unified Ingestion Loop
```rust
let (signal_type, batch) = match signal {
    SignalBatch::Logs(b) => (SignalType::Logs, b),
    SignalBatch::Metrics(b) => (SignalType::Metrics, b),
    SignalBatch::Traces(b) => (SignalType::Traces, b),
};

if batch.num_rows() == 0 {
    continue;
}

let buf = match signal_type {
    SignalType::Logs => &mut logs_buf,
    SignalType::Metrics => &mut metrics_buf,
    SignalType::Traces => &mut traces_buf,
};

let batch_bytes = batch.get_array_memory_size();
let batch_rows = batch.num_rows();

buf.bytes = buf.bytes.saturating_add(batch_bytes);
buf.records = buf.records.saturating_add(batch_rows);
buf.batches.push(batch);

// Buffer telemetry emitted for all modes
tracing::debug!(
    signal = signal_label,
    bytes = buf.bytes,
    records = buf.records,
    data_stream = self.data_stream_for(signal_type),
    "Buffer accumulated batch"
);

let should_flush = match batching {
    Some(cfg) => buf.bytes >= cfg.max_bytes || buf.records >= cfg.max_records,
    None => true, // Immediate flush when micro-batching is disabled
};

if should_flush {
    if let Err(e) = self
        .flush_buffer(buf, signal_type, self.data_stream_for(signal_type), &mut join_set)
        .await
    {
        let _ = Self::drain_join_set(&mut join_set).await;
        return Err(e);
    }
}
```

#### 3. Optimized `flush_buffer`
```rust
async fn flush_buffer(
    &self,
    buf: &mut BufferState,
    signal_type: SignalType,
    target_data_stream: &str,
    join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
) -> Result<(), PipelineError> {
    if buf.batches.is_empty() {
        return Ok(());
    }

    let batches = std::mem::take(&mut buf.batches);
    buf.bytes = 0;
    buf.records = 0;

    let sorter = self.sorter.clone();
    let unpack_attributes = self.config.unpack_attributes;
    let max_payload_bytes = self.config.max_payload_bytes;

    let chunks = tokio::task::spawn_blocking(move || -> Result<Vec<Bytes>, PipelineError> {
        // Fast path: if exactly 1 batch, avoid concat_batches
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap_or_default()
        } else {
            let schema = batches[0].schema();
            let refs: Vec<&RecordBatch> = batches.iter().collect();
            arrow::compute::concat_batches(&schema, refs).map_err(PipelineError::Arrow)?
        };

        if combined.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let sorted_batch = sorter.sort(&combined, signal_type)?;
        crate::serializer::serialize_batch_chunks(
            &sorted_batch,
            unpack_attributes,
            max_payload_bytes,
        )
        .map_err(PipelineError::from)
    })
    .await
    .map_err(|e| PipelineError::Internal(format!("Serialization task panicked: {e}")))??;

    for chunk in chunks {
        Self::dispatch(
            Arc::clone(&self.client),
            Arc::clone(&self.semaphore),
            join_set,
            target_data_stream.to_string(),
            chunk,
        )
        .await?;
    }

    Ok(())
}
```

---

## 4. Error Isolation & Resiliency

1. **Individual Row Outliers**:
   - If a single record exceeds `max_payload_bytes`, it is logged via `tracing::error!` and skipped; remaining records in the batch are preserved and successfully transmitted.
   - The sink task does not terminate.
2. **Infrastructural Errors**:
   - Connection timeouts, exhausted retries on HTTP 429/503, or persistent auth failures properly drain in-flight `join_set` tasks and propagate errors up to the pipeline supervisor for backpressure and shutdown.
3. **Zero-Panic Guarantee**:
   - No `unwrap()` or `expect()` in production code paths.
   - Checked arithmetic via `saturating_add`.

---

## 5. Testing & Verification Strategy

1. **Unit Tests (`serializer.rs`)**:
   - `test_serialize_batch_chunks_single_chunk`: Verifies normal-sized batch produces 1 chunk.
   - `test_serialize_batch_chunks_splits_oversized_batch`: Creates a batch whose total NDJSON size exceeds `max_payload_bytes`, verifying that it splits into multiple chunks, each $\le \text{max\_payload\_bytes}$, and all records are present.
   - `test_serialize_batch_chunks_drops_single_oversized_row`: Verifies that a row with an attribute larger than `max_payload_bytes` is safely skipped without returning an error or halting serialization of adjacent rows.
2. **Integration Tests (`lib.rs`)**:
   - `test_sink_non_batching_splits_oversized_batch_into_multiple_requests`: Verifies that when batching is disabled, sending a batch larger than `max_payload_bytes` results in multiple HTTP POST requests to Elasticsearch and succeeds without error.
   - `test_sink_batching_splits_oversized_batch_into_multiple_requests`: Verifies that when batching is enabled, flushing accumulated batches larger than `max_payload_bytes` dispatches multiple bounded requests.
3. **Quality Gates**:
   - `cargo fmt --all -- --check`
   - `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
   - `cargo test --workspace`
   - `cargo test -p elasticsearch-sink --features aws`
