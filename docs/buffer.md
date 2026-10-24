# Buffer Specification

This document describes how `opentelemetry-datalake` handles data buffering and batching within the pipeline.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

Each section separates **Current behavior** (what the code does today) from **Target contract** (what it must do, with the tracking issue). Do not assume a target contract is implemented.

---

## Pipeline Buffering

`opentelemetry-datalake` uses asynchronous, bounded `tokio::sync::mpsc` channels to buffer data between components.

### Current behavior

*   **Capacity**: Six channels (three source-to-transformer, three transformer-to-sink) are created with a hard-coded capacity of **1000** `SignalBatch`es (`src/main.rs`). There is no configuration knob for this (`worker_channel_capacity` applies only to the WASM transformer).
*   **Worst case**: Arrow `RecordBatch`es are typically 5–25 MB, so six full channels can hold 6 × 1000 × (5–25 MB) = **30–150 GB** (theoretical bound, excluding sink buffers) before backpressure engages.
*   **Backpressure**: When a channel is full, the upstream `.send().await` stays **pending** until capacity frees up. The OTLP receiver returns `503` / gRPC `UNAVAILABLE` **only when a channel is closed**, not when it is full; a full channel makes the client request wait (and eventually time out).
*   **Ingress limits**: The receivers do not enforce explicit body-size, decompression or concurrency limits.

### Target contract

*   Channel capacity is configurable and defaults to 32–64 batches, optionally backed by a byte budget (tracked in #125; precedent: `global_memory_limit_bytes` in `parquet-sink`).
*   Receivers apply a bounded wait and shed with `503` / `UNAVAILABLE` plus a shed metric when a channel stays full (#142).
*   HTTP and gRPC listeners enforce protocol-appropriate payload and concurrency limits, including after decompression (#125, #140).

---

## Sink Batching

While the pipeline uses channels for transient buffering, storage sinks implement their own batching logic to optimize for the target storage format. Thresholds are **per sink** and are not interchangeable.

### Iceberg Batching

The Iceberg sink accumulates data in-memory until one of two thresholds is met:

*   **Max Batch Size**: `max_batch_size_bytes` (Default: 128 MB). Triggers a flush when the accumulated Arrow batches exceed this size.
*   **Max Batch Interval**: `max_batch_interval_sec` (Default: 60 seconds). Triggers a flush if the specified time has elapsed since the last commit, even if the size threshold hasn't been reached.

These defaults balance data latency with write efficiency, ensuring that Parquet files are large enough for efficient querying while keeping data reasonably fresh in the lakehouse.

### Parquet Sink Batching

The Parquet sink rotates files using its own settings (see [configuration.md](configuration.md)): `max_file_size_bytes`, `max_file_interval_sec`, `max_records`, bounded by `max_open_partitions`, `max_concurrent_uploads` and `global_memory_limit_bytes`.

### Other sinks

StarRocks, Elasticsearch and Kafka batch according to their own configuration; see [starrocks.md](starrocks.md) and [elasticsearch.md](elasticsearch.md).

### Buffer Retention & Retry Policy

*   **Target contract**: Sinks MUST NOT discard or clear their in-memory batch buffers (`std::mem::take`) before persistent writes and ACID commits are verified. On transient write/commit errors the buffers are retained for retry; if retries are exhausted they are routed to a dead-letter path or surfaced as fatal errors.
*   **Current behavior**: The Iceberg sink takes and resets its buffer **before** concatenation, upload and catalog commit (`crates/storage/src/iceberg.rs`), so a failure in those steps loses the batch (#127). The Elasticsearch sink has the same pattern in `flush_buffer`.

---

## Acknowledgements & Delivery Guarantees

### Intended contract

`opentelemetry-datalake` is intended to provide **at-least-once** delivery for data in the pipeline during a graceful shutdown:

1.  **Ingress Stop**: On a shutdown signal the OTLP receiver stops accepting new connections and drops its senders.
2.  **Topological Drain**: Transformer and Sink tasks continue processing in topological order until their input channels are drained.
3.  **Final Commit & Producer Flush**: Sinks flush and commit all accumulated data (e.g., Iceberg transaction committed, `librdkafka::Producer::flush` completed) before the task exits.

### Known gaps (not yet at-least-once)

*   Only SIGINT/Ctrl-C triggers the drain today; SIGTERM is not handled, and when a transformer or sink exits first, `main.rs` signals shutdown without explicitly awaiting/inspecting `source_handle` or bounding the drain join (#126).
*   Iceberg and Elasticsearch drop buffered batches on flush failure (#127).

Note: since the pipeline channels reside in memory, an ungraceful crash (e.g., `kill -9` or OOM) may lose in-flight batches.
