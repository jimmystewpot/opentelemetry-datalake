# Buffer Specification

This document describes how `opentelemetry-datalake` handles data buffering and batching within the pipeline.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

---

## Pipeline Buffering

`opentelemetry-datalake` uses asynchronous, bounded `tokio::sync::mpsc` channels to buffer data between components. 

*   **Capacity**: By default, channels are created with a capacity of 1000 `SignalBatch`es.
*   **Backpressure**: When a channel is full, the upstream component's `.send().await` call will block. This backpressure propagates all the way to the OTLP receiver, which will then return a "503 Service Unavailable" or gRPC "UNAVAILABLE" status to the producer, signaling it to retry.
*   **Memory Management**: Because Arrow `RecordBatch`es can be large, the number of batches in-flight is limited to prevent out-of-memory (OOM) conditions.

---

## Sink Batching

While the pipeline uses channels for transient buffering, storage sinks (like Apache Iceberg) implement their own batching logic to optimize for the target storage format (e.g., Parquet).

### Iceberg Batching

The Iceberg sink accumulates data in-memory until one of two thresholds is met:

*   **Max Batch Size**: `max_batch_size_bytes` (Default: 128 MB). Triggers a flush when the accumulated Arrow batches exceed this size.
*   **Max Batch Interval**: `max_batch_interval_sec` (Default: 60 seconds). Triggers a flush if the specified time has elapsed since the last commit, even if the size threshold hasn't been reached.

These defaults are designed to balance data latency with write efficiency, ensuring that Parquet files are large enough for efficient querying while keeping data reasonably fresh in the lakehouse.

---

## Acknowledgements & Delivery Guarantees

`opentelemetry-datalake` provides **at-least-once** delivery guarantees for data currently in the pipeline during a graceful shutdown.

1.  **Graceful Shutdown**: When a shutdown signal is received, the OTLP source stops accepting new data.
2.  **Drain Phase**: Transformer and Sink tasks continue processing until their input channels are fully empty.
3.  **Final Commit**: Sinks ensure that all accumulated data is flushed and committed to the target (e.g., Iceberg transaction is finished) before the task exits.

Note: Since the current implementation uses in-memory channels, an ungraceful crash (e.g., `kill -9` or OOM) may result in the loss of data currently in the channels or the sink's internal buffer.
