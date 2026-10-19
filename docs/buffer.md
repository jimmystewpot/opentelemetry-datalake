# Buffer Specification

This document describes how `opentelemetry-datalake` handles data buffering and batching within the pipeline.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

---

## Pipeline Buffering

`opentelemetry-datalake` uses asynchronous, bounded `tokio::sync::mpsc` channels to buffer data between components. 

*   **Capacity**: Channels **MUST** use conservative, bounded capacities (default: 32–64 `SignalBatch`es per channel, configurable via `pipeline.channel_capacity`). Defaulting to large capacities (e.g., 1000 batches) across 6 pipeline channels can cause 30–120 GB of in-flight heap memory consumption during sink stalls, causing Out-Of-Memory (OOM) crashes.
*   **Backpressure**: When a channel is full, the upstream component's `.send().await` call will block. This backpressure propagates all the way to the OTLP receiver, which will then return a "503 Service Unavailable" or gRPC "UNAVAILABLE" status to the producer, signaling it to retry.
*   **Memory Management**: Because Arrow `RecordBatch`es can be large (5–25 MB each), channels and ingress endpoints enforce strict message and byte budgets to prevent OOM conditions under load spikes.

---

## Sink Batching

While the pipeline uses channels for transient buffering, storage sinks (like Apache Iceberg, Parquet, Elasticsearch, and StarRocks) implement their own batching logic to optimize for the target storage format (e.g., Parquet row groups or HTTP bulk payloads).

### Iceberg & Lakehouse Batching

The lakehouse sinks accumulate data in-memory until one of two thresholds is met:

*   **Max Batch Size**: `max_batch_size_bytes` (Default: 128 MB). Triggers a flush when the accumulated Arrow batches exceed this size.
*   **Max Batch Interval**: `max_batch_interval_sec` (Default: 60 seconds). Triggers a flush if the specified time has elapsed since the last commit, even if the size threshold hasn't been reached.

### Buffer Retention & Retry Policy

Sinks **MUST NOT** discard or clear their in-memory batch buffers (`std::mem::take`) before persistent writes and ACID transactions are verified:
*   On transient write or catalog commit errors (e.g., S3 timeouts or optimistic concurrency conflicts), buffers **MUST** be retained in memory for retry.
*   If retries are exhausted, the uncommitted batches **MUST** be routed to a dead-letter path or propagated upstream as fatal errors to prevent silent data loss.

---

## Acknowledgements & Delivery Guarantees

`opentelemetry-datalake` provides **at-least-once** delivery guarantees for data currently in the pipeline during a graceful shutdown.

1.  **Ingress Stop**: When a shutdown signal is received, the OTLP receiver stops accepting new connections and drops its senders.
2.  **Topological Drain**: Transformer and Sink tasks continue processing in topological order until their input channels are fully drained.
3.  **Final Commit & Producer Flush**: Sinks ensure that all accumulated in-memory data is flushed and committed to the target (e.g., Iceberg transaction committed, `librdkafka::Producer::flush` completed) before the task exits.

Note: Since the pipeline channels reside in memory, an ungraceful crash (e.g., `kill -9` or OOM) may result in the loss of in-flight batches. Maintaining strict byte bounds and avoiding executor thread starvation are critical to preventing such crashes.
