# Component Specification

This document describes the behavior and traits of `opentelemetry-datalake` components (sources, transformers, and sinks).

The key words “MUST”, “MUST NOT”, “REQUIRED”, “SHALL”, “SHALL NOT”, “SHOULD”,
“SHOULD NOT”, “RECOMMENDED”, “MAY”, and “OPTIONAL” in this document are to be
interpreted as described in [RFC 2119].

## Overview

`opentelemetry-datalake` uses a trait-based approach to define pipeline components. These traits are defined in `crates/core/src/pipeline.rs`.

---

## Core Traits

### 1. Source

A `Source` is responsible for ingesting external data and converting it into `SignalBatch`es (which wrap Apache Arrow `RecordBatch`es).

```rust
#[async_trait]
pub trait Source: Send + Sync {
    async fn run(&mut self) -> Result<(), PipelineError>;
}
```

*   **Implementation**: Currently implemented by `OtlpReceiverSource` in `crates/otlp-receiver`.
*   **Behavior**: It listens for OTLP requests, decodes them using `arrow-codec`, and sends them to downstream channels.

### 2. Transform

A `Transform` performs stateless or stateful operations on `SignalBatch`es.

```rust
#[async_trait]
pub trait Transform: Send + Sync {
    async fn transform(
        &mut self,
        input: PipelineReceiver,
        output: PipelineSender,
    ) -> Result<(), PipelineError>;
}
```

*   **Implementation**: Currently implemented by `NoopTransformer` in `crates/noop-transformer`.
*   **Behavior**: It pulls from an input channel and pushes to an output channel, potentially modifying the data in between.

### 3. Sink

A `Sink` writes `SignalBatch`es to a final storage destination.

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    async fn run(&mut self, input: PipelineReceiver) -> Result<(), PipelineError>;
}
```

*   **Implementation**: Implemented by `IcebergSink` in `crates/storage`, `KafkaSink` in `crates/kafka-sink`, `StarRocksSink` in `crates/starrocks-sink`, and `ElasticsearchSink` in `crates/elasticsearch-sink`.
*   **Behavior**: It consumes batches from its input channel and handles the complexities of external commits (e.g., Iceberg transactions or Kafka production).

---

## Data Flow (`SignalBatch`)

Data is passed between components using the `SignalBatch` enum, which identifies the telemetry signal type and carries the Apache Arrow data.

```rust
pub enum SignalBatch {
    Logs(RecordBatch),
    Metrics(RecordBatch),
    Traces(RecordBatch),
}
```

---

## Operational Requirements

### 1. Backpressure Handling & Channel Sizing

Components **MUST** respect backpressure:
*   Pipeline channels (`tokio::sync::mpsc`) carry `SignalBatch`es containing Arrow `RecordBatch`es. To prevent unbounded memory growth under ingestion spikes or sink latency, channel capacities **MUST** be bounded conservatively (defaulting to 32–64 batches, or strictly bounded via byte-budget semaphores).
*   When a channel is full, upstream `.send().await` calls block. This backpressure propagates back to `OtlpReceiverSource`, which returns `503 Service Unavailable` or gRPC `UNAVAILABLE` to producers.
*   Components **MUST NOT** drop batches silently under backpressure.

### 2. Error Handling & Task Supervision

*   Components **MUST** strictly adhere to the zero-panic policy. Panicking methods (`unwrap()`, `expect()`, `.as_string::<i32>()`, `.as_primitive::<T>()`) are prohibited.
*   Arrow array access **MUST** verify `.is_null(i)` before calling `.value(i)`.
*   All sink and worker tasks **MUST** return `Result<(), PipelineError>` and be actively supervised in the `main.rs` event loop. Components **MUST NOT** catch, log, and swallow fatal sink errors without notifying the supervisor.

### 3. Graceful Shutdown & Topological Drain

All components **MUST** support coordinated, graceful shutdown. To guarantee zero data loss, the pipeline **MUST** drain in strict topological order:
1.  **Ingress Termination**: `Source` receivers stop accepting new network connections and drop their channel senders (`logs_tx`, `traces_tx`, `metrics_tx`).
2.  **Source Task Drain**: The supervisor awaits `source_handle` completion.
3.  **Transformer Drain**: `Transform` tasks process all remaining queued batches from their input channels and drop their sink senders (`logs_sink_tx`, `traces_sink_tx`, `metrics_sink_tx`).
4.  **Sink Flush & Persistence**: `Sink` tasks process all pending batches, flush in-memory buffers to storage, execute ACID commits, and explicitly flush any background producer queues (e.g., `librdkafka::Producer::flush`).
5.  **Telemetry Flush**: Global tracer and meter providers are flushed and shut down.

Watch channel receivers for shutdown coordination **MUST** use `wait_for(|s| *s).await` rather than bare `.changed().await` to avoid missed-event deadlocks if pre-signaled.

### 4. Buffer Retention Across Flushes

Sinks that accumulate data in-memory (e.g., `IcebergSink`, `ElasticsearchSink`, `StarRocksSink`) **MUST NOT** discard their accumulated batch buffers prior to successful persistence or ACID transaction commits. On transient write or commit failures, buffers must be retained for retry or routed to dead-letter storage.

### 5. Async & Blocking Isolation

Synchronous, CPU-heavy operations **MUST NOT** run directly on Tokio async worker threads. The following operations **MUST** be isolated in `tokio::task::spawn_blocking`:
*   OTLP Protobuf and JSON decoding (> 64 KB).
*   Arrow batch concatenation (`compute::concat_batches`).
*   Arrow sorting (`compute::lexsort_to_indices` + `compute::take`).
*   Batch serialization (NDJSON, Arrow IPC, CSV, Parquet).
*   WebAssembly module compilation, instantiation, and rejuvenation.

---

## Instrumentation

Components **SHOULD** use the `tracing` crate for logging and instrumentation. Key events like batch reception, commit success, and errors should be logged with appropriate context (e.g., `table_identifier`, `count`). Sinks and buffers **MUST** emit standard OpenTelemetry metrics for buffer events, buffer bytes, and flush durations.

Refer to [instrumentation.md](instrumentation.md) for detailed logging and telemetry standards.

[RFC 2119]: https://datatracker.ietf.org/doc/html/rfc2119
