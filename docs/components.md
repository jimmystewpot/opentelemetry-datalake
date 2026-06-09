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

*   **Implementation**: Implemented by `IcebergSink` in `crates/storage` and `KafkaSink` in `crates/kafka-sink`.
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

### 1. Backpressure Handling

Components **MUST** respect backpressure. Since the pipeline uses bounded `mpsc` channels, `send().await` will naturally block if downstream components are slow. This backpressure propagates back to the OTLP receiver, which returns `503 Service Unavailable` or gRPC `UNAVAILABLE` to producers.

### 2. Error Handling

Components **MUST** follow the zero-panic policy. Errors **MUST** be propagated via `Result` and handled in `main.rs`. Fatal errors that prevent a task from continuing should be logged at the `error` level.

### 3. Graceful Shutdown

All components **MUST** support graceful shutdown by listening for a shutdown signal or by finishing their work when their input channel is closed.
*   **Sources** stop listening and drop their senders.
*   **Transformers** and **Sinks** continue processing until their input channels are drained.

---

## Instrumentation

Components **SHOULD** use the `tracing` crate for logging and instrumentation. Key events like batch reception, commit success, and errors should be logged with appropriate context (e.g., `table_identifier`, `count`).

Refer to [instrumentation.md](instrumentation.md) for detailed logging and telemetry standards.

[RFC 2119]: https://datatracker.ietf.org/doc/html/rfc2119
