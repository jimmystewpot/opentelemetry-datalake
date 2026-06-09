# Architecture

This document describes the high-level architecture of `opentelemetry-datalake`. It focuses on how the pipeline is structured and executed internally.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

---

## Overview

`opentelemetry-datalake` implements a high-performance, fixed-path telemetry pipeline. It is designed to ingest OpenTelemetry (OTLP) data via gRPC or HTTP, convert it into Apache Arrow `RecordBatch`es, and stream it into modern storage sinks like Apache Iceberg or Kafka.

### Logical Topology (ASCII Diagram)
```text
  [ OTLP Producer ]
          |
          v
  [ Source: OtlpReceiverSource ]
          |
   (Signal-specific channels)
     /      |      \
    v       v       v
[ Logs ] [ Traces ] [ Metrics ]
    |       |       |
    v       v       v
  [ Transform: NoopTransformer ]
    |       |       |
    v       v       v
  [ Sink: IcebergSink OR KafkaSink ]
```

The pipeline is orchestrated in `src/main.rs`, which spins up each component as an asynchronous `tokio` task and wires them together using bounded `mpsc` channels.

---

## Component Roles

### Sources (`otlp-receiver`)

The `OtlpReceiverSource` is the entry point for all telemetry. It runs both a `tonic` gRPC server and an `axum` HTTP server.

1.  **Ingestion**: Receives `ExportLogsServiceRequest`, `ExportTraceServiceRequest`, or `ExportMetricsServiceRequest`.
2.  **Decoding**: Uses `arrow-codec` to translate Protobuf messages directly into Apache Arrow `RecordBatch`es.
3.  **Demultiplexing**: Sends the resulting batches into one of three signal-specific channels (Logs, Traces, Metrics).

### Transformers (`noop-transformer`)

Transformers provide a location for data enrichment, filtering, or remapping. 

*   **Current State**: The project uses a `NoopTransformer` which passes data through without modification.
*   **Execution**: Each transformer runs in its own task, pulling from a source channel and pushing to a sink channel.

### Sinks (`storage`, `kafka-sink`)

Sinks are responsible for the final delivery of data.

*   **Iceberg Sink**: 
    *   Accumulates batches in-memory until a size or time threshold is reached (configured in `batching`).
    *   Uses `iceberg-rust` to commit Parquet files to an Iceberg table.
    *   Supports `Fixed`, `Auto` (additive), and `Catalog` schema modes.
*   **Kafka Sink**:
    *   Serializes Arrow batches into JSON or Protobuf.
    *   Produces messages to configured Kafka topics using `rdkafka`.

---

## Pipeline Orchestration

The pipeline is constructed in `src/main.rs` during startup:

1.  **Configuration**: Loaded via `figment` from `config.toml` and environment variables.
2.  **Channel Setup**: Bounded `mpsc` channels (default capacity 1000) are created to connect the components.
3.  **Task Spawning**:
    *   The `OtlpReceiverSource` is spawned.
    *   Three transformer tasks are spawned (one for each signal).
    *   Three sink tasks are spawned (one for each signal).
4.  **Graceful Shutdown**:
    *   The application listens for `SIGINT` (Ctrl+C).
    *   Upon shutdown, a signal is sent to the `OtlpReceiverSource` to stop accepting new connections.
    *   The source task finishes and its sender handles are dropped.
    *   Transformer and sink tasks continue until their input channels are drained, ensuring at-least-once delivery for in-flight data.

---

## Data Model (Apache Arrow)

`opentelemetry-datalake` uses Apache Arrow as its internal data representation. This allows for:

*   **Zero-Copy Transitions**: Data is converted to Arrow at the edge and stays in Arrow format until it reaches the sink.
*   **Vectorized Processing**: Transformations can leverage vectorized CPU operations.
*   **Storage Optimization**: Sinks like Iceberg can write Arrow batches directly to Parquet with high efficiency.

Schema mappings for Logs, Metrics, and Traces are defined in `crates/arrow-codec/src/`.
