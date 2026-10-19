# Instrumentation Specification

This document describes the instrumentation standards for `opentelemetry-datalake`. It focuses on how the pipeline should be monitored and how internal telemetry is collected.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

---

## Overview

`opentelemetry-datalake` uses the `tracing` crate for all internal logging and telemetry. This allows for structured logging and easy integration with external OpenTelemetry collectors.

---

## Logging Standards

Developers **SHOULD** use the `tracing` macros (`info!`, `warn!`, `error!`, `debug!`, `trace!`) to log important events.

### 1. Structured Context

Logs **SHOULD** include structured context using key-value pairs. This makes it easier to filter and analyze logs in centralized logging systems.

*   **Sinks**: Include `table_identifier` and `batch_size`.
*   **Sources**: Include `protocol` (gRPC/HTTP) and `remote_addr`.
*   **Errors**: Include the error message and any relevant IDs.

Example:
```rust
tracing::info!(
    table = %self.config.table_identifier,
    count = batch.num_rows(),
    "Committed batch to Iceberg"
);
```

### 2. Log Levels

*   **ERROR**: Fatal issues that cause a component or the entire pipeline to fail.
*   **WARN**: Non-fatal issues that might indicate misconfiguration or transient network problems (e.g., retrying a commit).
*   **INFO**: Significant lifecycle events (startup, shutdown) and high-level operational milestones (e.g., successful commits).
*   **DEBUG**: Detailed information useful for troubleshooting.
*   **TRACE**: Very high-volume information, such as individual record processing details.

---

## Internal Telemetry (Self-Monitoring)

`opentelemetry-datalake` can export its own traces and metrics to an external OTLP collector. This is configured in the `[telemetry]` section of the configuration file.

### 1. Configuration & Loopback Prevention

The core pipeline handles the initialization of the global OpenTelemetry tracer and meter providers using the settings in `telemetry.rs`.

> [!WARNING]
> **Loopback Amplification Hazard**: Never configure `otlp_endpoint` to point to the local ingestion port (e.g. `http://localhost:4317` when `pipeline.grpc_addr` is `127.0.0.1:4317`). Internal spans and logs emitted during request processing will be exported back into the ingestion receiver, triggering an infinite telemetry amplification loop. Set `enabled = false` by default, or point to an external collector:

```toml
[telemetry]
enabled = false
otlp_endpoint = "http://collector.monitoring.internal:4317"
service_name = "otel-datalake"
```

### 2. Shutdown Flush Guarantee

`TelemetryGuard` **MUST** implement `Drop` to explicitly invoke `.shutdown()` on the active `SdkTracerProvider` and `SdkMeterProvider`. When the pipeline terminates gracefully, all buffered diagnostic spans and metric samples are exported before process shutdown.

### 3. Context Propagation

Since `opentelemetry-datalake` is an async application, developers **SHOULD** use `tracing`'s instrumentation support (e.g., `#[tracing::instrument]`) to ensure that trace context is correctly propagated across task boundaries.

---

## Metrics Standards

In addition to `tracing` spans, all components and storage sinks **MUST** emit explicit OpenTelemetry metrics using the global meter provider (`opentelemetry::global::meter("opentelemetry-datalake")`).

### Standard Metrics:

| Metric Name | Type | Unit | Description |
|---|---|---|---|
| `pipeline_ingest_records_total` | Counter | `{records}` | Total telemetry records received by OTLP receivers |
| `pipeline_ingest_bytes_total` | Counter | `By` | Total raw bytes received at ingestion edge |
| `buffer_records` | UpDownCounter / Gauge | `{records}` | Current records buffered in memory |
| `buffer_bytes` | UpDownCounter / Gauge | `By` | Current byte occupancy of in-memory batch buffers |
| `sink_records_committed_total` | Counter | `{records}` | Total records successfully committed to persistent sink |
| `sink_records_dropped_total` | Counter | `{records}` | Total records dropped (oversized, rejected, or unrecoverable) |
| `sink_flush_duration_seconds` | Histogram | `s` | Latency distribution of sink batch flushes and commits |

Naming conventions:
*   Use `snake_case`.
*   Include units where applicable (e.g., `_bytes`, `_seconds`).
*   Monotonic counters **MUST** end with `_total`.
