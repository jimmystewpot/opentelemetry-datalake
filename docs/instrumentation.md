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
> **Loopback Amplification Hazard**: Never configure `otlp_endpoint` to point at the local gRPC ingestion address (`server.grpc_addr`, default `127.0.0.1:4317`). Internal spans and metrics emitted during request processing would be exported back into the gRPC receiver, causing an unbounded feedback loop. (Pointing `otlp_endpoint` at `server.http_addr` is also invalid because `telemetry.rs` uses the `tonic` OTLP/gRPC exporter, which fails protocol/route handling against the HTTP/JSON receiver.) Always point `otlp_endpoint` to an external OTLP/gRPC collector.
>
> **Current behavior:** telemetry export is always initialised (there is no `enabled` switch), and the built-in default `otlp_endpoint` is `http://localhost:4317`, which collides with the default gRPC receiver address. You **must** override it. A safer default and/or loopback detection is tracked in #124.

```toml
[telemetry]
otlp_endpoint = "http://collector.monitoring.internal:4317"
service_name = "otel-datalake"
```

### 2. Shutdown Flush Guarantee

**Target contract:** `TelemetryGuard` **MUST** implement `Drop` to explicitly invoke `.shutdown()` on the active `SdkTracerProvider` and `SdkMeterProvider` so buffered spans and metrics are exported before exit. **Current behavior:** no `Drop` is implemented (#124).

### 3. Context Propagation

Since `opentelemetry-datalake` is an async application, developers **SHOULD** use `tracing`'s instrumentation support (e.g., `#[tracing::instrument]`) to ensure that trace context is correctly propagated across task boundaries.

---

## Metrics Standards

> [!NOTE]
> **Status:** only `datalake.sink.parquet.*` metrics and the WASM transformer registry are implemented today. The table below is the **target contract** for all components, pending ratification in #145 and implementation in #139.

All components and storage sinks **MUST** emit OpenTelemetry metrics using the global meter provider.

### Naming and instruments

*   Names use the `datalake.<component>.<name>` namespace, matching the existing `datalake.sink.parquet.records_written_total`.
*   Use `snake_case` within a segment, include units where applicable (`_bytes`, `_seconds`), and end monotonic counters with `_total`.
*   Each metric has exactly **one** instrument type, as listed below. Buffer occupancy uses an `ObservableGauge` reading an `AtomicU64`, so error paths cannot cause drift (as an `UpDownCounter` could) and no lock or allocation occurs on the hot path.
*   Attributes: `signal` (`logs`/`traces`/`metrics`) and `sink`. Do not use unbounded-cardinality attributes (table names, service names, trace IDs).

### Target metrics

| Metric Name | Instrument | Unit | Description |
|---|---|---|---|
| `datalake.receiver.records_total` | Counter | `{record}` | Records received by OTLP receivers |
| `datalake.receiver.bytes_total` | Counter | `By` | Raw bytes received at the ingestion edge |
| `datalake.receiver.requests_shed_total` | Counter | `{request}` | Requests rejected because the pipeline was overloaded (#142) |
| `datalake.sink.buffer_records` | ObservableGauge | `{record}` | Records currently buffered in memory |
| `datalake.sink.buffer_bytes` | ObservableGauge | `By` | Bytes currently buffered in memory |
| `datalake.sink.records_committed_total` | Counter | `{record}` | Records committed to persistent storage |
| `datalake.sink.records_dropped_total` | Counter | `{record}` | Records dropped (oversized, rejected or unrecoverable) |
| `datalake.sink.flush_duration_seconds` | Histogram | `s` | Latency of sink flushes and commits |
