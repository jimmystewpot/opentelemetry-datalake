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

### 1. Configuration

The core pipeline handles the initialization of the global OpenTelemetry tracer provider using the settings in `telemetry.rs`.

```toml
[telemetry]
otlp_endpoint = "http://localhost:4317"
service_name = "otel-datalake"
```

### 2. Propagation

Since `otel-datalake` is an async application, developers **SHOULD** use `tracing`'s instrumentation support (e.g., `#[tracing::instrument]`) to ensure that trace context is correctly propagated across task boundaries.

---

## Metrics

Currently, metrics are primarily derived from `tracing` spans and events. In the future, explicit OpenTelemetry metrics (e.g., counters for received events) may be added using the `opentelemetry` crate's metrics API.

When adding metrics, follow these naming conventions:
*   Use `snake_case`.
*   Include units where applicable (e.g., `_bytes`, `_seconds`).
*   Counters should end with `_total`.
