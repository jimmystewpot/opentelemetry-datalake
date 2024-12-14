# Telemetry Schemas (Apache Iceberg V2)

This directory contains standard sample schemas and documentation for OpenTelemetry (OTLP) signals, optimized for ingestion and analytical queries using Apache Iceberg V2. These schemas serve as a reference implementation for the `opentelemetry-datalake` pipeline.

## Tables

### 1. `otel_metrics` (`metrics_table.sql`)
A consolidated table representing all OTLP metric types (Gauge, Sum, Histogram, and Exponential Histogram).
- **Consolidation Design**: To simplify the storage layer, all metric types are stored in a single table. Fields specific to certain metric types (such as `explicit_bounds` for Histograms or `scale` for Exponential Histograms) are defined as nullable columns. The `metric_type` column serves as the discriminator.
- **Partitioning**: Hourly granularity on the `time_unix` column.

### 2. `otel_traces` (`traces_table.sql`)
Stores OTLP spans, including their resource and scope metadata, attributes, events, and links.
- **Partitioning**: Hourly granularity on the `timestamp` column.

### 3. `otel_logs` (`logs_table.sql`)
Stores OTLP log records, including severity information and the log body.
- **Partitioning**: Hourly granularity on the `timestamp` column.

## Design Constraints & Conventions

- **Apache Iceberg V2**: All DDLs target the Iceberg V2 specification, enabling advanced features like row-level deletes and improved metadata management.
- **Hidden Partitioning**: All tables are **hourly partitioned** using Iceberg's `hours()` transform (e.g., `PARTITIONED BY (hours(timestamp))`). This approach provides high-performance time-series filtering without requiring users to manually manage partition columns.
- **Complex Types**:
    - **Attributes**: Mapped to `MAP<STRING, STRING>` for flexible key-value storage.
    - **Nested Structures**: Span events, links, and metric exemplars are implemented using `LIST<STRUCT<...>>` to maintain the relational integrity of OTLP nested data.
- **Type Mapping**:
    - High-precision timestamps (nanoseconds) are mapped to Iceberg's `TIMESTAMP` type.
    - OTLP `uint64` and `int64` fields are mapped to `BIGINT`.
    - Floating point values use `DOUBLE`.
