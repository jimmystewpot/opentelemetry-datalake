# Configuration Reference

`opentelemetry-datalake` is configured using a TOML file (defaulting to `config.toml` in the working directory) and environment variables.

## Overview

The configuration is divided into several main sections:

- `[server]`: OTLP receiver settings.
- `[iceberg]`: Apache Iceberg storage sink settings (optional).
- `[kafka]`: Kafka storage sink settings (optional).
- `[starrocks]`: StarRocks Stream Load sink settings (optional).
- `[telemetry]`: Internal self-monitoring telemetry settings (part of the core pipeline).

One of `[iceberg]`, `[kafka]`, or `[starrocks]` must be provided.

## Global Environment Overrides

All configuration values can be overridden using environment variables prefixed with `OTEL_DATALAKE_`. Use underscores to navigate nested sections.

Example:
- `OTEL_DATALAKE_SERVER_GRPC_ADDR=0.0.0.0:4317` overrides `server.grpc_addr`.
- `OTEL_DATALAKE_ICEBERG_CATALOG_URI=http://my-catalog:8181` overrides `iceberg.catalog_uri`.

---

## Server Section (`[server]`)

Configures the OTLP receiver that listens for incoming telemetry data.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `grpc_addr` | String | `"127.0.0.1:4317"` | Socket address for the gRPC OTLP receiver. |
| `http_addr` | String | `"127.0.0.1:4318"` | Socket address for the HTTP/JSON OTLP receiver. |

---

## Iceberg Section (`[iceberg]`)

Configures the Apache Iceberg sink. This sink converts OTLP data into Apache Arrow `RecordBatch`es and commits them to Iceberg tables.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `catalog_type` | String | `"Rest"` | Type of Iceberg catalog. Options: `Rest`, `Glue`, `S3Tables`. |
| `catalog_uri` | String | (Required) | URI for the Iceberg catalog. |
| `warehouse` | String | (Required) | Base location for the Iceberg warehouse (e.g., `s3://bucket/path/`). |
| `table_identifier` | String | (Required) | Default table identifier (e.g., `db.table`). |
| `logs_table_identifier` | String | `null` | Table for logs. If null, `table_identifier` is used. |
| `traces_table_identifier` | String | `null` | Table for traces. If null, `table_identifier` is used. |
| `metrics_table_identifier` | String | `null` | Table for metrics. If null, `table_identifier` is used. |
| `schema_mode` | String | `"fixed"` | How to handle schema validation. Options: `fixed`, `auto`, `catalog`. |
| `partition_granularity`| String | `"hourly"` | Time-based partitioning. Options: `hourly`, `daily`. |
| `log_dropped_fields` | Boolean | `true` | Whether to log warnings when fields are dropped due to schema mismatch. |
| `dry_run` | Boolean | `false` | If true, simulates commits without talking to a live catalog. |
| `properties` | Map | `{}` | Additional catalog-specific properties (e.g., S3 endpoint, credentials). |

### Iceberg Batching (`[iceberg.batching]`)

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `max_batch_size_bytes` | Integer | `134217728` | Max size in bytes before triggering a flush (default 128MB). |
| `max_batch_interval_sec`| Integer | `60` | Max time in seconds between flushes. |

---

## Kafka Section (`[kafka]`)

Configures the Kafka sink.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `brokers` | String | `"localhost:9092"` | Comma-separated list of Kafka brokers. |
| `logs_topic` | String | `"telemetry-logs"` | Kafka topic for logs. |
| `traces_topic` | String | `"telemetry-traces"`| Kafka topic for traces. |
| `metrics_topic` | String | `"telemetry-metrics"`| Kafka topic for metrics. |
| `logs_format` | String | `"json"` | Serialization format for logs (`json` or `protobuf`). |
| `traces_format` | String | `"json"` | Serialization format for traces (`json` or `protobuf`). |
| `metrics_format` | String | `"json"` | Serialization format for metrics (`json` or `protobuf`). |
| `options` | Map | `{}` | Additional `librdkafka` configuration options. |

---

## StarRocks Section (`[starrocks]`)

Configures the StarRocks Stream Load sink. See [`docs/starrocks.md`](starrocks.md) for a full operator guide including Arrow IPC version requirements and V1 vs V2 trade-offs.

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `frontend_urls` | `[String]` | (Required) | One or more StarRocks FE HTTP URLs. Round-robin failover is applied. |
| `database` | String | (Required) | Target StarRocks database. |
| `username` | String | (Required) | StarRocks username. |
| `password` | String | `null` | StarRocks password. **Supply via `OTEL_DATALAKE_STARROCKS__PASSWORD` env var** — never in config files. |
| `format` | String | `"ipc"` | Wire format: `"ipc"` (Arrow IPC, recommended), `"json"`, or `"csv"`. |
| `transaction_mode` | String | `"v1"` | `"v1"` (at-least-once) or `"v2"` (exactly-once, two-phase commit). |
| `max_payload_bytes` | Integer | `104857600` | Hard payload size limit in bytes (100 MiB). Must be ≤ BE `stream_load_max_mb`. |
| `connect_timeout_secs` | Integer | `10` | TCP connection timeout in seconds. |
| `request_timeout_secs` | Integer | `600` | HTTP read/request timeout in seconds. |
| `max_retries` | Integer | `3` | SDK-level retries per request before backpressure. |
| `retry_interval_secs` | Integer | `1` | Delay between retries in seconds. |

### StarRocks Table Mapping (`[starrocks.table_mapping]`)

Two variants are supported. Set `type` to select.

**`per_signal`** — a dedicated table per signal type (recommended):

```toml
[starrocks.table_mapping]
type    = "per_signal"
logs    = "otel_logs"
metrics = "otel_metrics"
traces  = "otel_traces"
```

**`unified`** — all signals to one table with an injected discriminator column:

```toml
[starrocks.table_mapping]
type               = "unified"
table              = "otel_all"
signal_type_column = "signal_type"  # injected; values: "logs", "metrics", "traces"
```

The target table DDL must include `signal_type_column` when using `unified` mode.

---

## Telemetry Section (`[telemetry]`)

Configures how `opentelemetry-datalake` exports its own internal telemetry (self-monitoring).

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `otlp_endpoint` | String | `"http://localhost:4317"` | OTLP gRPC endpoint for internal telemetry. |
| `service_name` | String | `"otel-datalake"` | Service name for internal telemetry. |
| `region` | String | `null` | Cloud region tag to attach to internal telemetry. |

---

## Example Configuration (`config.toml`)

```toml
[server]
grpc_addr = "0.0.0.0:4317"
http_addr = "0.0.0.0:4318"

[telemetry]
otlp_endpoint = "http://jaeger:4317"
service_name = "otel-datalake-prod"

[iceberg]
catalog_type = "Rest"
catalog_uri = "http://iceberg-catalog:8181"
warehouse = "s3://telemetry-warehouse/"
logs_table_identifier = "otel.logs"
traces_table_identifier = "otel.traces"
metrics_table_identifier = "otel.metrics"
schema_mode = "catalog"
partition_granularity = "hourly"

[iceberg.properties]
"s3.endpoint" = "http://minio:9000"
"s3.access-key-id" = "admin"
"s3.secret-access-key" = "password"
"s3.region" = "us-east-1"
"s3.path.style.access" = "true"

[iceberg.batching]
max_batch_size_bytes = 67108864
max_batch_interval_sec = 30
```
