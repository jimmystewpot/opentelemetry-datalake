# StarRocks Stream Load Sink

The `starrocks-sink` crate ingests OpenTelemetry signal batches (Logs, Metrics, Traces) from the internal pipeline and writes them to a [StarRocks](https://www.starrocks.io/) cluster using the [Stream Load HTTP API](https://docs.starrocks.io/docs/loading/StreamLoad/).

---

## Requirements

| Requirement | Details |
|---|---|
| StarRocks version (CSV / JSON) | Any supported version |
| StarRocks version (Arrow IPC) | ≥ 2.5, with Arrow enabled on all BE nodes |
| Network access | OTLP receiver → StarRocks Frontend (FE) HTTP port (default `8030`) |

---

## Arrow IPC Compatibility

> **Important**: Using `format = "ipc"` requires StarRocks Backend (BE) nodes to have Arrow support compiled in and enabled.

To verify Arrow IPC is available on your cluster, run against any FE or BE:

```sql
SHOW VARIABLES LIKE '%stream_load%';
```

If you send an IPC payload to a BE that does not support Arrow, StarRocks returns an HTTP error. The sink logs:

```
StarRocks V1 stream load failed after retries; propagating backpressure
```

and returns `503 Service Unavailable` to the upstream OTLP producer.

**For older StarRocks clusters, use `format = "json"` or `format = "csv"` instead.**

---

## Configuration Reference

Add a `[starrocks]` section to your `config.toml`. All fields with defaults are optional.

```toml
[starrocks]
# One or more Frontend (FE) HTTP URLs. Round-robin failover is applied across all.
frontend_urls = ["http://fe-1:8030", "http://fe-2:8030"]

# Target database.
database = "otel"

# StarRocks username. Password via env var (see Credentials section).
username = "otel_writer"

# Wire format. Options: "ipc" (default), "json", "csv".
# Arrow IPC requires StarRocks ≥ 2.5 with Arrow BE support. See above.
format = "ipc"

# Transaction mode. Options: "v1" (default, at-least-once), "v2" (exactly-once, 2PC).
# V2 requires StarRocks transaction support to be enabled cluster-wide.
transaction_mode = "v1"

# Hard limit on serialized payload size in bytes. Default: 128 MiB.
# If a batch exceeds this limit the sink returns an error and applies backpressure.
max_payload_bytes = 134217728

# TCP connection timeout in seconds. Default: 10.
connect_timeout_secs = 10

# HTTP request/read timeout in seconds. Default: 600.
request_timeout_secs = 600

# Maximum SDK-level retries per request. Default: 3.
max_retries = 3

# Delay between retries in seconds. Default: 1.
retry_interval_secs = 1
```

### Table mapping — `per_signal` (recommended)

Each OTLP signal type writes to its own dedicated StarRocks table.

```toml
[starrocks.table_mapping]
type = "per_signal"
logs    = "otel_logs"
metrics = "otel_metrics"
traces  = "otel_traces"
```

### Table mapping — `unified`

All signal types write to a single table. A literal string column is injected into every batch to discriminate by signal type.

```toml
[starrocks.table_mapping]
type               = "unified"
table              = "otel_all"
signal_type_column = "signal_type"  # injected column; values: "logs", "metrics", "traces"
```

The StarRocks target table must include the `signal_type_column` in its DDL:

```sql
CREATE TABLE otel_all (
    timestamp   DATETIME NOT NULL,
    signal_type VARCHAR(16) NOT NULL,
    -- ... other OTLP fields
) ENGINE=OLAP
DUPLICATE KEY(timestamp, signal_type)
...;
```

---

## Credentials

The password field should **not** be stored in plain text in `config.toml`. Supply it via environment variable using the workspace-standard prefix:

```shell
export DATALAKE_STARROCKS_PASSWORD="your_password"
```

The `figment` config loader merges environment variables with the `DATALAKE_` prefix after the TOML file, so the env var takes precedence.

---

## TLS Configuration

By default, `starrocks-sink` is compiled with `rustls` (no system TLS dependencies required). If your organisation mandates platform / system TLS, rebuild with `tls-native-tls`:

```toml
# In the root Cargo.toml or your binary's Cargo.toml:
starrocks-sink = { workspace = true, default-features = false, features = ["tls-native-tls"] }
```

> **Note:** `tls-rustls` and `tls-native-tls` are mutually exclusive. Enabling both produces a compile error. For plain HTTP connections (internal networks), disable both features:
>
> ```toml
> starrocks-sink = { workspace = true, default-features = false }
> ```

---

## V1 vs V2 Transaction Modes

| | V1 (default) | V2 (two-phase commit) |
|---|---|---|
| Delivery guarantee | At-least-once | Exactly-once |
| Latency | Lower (single HTTP round-trip) | Higher (begin + load + prepare + commit) |
| StarRocks requirement | Any version | Transaction support enabled on cluster |
| Failure behaviour | SDK retries, then backpressure | Rollback attempted, then backpressure |
| Best for | High-throughput telemetry | Audit/compliance workloads |

For most telemetry use-cases, V1 is sufficient. Use V2 when duplicate rows in StarRocks are not acceptable.

---

## Payload Size Limits

The `max_payload_bytes` field (default 128 MiB) enforces a hard upper bound on the serialized size of each batch.

StarRocks BEs also impose a server-side limit controlled by the `stream_load_max_mb` BE configuration variable (default 100 MiB). Set `max_payload_bytes` **below** the BE limit to catch oversized batches before the network round-trip.

If a batch exceeds the limit:
1. The sink logs an error with the actual payload size.
2. `PipelineError::Internal` is returned, which propagates backpressure to the OTLP receiver.
3. The OTLP receiver responds to the producer with `503 Service Unavailable`.

To resolve: reduce the upstream batch size, or increase both `max_payload_bytes` and the StarRocks BE `stream_load_max_mb`.

---

## Example: Full Configuration

```toml
[server]
grpc_addr = "0.0.0.0:4317"
http_addr = "0.0.0.0:4318"

[starrocks]
frontend_urls        = ["http://fe-1.internal:8030", "http://fe-2.internal:8030"]
database             = "telemetry"
username             = "otel_writer"
format               = "ipc"
transaction_mode     = "v1"
max_payload_bytes    = 104857600   # 100 MiB — below BE default
connect_timeout_secs = 10
request_timeout_secs = 120
max_retries          = 5
retry_interval_secs  = 2

[starrocks.table_mapping]
type    = "per_signal"
logs    = "telemetry_logs"
metrics = "telemetry_metrics"
traces  = "telemetry_traces"
```

```shell
DATALAKE_STARROCKS_PASSWORD="s3cr3t" ./opentelemetry-datalake --config config.toml
```
