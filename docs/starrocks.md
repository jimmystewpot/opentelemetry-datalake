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

## Wire Format Comparison

| Format | `StarRocks` version | Throughput | StarRocks DDL required? | Notes |
|---|---|---|---|---|
| `ipc` (default) | ≥ 2.5 with Arrow BE support | Highest — zero string conversion | No | Verify BE support before using; see below |
| `json` | Any | Medium | No | Universal fallback; larger payloads than IPC |
| `csv` | Any | Medium | No | Simplest format; no schema metadata |

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

# Hard limit on serialized payload size in bytes. Default: 100 MiB (104857600).
# If a batch exceeds this limit the sink returns an error and applies backpressure.
max_payload_bytes = 104857600

# TCP connection timeout in seconds. Default: 10.
connect_timeout_secs = 10

# HTTP request/read timeout in seconds. Default: 600.
request_timeout_secs = 600

# Maximum SDK-level retries per request. Default: 3.
max_retries = 3

# Delay between retries in seconds. Default: 1.
retry_interval_secs = 1

# Optional unified sink buffer and lease configuration (recommended).
# Note: [starrocks.batching] is still accepted as a deprecated alias, but specifying
# both [starrocks.buffer] and [starrocks.batching] is rejected at startup.
[starrocks.buffer]
mode                   = "memory"      # "memory" (default) or "disk"
max_buffer_bytes       = 268435456     # Default: 256 MiB across all signals
max_batch_size_bytes   = 52428800      # Default: 50 MiB per Stream Load lease
max_batch_interval_sec = 30            # Default: 30 seconds
# max_batch_records    = 100000        # Optional record count trigger
overflow_policy        = "block"       # "block" (default) or "drop_oldest"
max_attempts           = 5
initial_backoff_ms     = 500
max_backoff_sec        = 30
# dead_letter_path     = "/var/lib/otel-datalake/dlq"

# Optional pre-sorting configuration per signal type.
[starrocks.order_by]
logs    = ["timestamp ASC", "service_name ASC"]
metrics = ["timestamp ASC", "name ASC"]
traces  = ["timestamp ASC", "service_name ASC"]
on_missing_column = "skip"
```

---

## Buffering, Lease-Derived Labels, and Dead-Letter Quarantine

`StarRocksSink` consumes batches through the unified [`SinkBuffer`](buffer.md) transactional lease lifecycle:

1. **Offloaded Preparation (`Lease::prepare`)**: Batch concatenation, `order_by` sorting, unified `signal_type` column injection, and IPC/JSON/CSV serialization execute inside `tokio::task::spawn_blocking` via `Lease::prepare` so the async Tokio executor is never blocked.
2. **Deterministic Stream Load Labels (`format_lease_label`)**: Each lease derives a deterministic Stream Load label from its target table, signal kind, and monotonic sequence range:
   ```text
   dl_{table}_{signal}_{first_seq}_{last_seq}
   ```
   If the formatted label exceeds StarRocks's 128-character limit or `table` contains characters outside `[A-Za-z0-9_-]`, the table segment is truncated and suffixed with a 16-character SHA-256 hex digest so the resulting label remains deterministic, ASCII-safe, and `<= 128` characters.
3. **Idempotent Retries (`Label Already Exists`)**: When a lease is retried after a transient network timeout or HTTP `503` (or replayed from the disk buffer after a crash), `format_lease_label` produces the exact same label. If StarRocks had already committed the earlier attempt and responds with `Status: "Label Already Exists"`, the sink treats the response as idempotent success and acknowledges the lease (`Lease::ack`).
4. **Failure Classification & Dead-Letter Quarantine**:
   - **`Transient`**: Network/transport errors, HTTP `408`/`429`/`5xx`, and `Publish Timeout` / backend overload errors call `lease.nack(FailureClass::Transient)` and retry with exponential backoff + full jitter, preserving the lease in buffer memory/disk.
   - **`Rejected`**: Data-quality rejections (`Status: "Fail"` due to `too many filtered rows`, schema/column mismatch, HTTP `4xx` other than `401`/`403`/`408`/`429`, or payloads exceeding `max_payload_bytes`) call `lease.nack(FailureClass::Rejected { reason })`. After `max_attempts`, the offending Arrow `RecordBatch`es and a sidecar `.json` manifest (including any StarRocks `ErrorLogUrl`) are quarantined to `dead_letter_path` (if configured) and dropped so poison batches cannot stall the pipeline.
   - **`Fatal`**: Authentication/authorization failures (`401`/`403`, `Access denied`) and TLS configuration errors call `lease.nack(FailureClass::Fatal { reason })`, tripping `/readyz` to `503 Service Unavailable`.

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

The password field should **not** be stored in plain text in `config.toml`. Supply it via environment variable using the workspace-standard `OTEL_DATALAKE_` prefix with `__` as the nested-key separator:

```shell
export OTEL_DATALAKE_STARROCKS__PASSWORD="your_password"
```

The `figment` config loader merges environment variables with the `OTEL_DATALAKE_` prefix after the TOML file, so the env var takes precedence.

---

## TLS Configuration

The StarRocks sink supports standard TLS configuration for secure HTTPS stream loads:

```toml
[starrocks.tls]
ca_cert_path = "/etc/ssl/certs/starrocks-ca.pem"
verification = "full" # "full" (default)
```

- `ca_cert_path`: Optional path to custom CA certificate (validated at startup). Note: when using `tls-native-tls`, custom CAs must be installed in the host OS trust store.
- `verification`: `"full"` (default; validates certificate and hostname). Disabling verification is prohibited for security (CWE-295).

### Compile-time TLS Backends

By default, `starrocks-sink` is compiled with `rustls` (`tls-rustls`, no system TLS dependencies required). If your organisation mandates platform / system TLS, rebuild with `tls-native-tls`:

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

The `max_payload_bytes` field (default 100 MiB / `104857600`) enforces a hard upper bound on the serialized size of each batch.

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
OTEL_DATALAKE_STARROCKS__PASSWORD="s3cr3t" ./opentelemetry-datalake --config config.toml
```
