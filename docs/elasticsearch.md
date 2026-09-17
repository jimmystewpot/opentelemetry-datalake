# Elasticsearch & OpenSearch Data Streams Sink

The `elasticsearch-sink` crate ingests OpenTelemetry signal batches (Logs, Metrics, Traces) from the internal pipeline and writes them to [Elasticsearch](https://www.elastic.co/elasticsearch/) or [OpenSearch](https://opensearch.org/) clusters using the HTTP/2 NDJSON Bulk API (`/_bulk`) targeting data streams.

---

## Requirements & Compatibility

| Requirement | Details |
|---|---|
| **Elasticsearch Version** | ≥ 7.9+ (Data streams support recommended; 8.x+ fully supported) |
| **OpenSearch Version** | ≥ 1.0+ (including Amazon OpenSearch Service & OpenSearch Serverless) |
| **Wire Protocol** | HTTP/1.1 or HTTP/2 with persistent connection pooling and gzip compression |
| **Network Access** | OTLP receiver → Elasticsearch/OpenSearch HTTP/HTTPS port (default `9200` or `443`) |

---

## Architecture & Data Streams Convention

Elasticsearch and OpenSearch data streams provide an append-only, time-series storage abstraction backed by multiple auto-generated backing indices. `opentelemetry-datalake` streams Arrow `RecordBatch`es directly into data streams using the NDJSON bulk `create` action:

```json
{"create": {}}
{"@timestamp": "2026-09-17T06:00:00.000000000Z", "service.name": "frontend", ...}
```

### Standard Data Streams Naming

Following the OpenTelemetry and Elastic convention `<type>-<dataset>-<namespace>`:

* **Logs**: `logs-otel-default`
* **Metrics**: `metrics-otel-default`
* **Traces**: `traces-otel-default`

### Index Template Requirements

Before sending data, ensure an index template exists matching your data stream pattern with `data_stream: {}` enabled and `@timestamp` configured as a `date` or `date_nanos` field.

```json
PUT _index_template/otel_data_streams
{
  "index_patterns": ["logs-otel-*", "metrics-otel-*", "traces-otel-*"],
  "data_stream": {},
  "template": {
    "settings": {
      "index.mode": "time_series",
      "index.routing.allocation.total_shards_per_node": 3
    },
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "service.name": { "type": "keyword" }
      }
    }
  },
  "priority": 200
}
```

When `validate_on_startup = true` (the default), the sink validates the cluster health (`GET /`) and index template existence (`GET /_index_template/{template}`) during boot. If validation fails across all nodes, startup aborts immediately before accepting any OTLP traffic.

---

## Configuration Reference

Add an `[elasticsearch]` section to your `config.toml`. All fields with defaults are optional.

```toml
[elasticsearch]
# One or more cluster node HTTP/HTTPS URLs. Atomic round-robin load distribution
# and automated node-cooldown failover are applied across all endpoints.
endpoints = ["https://es01.internal:9200", "https://es02.internal:9200"]

# Target data streams per signal type.
[elasticsearch.data_streams]
logs    = "logs-otel-default"
metrics = "metrics-otel-default"
traces  = "traces-otel-default"

# Authentication configuration. Options: "none", "basic", "api_key", "bearer", or "aws_sigv4".
[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="

# TLS configuration.
[elasticsearch.tls]
ca_cert_path = "/etc/ssl/certs/es-ca.crt"
insecure_skip_verify = false

# Unpack stringified JSON map/struct attributes into native JSON objects. Default: true.
unpack_attributes = true

# Enable HTTP gzip Content-Encoding for bulk payloads. Default: true.
gzip_compression = true

# Maximum concurrent outbound bulk requests (throttled by semaphore). Default: 8.
max_concurrent_requests = 8

# Maximum serialized bulk payload size in bytes. Default: 20 MiB (20971520).
max_payload_bytes = 20971520

# TCP connection timeout in seconds. Default: 10.
connect_timeout_secs = 10

# HTTP request timeout in seconds. Default: 30.
request_timeout_secs = 30

# Maximum SDK-level retries for transient HTTP errors. Default: 3.
max_retries = 3

# Delay between retries in seconds (overridden by Retry-After when present). Default: 1.
retry_interval_secs = 1

# Perform startup cluster health and index template verification. Default: true.
validate_on_startup = true
```

---

## Authentication Schemes

### 1. No Authentication (`none`)
Used for local testing or secure private VPCs:
```toml
[elasticsearch.auth]
type = "none"
```

### 2. Basic Authentication (`basic`)
Username and password credentials:
```toml
[elasticsearch.auth]
type = "basic"
username = "elastic"
# Passwords should be provided via environment variables, not in plain text:
# export OTEL_DATALAKE_ELASTICSEARCH__AUTH__PASSWORD="secret_password"
```

### 3. API Key Authentication (`api_key`)
Elasticsearch and OpenSearch API keys (encoded `id:api_key` string):
```toml
[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="
```

### 4. Bearer Token Authentication (`bearer`)
OAuth2 or OIDC bearer token:
```toml
[elasticsearch.auth]
type = "bearer"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

### 5. AWS SigV4 Request Signing (`aws_sigv4`)
For Amazon OpenSearch Service and Amazon OpenSearch Serverless. Requires the binary to be compiled with the `aws` Cargo feature (`--features aws`).

```toml
[elasticsearch.auth]
type = "aws_sigv4"
region = "us-east-1"
service = "es" # Use "es" for OpenSearch Service, or "aoss" for OpenSearch Serverless
```

AWS credentials (access key, secret key, and optional session token) are automatically resolved from standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) or the AWS EC2/ECS/EKS instance metadata profile.

---

## Micro-Batching Buffer (`[elasticsearch.batching]`)

When configured, records are accumulated in memory across incoming OTLP batches until any threshold (bytes, records, or time) is met:

```toml
[elasticsearch.batching]
# Maximum accumulated Arrow byte size before flushing. Default: 10 MiB (10485760).
max_batch_size_bytes = 10485760

# Maximum record count before flushing. Default: 50000.
max_batch_records = 50000

# Maximum interval in seconds between flushes. Default: 10.
max_batch_interval_sec = 10
```

Flushing is handled on dedicated blocking threads via `tokio::task::spawn_blocking` to prevent CPU-intensive NDJSON serialization from starving Tokio async workers.

---

## Chronological Pre-Sorting (`[elasticsearch.order_by]`)

Pre-sorting records chronologically before emitting them to Elasticsearch or OpenSearch delivers major performance advantages:
1. **Lucene Segment Compaction**: Ingesting records ordered by `@timestamp` and high-cardinality terms (e.g. `service_name`) allows Lucene to build highly compressed, sequential index postings lists, reducing background merge I/O.
2. **Reduced Query Latency**: Time-series search queries can skip unindexed or out-of-range blocks efficiently.

```toml
[elasticsearch.order_by]
logs    = ["timestamp ASC", "service_name ASC NULLS LAST"]
metrics = ["timestamp ASC", "metric_name ASC"]
traces  = ["timestamp ASC"]
on_missing_column = "skip" # "skip" or "error"
```

Both SQL-like shorthand strings and structured syntax tables are supported.

---

## Resilience & Production Hardening

### Multi-Node Failover & Endpoint Cooldowns
The sink uses an atomic round-robin index over all configured `endpoints`. If a node experiences a network failure or returns an HTTP 5xx error:
1. The failing endpoint enters a 10-second cooldown period.
2. Subsequent bulk requests skip the cooled-down node and route to healthy siblings.
3. If all nodes are in cooldown (e.g. network partition), the sink attempts dispatch anyway rather than failing preemptively.

### Dynamic `Retry-After` Parsing
When Elasticsearch or OpenSearch nodes encounter high CPU or indexing pressure, they return HTTP `429 Too Many Requests` or `503 Service Unavailable`. The sink parses standard `Retry-After` headers (supporting both integer seconds and HTTP-date formats via `httpdate`) and sleeps dynamically up to a maximum cap of 60 seconds before retrying.

### Partial Bulk Item-Level Retries
Bulk responses returning HTTP 200 OK may still contain individual record rejections (`"errors": true`). The sink inspects the response `items` array:
* **Transient Item Errors (429 / 503)**: Sliced into a minimal retry NDJSON payload containing only the rejected records and retried across subsequent rounds.
* **Fatal Item Errors (400 Bad Request / Schema Mismatch)**: Logged with the exact rejection reason and dropped, preventing head-of-line blocking.
* **Successful Items**: Committed without re-transmission.

### Clean In-Flight Task Drainage
During shutdown or upon encountering a non-recoverable error, the sink awaits all in-flight asynchronous sibling tasks in its `JoinSet` before terminating, guaranteeing at-least-once delivery semantics without dropped batches.

---

## Full Example Configuration

```toml
[server]
grpc_addr = "0.0.0.0:4317"
http_addr = "0.0.0.0:4318"

[telemetry]
otlp_endpoint = "http://localhost:4317"
service_name = "opentelemetry-datalake"

[elasticsearch]
endpoints = [
  "https://es-node-01.internal:9200",
  "https://es-node-02.internal:9200",
  "https://es-node-03.internal:9200"
]
gzip_compression = true
unpack_attributes = true
max_concurrent_requests = 16
max_payload_bytes = 20971520 # 20 MiB
connect_timeout_secs = 5
request_timeout_secs = 30
max_retries = 5
retry_interval_secs = 2
validate_on_startup = true

[elasticsearch.data_streams]
logs    = "logs-otel-prod"
metrics = "metrics-otel-prod"
traces  = "traces-otel-prod"

[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="

[elasticsearch.tls]
ca_cert_path = "/etc/ssl/certs/corporate-root-ca.pem"
insecure_skip_verify = false

[elasticsearch.batching]
max_batch_size_bytes = 15728640 # 15 MiB
max_batch_records = 50000
max_batch_interval_sec = 5

[elasticsearch.order_by]
logs    = ["timestamp ASC", "service_name ASC"]
metrics = ["timestamp ASC", "metric_name ASC"]
traces  = ["timestamp ASC"]
```
