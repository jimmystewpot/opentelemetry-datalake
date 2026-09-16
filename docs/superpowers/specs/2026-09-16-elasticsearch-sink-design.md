# High-Performance Elasticsearch & OpenSearch Sink Design Specification

- **Status:** Approved / Ready for Implementation
- **Author:** Antigravity / DeepMind Pair Programming Assistant
- **Date:** 2026-09-16
- **Target Workspace Crate:** `crates/elasticsearch-sink`

---

## 1. Overview & Objectives

The goal of this design is to introduce an ultra-high-performance OpenSearch and Elasticsearch sink (`crates/elasticsearch-sink`) to `opentelemetry-datalake`. The sink ingests columnar Apache Arrow `RecordBatch` payloads from the pipeline channels (`SignalBatch::Logs`, `SignalBatch::Metrics`, and `SignalBatch::Traces`), transforms them into newline-delimited JSON (NDJSON) bulk actions, and streams them concurrently into OpenSearch or Elasticsearch Data Streams using HTTP/2 keep-alive connections.

### Key Objectives
* **Low-Allocation / Single-Pass Serialization:** Directly convert Arrow record batches to NDJSON bulk format into a single pre-allocated buffer without intermediate AST or Serde object generation (`serde_json::Value`). Note: this is not true zero-copy (which would require Arrow IPC over the wire), but minimizes heap allocations to a single buffer per batch.
* **OpenSearch & Elasticsearch Compatibility:** Native support for modern append-only Data Streams (`logs-*-*`, `metrics-*-*`, `traces-*-*`) with auto-generated document IDs (`{"create":{}}`), bypassing Lucene version lookups for maximum cluster-side indexing throughput.
* **Non-Blocking Pipelined Dispatch:** Micro-batch accumulation coupled with a semaphore-bounded asynchronous worker pool to decouple Arrow serialization from network roundtrip latency.
* **Pre-Sorting Integration:** Integrated with `pipeline_core::sort::BatchSorter` to sort time-series batches chronologically prior to serialization, maximizing Lucene segment compression.
* **Cluster Failover & Load Distribution:** Client-side round-robin across multiple node endpoints with jittered exponential backoff retries on `429 Too Many Requests` or `503 Service Unavailable`.
* **Pluggable Authentication:** Built-in support for Basic Auth, Bearer Token, API Key, and AWS SigV4 (for Amazon OpenSearch Service / Serverless).
* **Strict Backpressure & Zero-Panic:** Conforms to the workspace zero-panic policy; propagates unrecoverable downstream backpressure as `PipelineError::DownstreamClosed` to signal upstream OTLP producers (`503 Service Unavailable`).

---

## 2. Architecture & Component Layout

### Workspace Structure
```text
opentelemetry-datalake/
├── Cargo.toml                              # Workspace root: add crates/elasticsearch-sink to members
├── src/main.rs                             # AppConfig: add optional [elasticsearch] sink configuration
└── crates/
    ├── core/                               # pipeline-core traits: Sink, SignalBatch, BatchSorter
    └── elasticsearch-sink/                 # [NEW CRATE]
        ├── Cargo.toml
        └── src/
            ├── lib.rs                      # Sink trait implementation, run loop, channel orchestration
            ├── client.rs                   # HTTP/2 connection pool, multi-node round-robin, auth headers
            ├── config.rs                   # ElasticsearchSinkConfig, ElasticsearchAuthConfig, batching, TLS
            ├── error.rs                    # ElasticsearchSinkError and PipelineError conversions
            ├── serializer.rs               # Vectorized Arrow-to-NDJSON bulk serialization engine
            └── tls.rs                      # TLS configuration: custom CA certificates, skip-verify
```

### Component Data Flow
```text
                         ┌─────────────────────────────────┐
                         │ STARTUP PHASE                   │
                         │ GET / (cluster health check)    │
                         │ Validate index templates exist  │
                         └────────────┬────────────────────┘
                                      │
[ OTLP Receivers ]                    ▼
        │
        ▼ (mpsc::channel)
[ Arrow RecordBatch ] (SignalBatch::Logs / Metrics / Traces)
        │
        ▼
[ BufferState ] ──► (Flush on max_bytes, max_records, or interval timer)
        │
        ▼
[ BatchSorter ] ──► (Chronological sorting by timestamp)
        │
        ▼
[ NDJSON Serializer ] ──► (Single-pass string slices + ISO 8601 formatting + {"create":{}}\n)
        │
        ▼
[ Pipelined Worker Pool ] ──► (Bounded Semaphore: max_concurrent_requests)
        │
        ▼
[ HTTP/2 Connection Pool ] ──► (Round-robin across cluster nodes with gzip compression)
        │
        ▼
[ OpenSearch / ES Data Streams ] (POST /{data_stream}/_bulk)
```

---

## 3. High-Performance NDJSON Serialization

### 3.1 Bulk Action Header
Data streams are append-only. Because the target data stream is provided directly in the request URL (`POST /<data_stream>/_bulk`), every action header in the payload is static:
```ndjson
{"create":{}}
```
Omitting explicit `_id` values guarantees:
1. **Zero Client Overhead:** No UUID generation or string formatting in the hot path.
2. **Zero Lucene Version Checks:** OpenSearch/Elasticsearch completely skips version and term-dictionary lookups, writing directly to active segment memory buffers.

### 3.2 Vectorized Row Serialization
The serializer pre-allocates a contiguous byte buffer sized to `batch.get_array_memory_size() * 1.2`.

For each row in the `RecordBatch`:
1. Append static slice: `b"{\"create\":{}}\n"`.
2. Write document opening: `b"{"`.
3. Format `@timestamp`: Read `i64` nanoseconds from the Arrow `timestamp` column. Format into ISO 8601 UTC string (`YYYY-MM-DDTHH:MM:SS.sssssssssZ`) using a stack-allocated buffer (no heap allocations), writing `b"\"@timestamp\":\"..."`.
4. Iterate over remaining Arrow columns:
   * **Attributes (`attributes`, `resource_attributes`):**
     * When `unpack_attributes = true`: The existing column contains a validated JSON string (e.g. `{"http.method":"GET","http.status_code":200}`). The serializer writes `b",\"attributes\":"` followed directly by the raw byte slice of the string. This embeds the native JSON object with **zero deserialization, zero AST construction, and zero intermediate allocations**.
     * When `unpack_attributes = false`: The string is escaped and written as a standard JSON string field.
   * **Numeric Columns (`Int32`, `Int64`, `Float64`, `UInt32`):** Formatted using fast integer/float formatting routines (`itoa` / `ryu`).
   * **String Columns (`service_name`, `body`, `trace_id`, `span_id`, etc.):** Escaped directly into the target buffer.
5. Write document closing: `b"}\n"`.

### 3.3 Payload Protection
The final serialized buffer is verified against `max_payload_bytes` (default: 20 MiB). If exceeded, `PipelineError::Internal` is returned, preventing HTTP 413 Payload Too Large rejections and avoiding JVM heap pressure on the cluster side. The 20 MiB default aligns with Elasticsearch/OpenSearch best practice guidance (recommended bulk payload: 5–15 MiB).

---

## 4. Buffer Management, Pre-Sorting & Pipelined Transport

### 4.1 Micro-Batch Buffering (`BufferState`)
Batches are accumulated per signal type (`logs`, `metrics`, `traces`). The buffer flushes under any of the following conditions:
* `buffered_bytes >= max_batch_size_bytes` (default: 10 MiB — sized conservatively to produce ~10–15 MiB serialized JSON payloads, within the ES/OpenSearch recommended bulk size range)
* `buffered_records >= max_batch_records` (default: 50,000)
* Periodic interval timer fires (`max_batch_interval_sec`, default: 10s)
* Input channel closes (graceful pipeline shutdown)

### 4.2 Pre-Sorting Engine
Before serialization, accumulated batches are concatenated via `arrow::compute::concat_batches` and passed to `pipeline_core::sort::BatchSorter`. Pre-sorting telemetry records chronologically by `@timestamp`:
* Maximizes Lucene segment compression ratios.
* Decreases storage footprint on disk.
* Significantly improves time-range query and aggregation performance.

### 4.3 Pipelined Concurrency Model
To prevent network latency from blocking upstream ingestion:
* The sink decouples serialization from network transport using an internal `tokio::sync::Semaphore` with `max_concurrent_requests` permits (default: 8).
* Serialized `Bytes` are dispatched via a `tokio::task::JoinSet`, which tracks all spawned worker tasks.
* The main sink loop immediately continues draining the input channel.
* On shutdown, the sink stops accepting new batches, then calls `join_set.join_all()` to await completion of all in-flight HTTP requests. This guarantees no data is lost if the runtime exits immediately after the sink loop terminates.

---

## 5. HTTP/2 Transport & Multi-Node Failover

### 5.1 Connection Management
* **Shared HTTP Client:** Backed by `reqwest::Client` with HTTP/2 multiplexing, persistent connection pooling, TCP keepalive (60s), and TCP nodelay.
* **Gzip Compression:** When `gzip_compression = true` (default), request bodies are compressed with `flate2` / `async-compression`, setting `Content-Encoding: gzip` to reduce network socket transmission time by up to 80%.
* **Endpoint Load Balancing:** The client holds a `Vec<String>` of cluster URLs. Node selection rotates via `AtomicUsize::fetch_add(1, Ordering::Relaxed)`.

### 5.2 Authentication Providers
Configurable via `ElasticsearchAuthConfig`:
* `None`: Unauthenticated (development / private VPC).
* `Basic`: HTTP Basic Auth (`Authorization: Basic <base64>`).
* `ApiKey`: Elasticsearch/OpenSearch API Key (`Authorization: ApiKey <key>`).
* `Bearer`: Bearer Token (`Authorization: Bearer <token>`).
* `AwsSigv4` (under `feature = "aws"`): Signs requests using AWS IAM credentials (supporting AWS OpenSearch Service and Amazon OpenSearch Serverless).

---

## 6. Configuration Specification

```toml
[elasticsearch]
# List of cluster node URLs (round-robin failover)
endpoints = ["https://opensearch-node-1:9200", "https://opensearch-node-2:9200"]

# Authentication configuration
auth = { type = "basic", username = "admin", password = "secretpassword" }

# Data stream mapping per signal type
[elasticsearch.data_streams]
logs = "logs-otel-default"
metrics = "metrics-otel-default"
traces = "traces-otel-default"

# TLS configuration (optional)
[elasticsearch.tls]
ca_cert_path = "/etc/ssl/certs/opensearch-ca.pem"   # Custom CA certificate
insecure_skip_verify = false                          # Disable TLS verification (dev only)

# Performance and serialization options
unpack_attributes = true
gzip_compression = true
max_concurrent_requests = 8
max_payload_bytes = 20971520        # 20 MiB (ES best practice: 5-15 MiB bulk payloads)
connect_timeout_secs = 10
request_timeout_secs = 30
max_retries = 3
retry_interval_secs = 1

# Startup behavior
validate_on_startup = true           # GET / health check + index template validation

# Optional micro-batch accumulation
[elasticsearch.batching]
max_batch_size_bytes = 10485760      # 10 MiB (produces ~10-15 MiB serialized JSON)
max_batch_interval_sec = 10
max_batch_records = 50000

# Optional chronological pre-sorting
[elasticsearch.order_by]
primary_sort_column = "timestamp"
ascending = true
```

---

## 7. Error Handling & Backpressure

### 7.1 Response Status & Retries
* **HTTP 200/201 with `errors: false`:** Fast-path acceptance. The batch is confirmed successful without iterating individual items.
* **HTTP 200/201 with `errors: true` (Partial Failures):** The response processor parses the `items` array and classifies each failed document:
  * **Recoverable failures (429 Too Many Requests, 503 Service Unavailable):** The failed documents are extracted by their original index position in the NDJSON payload, a retry sub-payload is constructed containing only those documents, and the sub-payload is re-submitted with jittered exponential backoff on an alternate round-robin node (up to `max_retries`). This guarantees at-least-once delivery for transient cluster-side saturation.
  * **Unrecoverable failures (400 mapping conflict, invalid field type):** These documents are logged with full diagnostic details (`error.type`, `error.reason`, `error.caused_by`) and incremented in failure metrics. They are not retried, as re-submission would produce the same error.
* **HTTP 429 (Full Request Rejected) / 503 (Service Unavailable):** The entire bulk request is retried with jittered exponential backoff on an alternate round-robin node up to `max_retries`.
* **Fatal Errors (401 / 403):** Log diagnostic details and return `PipelineError::DownstreamClosed` immediately (authentication/authorization failures are not transient).

### 7.2 Startup Validation
When `validate_on_startup = true` (default), the sink performs the following checks before the pipeline begins accepting OTLP data:
1. **Cluster Health Check:** `GET /` to verify the cluster is reachable and returns a valid version response.
2. **Index Template Validation:** For each configured data stream, verify that a matching composable index template exists (via `GET /_index_template/<pattern>`). If a required template is missing, the sink returns `PipelineError::Internal` with a diagnostic message, preventing silent creation of standard indices instead of data streams.

### 7.3 Backpressure Contract
If retries are exhausted on cluster overload or network loss, the sink returns `PipelineError::DownstreamClosed`. This closes the internal pipeline channel and signals the OTLP receivers to return `503 Service Unavailable` to external producers, honoring the workspace at-least-once delivery contract.

---

## 8. Verification & Testing Plan

### 8.1 Automated Unit Tests
1. **NDJSON Serialization:** Verify that Arrow batches with timestamps, strings, and numeric values serialize into valid NDJSON with `@timestamp` and `{"create":{}}`.
2. **Low-Allocation Attribute Unpacking:** Verify that when `unpack_attributes = true`, valid JSON strings in attributes are correctly rendered as nested JSON objects in the document output, and when `false`, they are preserved as escaped strings.
3. **Batching & Buffering:** Validate that buffer flushes trigger on size, record limit, and interval ticks.
4. **Pre-Sorting:** Confirm that batches sorted with `BatchSorter` produce chronologically ordered NDJSON lines.
5. **Round-Robin Node Selection:** Verify even distribution across multiple configured node URLs.
6. **Partial Bulk Retry Construction:** Verify that when a bulk response contains per-item 429 failures, a correct retry sub-payload is constructed containing only the failed documents.
7. **TLS Configuration:** Verify that custom CA cert path and insecure skip verify flags are correctly applied to the HTTP client.

### 8.2 Integration Tests (Mock HTTP Server)
1. **Bulk API Ingestion:** Mock HTTP server receives and validates the NDJSON payload, headers, and target stream URL.
2. **Gzip Decompression:** Verify that mock server decompresses gzipped payloads correctly.
3. **Auth Verification:** Verify correct formatting of Basic Auth, ApiKey, and Bearer headers.
4. **Backpressure Propagation:** Simulate HTTP 429 / 503 and verify that retries occur, followed by `PipelineError::DownstreamClosed` when retries are exhausted.
5. **Partial 429 Retry:** Simulate a 200 OK response with `errors: true` containing mixed 429 and 400 item failures. Verify that only the 429 documents are retried and 400 documents are logged and dropped.
6. **Startup Validation:** Simulate missing index templates and verify that the sink returns `PipelineError::Internal` before accepting data.
