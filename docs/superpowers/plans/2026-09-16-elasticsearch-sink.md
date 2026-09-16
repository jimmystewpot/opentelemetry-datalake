# Elasticsearch & OpenSearch Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance OpenSearch/Elasticsearch data streams sink crate that ingests Arrow `RecordBatch` payloads, serializes them to NDJSON bulk format, and streams them concurrently over HTTP/2 with multi-node failover, micro-batch buffering, and per-item partial retry.

**Architecture:** New workspace crate `crates/elasticsearch-sink` implementing `pipeline_core::pipeline::Sink`. Uses `reqwest` with HTTP/2 keep-alive for transport, a custom single-pass Arrow-to-NDJSON serializer, `tokio::sync::Semaphore` + `tokio::task::JoinSet` for pipelined concurrent dispatch, and `pipeline_core::sort::BatchSorter` for chronological pre-sorting.

**Tech Stack:** `reqwest` (HTTP/2, gzip, rustls), `serde`/`serde_json` (config & response parsing), `bytes` (zero-copy payloads), `flate2` (gzip compression), `itoa`/`ryu` (fast numeric formatting), `chrono` (timestamp formatting), `pipeline-core` (Sink trait, BatchSorter, PipelineError), `aws-sigv4`/`aws-credential-types` (optional AWS SigV4 signing).

**Spec:** [`docs/superpowers/specs/2026-09-16-elasticsearch-sink-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-16-elasticsearch-sink-design.md)

## Global Constraints

- Rust edition 2024, workspace resolver 2
- Zero-panic policy: no `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` (allowed in `#[cfg(test)]`)
- Error handling via `thiserror` in library crates, `anyhow` only in binary targets
- Use `crate::` paths, never `super::` outside test modules
- All public items must have doc comments
- `cargo fmt --all -- --check` must pass
- `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc` must pass
- `cargo test --workspace` must pass
- No `unsafe` without explicit sign-off and `// SAFETY:` comment
- Prefer bounded channels for all queues

---

## File Structure

```text
crates/elasticsearch-sink/
├── Cargo.toml
└── src/
    ├── lib.rs           # Module declarations, re-exports, Sink trait impl, run loop
    ├── config.rs        # ElasticsearchSinkConfig, AuthConfig, TlsConfig, BatchingConfig, DataStreamMapping
    ├── client.rs        # HttpClient: connection pool, round-robin, auth headers, gzip, TLS
    ├── serializer.rs    # Arrow RecordBatch → NDJSON bulk payload serialization
    ├── error.rs         # ElasticsearchError enum, From<ElasticsearchError> for PipelineError
    └── tls.rs           # TLS configuration builder (custom CA, skip verify)
```

**Modified files:**
- `Cargo.toml` (workspace root): add `elasticsearch-sink` to workspace dependencies and members
- `src/main.rs`: add `elasticsearch: Option<elasticsearch_sink::ElasticsearchSinkConfig>` to `AppConfig`, add sink initialization branch

---

### Task 1: Crate Scaffold, Config & Error Types

**Files:**
- Create: `crates/elasticsearch-sink/Cargo.toml`
- Create: `crates/elasticsearch-sink/src/lib.rs`
- Create: `crates/elasticsearch-sink/src/error.rs`
- Create: `crates/elasticsearch-sink/src/config.rs`
- Create: `crates/elasticsearch-sink/src/tls.rs`
- Modify: `Cargo.toml` (workspace root, lines 17-21 members, lines 50-58 workspace.dependencies)

**Interfaces:**
- Produces: `ElasticsearchSinkConfig`, `ElasticsearchAuthConfig`, `ElasticsearchBatchingConfig`, `DataStreamMapping`, `TlsConfig`, `ElasticsearchError`

- [ ] **Step 1: Create `crates/elasticsearch-sink/Cargo.toml`**

```toml
[package]
name = "elasticsearch-sink"
version = { workspace = true }
edition = { workspace = true }
license = { workspace = true }

[features]
default = []
aws = ["dep:aws-sigv4", "dep:aws-credential-types"]

[dependencies]
pipeline-core = { workspace = true }
arrow = { workspace = true, features = ["json"] }
bytes = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "http2", "gzip", "json"] }
chrono = { workspace = true }
itoa = "1"
ryu = "1"
flate2 = "1"

# Optional AWS SigV4 signing
aws-sigv4 = { version = "1", optional = true }
aws-credential-types = { version = "1", optional = true }

[dev-dependencies]
toml = { workspace = true }
wiremock = "0.6"
```

- [ ] **Step 2: Register crate in workspace root `Cargo.toml`**

Add to `[workspace.dependencies]` section (after line 58):
```toml
elasticsearch-sink = { path = "crates/elasticsearch-sink", version = "0.1.0" }
```

Verify workspace members glob `crates/*` already covers the new crate (it does — line 20).

- [ ] **Step 3: Create `crates/elasticsearch-sink/src/error.rs`**

```rust
use thiserror::Error;

/// Domain errors for the Elasticsearch/OpenSearch sink.
#[derive(Error, Debug)]
pub enum ElasticsearchError {
    /// HTTP transport or connection failure.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Bulk response indicated all items failed after retries exhausted.
    #[error("bulk request failed after {retries} retries: {message}")]
    BulkFailed {
        /// Number of retry attempts made.
        retries: usize,
        /// Diagnostic message from the last attempt.
        message: String,
    },

    /// Authentication or authorization failure (401/403).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Startup validation failure (cluster unreachable or missing index templates).
    #[error("startup validation failed: {0}")]
    StartupValidation(String),

    /// Serialization error during NDJSON construction.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Payload exceeds configured maximum size.
    #[error("payload size {actual} bytes exceeds limit {limit} bytes")]
    PayloadTooLarge {
        /// Actual serialized payload size.
        actual: usize,
        /// Configured maximum payload size.
        limit: usize,
    },
}

impl From<ElasticsearchError> for pipeline_core::error::PipelineError {
    fn from(err: ElasticsearchError) -> Self {
        match err {
            ElasticsearchError::AuthenticationFailed(_)
            | ElasticsearchError::BulkFailed { .. } => Self::DownstreamClosed,
            other => Self::Internal(other.to_string()),
        }
    }
}
```

- [ ] **Step 4: Create `crates/elasticsearch-sink/src/tls.rs`**

```rust
use serde::{Deserialize, Serialize};

/// TLS configuration for the Elasticsearch/OpenSearch HTTP client.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TlsConfig {
    /// Path to a PEM-encoded CA certificate file for custom certificate authorities.
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// Skip TLS certificate verification (development only).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}
```

- [ ] **Step 5: Create `crates/elasticsearch-sink/src/config.rs`**

```rust
use crate::tls::TlsConfig;
use serde::{Deserialize, Serialize};

fn default_max_concurrent_requests() -> usize { 8 }
fn default_max_payload_bytes() -> usize { 20_971_520 } // 20 MiB
fn default_connect_timeout_secs() -> u64 { 10 }
fn default_request_timeout_secs() -> u64 { 30 }
fn default_max_retries() -> usize { 3 }
fn default_retry_interval_secs() -> u64 { 1 }
fn default_true() -> bool { true }
fn default_max_batch_size_bytes() -> usize { 10_485_760 } // 10 MiB
fn default_max_batch_interval_sec() -> u64 { 10 }
fn default_max_batch_records() -> usize { 50_000 }

/// Data stream target names for each OTLP signal type.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataStreamMapping {
    /// Target data stream for log records.
    pub logs: String,
    /// Target data stream for metric records.
    pub metrics: String,
    /// Target data stream for trace/span records.
    pub traces: String,
}

/// Micro-batch accumulation thresholds.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ElasticsearchBatchingConfig {
    /// Maximum accumulated Arrow byte size before flushing.
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    /// Maximum interval in seconds between flushes.
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    /// Maximum accumulated record count before flushing.
    #[serde(default = "default_max_batch_records")]
    pub max_batch_records: usize,
}

/// Authentication configuration for the Elasticsearch/OpenSearch cluster.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ElasticsearchAuthConfig {
    /// No authentication (development / private VPC).
    #[default]
    None,
    /// HTTP Basic Authentication.
    Basic {
        /// Username for Basic Auth.
        username: String,
        /// Password for Basic Auth (prefer env var override).
        #[serde(default)]
        password: Option<String>,
    },
    /// Elasticsearch/OpenSearch API Key authentication.
    ApiKey {
        /// The API key value.
        api_key: String,
    },
    /// Bearer token authentication.
    Bearer {
        /// The bearer token value.
        token: String,
    },
    /// AWS SigV4 request signing (requires `aws` feature).
    #[cfg(feature = "aws")]
    AwsSigv4 {
        /// AWS region for signing.
        region: String,
        /// AWS service name (`es` for managed, `aoss` for serverless).
        #[serde(default = "default_aws_service")]
        service: String,
    },
}

#[cfg(feature = "aws")]
fn default_aws_service() -> String { "es".to_string() }

/// Top-level configuration for the Elasticsearch/OpenSearch sink.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ElasticsearchSinkConfig {
    /// One or more cluster node HTTP URLs for round-robin load distribution.
    pub endpoints: Vec<String>,

    /// Authentication configuration.
    #[serde(default)]
    pub auth: ElasticsearchAuthConfig,

    /// Data stream targets per signal type.
    pub data_streams: DataStreamMapping,

    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Whether to parse stringified JSON attributes into native JSON objects.
    #[serde(default = "default_true")]
    pub unpack_attributes: bool,

    /// Enable gzip Content-Encoding for bulk HTTP requests.
    #[serde(default = "default_true")]
    pub gzip_compression: bool,

    /// Maximum concurrent in-flight bulk requests.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Maximum serialized payload size in bytes.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,

    /// Connection timeout in seconds.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum retries for transient errors.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Delay between retries in seconds.
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,

    /// Perform startup health check and index template validation.
    #[serde(default = "default_true")]
    pub validate_on_startup: bool,

    /// Optional micro-batch accumulation thresholds.
    #[serde(default)]
    pub batching: Option<ElasticsearchBatchingConfig>,

    /// Optional pre-sorting configuration.
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,
}
```

- [ ] **Step 6: Create initial `crates/elasticsearch-sink/src/lib.rs`**

```rust
pub mod client;
pub mod config;
pub mod error;
pub mod serializer;
pub mod tls;

pub use config::ElasticsearchSinkConfig;
pub use error::ElasticsearchError;
```

- [ ] **Step 7: Write config deserialization tests**

Add to `crates/elasticsearch-sink/src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserializes_minimal() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.endpoints.len(), 1);
        assert!(cfg.unpack_attributes);
        assert!(cfg.gzip_compression);
        assert_eq!(cfg.max_concurrent_requests, 8);
        assert_eq!(cfg.max_payload_bytes, 20_971_520);
        assert!(cfg.validate_on_startup);
    }

    #[test]
    fn test_config_deserializes_basic_auth() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [auth]
            type = "basic"
            username = "admin"
            password = "secret"
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        assert!(matches!(cfg.auth, ElasticsearchAuthConfig::Basic { .. }));
    }

    #[test]
    fn test_config_deserializes_with_batching() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
            [batching]
            max_batch_size_bytes = 5242880
            max_batch_interval_sec = 5
            max_batch_records = 10000
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        let batching = cfg.batching.unwrap();
        assert_eq!(batching.max_batch_size_bytes, 5_242_880);
        assert_eq!(batching.max_batch_interval_sec, 5);
        assert_eq!(batching.max_batch_records, 10_000);
    }
}
```

- [ ] **Step 8: Run tests and verify they pass**

Run: `cargo test -p elasticsearch-sink`
Expected: All 3 config tests PASS

- [ ] **Step 9: Run clippy and fmt**

Run: `cargo clippy -p elasticsearch-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Run: `cargo fmt --all -- --check`
Expected: Zero warnings, zero formatting issues

- [ ] **Step 10: Commit**

```bash
git add crates/elasticsearch-sink/ Cargo.toml
git commit -m "feat(elasticsearch-sink): scaffold crate with config, error, and TLS types"
```

---

### Task 2: NDJSON Serializer

**Files:**
- Create: `crates/elasticsearch-sink/src/serializer.rs`

**Interfaces:**
- Consumes: `arrow::record_batch::RecordBatch`, `ElasticsearchSinkConfig.unpack_attributes` (bool), `ElasticsearchSinkConfig.max_payload_bytes` (usize)
- Produces: `fn serialize_batch(batch: &RecordBatch, unpack_attributes: bool, max_payload_bytes: usize) -> Result<Bytes, ElasticsearchError>`

- [ ] **Step 1: Write failing tests for NDJSON serialization**

Add to `crates/elasticsearch-sink/src/serializer.rs`:

```rust
//! Single-pass Arrow RecordBatch to NDJSON bulk serialization engine.

use crate::error::ElasticsearchError;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

/// Static bulk action header for data stream `create` operations.
const BULK_ACTION: &[u8] = b"{\"create\":{}}\n";

/// Serializes an Arrow `RecordBatch` into NDJSON bulk format for the Elasticsearch Bulk API.
///
/// Each row produces two lines: `{"create":{}}\n` followed by the JSON document `{...}\n`.
/// The `timestamp` column (nanosecond i64) is mapped to `@timestamp` in ISO 8601 format.
/// Attribute columns are either unpacked as native JSON objects or preserved as strings.
pub fn serialize_batch(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Bytes, ElasticsearchError> {
    if batch.num_rows() == 0 {
        return Ok(Bytes::new());
    }

    let mut buf = Vec::with_capacity(batch.get_array_memory_size() + batch.get_array_memory_size() / 5);
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    for row in 0..num_rows {
        buf.extend_from_slice(BULK_ACTION);
        buf.push(b'{');

        let mut first_field = true;
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let name = field.name();

            // Map "timestamp" to "@timestamp" with ISO 8601 formatting
            let output_name = if name == "timestamp" { "@timestamp" } else { name.as_str() };

            if col.is_null(row) {
                continue;
            }

            if !first_field {
                buf.push(b',');
            }
            first_field = false;

            // Write field name
            buf.push(b'"');
            buf.extend_from_slice(output_name.as_bytes());
            buf.extend_from_slice(b"\":");

            // Check if this is an attribute field that should be unpacked
            let is_attr_field = name == "attributes" || name == "resource_attributes";

            write_value(&mut buf, col, row, field.data_type(), unpack_attributes && is_attr_field)?;
        }

        buf.extend_from_slice(b"}\n");
    }

    let actual = buf.len();
    if actual > max_payload_bytes {
        return Err(ElasticsearchError::PayloadTooLarge {
            actual,
            limit: max_payload_bytes,
        });
    }

    Ok(Bytes::from(buf))
}

/// Writes a single Arrow value to the output buffer.
fn write_value(
    buf: &mut Vec<u8>,
    col: &dyn Array,
    row: usize,
    data_type: &DataType,
    unpack_json: bool,
) -> Result<(), ElasticsearchError> {
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
            let nanos = arr.value(row);
            let secs = nanos / 1_000_000_000;
            let subsec_nanos = (nanos % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, subsec_nanos) {
                buf.push(b'"');
                buf.extend_from_slice(dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string().as_bytes());
                buf.push(b'"');
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row);
            if unpack_json && (val.starts_with('{') || val.starts_with('[')) {
                // Embed raw JSON directly without escaping
                buf.extend_from_slice(val.as_bytes());
            } else {
                write_escaped_string(buf, val);
            }
        }
        DataType::Int32 => {
            let arr = col.as_primitive::<arrow::datatypes::Int32Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Int64 => {
            let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::UInt32 => {
            let arr = col.as_primitive::<arrow::datatypes::UInt32Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Float64 => {
            let arr = col.as_primitive::<arrow::datatypes::Float64Type>();
            let mut ryu_buf = ryu::Buffer::new();
            buf.extend_from_slice(ryu_buf.format(arr.value(row)).as_bytes());
        }
        _ => {
            // Fallback: format as JSON string
            let display = arrow::util::display::ArrayFormatter::try_new(col, &Default::default())
                .map_err(|e| ElasticsearchError::Serialization(e.to_string()))?;
            write_escaped_string(buf, &display.value(row).to_string());
        }
    }
    Ok(())
}

/// Writes a JSON-escaped string to the buffer.
fn write_escaped_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for byte in s.bytes() {
        match byte {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                buf.extend_from_slice(b"\\u00");
                let hi = b >> 4;
                let lo = b & 0x0f;
                buf.push(if hi < 10 { b'0' + hi } else { b'a' + hi - 10 });
                buf.push(if lo < 10 { b'0' + lo } else { b'a' + lo - 10 });
            }
            _ => buf.push(byte),
        }
    }
    buf.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_log_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(TimestampNanosecondArray::from(vec![1_726_500_000_000_000_000i64])),
            Arc::new(StringArray::from(vec!["frontend"])),
            Arc::new(Int32Array::from(vec![9])),
            Arc::new(StringArray::from(vec!["Request processed"])),
            Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#])),
        ]).unwrap()
    }

    #[test]
    fn test_serialize_produces_valid_ndjson() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2, "Expected 2 lines (action + document)");
        assert_eq!(lines[0], r#"{"create":{}}"#);
        assert!(lines[1].contains("@timestamp"));
        assert!(lines[1].contains("frontend"));
    }

    #[test]
    fn test_serialize_unpacks_attributes() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // Attributes should appear as native JSON object, not escaped string
        assert!(text.contains(r#""attributes":{"http.method":"GET"}"#));
    }

    #[test]
    fn test_serialize_preserves_attributes_as_string() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, false, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // Attributes should be an escaped JSON string
        assert!(text.contains(r#""attributes":"{\"http.method\":\"GET\"}"#));
    }

    #[test]
    fn test_serialize_empty_batch_returns_empty() {
        let batch = make_log_batch().slice(0, 0);
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_serialize_payload_too_large() {
        let batch = make_log_batch();
        let result = serialize_batch(&batch, true, 10); // 10 bytes limit
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ElasticsearchError::PayloadTooLarge { .. }));
    }

    #[test]
    fn test_serialize_timestamp_formatting() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // 1726500000 seconds = 2024-09-16T17:20:00Z
        assert!(text.contains("2024-09-16T"), "Timestamp must be ISO 8601: {text}");
    }
}
```

- [ ] **Step 2: Run tests and verify they pass**

Run: `cargo test -p elasticsearch-sink`
Expected: All serializer tests PASS

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p elasticsearch-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Zero warnings

- [ ] **Step 4: Commit**

```bash
git add crates/elasticsearch-sink/src/serializer.rs
git commit -m "feat(elasticsearch-sink): add single-pass Arrow-to-NDJSON bulk serializer"
```

---

### Task 3: HTTP Client with Round-Robin, Auth & TLS

**Files:**
- Create: `crates/elasticsearch-sink/src/client.rs`

**Interfaces:**
- Consumes: `ElasticsearchSinkConfig`, `TlsConfig`, `ElasticsearchAuthConfig`
- Produces: `HttpClient` with methods:
  - `fn try_new(config: &ElasticsearchSinkConfig) -> Result<Self, ElasticsearchError>`
  - `async fn send_bulk(&self, data_stream: &str, payload: Bytes) -> Result<BulkResponse, ElasticsearchError>`
  - `async fn health_check(&self) -> Result<(), ElasticsearchError>`
  - `async fn validate_index_template(&self, data_stream: &str) -> Result<(), ElasticsearchError>`

- [ ] **Step 1: Implement `HttpClient`**

Create `crates/elasticsearch-sink/src/client.rs` with:
- `reqwest::Client` initialization with TLS, timeouts, gzip
- `AtomicUsize` round-robin endpoint selector
- Auth header injection per request
- `send_bulk` method: `POST /<data_stream>/_bulk` with `Content-Type: application/x-ndjson`
- `BulkResponse` struct for parsing the root `errors` boolean and `items` array
- `health_check`: `GET /` with version validation
- `validate_index_template`: `GET /_index_template/<pattern>` existence check
- Jittered exponential backoff retry loop for 429/503

- [ ] **Step 2: Write tests for round-robin selection**

```rust
#[cfg(test)]
mod tests {
    // Test that next_endpoint() cycles through endpoints evenly
    // Test that auth headers are correctly formatted for Basic, ApiKey, Bearer
    // Test BulkResponse parsing with errors: false and errors: true
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p elasticsearch-sink`
Expected: All client tests PASS

- [ ] **Step 4: Run clippy**

Run: `cargo clippy -p elasticsearch-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/client.rs
git commit -m "feat(elasticsearch-sink): add HTTP/2 client with round-robin, auth, TLS, and retry"
```

---

### Task 4: Sink Trait Implementation (Run Loop, Buffering, Pipelined Dispatch)

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs`

**Interfaces:**
- Consumes: `HttpClient`, `serialize_batch()`, `BatchSorter`, `ElasticsearchSinkConfig`, `PipelineReceiver`, `SignalBatch`
- Produces: `ElasticsearchSink` implementing `pipeline_core::pipeline::Sink`

- [ ] **Step 1: Implement `ElasticsearchSink` struct and `Sink` trait**

In `crates/elasticsearch-sink/src/lib.rs`, implement:
- `ElasticsearchSink::try_new(config: ElasticsearchSinkConfig) -> Result<Self, PipelineError>`
- `Sink::run(&mut self, input: PipelineReceiver) -> Result<(), PipelineError>` with:
  - Startup validation (`health_check`, `validate_index_template`)
  - Per-signal `BufferState` accumulation
  - `tokio::select!` loop with interval timer and channel recv
  - `flush_buffer` → `concat_batches` → `sorter.sort` → `serialize_batch` → `send_bulk`
  - `Semaphore` + `JoinSet` pipelined dispatch
  - Graceful shutdown via `join_set.join_all()`

- [ ] **Step 2: Write integration test with `wiremock` mock server**

```rust
#[cfg(test)]
mod tests {
    // Test that the sink sends valid NDJSON to a mock server
    // Test that 429 responses trigger retries
    // Test that exhausted retries produce PipelineError::DownstreamClosed
    // Test graceful shutdown drains all in-flight requests
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p elasticsearch-sink`
Expected: All tests PASS

- [ ] **Step 4: Run clippy and fmt**

Run: `cargo clippy -p elasticsearch-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Run: `cargo fmt --all -- --check`

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -m "feat(elasticsearch-sink): implement Sink trait with buffering, sorting, and pipelined dispatch"
```

---

### Task 5: Wire into `main.rs` and Workspace Integration

**Files:**
- Modify: `Cargo.toml` (workspace root, add `elasticsearch-sink` to `[dependencies]`)
- Modify: `src/main.rs` (add `ElasticsearchSinkConfig` to `AppConfig`, add sink initialization branch)

**Interfaces:**
- Consumes: `ElasticsearchSink::try_new()`, `Sink::run()`
- Produces: Full pipeline integration with Elasticsearch/OpenSearch sink option

- [ ] **Step 1: Add `elasticsearch-sink` dependency to root `Cargo.toml`**

Add to `[dependencies]` section (after line 38):
```toml
elasticsearch-sink = { workspace = true }
```

- [ ] **Step 2: Add `elasticsearch` config field to `AppConfig` in `src/main.rs`**

Add after line 20 (`starrocks: Option<...>`):
```rust
elasticsearch: Option<elasticsearch_sink::ElasticsearchSinkConfig>,
```

- [ ] **Step 3: Update config validation in `src/main.rs`**

Update the validation check (around line 157) to include `elasticsearch`:
```rust
} else if config.kafka.is_none() && config.starrocks.is_none() && config.elasticsearch.is_none() {
```

And the final else branch (around line 371):
```rust
return Err(anyhow::anyhow!(
    "One of [kafka], [iceberg], [starrocks], or [elasticsearch] configuration must be provided"
));
```

- [ ] **Step 4: Add Elasticsearch sink initialization branch**

After the StarRocks branch (around line 369), add:
```rust
} else if let Some(es_cfg) = config.elasticsearch {
    tracing::info!(
        endpoints = ?es_cfg.endpoints,
        "Initializing Elasticsearch sinks"
    );

    let mut logs_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
    let mut traces_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
    let mut metrics_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg)?;

    logs_sink_handle = tokio::spawn(async move {
        if let Err(e) = logs_sink.run(logs_sink_rx).await {
            tracing::error!("Logs Elasticsearch sink error: {}", e);
        }
    });

    traces_sink_handle = tokio::spawn(async move {
        if let Err(e) = traces_sink.run(traces_sink_rx).await {
            tracing::error!("Traces Elasticsearch sink error: {}", e);
        }
    });

    metrics_sink_handle = tokio::spawn(async move {
        if let Err(e) = metrics_sink.run(metrics_sink_rx).await {
            tracing::error!("Metrics Elasticsearch sink error: {}", e);
        }
    });
```

- [ ] **Step 5: Run full workspace build and tests**

Run: `cargo build --workspace`
Run: `cargo test --workspace`
Run: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Run: `cargo fmt --all -- --check`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/main.rs
git commit -m "feat: wire elasticsearch-sink into pipeline orchestration"
```

---

### Task 6: Partial Bulk 429 Retry Engine

**Files:**
- Modify: `crates/elasticsearch-sink/src/client.rs`

**Interfaces:**
- Consumes: `BulkResponse.items` array with per-item status codes
- Produces: Enhanced `send_bulk` that extracts recoverable (429/503) document failures and constructs retry sub-payloads

- [x] **Step 1: Implement partial retry logic in `send_bulk`**

After receiving a `BulkResponse` with `errors: true`:
1. Parse the `items` array
2. Identify items with status 429 or 503
3. Extract the corresponding NDJSON line pairs (action + document) by index
4. Construct a retry sub-payload from only those lines
5. Re-submit the sub-payload with backoff
6. Log and count unrecoverable failures (400, etc.)

- [x] **Step 2: Write tests for partial retry**

```rust
#[cfg(test)]
mod tests {
    // Test: bulk response with mixed 200/429/400 items
    //   - 429 items are retried
    //   - 400 items are logged and dropped
    //   - 200 items are counted as successful
}
```

- [x] **Step 3: Run tests**

Run: `cargo test -p elasticsearch-sink`
Expected: All tests PASS

- [x] **Step 4: Commit**

```bash
git add crates/elasticsearch-sink/src/client.rs
git commit -m "feat(elasticsearch-sink): add partial bulk 429/503 per-item retry engine"
```

---

### Task 7: Final Verification & Documentation

**Files:**
- Verify: All crate files pass `cargo fmt`, `cargo clippy`, `cargo test`
- Verify: `cargo build --workspace` succeeds
- Verify: All doc comments are present on public items

- [x] **Step 1: Run full verification suite**

```bash
make all
```

Expected: `fmt`, `clippy`, `test`, `bench` all pass

- [x] **Step 2: Verify the full diff is clean**

```bash
git diff --stat origin/main..HEAD
```

- [x] **Step 3: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "chore(elasticsearch-sink): final cleanup and doc polish"
```
