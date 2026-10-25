//! `StarRocks` Stream Load sink for the `opentelemetry-datalake` pipeline.
//!
//! Receives [`pipeline_core::pipeline::SignalBatch`] events via [`SinkBuffer`], serializes
//! Arrow [`RecordBatch`] data into the configured format, and writes to a `StarRocks`
//! cluster via the [`starrocks_stream_load`] SDK.
//!
//! # Transaction modes
//!
//! - [`TransactionMode::V1`] — one-shot stream load per batch (at-least-once).
//! - [`TransactionMode::V2`] — two-phase commit (begin / load / prepare / commit),
//!   providing exactly-once semantics. A failed prepare triggers a best-effort rollback
//!   and propagates [`PipelineError::DownstreamClosed`] upstream so the OTLP receiver
//!   can return `503 Service Unavailable` to the producer.
//!
//! # TLS
//!
//! `rustls` is the default. See `[features]` in `Cargo.toml` for `tls-native-tls`.

// Compile-time guard: the SDK treats rustls and native-tls as mutually exclusive.
// Enabling both features in this crate will produce a clear build error here rather
// than a confusing linker failure inside the SDK.
#[cfg(all(feature = "tls-rustls", feature = "tls-native-tls"))]
compile_error!(
    "Features `tls-rustls` and `tls-native-tls` are mutually exclusive. \
     Disable default features and enable only one TLS backend."
);

use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::ipc::writer::StreamWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use pipeline_core::buffer::{
    BufferConfig, FailureClass, LeaseId, LegacyBatchingConfig, SinkBuffer,
};
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::Sink;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::time::Duration;
use tracing::{error, info, warn};

/// Type alias for the underlying `StarRocks` Stream Load SDK error.
pub type StreamLoadError = starrocks_stream_load::Error;

const MAX_STREAM_LOAD_LABEL_LEN: usize = 128;

/// Formats a deterministic `StarRocks` Stream Load transaction label from a table name
/// and [`LeaseId`] (`dl_{table}_{instance_id:016x}_{signal}_{first_seq}_{last_seq}`).
///
/// If the formatted label exceeds `StarRocks`'s 128-character limit or `table` contains
/// characters outside `[A-Za-z0-9_-]`, the table segment is sanitized and truncated with
/// a 16-character SHA-256 hex digest so retries of the same lease remain deterministic
/// and collision-resistant within `<= 128` ASCII characters.
#[must_use]
pub fn format_lease_label(table: &str, id: LeaseId) -> String {
    let suffix = format!(
        "_{:016x}_{}_{}_{}",
        id.instance_id,
        id.signal.as_str(),
        id.first_seq,
        id.last_seq
    );
    let sanitized_table: String = table
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let candidate = format!("dl_{sanitized_table}{suffix}");
    if candidate.len() <= MAX_STREAM_LOAD_LABEL_LEN && sanitized_table == table {
        return candidate;
    }

    let digest = Sha256::digest(table.as_bytes());
    let mut hex_buf = [0u8; 64];
    let hex_str = hex::encode_to_slice(digest, &mut hex_buf)
        .ok()
        .and_then(|()| std::str::from_utf8(&hex_buf[..16]).ok())
        .unwrap_or("0000000000000000");

    let overhead = 3usize
        .saturating_add(1)
        .saturating_add(16)
        .saturating_add(suffix.len());
    let max_prefix_len = MAX_STREAM_LOAD_LABEL_LEN.saturating_sub(overhead);
    let prefix_len = sanitized_table.len().min(max_prefix_len);
    let table_prefix = &sanitized_table[..prefix_len];

    format!("dl_{table_prefix}_{hex_str}{suffix}")
}

/// Returns `true` if the `StarRocks` Stream Load error indicates that the transaction label
/// was already committed (`Label Already Exists`), which is treated as an idempotent success.
#[must_use]
pub fn is_label_already_exists(err: &StreamLoadError) -> bool {
    match err {
        StreamLoadError::StarRocksFailure {
            status, message, ..
        } => {
            status.trim().eq_ignore_ascii_case("Label Already Exists")
                || status.trim().eq_ignore_ascii_case("LABEL_ALREADY_EXISTS")
                || message
                    .to_ascii_lowercase()
                    .contains("label already exists")
        }
        StreamLoadError::Transaction(msg) => {
            msg.to_ascii_lowercase().contains("label already exists")
        }
        _ => false,
    }
}

/// Classifies a [`StreamLoadError`] into a [`FailureClass`] for [`SinkBuffer`] lease handling.
#[must_use]
pub fn classify_stream_load_error(err: &StreamLoadError) -> FailureClass {
    match err {
        StreamLoadError::Network(_) | StreamLoadError::Io(_) | StreamLoadError::Json(_) => {
            FailureClass::Transient
        }
        StreamLoadError::UrlParse(_) => FailureClass::Fatal {
            reason: err.to_string(),
        },
        StreamLoadError::Transaction(msg) => {
            let lower = msg.to_ascii_lowercase();
            if lower.contains("access denied")
                || lower.contains("unauthorized")
                || lower.contains("authentication")
                || lower.contains("https scheme requires")
                || lower.contains("no configured load urls")
            {
                FailureClass::Fatal {
                    reason: err.to_string(),
                }
            } else if lower.contains("invalid ") {
                FailureClass::Rejected {
                    reason: err.to_string(),
                }
            } else {
                FailureClass::Transient
            }
        }
        StreamLoadError::StarRocksFailure {
            status, message, ..
        } => {
            let status_trimmed = status.trim();
            let http_code = status_trimmed
                .split_whitespace()
                .next()
                .and_then(|tok| tok.parse::<u16>().ok());

            if let Some(code) = http_code {
                return match code {
                    401 | 403 => FailureClass::Fatal {
                        reason: err.to_string(),
                    },
                    408 | 429 | 500..=599 => FailureClass::Transient,
                    400..=499 => FailureClass::Rejected {
                        reason: err.to_string(),
                    },
                    _ => FailureClass::Transient,
                };
            }

            let status_lower = status_trimmed.to_ascii_lowercase();
            let msg_lower = message.to_ascii_lowercase();

            if msg_lower.contains("access denied")
                || msg_lower.contains("authentication failed")
                || msg_lower.contains("permission denied")
                || msg_lower.contains("unauthorized")
            {
                return FailureClass::Fatal {
                    reason: err.to_string(),
                };
            }

            if status_lower == "publish timeout"
                || status_lower == "internal error"
                || msg_lower.contains("timeout")
                || msg_lower.contains("timed out")
                || msg_lower.contains("service unavailable")
                || msg_lower.contains("too many versions")
                || msg_lower.contains("too manyversions")
                || msg_lower.contains("memory limit exceeded")
                || msg_lower.contains("mem limit exceeded")
                || msg_lower.contains("no alive backend")
                || msg_lower.contains("backend not found")
                || msg_lower.contains("system busy")
                || msg_lower.contains("overload")
                || msg_lower.contains("try again")
                || msg_lower.contains("rpc failed")
                || msg_lower.contains("connection")
            {
                return FailureClass::Transient;
            }

            FailureClass::Rejected {
                reason: err.to_string(),
            }
        }
    }
}

// ─── Data format ────────────────────────────────────────────────────────────

/// Wire format used when sending data to `StarRocks`.
///
/// Arrow IPC is the highest-throughput option but requires BE nodes to have
/// Arrow support enabled. CSV and JSON are universal fallbacks.
///
/// See `docs/starrocks.md` for `StarRocks` version requirements per format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StarRocksFormat {
    /// Apache Arrow IPC streaming format (recommended; zero string conversion).
    ///
    /// Requires `StarRocks` ≥ 2.5 with Arrow enabled on all BE nodes.
    #[default]
    Ipc,
    /// Line-delimited JSON. Compatible with all `StarRocks` versions.
    Json,
    /// Comma-separated values. Compatible with all `StarRocks` versions.
    Csv,
}

impl std::fmt::Display for StarRocksFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ipc => write!(f, "ipc"),
            Self::Json => write!(f, "json"),
            Self::Csv => write!(f, "csv"),
        }
    }
}

// ─── Transaction mode ────────────────────────────────────────────────────────

/// Controls the `StarRocks` Stream Load API version used for each batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TransactionMode {
    /// V1: single-shot `send_single_batch`. At-least-once delivery.
    ///
    /// Simpler and lower latency; suitable for telemetry workloads where
    /// duplicate rows are acceptable.
    #[default]
    V1,
    /// V2: two-phase commit (`begin` / `load` / `prepare` / `commit`).
    /// Exactly-once delivery.
    ///
    /// Requires `StarRocks` to have 2PC transactions enabled. Any failure
    /// after `begin` triggers a best-effort `rollback`.
    V2,
}

// ─── Table mapping ───────────────────────────────────────────────────────────

/// Controls how the three OTLP signal types map to `StarRocks` tables.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableMapping {
    /// Write each signal type to a dedicated table.
    PerSignal {
        /// Target table for OpenTelemetry log records.
        logs: String,
        /// Target table for OpenTelemetry metric records.
        metrics: String,
        /// Target table for OpenTelemetry trace / span records.
        traces: String,
    },
    /// Write all signal types to one table with a discriminator column.
    Unified {
        /// Target table name.
        table: String,
        /// Column name that will be injected with the signal type string
        /// (`"logs"`, `"metrics"`, or `"traces"`).
        signal_type_column: String,
    },
}

impl TableMapping {
    /// Returns the resolved table name for the given signal type.
    ///
    /// For [`TableMapping::Unified`], this always returns the single shared table.
    #[must_use]
    pub fn table_for(&self, signal_type: &str) -> &str {
        match self {
            Self::PerSignal {
                logs,
                metrics,
                traces,
            } => match signal_type {
                "metrics" => metrics,
                "traces" => traces,
                "logs" => logs,
                other => {
                    debug_assert!(false, "unexpected signal_type: {other}");
                    logs
                }
            },
            Self::Unified { table, .. } => table,
        }
    }

    /// Returns the discriminator column name, if this is a `Unified` mapping.
    #[must_use]
    pub fn signal_type_column(&self) -> Option<&str> {
        match self {
            Self::Unified {
                signal_type_column, ..
            } => Some(signal_type_column.as_str()),
            Self::PerSignal { .. } => None,
        }
    }
}

// ─── Configuration ───────────────────────────────────────────────────────────

fn default_max_payload_bytes() -> usize {
    104_857_600 // 100 MiB — matches default StarRocks BE stream_load_max_mb.
}

fn default_connect_timeout_secs() -> u64 {
    10
}

fn default_request_timeout_secs() -> u64 {
    600
}

fn default_max_retries() -> usize {
    3
}

fn default_retry_interval_secs() -> u64 {
    1
}

fn default_max_batch_size_bytes() -> usize {
    52_428_800 // 50 MiB
}

fn default_max_batch_interval_sec() -> u64 {
    30
}

/// Configuration for buffering and accumulating batches prior to stream loading.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StarRocksBatchingConfig {
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    #[serde(default)]
    pub max_batch_records: Option<usize>,
}

/// Configuration for the `StarRocks` Stream Load sink.
///
/// Credentials: `username` can be set in config; `password` should be supplied
/// via the `OTEL_DATALAKE_STARROCKS__PASSWORD` environment variable (consistent with
/// the workspace-wide `Env::prefixed("OTEL_DATALAKE_")` figment pattern,
/// using `__` as the nested-key separator).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StarRocksSinkConfig {
    /// One or more `StarRocks` Frontend (FE) HTTP URLs.
    ///
    /// The SDK performs round-robin failover across all listed nodes.
    ///
    /// Example: `["http://fe-1:8030", "http://fe-2:8030"]`
    pub frontend_urls: Vec<String>,

    /// Target `StarRocks` database name.
    pub database: String,

    /// `StarRocks` username.
    pub username: String,

    /// `StarRocks` password. Override with `OTEL_DATALAKE_STARROCKS__PASSWORD` env var.
    #[serde(default)]
    pub password: Option<String>,

    /// Wire format for payload serialization.
    #[serde(default)]
    pub format: StarRocksFormat,

    /// V1 (at-least-once) or V2 (exactly-once, two-phase commit).
    #[serde(default)]
    pub transaction_mode: TransactionMode,

    /// How OTLP signal types map to `StarRocks` tables.
    pub table_mapping: TableMapping,

    /// Hard limit on serialized payload size in bytes.
    ///
    /// If a serialized batch exceeds this limit the sink returns
    /// [`PipelineError::Internal`] and the pipeline propagates backpressure.
    /// Default: 128 MiB.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,

    /// TCP connection timeout in seconds. Default: 10.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// HTTP request / read timeout in seconds. Default: 600.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum SDK-level retries per request. Default: 3.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Delay between retries in seconds. Default: 1.
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,

    /// Optional row order configuration for incoming signal batches.
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,

    /// Optional unified sink buffer configuration (`[starrocks.buffer]`).
    #[serde(default)]
    pub buffer: Option<BufferConfig>,

    /// Legacy batch buffering configuration (`[starrocks.batching]`).
    #[serde(default)]
    pub batching: Option<StarRocksBatchingConfig>,

    /// Standardized TLS configuration for secure connections.
    #[serde(default)]
    pub tls: pipeline_core::tls::TlsConfig,
}

impl StarRocksSinkConfig {
    /// Resolves the effective [`BufferConfig`] for this sink, mapping legacy `[starrocks.batching]`
    /// onto [`BufferConfig`] triggers and enforcing startup validation rules V1–V5.
    ///
    /// # Errors
    /// Returns [`PipelineError::Configuration`] if both `buffer` and `batching` are set (V4)
    /// or if any buffer invariant V1–V5 is violated.
    pub fn resolved_buffer_config(&self) -> Result<BufferConfig, PipelineError> {
        let legacy = self.batching.as_ref().map(|b| LegacyBatchingConfig {
            max_batch_size_bytes: b.max_batch_size_bytes,
            max_batch_records: b.max_batch_records,
            max_batch_interval_sec: b.max_batch_interval_sec,
        });
        BufferConfig::resolve("starrocks", self.buffer.clone(), legacy, true)
    }

    /// Validates `StarRocks` sink configuration parameters including TLS, URLs, and authentication.
    ///
    /// # Errors
    /// Returns [`PipelineError`] if:
    /// - TLS validation fails (e.g. insecure mode or missing CA file).
    /// - `frontend_urls` is empty.
    /// - `username` or `database` is blank.
    /// - `order_by` configuration fails to parse.
    /// - `[starrocks.buffer]` / `[starrocks.batching]` fails validation (V1–V5).
    pub fn validate(&self) -> Result<(), PipelineError> {
        self.tls.validate()?;
        if self.frontend_urls.is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `frontend_urls` must contain at least one FE URL"
                    .to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `username` must not be empty".to_string(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `database` must not be empty".to_string(),
            ));
        }
        match &self.table_mapping {
            TableMapping::PerSignal {
                logs,
                metrics,
                traces,
            } => {
                if logs.trim().is_empty() {
                    return Err(PipelineError::Internal(
                        "StarRocks configuration error: `table_mapping.logs` must not be empty"
                            .to_string(),
                    ));
                }
                if metrics.trim().is_empty() {
                    return Err(PipelineError::Internal(
                        "StarRocks configuration error: `table_mapping.metrics` must not be empty"
                            .to_string(),
                    ));
                }
                if traces.trim().is_empty() {
                    return Err(PipelineError::Internal(
                        "StarRocks configuration error: `table_mapping.traces` must not be empty"
                            .to_string(),
                    ));
                }
            }
            TableMapping::Unified {
                table,
                signal_type_column,
            } => {
                if table.trim().is_empty() {
                    return Err(PipelineError::Internal(
                        "StarRocks configuration error: `table_mapping.table` must not be empty"
                            .to_string(),
                    ));
                }
                if signal_type_column.trim().is_empty() {
                    return Err(PipelineError::Internal(
                        "StarRocks configuration error: `table_mapping.signal_type_column` must not be empty"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(ref sort_cfg) = self.order_by {
            pipeline_core::sort::BatchSorter::from_config(sort_cfg)?;
        }
        self.resolved_buffer_config()?;
        Ok(())
    }
}

// ─── Sink ────────────────────────────────────────────────────────────────────

/// `StarRocks` stream-load sink.
///
/// Implements [`Sink`] by receiving [`SignalBatch`] events, serializing them
/// to the configured wire format, and submitting them to `StarRocks` via the
/// [`StreamLoadManager`].
///
/// No internal buffer is maintained: `StarRocks` Stream Load is a streaming HTTP
/// protocol and the SDK manages connection pooling and FE failover internally.
/// Upstream batching is the responsibility of the pipeline channel capacity.
use std::sync::Arc;

pub struct StarRocksSink {
    config: StarRocksSinkConfig,
    logs_manager: Arc<StreamLoadManager>,
    metrics_manager: Arc<StreamLoadManager>,
    traces_manager: Arc<StreamLoadManager>,
    sorter: pipeline_core::sort::BatchSorter,
}

impl std::fmt::Debug for StarRocksSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarRocksSink")
            .field("config", &self.config)
            .field("logs_manager", &"<StreamLoadManager>")
            .field("metrics_manager", &"<StreamLoadManager>")
            .field("traces_manager", &"<StreamLoadManager>")
            .field("sorter", &self.sorter)
            .finish()
    }
}

fn build_manager(
    config: &StarRocksSinkConfig,
    table: &str,
) -> Result<Arc<StreamLoadManager>, PipelineError> {
    let sdk_config = StreamLoadConfig::builder(
        config.frontend_urls.clone(),
        config.database.clone(),
        config.username.clone(),
    )
    .password(config.password.clone().unwrap_or_default())
    .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
    .request_timeout(Duration::from_secs(config.request_timeout_secs))
    .max_retries(config.max_retries)
    .retry_interval(Duration::from_secs(config.retry_interval_secs))
    .enable_transaction(matches!(config.transaction_mode, TransactionMode::V2))
    .build();

    let sdk_format = match config.format {
        StarRocksFormat::Ipc => DataFormat::ARROW,
        StarRocksFormat::Json => DataFormat::JSON,
        StarRocksFormat::Csv => DataFormat::CSV,
    };

    let properties = StreamLoadTableProperties::builder()
        .table(table)
        .format(sdk_format)
        .build();

    let manager = StreamLoadManager::new(sdk_config, properties).map_err(|e| {
        PipelineError::Internal(format!("Failed to initialise StarRocks manager: {e}"))
    })?;

    Ok(Arc::new(manager))
}

impl StarRocksSink {
    /// Creates a new `StarRocksSink` with a shared [`StreamLoadManager`].
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if `order_by` configuration fails to parse.
    pub fn with_manager(
        config: StarRocksSinkConfig,
        manager: Arc<StreamLoadManager>,
    ) -> Result<Self, PipelineError> {
        config.validate()?;
        if config.tls.ca_cert_path.is_some() {
            tracing::warn!(
                sink = "starrocks",
                ca_cert_path = ?config.tls.ca_cert_path,
                "StarRocks Stream Load SDK resolves TLS roots via compile-time backend features (tls-rustls / tls-native-tls); ensure custom CA certificates are installed in the host trust store if using tls-native-tls"
            );
        }
        let sorter = if let Some(ref sort_cfg) = config.order_by {
            pipeline_core::sort::BatchSorter::from_config(sort_cfg)?
        } else {
            pipeline_core::sort::BatchSorter::default()
        };

        Ok(Self {
            config,
            logs_manager: Arc::clone(&manager),
            metrics_manager: Arc::clone(&manager),
            traces_manager: manager,
            sorter,
        })
    }

    /// Returns a reference to the underlying [`StreamLoadManager`] connection pool for logs.
    #[must_use]
    pub fn manager(&self) -> Arc<StreamLoadManager> {
        Arc::clone(&self.logs_manager)
    }

    /// Returns the [`StreamLoadManager`] instance dedicated to the given signal type.
    #[must_use]
    pub fn manager_for(&self, signal_type: &str) -> Arc<StreamLoadManager> {
        match signal_type {
            "metrics" => Arc::clone(&self.metrics_manager),
            "traces" => Arc::clone(&self.traces_manager),
            _ => Arc::clone(&self.logs_manager),
        }
    }

    /// Returns a reference to the sink's [`pipeline_core::sort::BatchSorter`].
    #[must_use]
    pub fn sorter(&self) -> &pipeline_core::sort::BatchSorter {
        &self.sorter
    }

    /// Creates a new `StarRocksSink` from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if configuration validation fails or
    /// if the [`StreamLoadManager`] cannot be initialised.
    pub fn try_new(config: StarRocksSinkConfig) -> Result<Self, PipelineError> {
        config.validate()?;
        if config.tls.ca_cert_path.is_some() {
            tracing::warn!(
                sink = "starrocks",
                ca_cert_path = ?config.tls.ca_cert_path,
                "StarRocks Stream Load SDK resolves TLS roots via compile-time backend features (tls-rustls / tls-native-tls); ensure custom CA certificates are installed in the host trust store if using tls-native-tls"
            );
        }

        let sorter = if let Some(ref sort_cfg) = config.order_by {
            pipeline_core::sort::BatchSorter::from_config(sort_cfg)?
        } else {
            pipeline_core::sort::BatchSorter::default()
        };

        let (logs_manager, metrics_manager, traces_manager) = match &config.table_mapping {
            TableMapping::Unified { table, .. } => {
                let mgr = build_manager(&config, table)?;
                (Arc::clone(&mgr), Arc::clone(&mgr), mgr)
            }
            TableMapping::PerSignal {
                logs,
                metrics,
                traces,
            } => {
                let logs_mgr = build_manager(&config, logs)?;
                let metrics_mgr = build_manager(&config, metrics)?;
                let traces_mgr = build_manager(&config, traces)?;
                (logs_mgr, metrics_mgr, traces_mgr)
            }
        };

        Ok(Self {
            config,
            logs_manager,
            metrics_manager,
            traces_manager,
            sorter,
        })
    }

    /// Serializes a [`RecordBatch`] into [`Bytes`] using the given [`StarRocksFormat`].
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if serialization fails.
    pub fn serialize_record_batch(
        batch: &RecordBatch,
        format: StarRocksFormat,
    ) -> Result<Bytes, PipelineError> {
        let mut buf = Vec::with_capacity(batch.get_array_memory_size());

        match format {
            StarRocksFormat::Ipc => {
                let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                    .map_err(|e| PipelineError::Internal(format!("IPC writer init failed: {e}")))?;
                writer
                    .write(batch)
                    .map_err(|e| PipelineError::Internal(format!("IPC write failed: {e}")))?;
                writer
                    .finish()
                    .map_err(|e| PipelineError::Internal(format!("IPC finish failed: {e}")))?;
            }
            StarRocksFormat::Json => {
                let mut writer = LineDelimitedWriter::new(&mut buf);
                writer
                    .write(batch)
                    .map_err(|e| PipelineError::Internal(format!("JSON write failed: {e}")))?;
            }
            StarRocksFormat::Csv => {
                let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut buf);
                writer
                    .write(batch)
                    .map_err(|e| PipelineError::Internal(format!("CSV write failed: {e}")))?;
            }
        }

        Ok(Bytes::from(buf))
    }

    /// Serializes a [`RecordBatch`] into [`Bytes`] using the configured format.
    ///
    /// Serialization happens into a pre-allocated `Vec<u8>` to avoid repeated
    /// small allocations. The resulting bytes are checked against
    /// `max_payload_bytes` before being returned.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if serialization fails or if the
    /// serialized payload exceeds `max_payload_bytes`.
    pub fn serialize_batch(&self, batch: &RecordBatch) -> Result<Bytes, PipelineError> {
        let bytes = Self::serialize_record_batch(batch, self.config.format)?;
        let n = bytes.len();
        let max = self.config.max_payload_bytes;
        if n > max {
            return Err(PipelineError::Internal(format!(
                "Serialized payload {n} bytes exceeds max_payload_bytes {max}; \
                 reduce upstream batch size or increase the limit in config"
            )));
        }

        Ok(bytes)
    }

    /// Injects a literal string column into a [`RecordBatch`] for unified table mapping.
    ///
    /// When [`TableMapping::Unified`] is configured, every batch is augmented with a
    /// column named `signal_type_column` containing the signal type string for all rows.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Arrow`] if the schema or batch construction fails.
    pub fn inject_signal_type_column(
        batch: &RecordBatch,
        column_name: &str,
        signal_type: &str,
    ) -> Result<RecordBatch, PipelineError> {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let discriminator: arrow::array::ArrayRef =
            Arc::new(StringArray::from(vec![signal_type; batch.num_rows()]));

        let new_field = Field::new(column_name, arrow::datatypes::DataType::Utf8, false);

        let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(new_field));
        let new_schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
        columns.push(discriminator);

        RecordBatch::try_new(new_schema, columns).map_err(PipelineError::Arrow)
    }

    /// Processes one batch via the V1 (at-least-once) path.
    async fn send_v1(
        manager: &StreamLoadManager,
        table: &str,
        label: &str,
        payload: Bytes,
    ) -> Result<(), StreamLoadError> {
        match manager.send_single_batch(label, payload).await {
            Ok(response) => {
                info!(
                    label = label,
                    table = table,
                    status = %response.status,
                    rows = ?response.number_loaded_rows,
                    "StarRocks V1 load committed"
                );
                Ok(())
            }
            Err(e) if is_label_already_exists(&e) => {
                info!(
                    label = label,
                    table = table,
                    "StarRocks V1 stream load returned Label Already Exists; treating as idempotent success"
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    label = label,
                    table = table,
                    error = %e,
                    "StarRocks V1 stream load failed"
                );
                Err(e)
            }
        }
    }

    fn is_load_state_committed(state: Option<&str>, existing_job_status: Option<&str>) -> bool {
        let matches_committed = |val: &str| {
            let trimmed = val.trim();
            trimmed.eq_ignore_ascii_case("VISIBLE")
                || trimmed.eq_ignore_ascii_case("COMMITTED")
                || trimmed.eq_ignore_ascii_case("FINISHED")
        };
        state.is_some_and(matches_committed) || existing_job_status.is_some_and(matches_committed)
    }

    async fn confirm_v2_label_committed(
        manager: &StreamLoadManager,
        table: &str,
        label: &str,
    ) -> Result<(), StreamLoadError> {
        match manager.get_load_status(label).await {
            Ok(status_resp)
                if (status_resp.status.trim().eq_ignore_ascii_case("Success")
                    || status_resp.status.trim().eq_ignore_ascii_case("OK"))
                    && Self::is_load_state_committed(
                        status_resp.state.as_deref(),
                        status_resp.existing_job_status.as_deref(),
                    ) =>
            {
                info!(
                    label = label,
                    table = table,
                    state = ?status_resp.state,
                    existing_job_status = ?status_resp.existing_job_status,
                    "StarRocks V2 confirmed existing label is committed; treating as idempotent success"
                );
                Ok(())
            }
            Ok(status_resp) => {
                warn!(
                    label = label,
                    table = table,
                    status = %status_resp.status,
                    state = ?status_resp.state,
                    existing_job_status = ?status_resp.existing_job_status,
                    "StarRocks V2 existing label is not committed; attempting rollback before retry"
                );
                if let Err(rb_err) = manager.rollback_transaction(label).await {
                    warn!(
                        label = label,
                        table = table,
                        error = %rb_err,
                        "StarRocks V2 rollback of uncommitted existing label failed"
                    );
                }
                Err(StreamLoadError::Transaction(format!(
                    "StarRocks V2 label '{label}' already exists in uncommitted state (status='{}', state={:?}, existing_job_status={:?})",
                    status_resp.status, status_resp.state, status_resp.existing_job_status
                )))
            }
            Err(status_err) => {
                warn!(
                    label = label,
                    table = table,
                    error = %status_err,
                    "StarRocks V2 failed to query load status for existing label; retaining lease for retry"
                );
                Err(status_err)
            }
        }
    }

    /// Processes one batch via the V2 (exactly-once, 2PC) path.
    ///
    /// Steps: `begin` → `load` → `prepare` → `commit`.
    /// Any failure after `begin` triggers a best-effort `rollback`; the rollback
    /// error is logged but not returned (the original error is propagated).
    async fn send_v2(
        &self,
        manager: &StreamLoadManager,
        table: &str,
        label: &str,
        payload: Bytes,
    ) -> Result<(), StreamLoadError> {
        let txn_id = match manager.begin_transaction(label).await {
            Ok(id) => id,
            Err(e) if is_label_already_exists(&e) => {
                return Self::confirm_v2_label_committed(manager, table, label).await;
            }
            Err(e) => {
                error!(label = label, error = %e, "StarRocks V2 begin_transaction failed");
                return Err(e);
            }
        };

        info!(
            label = label,
            txn_id = txn_id,
            "StarRocks V2 transaction begun"
        );

        if let Err(e) = manager
            .load_transaction_data(label, &self.config.database, table, 0, payload)
            .await
        {
            error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 load failed; attempting rollback");
            if let Err(rb_err) = manager.rollback_transaction(label).await {
                warn!(label = label, error = %rb_err, "StarRocks V2 rollback also failed — manual intervention may be needed");
            }
            return Err(e);
        }

        if let Err(e) = manager.prepare_transaction(label).await {
            error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 prepare failed; attempting rollback");
            if let Err(rb_err) = manager.rollback_transaction(label).await {
                warn!(label = label, error = %rb_err, "StarRocks V2 rollback also failed — manual intervention may be needed");
            }
            return Err(e);
        }

        let commit_response = match manager.commit_transaction(label).await {
            Ok(resp) => resp,
            Err(e) if is_label_already_exists(&e) => {
                return Self::confirm_v2_label_committed(manager, table, label).await;
            }
            Err(e) => {
                error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 commit failed; attempting rollback");
                if let Err(rb_err) = manager.rollback_transaction(label).await {
                    warn!(
                        label = label,
                        error = %rb_err,
                        "StarRocks V2 rollback after commit failure also failed — transaction {txn_id} may require manual resolution"
                    );
                }
                return Err(e);
            }
        };

        info!(
            label = label,
            txn_id = txn_id,
            table = table,
            status = %commit_response.status,
            "StarRocks V2 transaction committed"
        );
        Ok(())
    }
}

#[async_trait]
impl Sink for StarRocksSink {
    /// Runs the sink loop, acquiring transactional [`pipeline_core::buffer::Lease`]s from
    /// [`SinkBuffer`], offloading sorting and serialization via [`pipeline_core::buffer::Lease::prepare`],
    /// and acknowledging or nacking each lease according to the `StarRocks` Stream Load outcome.
    async fn run(&mut self, mut buffer: SinkBuffer) -> Result<(), PipelineError> {
        info!(
            database = %self.config.database,
            format = %self.config.format,
            mode = ?self.config.transaction_mode,
            "StarRocksSink started"
        );

        while let Some(lease) = buffer.next_lease().await? {
            if lease.num_rows() == 0 {
                lease.ack().await?;
                continue;
            }

            let signal = lease.signal();
            let sorter = self.sorter.clone();
            let format = self.config.format;
            let max_payload_bytes = self.config.max_payload_bytes;
            let signal_col_opt = self
                .config
                .table_mapping
                .signal_type_column()
                .map(str::to_owned);

            let prep_res = lease
                .prepare(move |batch| {
                    if batch.num_rows() == 0 {
                        return Ok(None);
                    }
                    let sorted_batch = sorter.sort(&batch, signal)?;
                    let final_batch = if let Some(ref col_name) = signal_col_opt {
                        Self::inject_signal_type_column(&sorted_batch, col_name, signal.as_str())?
                    } else {
                        sorted_batch
                    };
                    let payload = Self::serialize_record_batch(&final_batch, format)?;
                    let n = payload.len();
                    if n > max_payload_bytes {
                        return Err(PipelineError::Internal(format!(
                            "Serialized payload {n} bytes exceeds max_payload_bytes {max_payload_bytes}; \
                             reduce upstream batch size or increase the limit in config"
                        )));
                    }
                    Ok(Some(payload))
                })
                .await;

            let payload = match prep_res {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    lease.ack().await?;
                    continue;
                }
                Err(PipelineError::Storage(e)) => {
                    return Err(PipelineError::Storage(e));
                }
                Err(err) if err.to_string().starts_with("Lease::prepare join error") => {
                    return Err(err);
                }
                Err(err) => {
                    warn!(
                        signal = signal.as_str(),
                        first_seq = lease.id().first_seq,
                        last_seq = lease.id().last_seq,
                        attempt = lease.attempt(),
                        error = %err,
                        "StarRocks lease preparation failed; nacking as Rejected"
                    );
                    lease
                        .nack(FailureClass::Rejected {
                            reason: err.to_string(),
                        })
                        .await?;
                    continue;
                }
            };

            let signal_str = signal.as_str();
            let manager = self.manager_for(signal_str);
            let table = self.config.table_mapping.table_for(signal_str).to_owned();
            let label = format_lease_label(&table, lease.id());

            let send_res = match self.config.transaction_mode {
                TransactionMode::V1 => Self::send_v1(&manager, &table, &label, payload).await,
                TransactionMode::V2 => self.send_v2(&manager, &table, &label, payload).await,
            };

            match send_res {
                Ok(()) => {
                    lease.ack().await?;
                }
                Err(err) => {
                    let class = classify_stream_load_error(&err);
                    lease.nack(class).await?;
                }
            }
        }

        info!("StarRocksSink channel closed; shutting down gracefully");
        Ok(())
    }
}
// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use pipeline_core::pipeline::SignalBatch;
    use std::sync::Arc;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch() -> RecordBatch {
        let schema = make_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
            ],
        )
        .unwrap()
    }

    /// Helper: build a minimal config with a fixed FE URL.
    /// The SDK is not called in any of these unit tests.
    fn base_config() -> StarRocksSinkConfig {
        StarRocksSinkConfig {
            frontend_urls: vec!["http://127.0.0.1:8030".to_string()],
            database: "otel".to_string(),
            username: "root".to_string(),
            password: None,
            format: StarRocksFormat::Ipc,
            transaction_mode: TransactionMode::V1,
            table_mapping: TableMapping::PerSignal {
                logs: "otel_logs".to_string(),
                metrics: "otel_metrics".to_string(),
                traces: "otel_traces".to_string(),
            },
            max_payload_bytes: default_max_payload_bytes(),
            connect_timeout_secs: default_connect_timeout_secs(),
            request_timeout_secs: default_request_timeout_secs(),
            max_retries: default_max_retries(),
            retry_interval_secs: default_retry_interval_secs(),
            order_by: None,
            buffer: None,
            batching: None,
            tls: pipeline_core::tls::TlsConfig::default(),
        }
    }

    // ── Format ───────────────────────────────────────────────────────────────

    #[test]
    fn test_serialize_batch_ipc_produces_valid_bytes() {
        let config = base_config();
        let sink = StarRocksSink::try_new(config).unwrap();
        let batch = make_batch();
        let bytes = sink.serialize_batch(&batch).unwrap();
        assert!(!bytes.is_empty(), "IPC output must not be empty");
        // Validate by round-tripping through the IPC reader.
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        let recovered = reader.next().unwrap().unwrap();
        assert_eq!(recovered.num_rows(), 3);
    }

    #[test]
    fn test_serialize_batch_json_contains_expected_fields() {
        let mut config = base_config();
        config.format = StarRocksFormat::Json;
        let sink = StarRocksSink::try_new(config).unwrap();
        let batch = make_batch();
        let bytes = sink.serialize_batch(&batch).unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("\"id\""), "JSON must contain field 'id'");
        assert!(text.contains("\"name\""), "JSON must contain field 'name'");
        assert!(text.contains("\"a\""), "JSON must contain value 'a'");
    }

    #[test]
    fn test_serialize_batch_csv_contains_header_and_values() {
        let mut config = base_config();
        config.format = StarRocksFormat::Csv;
        let sink = StarRocksSink::try_new(config).unwrap();
        let batch = make_batch();
        let bytes = sink.serialize_batch(&batch).unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("id"), "CSV must contain column header 'id'");
        assert!(
            text.contains('1'.to_string().as_str()),
            "CSV must contain value 1"
        );
    }

    // ── Payload size limit ────────────────────────────────────────────────────

    #[test]
    fn test_payload_size_limit_returns_error_when_exceeded() {
        let mut config = base_config();
        config.max_payload_bytes = 1; // trivially small to guarantee rejection
        let sink = StarRocksSink::try_new(config).unwrap();
        let batch = make_batch();
        let result = sink.serialize_batch(&batch);
        assert!(
            result.is_err(),
            "Serialization must fail when payload exceeds max_payload_bytes"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::Internal(_)),
            "Expected PipelineError::Internal, got: {err}"
        );
    }

    // ── Unified table discriminator column ────────────────────────────────────

    #[test]
    fn test_inject_signal_type_column_adds_column_with_correct_values() {
        let batch = make_batch();
        let augmented =
            StarRocksSink::inject_signal_type_column(&batch, "signal_type", "traces").unwrap();

        assert_eq!(
            augmented.num_columns(),
            3,
            "Augmented batch must have original 2 columns + discriminator"
        );

        let col = augmented
            .column_by_name("signal_type")
            .expect("signal_type column must exist");

        let str_col = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("signal_type column must be StringArray");

        for i in 0..str_col.len() {
            assert_eq!(str_col.value(i), "traces");
        }
    }

    // ── Table resolution ──────────────────────────────────────────────────────

    #[test]
    fn test_per_signal_table_mapping_resolves_correct_table() {
        let mapping = TableMapping::PerSignal {
            logs: "log_tbl".to_string(),
            metrics: "metric_tbl".to_string(),
            traces: "trace_tbl".to_string(),
        };
        assert_eq!(mapping.table_for("logs"), "log_tbl");
        assert_eq!(mapping.table_for("metrics"), "metric_tbl");
        assert_eq!(mapping.table_for("traces"), "trace_tbl");
    }

    #[test]
    fn test_unified_table_mapping_always_returns_shared_table() {
        let mapping = TableMapping::Unified {
            table: "otel_all".to_string(),
            signal_type_column: "signal_type".to_string(),
        };
        assert_eq!(mapping.table_for("logs"), "otel_all");
        assert_eq!(mapping.table_for("metrics"), "otel_all");
        assert_eq!(mapping.table_for("traces"), "otel_all");
        assert_eq!(mapping.signal_type_column(), Some("signal_type"));
    }

    #[test]
    fn test_per_signal_mapping_has_no_discriminator_column() {
        let mapping = TableMapping::PerSignal {
            logs: "l".to_string(),
            metrics: "m".to_string(),
            traces: "t".to_string(),
        };
        assert_eq!(mapping.signal_type_column(), None);
    }

    // ── Config deserialisation ────────────────────────────────────────────────

    #[test]
    fn test_config_deserialises_from_toml_with_defaults() {
        let toml = r#"
            frontend_urls = ["http://fe-1:8030"]
            database = "telemetry"
            username = "writer"

            [table_mapping]
            type = "per_signal"
            logs = "otel_logs"
            metrics = "otel_metrics"
            traces = "otel_traces"
        "#;

        let config: StarRocksSinkConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.database, "telemetry");
        assert!(config.password.is_none());
        assert_eq!(config.max_payload_bytes, default_max_payload_bytes());
        assert!(matches!(config.format, StarRocksFormat::Ipc));
        assert!(matches!(config.transaction_mode, TransactionMode::V1));
        assert!(config.order_by.is_none());
    }

    #[test]
    fn test_config_deserialises_unified_mapping() {
        let toml = r#"
            frontend_urls = ["http://fe-1:8030"]
            database = "telemetry"
            username = "writer"
            format = "json"
            transaction_mode = "v2"

            [table_mapping]
            type = "unified"
            table = "otel_all"
            signal_type_column = "signal_type"
        "#;

        let config: StarRocksSinkConfig = toml::from_str(toml).unwrap();
        assert!(matches!(config.format, StarRocksFormat::Json));
        assert!(matches!(config.transaction_mode, TransactionMode::V2));
        assert!(matches!(config.table_mapping, TableMapping::Unified { .. }));
    }

    // ── Label generation ──────────────────────────────────────────────────────

    #[test]
    fn test_make_label_is_unique_across_calls() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        let a = format_lease_label(
            "otel_traces",
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Traces,
                first_seq: 1,
                last_seq: 1,
            },
        );
        let b = format_lease_label(
            "otel_traces",
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Traces,
                first_seq: 2,
                last_seq: 2,
            },
        );
        assert_ne!(a, b, "Labels must be distinct across lease sequence ranges");
        assert!(
            a.starts_with("dl_otel_traces_0123456789abcdef_traces_"),
            "Label must include table, instance_id, and signal type prefix"
        );
    }

    // ── Validation & Pool Sharing ─────────────────────────────────────────────

    #[test]
    fn test_try_new_validates_empty_frontend_urls() {
        let mut config = base_config();
        config.frontend_urls.clear();
        let err = StarRocksSink::try_new(config).unwrap_err();
        assert!(
            matches!(err, PipelineError::Internal(ref msg) if msg.contains("frontend_urls")),
            "Expected empty frontend_urls error, got: {err}"
        );
    }

    #[test]
    fn test_try_new_validates_empty_username_and_database() {
        let mut config = base_config();
        config.username = "  ".to_string();
        let err = StarRocksSink::try_new(config).unwrap_err();
        assert!(
            matches!(err, PipelineError::Internal(ref msg) if msg.contains("username")),
            "Expected empty username error, got: {err}"
        );

        let mut config2 = base_config();
        config2.database = String::new();
        let err2 = StarRocksSink::try_new(config2).unwrap_err();
        assert!(
            matches!(err2, PipelineError::Internal(ref msg) if msg.contains("database")),
            "Expected empty database error, got: {err2}"
        );
    }

    #[test]
    fn test_starrocks_v1_table_name_resolution() {
        let config = base_config();
        let sink = StarRocksSink::try_new(config).unwrap();

        assert_eq!(
            sink.manager_for("logs").properties().table.as_deref(),
            Some("otel_logs")
        );
        assert_eq!(
            sink.manager_for("metrics").properties().table.as_deref(),
            Some("otel_metrics")
        );
        assert_eq!(
            sink.manager_for("traces").properties().table.as_deref(),
            Some("otel_traces")
        );
        assert!(
            !Arc::ptr_eq(&sink.manager_for("logs"), &sink.manager_for("traces")),
            "PerSignal mapping must build distinct managers per signal table"
        );
        assert!(
            !Arc::ptr_eq(&sink.manager_for("logs"), &sink.manager_for("metrics")),
            "PerSignal mapping must build distinct managers per signal table"
        );
        assert!(
            !Arc::ptr_eq(&sink.manager_for("metrics"), &sink.manager_for("traces")),
            "PerSignal mapping must build distinct managers per signal table"
        );
    }

    #[test]
    fn test_unified_mapping_shares_single_manager_with_table_set() {
        let mut config = base_config();
        config.table_mapping = TableMapping::Unified {
            table: "otel_all".to_string(),
            signal_type_column: "signal_type".to_string(),
        };
        let sink = StarRocksSink::try_new(config).unwrap();

        let logs_mgr = sink.manager_for("logs");
        let metrics_mgr = sink.manager_for("metrics");
        let traces_mgr = sink.manager_for("traces");

        assert_eq!(logs_mgr.properties().table.as_deref(), Some("otel_all"));
        assert!(
            Arc::ptr_eq(&logs_mgr, &metrics_mgr) && Arc::ptr_eq(&logs_mgr, &traces_mgr),
            "Unified mapping must share a single Arc<StreamLoadManager> across all signals"
        );
    }

    #[test]
    fn test_try_new_validates_empty_table_mapping_fields() {
        let mut cfg_empty_logs = base_config();
        cfg_empty_logs.table_mapping = TableMapping::PerSignal {
            logs: "   ".to_string(),
            metrics: "otel_metrics".to_string(),
            traces: "otel_traces".to_string(),
        };
        assert!(StarRocksSink::try_new(cfg_empty_logs).is_err());

        let mut cfg_empty_unified_table = base_config();
        cfg_empty_unified_table.table_mapping = TableMapping::Unified {
            table: String::new(),
            signal_type_column: "signal_type".to_string(),
        };
        assert!(StarRocksSink::try_new(cfg_empty_unified_table).is_err());

        let mut cfg_empty_unified_col = base_config();
        cfg_empty_unified_col.table_mapping = TableMapping::Unified {
            table: "otel_all".to_string(),
            signal_type_column: " ".to_string(),
        };
        assert!(StarRocksSink::try_new(cfg_empty_unified_col).is_err());
    }

    #[test]
    fn test_with_manager_shares_underlying_arc() {
        let config = base_config();
        let sink1 = StarRocksSink::try_new(config.clone()).unwrap();
        let manager_arc = sink1.manager();

        let sink2 = StarRocksSink::with_manager(config, Arc::clone(&manager_arc)).unwrap();
        assert!(
            Arc::ptr_eq(&sink1.manager(), &sink2.manager()),
            "Arc pointers must be equal when using with_manager"
        );
    }

    #[test]
    fn test_default_max_payload_bytes_is_100mb() {
        assert_eq!(default_max_payload_bytes(), 104_857_600);
    }

    /// All `StarRocksFormat` variants must produce expected Display strings.
    #[test]
    fn test_starrocks_format_display() {
        assert_eq!(StarRocksFormat::Ipc.to_string(), "ipc");
        assert_eq!(StarRocksFormat::Json.to_string(), "json");
        assert_eq!(StarRocksFormat::Csv.to_string(), "csv");
    }

    /// `format_lease_label` must produce distinct labels for all three signal type prefixes.
    #[test]
    fn test_make_label_for_all_signal_types() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        let logs_label = format_lease_label(
            "otel_logs",
            LeaseId {
                instance_id: 0x0000_0000_0000_0001,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 1,
            },
        );
        let metrics_label = format_lease_label(
            "otel_metrics",
            LeaseId {
                instance_id: 0x0000_0000_0000_0001,
                signal: SignalKind::Metrics,
                first_seq: 1,
                last_seq: 1,
            },
        );
        let traces_label = format_lease_label(
            "otel_traces",
            LeaseId {
                instance_id: 0x0000_0000_0000_0001,
                signal: SignalKind::Traces,
                first_seq: 1,
                last_seq: 1,
            },
        );

        assert!(logs_label.starts_with("dl_otel_logs_0000000000000001_logs_"));
        assert!(metrics_label.starts_with("dl_otel_metrics_0000000000000001_metrics_"));
        assert!(traces_label.starts_with("dl_otel_traces_0000000000000001_traces_"));

        // All labels must be unique
        assert_ne!(logs_label, metrics_label);
        assert_ne!(logs_label, traces_label);
    }

    /// `inject_signal_type_column` must successfully add a discriminator
    /// column even when the input batch has zero rows.
    #[test]
    fn test_inject_signal_type_column_empty_batch() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let empty_batch = RecordBatch::new_empty(schema);

        let result = StarRocksSink::inject_signal_type_column(&empty_batch, "signal_type", "logs");
        assert!(
            result.is_ok(),
            "inject_signal_type_column must succeed on empty batch"
        );
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 0);
        // The signal_type column must still be present
        assert!(
            batch.column_by_name("signal_type").is_some(),
            "signal_type column must be added even for zero-row batches"
        );
        // Its value array must have Utf8 type
        let col = batch.column_by_name("signal_type").unwrap();
        assert_eq!(*col.data_type(), DataType::Utf8);
    }

    /// `inject_signal_type_column` for a metrics signal must use "metrics" as the value.
    #[test]
    fn test_inject_signal_type_column_metrics_signal() {
        use arrow::array::{AsArray, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))])
                .unwrap();

        let result =
            StarRocksSink::inject_signal_type_column(&batch, "signal_type", "metrics").unwrap();
        assert_eq!(result.num_rows(), 2);
        let sig_col = result
            .column_by_name("signal_type")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(sig_col.value(0), "metrics");
        assert_eq!(sig_col.value(1), "metrics");
    }

    /// Helper: alias for `base_config` for plan compatibility.
    fn make_config() -> StarRocksSinkConfig {
        base_config()
    }

    /// The `PerSignal` table mapping must correctly select the right table
    /// for each signal type.
    #[test]
    fn test_table_mapping_per_signal_selects_correct_table() {
        let mapping = TableMapping::PerSignal {
            logs: "logs_tbl".to_string(),
            metrics: "metrics_tbl".to_string(),
            traces: "traces_tbl".to_string(),
        };
        assert_eq!(mapping.table_for("logs"), "logs_tbl");
        assert_eq!(mapping.table_for("metrics"), "metrics_tbl");
        assert_eq!(mapping.table_for("traces"), "traces_tbl");
    }

    #[test]
    fn test_starrocks_sink_pre_sorted_serialization() {
        let mut config = make_config();
        config.order_by = Some(pipeline_core::sort::SortConfig {
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "id DESC".to_string(),
            )],
            ..Default::default()
        });
        let sink = StarRocksSink::try_new(config).expect("try_new failed");

        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 3, 2])),
                Arc::new(StringArray::from(vec!["a", "c", "b"])),
            ],
        )
        .unwrap();

        let sorted = sink
            .sorter()
            .sort(&batch, pipeline_core::sort::SignalType::Logs)
            .unwrap();
        let id_col = sorted
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 3);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 1);
    }

    #[test]
    fn test_starrocks_sink_pre_sorted_metrics_and_traces() {
        let mut config = make_config();
        config.order_by = Some(pipeline_core::sort::SortConfig {
            metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "id ASC".to_string(),
            )],
            traces: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "name DESC".to_string(),
            )],
            ..Default::default()
        });
        let sink = StarRocksSink::try_new(config).expect("try_new failed");

        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![5, 1, 3])),
                Arc::new(StringArray::from(vec!["alpha", "charlie", "bravo"])),
            ],
        )
        .unwrap();

        // Sort metrics by id ASC
        let sorted_metrics = sink
            .sorter()
            .sort(&batch, pipeline_core::sort::SignalType::Metrics)
            .unwrap();
        let id_col = sorted_metrics
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 3);
        assert_eq!(id_col.value(2), 5);

        // Sort traces by name DESC
        let sorted_traces = sink
            .sorter()
            .sort(&batch, pipeline_core::sort::SignalType::Traces)
            .unwrap();
        let name_col = sorted_traces
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "charlie");
        assert_eq!(name_col.value(1), "bravo");
        assert_eq!(name_col.value(2), "alpha");
    }

    #[test]
    fn test_starrocks_sink_with_manager_preserves_sorter() {
        let mut config = make_config();
        config.order_by = Some(pipeline_core::sort::SortConfig {
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "id DESC".to_string(),
            )],
            ..Default::default()
        });
        let sink1 = StarRocksSink::try_new(config.clone()).unwrap();
        let sink2 = StarRocksSink::with_manager(config, sink1.manager()).unwrap();

        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 30, 20])),
                Arc::new(StringArray::from(vec!["x", "z", "y"])),
            ],
        )
        .unwrap();

        let sorted = sink2
            .sorter()
            .sort(&batch, pipeline_core::sort::SignalType::Logs)
            .unwrap();
        let id_col = sorted
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 30);
        assert_eq!(id_col.value(1), 20);
        assert_eq!(id_col.value(2), 10);
    }

    #[test]
    fn test_starrocks_sink_order_by_toml_deserialization() {
        let toml = r#"
            frontend_urls = ["http://fe-1:8030"]
            database = "telemetry"
            username = "writer"

            [table_mapping]
            type = "per_signal"
            logs = "otel_logs"
            metrics = "otel_metrics"
            traces = "otel_traces"

            [order_by]
            logs = ["timestamp DESC", "service_name ASC"]
            metrics = ["metric_name ASC"]
        "#;

        let config: StarRocksSinkConfig = toml::from_str(toml).unwrap();
        let order_by = config
            .order_by
            .as_ref()
            .expect("order_by should be present");
        assert_eq!(order_by.logs.len(), 2);
        assert_eq!(order_by.metrics.len(), 1);
        assert!(order_by.traces.is_empty());

        let sink =
            StarRocksSink::try_new(config).expect("try_new should succeed with valid order_by");

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("service_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 100])),
                Arc::new(StringArray::from(vec![
                    "b_service",
                    "a_service",
                    "a_service",
                ])),
            ],
        )
        .unwrap();

        let sorted = sink
            .sorter()
            .sort(&batch, pipeline_core::sort::SignalType::Logs)
            .unwrap();
        let ts_col = sorted
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let svc_col = sorted
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(ts_col.value(0), 200);
        assert_eq!(svc_col.value(0), "a_service");
        assert_eq!(ts_col.value(1), 100);
        assert_eq!(svc_col.value(1), "a_service");
        assert_eq!(ts_col.value(2), 100);
        assert_eq!(svc_col.value(2), "b_service");
    }

    #[test]
    fn test_starrocks_sink_invalid_order_by_error() {
        let mut config = make_config();
        config.order_by = Some(pipeline_core::sort::SortConfig {
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "col ASC extra invalid token".to_string(),
            )],
            ..Default::default()
        });

        let err = StarRocksSink::try_new(config.clone()).unwrap_err();
        assert!(
            matches!(err, PipelineError::Internal(ref msg) if msg.contains("sort column shorthand")),
            "Expected sort column shorthand error, got: {err}"
        );

        let primary = StarRocksSink::try_new(base_config()).unwrap();
        let err_wm = StarRocksSink::with_manager(config, primary.manager()).unwrap_err();
        assert!(
            matches!(err_wm, PipelineError::Internal(ref msg) if msg.contains("sort column shorthand")),
            "Expected sort column shorthand error from with_manager, got: {err_wm}"
        );
    }

    #[tokio::test]
    async fn test_starrocks_batch_accumulation_flush() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);

        let server_task = tokio::spawn(async move {
            let response_fn =
                |_method: &str, _path: &str| (200, r#"{"Status":"Success","NumberLoadedRows":6}"#);
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.max_retries = 0;
        config.batching = Some(StarRocksBatchingConfig {
            max_batch_size_bytes: 1024 * 1024,
            max_batch_interval_sec: 10,
            max_batch_records: Some(6),
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = StarRocksSink::try_new(config).unwrap();
        let (sender, receiver) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let expected_label = format!("dl_otel_logs_{:016x}_logs_1_2", receiver.instance_id());

        // Enqueue two 3-row batches (total 6 rows <= max_batch_records = 6) before the sink drains
        sender.send(SignalBatch::Logs(make_batch())).await.unwrap();
        sender.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(sender);

        let handle = tokio::spawn(async move { sink.run(receiver).await });
        handle.await.unwrap().unwrap();
        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(reqs.len(), 1, "Expected a single coalesced flush");
        assert_eq!(
            reqs[0].headers.get("label").map(String::as_str),
            Some(expected_label.as_str()),
            "Coalesced lease must carry sequence range 1..=2 and instance_id"
        );
    }

    #[test]
    fn test_starrocks_resolved_buffer_config_handles_buffer_and_legacy_batching() {
        use pipeline_core::buffer::BufferConfig;

        // Default (neither set)
        let cfg_default = base_config();
        let resolved_default = cfg_default.resolved_buffer_config().unwrap();
        assert_eq!(
            resolved_default.max_batch_size_bytes,
            BufferConfig::default().max_batch_size_bytes
        );

        // Legacy [batching] set
        let mut cfg_legacy = base_config();
        cfg_legacy.batching = Some(StarRocksBatchingConfig {
            max_batch_size_bytes: 2_097_152,
            max_batch_interval_sec: 15,
            max_batch_records: Some(500),
        });
        let resolved_legacy = cfg_legacy.resolved_buffer_config().unwrap();
        assert_eq!(resolved_legacy.max_batch_size_bytes, 2_097_152);
        assert_eq!(resolved_legacy.max_batch_interval_sec, 15);
        assert_eq!(resolved_legacy.max_batch_records, Some(500));

        // Both [buffer] and [batching] set -> V4 error naming both sections
        let mut cfg_both = cfg_legacy;
        cfg_both.buffer = Some(BufferConfig::default());
        let err = cfg_both.resolved_buffer_config().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("[starrocks.buffer]"), "Got: {msg}");
        assert!(msg.contains("[starrocks.batching]"), "Got: {msg}");
    }

    #[test]
    fn test_classify_stream_load_error_variants() {
        let label_exists = StreamLoadError::StarRocksFailure {
            status: "Label Already Exists".to_string(),
            message: "Label [dl_otel_logs_logs_1_1] has already been used".to_string(),
            error_log_url: None,
        };
        assert!(is_label_already_exists(&label_exists));

        let http_503 = StreamLoadError::StarRocksFailure {
            status: "503 Service Unavailable".to_string(),
            message: "Backend overloaded".to_string(),
            error_log_url: None,
        };
        assert!(matches!(
            classify_stream_load_error(&http_503),
            FailureClass::Transient
        ));

        let publish_timeout = StreamLoadError::StarRocksFailure {
            status: "Publish Timeout".to_string(),
            message: "Publish version timeout".to_string(),
            error_log_url: None,
        };
        assert!(matches!(
            classify_stream_load_error(&publish_timeout),
            FailureClass::Transient
        ));

        let filtered_rows = StreamLoadError::StarRocksFailure {
            status: "Fail".to_string(),
            message: "too many filtered rows".to_string(),
            error_log_url: Some("http://fe:8030/api/_load_error_log?file=err1".to_string()),
        };
        assert!(matches!(
            classify_stream_load_error(&filtered_rows),
            FailureClass::Rejected { ref reason } if reason.contains("err1")
        ));

        let auth_fail = StreamLoadError::StarRocksFailure {
            status: "401 Unauthorized".to_string(),
            message: "Access denied for user 'root'".to_string(),
            error_log_url: None,
        };
        assert!(matches!(
            classify_stream_load_error(&auth_fail),
            FailureClass::Fatal { .. }
        ));

        let txn_fatal =
            StreamLoadError::Transaction("HTTPS scheme requires TLS feature".to_string());
        assert!(matches!(
            classify_stream_load_error(&txn_fatal),
            FailureClass::Fatal { .. }
        ));

        let io_err = StreamLoadError::Io(std::io::Error::other("broken pipe"));
        assert!(matches!(
            classify_stream_load_error(&io_err),
            FailureClass::Transient
        ));
    }

    #[test]
    fn test_starrocks_tls_config_default_and_deserialization() {
        let toml_str = r#"
            frontend_urls = ["http://fe-1:8030"]
            database = "otel"
            username = "root"
            [table_mapping]
            type = "unified"
            table = "otel_events"
            signal_type_column = "signal_type"
            [tls]
            ca_cert_path = "/path/to/custom-ca.pem"
            verification = "disabled"
        "#;
        let config: StarRocksSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.tls.ca_cert_path.as_deref(),
            Some("/path/to/custom-ca.pem")
        );
        assert!(config.tls.is_insecure());
        assert_eq!(
            config.tls.verification,
            pipeline_core::tls::TlsVerificationMode::Disabled
        );
    }

    #[test]
    fn test_starrocks_tls_validation_rejects_missing_ca_path() {
        let mut config = base_config();
        config.tls.ca_cert_path = Some("/nonexistent/ca.pem".to_string());
        let res = StarRocksSink::try_new(config);
        assert!(matches!(res, Err(PipelineError::Configuration(_))));
    }

    #[test]
    fn test_starrocks_tls_validation_rejects_insecure_mode() {
        let mut config = base_config();
        config.tls.verification = pipeline_core::tls::TlsVerificationMode::Disabled;
        let res = StarRocksSink::try_new(config);
        assert!(matches!(res, Err(PipelineError::Configuration(_))));
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("disabling TLS certificate verification is prohibited")
        );
    }

    #[test]
    fn test_starrocks_tls_custom_ca_path_warns_and_succeeds() {
        let mut config = base_config();
        config.tls.ca_cert_path = Some(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR")));
        let sink = StarRocksSink::try_new(config.clone()).unwrap();
        assert_eq!(
            sink.config.tls.ca_cert_path.as_deref(),
            Some(format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR")).as_str())
        );

        let manager = sink.manager();
        let sink2 = StarRocksSink::with_manager(config, manager).unwrap();
        assert!(sink2.config.tls.ca_cert_path.is_some());
    }

    #[derive(Debug, Clone)]
    struct MockHttpRequest {
        start_line: String,
        path: String,
        headers: std::collections::HashMap<String, String>,
    }

    async fn handle_mock_http_connection<F>(
        mut stream: tokio::net::TcpStream,
        response_fn: &F,
        recorded: &std::sync::Arc<tokio::sync::Mutex<Vec<MockHttpRequest>>>,
    ) -> std::io::Result<()>
    where
        F: Fn(&str, &str) -> (u16, &'static str) + Send + Sync,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];

        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos;
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        };

        let header_str = String::from_utf8_lossy(&buf[..header_end]);
        let mut lines = header_str.split("\r\n");
        let start_line = lines.next().unwrap_or("").to_string();
        let mut parts = start_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();

        let mut headers = std::collections::HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }

        if headers.get("expect").map(String::as_str) == Some("100-continue") {
            stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
        }

        let content_length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let already_read = buf.len() - (header_end + 4);
        if already_read < content_length {
            let to_read = content_length - already_read;
            let mut remaining = vec![0u8; to_read];
            stream.read_exact(&mut remaining).await?;
        }

        recorded.lock().await.push(MockHttpRequest {
            start_line,
            path: path.clone(),
            headers,
        });

        let (status, body) = response_fn(&method, &path);
        let resp = format!(
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(resp.as_bytes()).await?;
        stream.flush().await?;
        let _ = stream.shutdown().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_per_signal_sinks_send_to_correct_table_url_paths() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);

        let server_task = tokio::spawn(async move {
            let response_fn =
                |_method: &str, _path: &str| (200, r#"{"Status":"Success","NumberLoadedRows":3}"#);
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.transaction_mode = TransactionMode::V1;

        let primary_sink = StarRocksSink::try_new(config.clone()).unwrap();
        let traces_manager = primary_sink.manager_for("traces");
        let metrics_manager = primary_sink.manager_for("metrics");

        let mut logs_sink = primary_sink;
        let mut traces_sink = StarRocksSink::with_manager(config.clone(), traces_manager).unwrap();
        let mut metrics_sink = StarRocksSink::with_manager(config, metrics_manager).unwrap();

        let (logs_tx, logs_rx) = SinkBuffer::passthrough_pair();
        let (traces_tx, traces_rx) = SinkBuffer::passthrough_pair();
        let (metrics_tx, metrics_rx) = SinkBuffer::passthrough_pair();

        let logs_handle = tokio::spawn(async move { logs_sink.run(logs_rx).await });
        let traces_handle = tokio::spawn(async move { traces_sink.run(traces_rx).await });
        let metrics_handle = tokio::spawn(async move { metrics_sink.run(metrics_rx).await });

        logs_tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(logs_tx);
        logs_handle.await.unwrap().unwrap();

        traces_tx
            .send(SignalBatch::Traces(make_batch()))
            .await
            .unwrap();
        drop(traces_tx);
        traces_handle.await.unwrap().unwrap();

        metrics_tx
            .send(SignalBatch::Metrics(make_batch()))
            .await
            .unwrap();
        drop(metrics_tx);
        metrics_handle.await.unwrap().unwrap();

        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(reqs.len(), 3, "Expected 3 requests, got {}", reqs.len());
        assert_eq!(reqs[0].path, "/api/otel/otel_logs/_stream_load");
        assert_eq!(reqs[1].path, "/api/otel/otel_traces/_stream_load");
        assert_eq!(reqs[2].path, "/api/otel/otel_metrics/_stream_load");
        assert!(
            reqs[0]
                .start_line
                .starts_with("PUT /api/otel/otel_logs/_stream_load "),
            "Unexpected start-line: {}",
            reqs[0].start_line
        );
        assert!(
            reqs[1]
                .start_line
                .starts_with("PUT /api/otel/otel_traces/_stream_load "),
            "Unexpected start-line: {}",
            reqs[1].start_line
        );
        assert!(
            reqs[2]
                .start_line
                .starts_with("PUT /api/otel/otel_metrics/_stream_load "),
            "Unexpected start-line: {}",
            reqs[2].start_line
        );
    }

    #[tokio::test]
    async fn test_v2_per_signal_sends_matching_table_header_across_all_phases() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);

        let server_task = tokio::spawn(async move {
            let response_fn = |_method: &str, path: &str| {
                if path == "/api/transaction/begin" {
                    (200, r#"{"Status":"OK","TxnId":42}"#)
                } else {
                    (200, r#"{"Status":"OK"}"#)
                }
            };
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.transaction_mode = TransactionMode::V2;

        let primary_sink = StarRocksSink::try_new(config.clone()).unwrap();
        let traces_manager = primary_sink.manager_for("traces");
        let mut traces_sink = StarRocksSink::with_manager(config, traces_manager).unwrap();

        let (traces_tx, traces_rx) = SinkBuffer::passthrough_pair();
        let traces_handle = tokio::spawn(async move { traces_sink.run(traces_rx).await });

        traces_tx
            .send(SignalBatch::Traces(make_batch()))
            .await
            .unwrap();
        drop(traces_tx);
        traces_handle.await.unwrap().unwrap();

        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(
            reqs.len(),
            4,
            "Expected 4 requests in V2 transaction sequence, got {}",
            reqs.len()
        );
        assert_eq!(reqs[0].path, "/api/transaction/begin");
        assert_eq!(reqs[1].path, "/api/transaction/load");
        assert_eq!(reqs[2].path, "/api/transaction/prepare");
        assert_eq!(reqs[3].path, "/api/transaction/commit");

        for (i, req) in reqs.iter().enumerate() {
            assert_eq!(
                req.headers.get("table").map(String::as_str),
                Some("otel_traces"),
                "Request {} ({}) missing or mismatched `table` header",
                i,
                req.path
            );
        }
    }

    #[test]
    fn test_format_lease_label_is_deterministic_across_retries() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        let id = LeaseId {
            instance_id: 0xfeed_face_cafe_beef,
            signal: SignalKind::Logs,
            first_seq: 10,
            last_seq: 25,
        };
        let label1 = format_lease_label("otel_logs", id);
        let label2 = format_lease_label("otel_logs", id);
        assert_eq!(label1, "dl_otel_logs_feedfacecafebeef_logs_10_25");
        assert_eq!(label1, label2);
        assert!(label1.len() <= 128);
        assert!(label1.is_ascii());

        // Distinct buffer instance_ids with identical sequence range must produce distinct labels
        let other_instance_id = LeaseId {
            instance_id: 0x1111_2222_3333_4444,
            ..id
        };
        let other_label = format_lease_label("otel_logs", other_instance_id);
        assert_ne!(
            label1, other_label,
            "Restarted memory buffers or distinct replicas must not collide on StarRocks labels"
        );
    }

    #[test]
    fn test_format_lease_label_truncates_or_hashes_when_exceeding_128_chars() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        let id = LeaseId {
            instance_id: 0x0102_0304_0506_0708,
            signal: SignalKind::Traces,
            first_seq: 100,
            last_seq: 200,
        };
        let long_table_a = format!("very_long_table_{}_alpha", "x".repeat(140));
        let long_table_b = format!("very_long_table_{}_bravo", "x".repeat(140));

        let label_a1 = format_lease_label(&long_table_a, id);
        let label_a2 = format_lease_label(&long_table_a, id);
        let label_b = format_lease_label(&long_table_b, id);

        assert_eq!(label_a1, label_a2, "Long table label must be deterministic");
        assert_ne!(
            label_a1, label_b,
            "Distinct long table names sharing a prefix must produce distinct hashed labels"
        );
        assert!(
            label_a1.len() <= 128,
            "Label length {} must not exceed 128 chars",
            label_a1.len()
        );
        assert!(label_a1.starts_with("dl_"));
        assert!(label_a1.ends_with("_0102030405060708_traces_100_200"));
        assert!(
            label_a1
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        );
    }

    #[tokio::test]
    async fn test_starrocks_sink_retains_lease_on_transient_503_and_acks_on_label_already_exists() {
        use pipeline_core::buffer::{BufferConfig, SignalKind};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);
        let attempt_counter = std::sync::Arc::new(AtomicUsize::new(0));
        let attempt_counter_clone = std::sync::Arc::clone(&attempt_counter);

        let server_task = tokio::spawn(async move {
            let response_fn = move |_method: &str, _path: &str| {
                let n = attempt_counter_clone.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    (503, r#"{"Status":"Fail","Message":"Service Unavailable"}"#)
                } else {
                    (
                        200,
                        r#"{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED","Message":"Label already exists"}"#,
                    )
                }
            };
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.max_retries = 0;
        config.buffer = Some(BufferConfig {
            initial_backoff_ms: 10,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let (tx, rx) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let expected_label = format!("dl_otel_logs_{:016x}_logs_1_1", rx.instance_id());
        let ctrl = rx.controller();
        let mut sink = StarRocksSink::try_new(config).unwrap();

        let sink_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(tx);

        sink_handle.await.unwrap().unwrap();
        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(reqs.len(), 2, "Expected 2 attempts (503 then 200)");
        assert_eq!(
            reqs[0].headers.get("label").map(String::as_str),
            Some(expected_label.as_str())
        );
        assert_eq!(
            reqs[1].headers.get("label").map(String::as_str),
            Some(expected_label.as_str()),
            "Retry must reuse the exact deterministic lease label"
        );
        assert!(ctrl.is_ready());
        let _ = SignalKind::Logs;
    }

    #[tokio::test]
    async fn test_starrocks_sink_acks_on_label_already_exists_and_retries_transient_with_identical_label()
     {
        use pipeline_core::buffer::BufferConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);
        let attempt_counter = std::sync::Arc::new(AtomicUsize::new(0));
        let attempt_counter_clone = std::sync::Arc::clone(&attempt_counter);

        let server_task = tokio::spawn(async move {
            let response_fn = move |_method: &str, _path: &str| {
                let n = attempt_counter_clone.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    (
                        503,
                        r#"{"Status":"Fail","Message":"BE temporarily overloaded"}"#,
                    )
                } else {
                    (
                        200,
                        r#"{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED","Message":"Label already exists"}"#,
                    )
                }
            };
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.max_retries = 0;
        config.buffer = Some(BufferConfig {
            initial_backoff_ms: 10,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let (tx, rx) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let expected_label = format!("dl_otel_logs_{:016x}_logs_1_1", rx.instance_id());
        let mut sink = StarRocksSink::try_new(config).unwrap();

        let sink_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(tx);

        sink_handle.await.unwrap().unwrap();
        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(reqs.len(), 2);
        assert_eq!(
            reqs[0].headers.get("label").map(String::as_str),
            Some(expected_label.as_str())
        );
        assert_eq!(
            reqs[1].headers.get("label").map(String::as_str),
            Some(expected_label.as_str())
        );
    }

    #[tokio::test]
    async fn test_starrocks_sink_routes_data_quality_fail_to_rejected_dlq() {
        use pipeline_core::buffer::{BufferConfig, DeadLetterManifest, SignalKind};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);

        let server_task = tokio::spawn(async move {
            let response_fn = |_method: &str, _path: &str| {
                (
                    200,
                    r#"{"Status":"Fail","Message":"too many filtered rows","ErrorLogUrl":"http://127.0.0.1:8040/api/_load_error_log?file=err_1"}"#,
                )
            };
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let dlq_dir =
            std::env::temp_dir().join(format!("otel-sr-dlq-test-{}-{}", std::process::id(), port));
        let _ = std::fs::remove_dir_all(&dlq_dir);

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.max_retries = 0;
        config.buffer = Some(BufferConfig {
            max_attempts: 2,
            dead_letter_path: Some(dlq_dir.clone()),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let (tx, rx) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let mut sink = StarRocksSink::try_new(config).unwrap();

        let sink_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(tx);

        sink_handle.await.unwrap().unwrap();
        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(reqs.len(), 2, "Expected 2 attempts before DLQ quarantine");

        let sr_dlq = dlq_dir.join("starrocks");
        assert!(sr_dlq.exists(), "Expected starrocks DLQ directory to exist");
        let mut json_files = Vec::new();
        let mut arrow_files = Vec::new();
        for entry in std::fs::read_dir(&sr_dlq).unwrap() {
            let p = entry.unwrap().path();
            match p.extension().and_then(|e| e.to_str()) {
                Some("json") => json_files.push(p),
                Some("arrow") => arrow_files.push(p),
                _ => {}
            }
        }
        assert_eq!(arrow_files.len(), 1);
        assert_eq!(json_files.len(), 1);

        let manifest: DeadLetterManifest =
            serde_json::from_slice(&std::fs::read(&json_files[0]).unwrap()).unwrap();
        assert_eq!(manifest.sink, "starrocks");
        assert_eq!(manifest.signal, SignalKind::Logs);
        assert_eq!(manifest.row_count, 3);
        assert_eq!(manifest.attempts, 2);
        assert!(manifest.reason.contains("too many filtered rows"));

        let _ = std::fs::remove_dir_all(&dlq_dir);
    }

    #[tokio::test]
    async fn test_starrocks_sink_routes_rejected_payload_to_dead_letter_after_max_attempts() {
        use pipeline_core::buffer::{BufferConfig, DeadLetterManifest, SignalKind};

        let dlq_dir =
            std::env::temp_dir().join(format!("otel-sr-dlq-oversize-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dlq_dir);

        let mut config = base_config();
        config.max_payload_bytes = 1; // Guarantee oversized payload rejection in prepare
        config.buffer = Some(BufferConfig {
            max_attempts: 2,
            dead_letter_path: Some(dlq_dir.clone()),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let (tx, rx) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let mut sink = StarRocksSink::try_new(config).unwrap();

        let sink_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(tx);

        sink_handle.await.unwrap().unwrap();

        let sr_dlq = dlq_dir.join("starrocks");
        assert!(sr_dlq.exists(), "Expected starrocks DLQ directory to exist");
        let mut json_files = Vec::new();
        for entry in std::fs::read_dir(&sr_dlq).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) == Some("json") {
                json_files.push(p);
            }
        }
        assert_eq!(json_files.len(), 1);
        let manifest: DeadLetterManifest =
            serde_json::from_slice(&std::fs::read(&json_files[0]).unwrap()).unwrap();
        assert_eq!(manifest.sink, "starrocks");
        assert_eq!(manifest.signal, SignalKind::Logs);
        assert_eq!(manifest.attempts, 2);
        assert!(manifest.reason.contains("max_payload_bytes"));

        let _ = std::fs::remove_dir_all(&dlq_dir);
    }

    #[tokio::test]
    async fn test_starrocks_v2_label_already_exists_confirms_load_status_before_acking() {
        use pipeline_core::buffer::BufferConfig;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorded_clone = std::sync::Arc::clone(&recorded);
        let status_queries = std::sync::Arc::new(AtomicUsize::new(0));
        let status_queries_clone = std::sync::Arc::clone(&status_queries);

        let server_task = tokio::spawn(async move {
            let response_fn = move |_method: &str, path: &str| {
                if path == "/api/transaction/begin" {
                    (
                        200,
                        r#"{"Status":"Label Already Exists","Message":"Label already exists, state: PREPARE"}"#,
                    )
                } else if path.starts_with("/api/otel/get_load_state?label=") {
                    let q = status_queries_clone.fetch_add(1, Ordering::SeqCst);
                    if q == 0 {
                        // First status check: transaction is still uncommitted (PREPARE)
                        (200, r#"{"Status":"Success","State":"PREPARE"}"#)
                    } else {
                        // Second status check: transaction is confirmed committed (VISIBLE)
                        (200, r#"{"Status":"Success","State":"VISIBLE"}"#)
                    }
                } else if path == "/api/transaction/rollback" {
                    (200, r#"{"Status":"OK"}"#)
                } else {
                    (500, r#"{"Status":"Fail","Message":"Unexpected endpoint"}"#)
                }
            };
            while let Ok((stream, _)) = listener.accept().await {
                let _ = handle_mock_http_connection(stream, &response_fn, &recorded_clone).await;
            }
        });

        let mut config = base_config();
        config.frontend_urls = vec![format!("http://127.0.0.1:{port}")];
        config.transaction_mode = TransactionMode::V2;
        config.max_retries = 0;
        config.buffer = Some(BufferConfig {
            initial_backoff_ms: 10,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        let (tx, rx) = SinkBuffer::new("starrocks", buf_cfg).unwrap();
        let expected_label = format!("dl_otel_logs_{:016x}_logs_1_1", rx.instance_id());
        let mut sink = StarRocksSink::try_new(config).unwrap();

        let sink_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_batch())).await.unwrap();
        drop(tx);

        sink_handle.await.unwrap().unwrap();
        server_task.abort();

        let reqs = recorded.lock().await;
        assert_eq!(
            reqs.len(),
            5,
            "Expected begin -> get_load_state(PREPARE) -> rollback -> begin -> get_load_state(VISIBLE), got: {:?}",
            reqs.iter().map(|r| &r.path).collect::<Vec<_>>()
        );
        assert_eq!(reqs[0].path, "/api/transaction/begin");
        assert_eq!(
            reqs[1].path,
            format!("/api/otel/get_load_state?label={expected_label}")
        );
        assert_eq!(reqs[2].path, "/api/transaction/rollback");
        assert_eq!(reqs[3].path, "/api/transaction/begin");
        assert_eq!(
            reqs[4].path,
            format!("/api/otel/get_load_state?label={expected_label}")
        );
    }
}
