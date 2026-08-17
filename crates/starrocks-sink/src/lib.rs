//! `StarRocks` Stream Load sink for the `opentelemetry-datalake` pipeline.
//!
//! Receives [`SignalBatch`] events from the internal pipeline channel, serializes
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
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, SignalBatch, Sink};
use serde::{Deserialize, Serialize};
use starrocks_stream_load::{
    DataFormat, StreamLoadConfig, StreamLoadManager, StreamLoadTableProperties,
};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

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
    manager: Arc<StreamLoadManager>,
}

impl std::fmt::Debug for StarRocksSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarRocksSink")
            .field("config", &self.config)
            .field("manager", &"<StreamLoadManager>")
            .finish()
    }
}

impl StarRocksSink {
    /// Creates a new `StarRocksSink` with a shared [`StreamLoadManager`].
    #[must_use]
    pub fn with_manager(config: StarRocksSinkConfig, manager: Arc<StreamLoadManager>) -> Self {
        Self { config, manager }
    }

    /// Returns a reference to the underlying [`StreamLoadManager`] connection pool.
    #[must_use]
    pub fn manager(&self) -> Arc<StreamLoadManager> {
        Arc::clone(&self.manager)
    }

    /// Creates a new `StarRocksSink` from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if configuration validation fails or
    /// if the [`StreamLoadManager`] cannot be initialised.
    pub fn try_new(config: StarRocksSinkConfig) -> Result<Self, PipelineError> {
        if config.frontend_urls.is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `frontend_urls` must contain at least one FE URL"
                    .to_string(),
            ));
        }
        if config.username.trim().is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `username` must not be empty".to_string(),
            ));
        }
        if config.database.trim().is_empty() {
            return Err(PipelineError::Internal(
                "StarRocks configuration error: `database` must not be empty".to_string(),
            ));
        }

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
            .format(sdk_format)
            .build();

        let manager = StreamLoadManager::new(sdk_config, properties).map_err(|e| {
            PipelineError::Internal(format!("Failed to initialise StarRocks manager: {e}"))
        })?;

        Ok(Self {
            config,
            manager: Arc::new(manager),
        })
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
        let mut buf = Vec::with_capacity(batch.get_array_memory_size());

        match self.config.format {
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

        let n = buf.len();
        let max = self.config.max_payload_bytes;
        if n > max {
            return Err(PipelineError::Internal(format!(
                "Serialized payload {n} bytes exceeds max_payload_bytes {max}; \
                 reduce upstream batch size or increase the limit in config"
            )));
        }

        Ok(Bytes::from(buf))
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

    /// Generates a unique V1/V2 transaction label.
    ///
    /// Labels use `UUIDv7` (time-ordered) to make them monotonic and debuggable.
    fn make_label(signal_type: &str) -> String {
        format!("otel-{signal_type}-{}", Uuid::now_v7())
    }

    /// Processes one batch via the V1 (at-least-once) path.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::DownstreamClosed`] if the SDK exhausts all
    /// retries, propagating backpressure to the OTLP receiver.
    async fn send_v1(&self, table: &str, label: &str, payload: Bytes) -> Result<(), PipelineError> {
        let response = self
            .manager
            .send_single_batch(label, payload)
            .await
            .map_err(|e| {
                error!(
                    label = label,
                    table = table,
                    error = %e,
                    "StarRocks V1 stream load failed after retries; propagating backpressure"
                );
                PipelineError::DownstreamClosed
            })?;

        info!(
            label = label,
            table = table,
            status = %response.status,
            rows = ?response.number_loaded_rows,
            "StarRocks V1 load committed"
        );
        Ok(())
    }

    /// Processes one batch via the V2 (exactly-once, 2PC) path.
    ///
    /// Steps: `begin` → `load` → `prepare` → `commit`.
    /// Any failure after `begin` triggers a best-effort `rollback`; the rollback
    /// error is logged but not returned (the original error is propagated).
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::DownstreamClosed`] on any SDK-level failure,
    /// propagating backpressure to the OTLP receiver.
    async fn send_v2(&self, table: &str, label: &str, payload: Bytes) -> Result<(), PipelineError> {
        let txn_id = self.manager.begin_transaction(label).await.map_err(|e| {
            error!(label = label, error = %e, "StarRocks V2 begin_transaction failed");
            PipelineError::DownstreamClosed
        })?;

        info!(
            label = label,
            txn_id = txn_id,
            "StarRocks V2 transaction begun"
        );

        let load_result = self
            .manager
            .load_transaction_data(label, &self.config.database, table, 0, payload)
            .await;

        if let Err(e) = load_result {
            error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 load failed; attempting rollback");
            if let Err(rb_err) = self.manager.rollback_transaction(label).await {
                warn!(label = label, error = %rb_err, "StarRocks V2 rollback also failed — manual intervention may be needed");
            }
            return Err(PipelineError::DownstreamClosed);
        }

        let prep_result = self.manager.prepare_transaction(label).await;
        if let Err(e) = prep_result {
            error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 prepare failed; attempting rollback");
            if let Err(rb_err) = self.manager.rollback_transaction(label).await {
                warn!(label = label, error = %rb_err, "StarRocks V2 rollback also failed — manual intervention may be needed");
            }
            return Err(PipelineError::DownstreamClosed);
        }

        let commit_result = self.manager.commit_transaction(label).await;
        if let Err(e) = commit_result {
            error!(label = label, txn_id = txn_id, error = %e, "StarRocks V2 commit failed; attempting rollback");
            if let Err(rb_err) = self.manager.rollback_transaction(label).await {
                warn!(
                    label = label,
                    error = %rb_err,
                    "StarRocks V2 rollback after commit failure also failed — transaction {txn_id} may require manual resolution"
                );
            }
            return Err(PipelineError::DownstreamClosed);
        }
        let commit_response = commit_result.map_err(|_| PipelineError::DownstreamClosed)?;

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
    /// Runs the sink loop, processing [`SignalBatch`] events until the channel closes.
    ///
    /// On serialization or network errors the sink propagates
    /// [`PipelineError::DownstreamClosed`] so upstream OTLP receivers can apply
    /// backpressure (returning `503 Service Unavailable` to producers).
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        info!(
            database = %self.config.database,
            format = %self.config.format,
            mode = ?self.config.transaction_mode,
            "StarRocksSink started"
        );

        while let Some(signal) = input.recv().await {
            let signal_type = match &signal {
                SignalBatch::Logs(_) => "logs",
                SignalBatch::Metrics(_) => "metrics",
                SignalBatch::Traces(_) => "traces",
            };

            let batch = match signal {
                SignalBatch::Logs(b) | SignalBatch::Metrics(b) | SignalBatch::Traces(b) => b,
            };

            if batch.num_rows() == 0 {
                continue;
            }

            // For Unified mapping, inject the discriminator column before serialization.
            let batch = if let Some(col_name) = self.config.table_mapping.signal_type_column() {
                Self::inject_signal_type_column(&batch, col_name, signal_type)?
            } else {
                batch
            };

            let table = self.config.table_mapping.table_for(signal_type).to_owned();
            let label = Self::make_label(signal_type);
            let payload = self.serialize_batch(&batch)?;

            match self.config.transaction_mode {
                TransactionMode::V1 => self.send_v1(&table, &label, payload).await?,
                TransactionMode::V2 => self.send_v2(&table, &label, payload).await?,
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
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
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
        let a = StarRocksSink::make_label("traces");
        let b = StarRocksSink::make_label("traces");
        assert_ne!(a, b, "Labels must be unique per call");
        assert!(
            a.starts_with("otel-traces-"),
            "Label must include signal type prefix"
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
        config2.database = "".to_string();
        let err2 = StarRocksSink::try_new(config2).unwrap_err();
        assert!(
            matches!(err2, PipelineError::Internal(ref msg) if msg.contains("database")),
            "Expected empty database error, got: {err2}"
        );
    }

    #[test]
    fn test_with_manager_shares_underlying_arc() {
        let config = base_config();
        let sink1 = StarRocksSink::try_new(config.clone()).unwrap();
        let manager_arc = sink1.manager();

        let sink2 = StarRocksSink::with_manager(config, Arc::clone(&manager_arc));
        assert!(
            Arc::ptr_eq(&sink1.manager(), &sink2.manager()),
            "Arc pointers must be equal when using with_manager"
        );
    }

    #[test]
    fn test_default_max_payload_bytes_is_100mb() {
        assert_eq!(default_max_payload_bytes(), 104_857_600);
    }

    /// All StarRocksFormat variants must produce expected Display strings.
    #[test]
    fn test_starrocks_format_display() {
        assert_eq!(StarRocksFormat::Ipc.to_string(), "ipc");
        assert_eq!(StarRocksFormat::Json.to_string(), "json");
        assert_eq!(StarRocksFormat::Csv.to_string(), "csv");
    }

    /// make_label must produce distinct labels for all three signal type prefixes.
    #[test]
    fn test_make_label_for_all_signal_types() {
        let logs_label = StarRocksSink::make_label("logs");
        let metrics_label = StarRocksSink::make_label("metrics");
        let traces_label = StarRocksSink::make_label("traces");

        assert!(logs_label.starts_with("otel-logs-"));
        assert!(metrics_label.starts_with("otel-metrics-"));
        assert!(traces_label.starts_with("otel-traces-"));

        // All labels must be unique
        assert_ne!(logs_label, metrics_label);
        assert_ne!(logs_label, traces_label);
    }

    /// inject_signal_type_column must successfully add a discriminator
    /// column even when the input batch has zero rows.
    #[test]
    fn test_inject_signal_type_column_empty_batch() {
        use arrow::array::AsArray;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let empty_batch = RecordBatch::new_empty(schema);

        let result =
            StarRocksSink::inject_signal_type_column(&empty_batch, "signal_type", "logs");
        assert!(result.is_ok(), "inject_signal_type_column must succeed on empty batch");
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

    /// inject_signal_type_column for a metrics signal must use "metrics" as the value.
    #[test]
    fn test_inject_signal_type_column_metrics_signal() {
        use arrow::array::{AsArray, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))],
        )
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

    /// The PerSignal table mapping must correctly select the right table
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
}
