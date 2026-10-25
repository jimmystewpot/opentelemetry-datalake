use crate::{CatalogType, PartitionGranularity, SchemaMode};
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{Array, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use iceberg::Catalog;
use iceberg::CatalogBuilder;
use iceberg::TableIdent;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
#[cfg(feature = "aws")]
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
#[cfg(feature = "aws")]
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use pipeline_core::buffer::{
    BufferConfig, FailureClass, Lease, LeaseId, LegacyBatchingConfig, SignalKind, SinkBuffer,
};
use pipeline_core::error::PipelineError;
#[cfg(test)]
use pipeline_core::pipeline::SignalBatch;
use pipeline_core::pipeline::Sink;
use pipeline_core::sort::{
    BatchSorter, MissingColumnAction, SignalType, SortColumnDef, SortConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Returns the Iceberg snapshot summary property key used to record the last committed
/// [`LeaseId`] for a given [`SignalKind`].
#[must_use]
pub const fn snapshot_property_key(signal: SignalKind) -> &'static str {
    match signal {
        SignalKind::Logs => "datalake.lease.logs",
        SignalKind::Metrics => "datalake.lease.metrics",
        SignalKind::Traces => "datalake.lease.traces",
    }
}

/// Alias for [`snapshot_property_key`] returning the Iceberg snapshot summary property key
/// for the given [`SignalKind`].
#[must_use]
pub const fn lease_snapshot_property_key(signal: SignalKind) -> &'static str {
    snapshot_property_key(signal)
}

/// Returns the instance-scoped Iceberg snapshot summary property key
/// (`"datalake.lease.<signal>.<instance_id:016x>"`).
#[must_use]
pub fn instance_snapshot_property_key(signal: SignalKind, instance_id: u64) -> String {
    format!("{}.{:016x}", snapshot_property_key(signal), instance_id)
}

/// Formats a [`LeaseId`] into a snapshot delivery token (`"{instance_id:016x}:{first_seq}:{last_seq}"`).
#[must_use]
pub fn format_lease_token(id: LeaseId) -> String {
    format!("{:016x}:{}:{}", id.instance_id, id.first_seq, id.last_seq)
}

/// Formats a [`LeaseId`] sequence range (`"{instance_id:016x}:{first_seq}-{last_seq}"`).
#[must_use]
pub fn format_lease_range(id: LeaseId) -> String {
    format!("{:016x}:{}-{}", id.instance_id, id.first_seq, id.last_seq)
}

/// Parses a snapshot delivery token (`"{instance_id:016x}:{first_seq}:{last_seq}"` or
/// `"{instance_id:016x}:{first_seq}-{last_seq}"`) into `(instance_id, first_seq, last_seq)`
/// when `instance_id != 0` and `first_seq <= last_seq`.
#[must_use]
pub fn parse_lease_token(token: &str) -> Option<(u64, u64, u64)> {
    let (inst_str, seq_part) = token.split_once(':')?;
    let instance_id = u64::from_str_radix(inst_str, 16).ok()?;
    if instance_id == 0 {
        return None;
    }
    let (first_str, last_str) = seq_part
        .split_once(':')
        .or_else(|| seq_part.split_once('-'))?;
    let first_seq = first_str.parse::<u64>().ok()?;
    let last_seq = last_str.parse::<u64>().ok()?;
    if first_seq <= last_seq {
        Some((instance_id, first_seq, last_seq))
    } else {
        None
    }
}

fn token_matches_lease(raw: &str, id: LeaseId) -> bool {
    let Some((committed_instance_id, _committed_first_seq, committed_last_seq)) =
        parse_lease_token(raw)
    else {
        return false;
    };
    committed_instance_id == id.instance_id && committed_last_seq >= id.last_seq
}

/// Abstraction over an Iceberg [`iceberg::table::Table`], [`iceberg::spec::TableMetadata`],
/// [`iceberg::spec::Snapshot`], or snapshot summary property [`HashMap`] for lease deduplication.
pub trait SnapshotPropertiesSource {
    /// Looks up a snapshot summary property by key.
    fn lease_property(&self, key: &str) -> Option<&str>;

    /// Returns `true` if `self` records a committed lease matching `id.instance_id` with
    /// `committed_last_seq >= id.last_seq`.
    fn has_committed_lease(&self, id: LeaseId) -> bool {
        if id.instance_id == 0 {
            return false;
        }
        let inst_key = instance_snapshot_property_key(id.signal, id.instance_id);
        if let Some(raw) = self.lease_property(&inst_key)
            && token_matches_lease(raw, id)
        {
            return true;
        }
        if let Some(raw) = self.lease_property(snapshot_property_key(id.signal))
            && token_matches_lease(raw, id)
        {
            return true;
        }
        false
    }
}

impl SnapshotPropertiesSource for iceberg::table::Table {
    fn lease_property(&self, key: &str) -> Option<&str> {
        self.metadata()
            .current_snapshot()?
            .summary()
            .additional_properties
            .get(key)
            .map(String::as_str)
    }

    fn has_committed_lease(&self, id: LeaseId) -> bool {
        self.metadata().has_committed_lease(id)
    }
}

impl SnapshotPropertiesSource for iceberg::spec::TableMetadata {
    fn lease_property(&self, key: &str) -> Option<&str> {
        self.current_snapshot()?
            .summary()
            .additional_properties
            .get(key)
            .map(String::as_str)
    }

    fn has_committed_lease(&self, id: LeaseId) -> bool {
        if id.instance_id == 0 {
            return false;
        }
        if let Some(curr) = self.current_snapshot()
            && curr.has_committed_lease(id)
        {
            return true;
        }
        self.snapshots()
            .any(|snap| snap.as_ref().has_committed_lease(id))
    }
}

impl SnapshotPropertiesSource for iceberg::spec::Snapshot {
    fn lease_property(&self, key: &str) -> Option<&str> {
        self.summary()
            .additional_properties
            .get(key)
            .map(String::as_str)
    }
}

impl<S: std::hash::BuildHasher> SnapshotPropertiesSource for HashMap<String, String, S> {
    fn lease_property(&self, key: &str) -> Option<&str> {
        self.get(key).map(String::as_str)
    }
}

impl<T: SnapshotPropertiesSource + ?Sized> SnapshotPropertiesSource for &T {
    fn lease_property(&self, key: &str) -> Option<&str> {
        (*self).lease_property(key)
    }

    fn has_committed_lease(&self, id: LeaseId) -> bool {
        (*self).has_committed_lease(id)
    }
}

/// Returns `true` if the target table's snapshot summary properties already record
/// a committed sequence range for `id.instance_id` and `id.signal` with
/// `committed_last_seq >= id.last_seq`.
#[must_use]
pub fn is_lease_already_committed<S: SnapshotPropertiesSource + ?Sized>(
    source: &S,
    id: LeaseId,
) -> bool {
    source.has_committed_lease(id)
}

/// Classifies an Iceberg [`PipelineError`] into a [`FailureClass`] for [`SinkBuffer`] lease handling.
#[must_use]
pub fn classify_iceberg_error(err: &PipelineError) -> FailureClass {
    match err {
        PipelineError::Internal(msg) => FailureClass::Rejected {
            reason: msg.clone(),
        },
        PipelineError::Arrow(e) => FailureClass::Rejected {
            reason: e.to_string(),
        },
        PipelineError::Configuration(e) => FailureClass::Fatal {
            reason: e.to_string(),
        },
        _ => {
            let msg = err.to_string();
            let lower = msg.to_ascii_lowercase();
            if lower.contains("401")
                || lower.contains("403")
                || lower.contains("unauthorized")
                || lower.contains("forbidden")
                || lower.contains("access denied")
                || lower.contains("authentication")
                || lower.contains("tablenotfound")
                || lower.contains("namespacenotfound")
                || lower.contains("featureunsupported")
            {
                FailureClass::Fatal { reason: msg }
            } else if lower.contains("datainvalid") {
                FailureClass::Rejected { reason: msg }
            } else {
                FailureClass::Transient
            }
        }
    }
}

/// Configuration for commit batching.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchingConfig {
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    #[serde(default)]
    pub max_batch_records: Option<usize>,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_batch_interval_sec: default_max_batch_interval_sec(),
            max_batch_records: None,
        }
    }
}

const fn default_max_batch_size_bytes() -> usize {
    134_217_728 // 128MB
}

const fn default_max_batch_interval_sec() -> u64 {
    60 // 60 seconds
}

/// Configuration for the Apache Iceberg storage sink.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IcebergSinkConfig {
    pub catalog_type: CatalogType,
    pub catalog_uri: String,
    pub warehouse: String,
    pub table_identifier: String,
    #[serde(default)]
    pub logs_table_identifier: Option<String>,
    #[serde(default)]
    pub traces_table_identifier: Option<String>,
    #[serde(default)]
    pub metrics_table_identifier: Option<String>,
    #[serde(default = "default_schema_mode")]
    pub schema_mode: SchemaMode,
    #[serde(default = "default_partition_granularity")]
    pub partition_granularity: PartitionGranularity,
    #[serde(default = "default_log_dropped_fields")]
    pub log_dropped_fields: bool,
    /// Optional unified sink buffer configuration (`[iceberg.buffer]`).
    #[serde(default)]
    pub buffer: Option<BufferConfig>,
    /// Legacy commit batching configuration (`[iceberg.batching]`).
    #[serde(default)]
    pub batching: Option<BatchingConfig>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// When true, catalog operations are skipped and transaction commits
    /// are simulated. Used for testing without a live catalog.
    #[serde(default)]
    pub dry_run: bool,
    /// Optional pre-sorting configuration per telemetry signal.
    #[serde(default)]
    pub order_by: Option<SortConfig>,
}

impl Default for IcebergSinkConfig {
    fn default() -> Self {
        Self {
            catalog_type: CatalogType::Rest,
            catalog_uri: String::new(),
            warehouse: String::new(),
            table_identifier: String::new(),
            logs_table_identifier: None,
            traces_table_identifier: None,
            metrics_table_identifier: None,
            schema_mode: default_schema_mode(),
            partition_granularity: default_partition_granularity(),
            log_dropped_fields: default_log_dropped_fields(),
            buffer: None,
            batching: None,
            properties: HashMap::new(),
            dry_run: false,
            order_by: None,
        }
    }
}

impl IcebergSinkConfig {
    /// Resolves the effective [`BufferConfig`] for this sink, mapping legacy `[iceberg.batching]`
    /// onto [`BufferConfig`] triggers (defaulting to 128 MiB / 60s when neither is set) and
    /// enforcing startup validation rules V1–V5.
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
        let mut resolved = BufferConfig::resolve("iceberg", self.buffer.clone(), legacy, false)?;
        if self.buffer.is_none() && self.batching.is_none() {
            resolved.max_batch_size_bytes = default_max_batch_size_bytes();
            resolved.max_batch_interval_sec = default_max_batch_interval_sec();
            resolved.validate("iceberg")?;
        }
        Ok(resolved)
    }
}

const fn default_schema_mode() -> SchemaMode {
    SchemaMode::Fixed
}

const fn default_partition_granularity() -> PartitionGranularity {
    PartitionGranularity::Hourly
}

const fn default_log_dropped_fields() -> bool {
    true
}

/// Apache Iceberg storage sink.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    sorter: BatchSorter,
    catalog_override: Option<Arc<dyn Catalog>>,
}

impl IcebergSink {
    /// Creates a new `IcebergSink` from configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig) -> Self {
        let default_cfg = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![
                SortColumnDef::Shorthand("service_name ASC".to_string()),
                SortColumnDef::Shorthand("severity_text ASC".to_string()),
                SortColumnDef::Shorthand("timestamp ASC".to_string()),
            ],
            metrics: vec![
                SortColumnDef::Shorthand("service_name ASC".to_string()),
                SortColumnDef::Shorthand("name ASC".to_string()),
                SortColumnDef::Shorthand("attributes ASC".to_string()),
                SortColumnDef::Shorthand("timestamp ASC".to_string()),
            ],
            traces: vec![
                SortColumnDef::Shorthand("service_name ASC".to_string()),
                SortColumnDef::Shorthand("name ASC".to_string()),
                SortColumnDef::Shorthand("timestamp ASC".to_string()),
            ],
        };

        let sorter = if let Some(ref sort_cfg) = config.order_by {
            BatchSorter::from_config(sort_cfg).unwrap_or_else(|err| {
                tracing::error!(
                    "Invalid Iceberg order_by configuration: {err}; falling back to default legacy sorting"
                );
                BatchSorter::from_config(&default_cfg).unwrap_or_default()
            })
        } else {
            BatchSorter::from_config(&default_cfg).unwrap_or_default()
        };

        Self {
            config,
            sorter,
            catalog_override: None,
        }
    }

    /// Creates a new `IcebergSink` using a pre-constructed [`Catalog`] implementation.
    #[must_use]
    pub fn with_catalog(config: IcebergSinkConfig, catalog: Arc<dyn Catalog>) -> Self {
        let mut sink = Self::new(config);
        sink.catalog_override = Some(catalog);
        sink
    }

    /// Returns the default [`BufferConfig`] for `IcebergSink` (128 MiB / 60s flush triggers).
    #[must_use]
    pub fn default_buffer_config() -> BufferConfig {
        BufferConfig {
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_batch_interval_sec: default_max_batch_interval_sec(),
            ..BufferConfig::default()
        }
    }

    /// Sorts a log batch based on the configured or default sort order.
    pub fn sort_logs(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, SignalType::Logs)
    }

    /// Sorts a metrics batch based on the configured or default sort order.
    pub fn sort_metrics(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, SignalType::Metrics)
    }

    /// Sorts a traces batch based on the configured or default sort order.
    pub fn sort_traces(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, SignalType::Traces)
    }

    /// Appends `SchemaMode` compliance (including catalog-based field pruning).
    ///
    /// Accepts `batch` by value to avoid unnecessary `Arc` refcount bumps
    /// in the `Fixed` and `Auto` pass-through paths.
    pub fn apply_schema_mode(
        &self,
        batch: RecordBatch,
        table: Option<&iceberg::table::Table>,
    ) -> Result<RecordBatch, PipelineError> {
        apply_schema_mode_with_config(
            batch,
            self.config.schema_mode,
            self.config.log_dropped_fields,
            table,
        )
    }
}

fn apply_schema_mode_with_config(
    batch: RecordBatch,
    schema_mode: SchemaMode,
    log_dropped_fields: bool,
    table: Option<&iceberg::table::Table>,
) -> Result<RecordBatch, PipelineError> {
    match schema_mode {
        SchemaMode::Fixed => Ok(batch),
        SchemaMode::Auto => {
            let Some(table) = table else {
                return Ok(batch);
            };
            let schema = table.current_schema_ref();
            let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&schema)
                .map_err(|e| PipelineError::Storage(Box::new(e)))?;

            for field in batch.schema().fields() {
                match arrow_schema.field_with_name(field.name()) {
                    Ok(table_field) => {
                        if field.data_type() != table_field.data_type() {
                            return Err(PipelineError::Internal(format!(
                                "Schema mismatch for field '{}': batch type {:?} is incompatible with table type {:?}",
                                field.name(),
                                field.data_type(),
                                table_field.data_type()
                            )));
                        }
                    }
                    Err(_) => {
                        return Err(PipelineError::Internal(format!(
                            "Additive schema evolution failed: new column '{}' detected but schema evolution is not supported by the client library",
                            field.name()
                        )));
                    }
                }
            }
            Ok(batch)
        }
        SchemaMode::Catalog => {
            let Some(table) = table else {
                return Ok(batch);
            };
            let schema = table.current_schema_ref();
            let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&schema)
                .map_err(|e| PipelineError::Storage(Box::new(e)))?;
            let mut final_columns = Vec::new();
            let mut final_fields = Vec::new();
            let mut dropped_fields = Vec::new();

            for field in batch.schema().fields() {
                if let Ok(table_field) = arrow_schema.field_with_name(field.name()) {
                    let col = batch.column_by_name(field.name()).ok_or_else(|| {
                        PipelineError::Internal(format!(
                            "Column {} missing from batch",
                            field.name()
                        ))
                    })?;
                    let casted_col = if col.data_type() == table_field.data_type() {
                        col.clone()
                    } else {
                        arrow::compute::cast(col, table_field.data_type())
                            .map_err(PipelineError::Arrow)?
                    };
                    final_columns.push(casted_col);
                    final_fields.push(table_field.clone());
                } else {
                    dropped_fields.push(field.name().clone());
                }
            }

            if !dropped_fields.is_empty() && log_dropped_fields {
                tracing::warn!(
                    "Dropped fields not present in catalog schema: {:?}",
                    dropped_fields
                );
            }

            let new_schema = Arc::new(arrow::datatypes::Schema::new(final_fields));
            RecordBatch::try_new(new_schema, final_columns).map_err(PipelineError::Arrow)
        }
    }
}

/// Applies [`SchemaMode`] validation/casting, signal-specific sorting via [`BatchSorter`],
/// and timestamp partitioning inside a [`Lease::prepare`] blocking closure.
///
/// # Errors
///
/// Returns [`PipelineError`] if schema validation fails, a required sort column is missing,
/// or timestamp partitioning encounters an invalid/missing timestamp column.
pub fn prepare_iceberg_batches(
    batch: RecordBatch,
    signal: SignalKind,
    schema_mode: SchemaMode,
    log_dropped_fields: bool,
    table: Option<&iceberg::table::Table>,
    sorter: &BatchSorter,
    granularity: PartitionGranularity,
) -> Result<Vec<(String, RecordBatch)>, PipelineError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let final_batch = apply_schema_mode_with_config(batch, schema_mode, log_dropped_fields, table)?;
    let sorted_batch = sorter.sort(&final_batch, signal)?;
    partition_batch(&sorted_batch, "timestamp", granularity)
}

/// Computes partition paths according to ISO-8601 derived timestamp rules.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if the timestamp is invalid.
pub fn get_partition_path(
    timestamp_nanos: i64,
    granularity: PartitionGranularity,
) -> Result<String, PipelineError> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000);
    let nanos_u32 = u32::try_from(nanos).unwrap_or(0);
    let datetime = Utc
        .timestamp_opt(secs, nanos_u32)
        .single()
        .ok_or_else(|| PipelineError::Internal("Malformed OTLP timestamp".to_string()))?;
    match granularity {
        PartitionGranularity::Hourly => Ok(format!(
            "year={}/month={:02}/day={:02}/hour={:02}/",
            datetime.format("%Y"),
            datetime.format("%m"),
            datetime.format("%d"),
            datetime.format("%H")
        )),
        PartitionGranularity::Daily => Ok(format!(
            "year={}/month={:02}/day={:02}/",
            datetime.format("%Y"),
            datetime.format("%m"),
            datetime.format("%d")
        )),
    }
}

/// Groups `RecordBatch` rows into partition-specific sub-batches.
pub fn partition_batch(
    batch: &RecordBatch,
    timestamp_col_name: &str,
    granularity: PartitionGranularity,
) -> Result<Vec<(String, RecordBatch)>, PipelineError> {
    let ts_col = batch
        .column_by_name(timestamp_col_name)
        .ok_or_else(|| PipelineError::Internal(format!("Missing {timestamp_col_name}")))?;

    let mut timestamps_nanos = Vec::with_capacity(batch.num_rows());
    match ts_col.data_type() {
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let ts_array = ts_col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    PipelineError::Internal("Invalid TimestampNanosecondArray".to_string())
                })?;
            for i in 0..batch.num_rows() {
                if ts_array.is_null(i) {
                    return Err(PipelineError::Internal(
                        "Null timestamp in partition column".to_string(),
                    ));
                }
                timestamps_nanos.push(ts_array.value(i));
            }
        }
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let ts_array = ts_col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    PipelineError::Internal("Invalid TimestampMicrosecondArray".to_string())
                })?;
            for i in 0..batch.num_rows() {
                if ts_array.is_null(i) {
                    return Err(PipelineError::Internal(
                        "Null timestamp in partition column".to_string(),
                    ));
                }
                let nanos = ts_array
                    .value(i)
                    .checked_mul(1_000)
                    .ok_or_else(|| PipelineError::Internal("Timestamp overflow".to_string()))?;
                timestamps_nanos.push(nanos);
            }
        }
        other => {
            return Err(PipelineError::Internal(format!(
                "Unexpected timestamp column datatype: {other:?}"
            )));
        }
    }

    let mut partitions: HashMap<String, Vec<u32>> = HashMap::new();
    for (i, &val) in timestamps_nanos.iter().enumerate() {
        let path = get_partition_path(val, granularity)?;
        let idx = u32::try_from(i).map_err(|e| PipelineError::Internal(e.to_string()))?;
        partitions.entry(path).or_default().push(idx);
    }

    let mut result = Vec::new();
    for (path, indices) in partitions {
        let indices_array = arrow::array::UInt32Array::from(indices);
        let columns = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices_array, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(PipelineError::Arrow)?;

        let sub_batch =
            RecordBatch::try_new(batch.schema(), columns).map_err(PipelineError::Arrow)?;

        result.push((path, sub_batch));
    }

    Ok(result)
}

impl IcebergSink {
    /// Loads a table from the catalog, returning `None` if no catalog is available.
    async fn load_table(
        &self,
        catalog: Option<&Arc<dyn Catalog>>,
        table_name: &str,
    ) -> Result<Option<iceberg::table::Table>, PipelineError> {
        let Some(cat) = catalog else {
            return Ok(None);
        };
        let table_ident = TableIdent::from_strs(table_name.split('.'))
            .map_err(|e| PipelineError::Storage(Box::new(e)))?;
        let table = cat
            .load_table(&table_ident)
            .await
            .map_err(|e| PipelineError::Storage(Box::new(e)))?;
        Ok(Some(table))
    }

    /// Processes and commits a single [`Lease`] to the configured Iceberg table.
    #[allow(clippy::too_many_lines)]
    async fn commit_lease(
        &self,
        lease: Lease,
        catalog: Option<&Arc<dyn Catalog>>,
    ) -> Result<(), PipelineError> {
        if lease.num_rows() == 0 {
            lease.ack().await?;
            return Ok(());
        }

        let table_name = &self.config.table_identifier;
        let signal = lease.signal();

        let loaded_table = match self.load_table(catalog, table_name).await {
            Ok(t) => t,
            Err(err) => {
                let class = classify_iceberg_error(&err);
                let is_fatal = matches!(class, FailureClass::Fatal { .. });
                warn!(
                    signal = signal.as_str(),
                    first_seq = lease.id().first_seq,
                    last_seq = lease.id().last_seq,
                    attempt = lease.attempt(),
                    table = %table_name,
                    error = %err,
                    "Iceberg load_table failed; nacking lease"
                );
                lease.nack(class).await?;
                if is_fatal {
                    return Err(err);
                }
                return Ok(());
            }
        };

        if let Some(ref table) = loaded_table
            && is_lease_already_committed(table, lease.id())
        {
            info!(
                signal = signal.as_str(),
                first_seq = lease.id().first_seq,
                last_seq = lease.id().last_seq,
                attempt = lease.attempt(),
                table = %table_name,
                "Iceberg snapshot already contains delivery token for lease; skipping duplicate Parquet write and acknowledging"
            );
            lease.ack().await?;
            return Ok(());
        }

        info!(
            "Flushing {} accumulated records for {} to table {}",
            lease.num_rows(),
            signal.as_str(),
            table_name
        );

        let schema_mode = self.config.schema_mode;
        let log_dropped_fields = self.config.log_dropped_fields;
        let granularity = self.config.partition_granularity;
        let sorter = self.sorter.clone();
        let table_for_prep = loaded_table.clone();

        let prep_res = lease
            .prepare(move |batch| {
                prepare_iceberg_batches(
                    batch,
                    signal,
                    schema_mode,
                    log_dropped_fields,
                    table_for_prep.as_ref(),
                    &sorter,
                    granularity,
                )
            })
            .await;

        let sub_batches = match prep_res {
            Ok(batches) => batches,
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
                    table = %table_name,
                    error = %err,
                    "Iceberg lease preparation failed; nacking as Rejected"
                );
                lease
                    .nack(FailureClass::Rejected {
                        reason: err.to_string(),
                    })
                    .await?;
                return Ok(());
            }
        };

        if let Some(ref table) = loaded_table {
            let mut all_data_files = Vec::new();
            for (path, sub_batch) in sub_batches {
                info!(
                    "Writing sorted {} batch (rows: {}) to Iceberg partition: {}{}",
                    signal.as_str(),
                    sub_batch.num_rows(),
                    self.config.warehouse,
                    path
                );
                match write_batch_to_table(table, sub_batch).await {
                    Ok(data_files) => all_data_files.extend(data_files),
                    Err(err) => {
                        let class = classify_iceberg_error(&err);
                        let is_fatal = matches!(class, FailureClass::Fatal { .. });
                        warn!(
                            signal = signal.as_str(),
                            first_seq = lease.id().first_seq,
                            last_seq = lease.id().last_seq,
                            attempt = lease.attempt(),
                            table = %table_name,
                            error = %err,
                            "Iceberg Parquet data file write failed; nacking lease"
                        );
                        lease.nack(class).await?;
                        if is_fatal {
                            return Err(err);
                        }
                        return Ok(());
                    }
                }
            }

            if let (Some(cat), false) = (catalog, all_data_files.is_empty()) {
                let tx = Transaction::new(table);
                let token = format_lease_token(lease.id());
                let snapshot_props = HashMap::from([
                    (snapshot_property_key(signal).to_string(), token.clone()),
                    (
                        instance_snapshot_property_key(signal, lease.id().instance_id),
                        token,
                    ),
                ]);
                let action = tx
                    .fast_append()
                    .set_snapshot_properties(snapshot_props)
                    .add_data_files(all_data_files);
                let tx = match action.apply(tx) {
                    Ok(t) => t,
                    Err(e) => {
                        let err = PipelineError::Storage(Box::new(e));
                        let class = classify_iceberg_error(&err);
                        let is_fatal = matches!(class, FailureClass::Fatal { .. });
                        lease.nack(class).await?;
                        if is_fatal {
                            return Err(err);
                        }
                        return Ok(());
                    }
                };
                match tx.commit(cat.as_ref()).await {
                    Ok(_) => {
                        info!(
                            "Committed ACID transaction with data files for table: {}",
                            table_name
                        );
                    }
                    Err(commit_err) => {
                        if let Ok(Some(refreshed)) = self.load_table(catalog, table_name).await
                            && is_lease_already_committed(&refreshed, lease.id())
                        {
                            info!(
                                signal = signal.as_str(),
                                first_seq = lease.id().first_seq,
                                last_seq = lease.id().last_seq,
                                table = %table_name,
                                "Iceberg commit returned error but refreshed table snapshot contains lease token; acknowledging as committed"
                            );
                            lease.ack().await?;
                            return Ok(());
                        }
                        let err = PipelineError::Storage(Box::new(commit_err));
                        let class = classify_iceberg_error(&err);
                        let is_fatal = matches!(class, FailureClass::Fatal { .. });
                        warn!(
                            signal = signal.as_str(),
                            first_seq = lease.id().first_seq,
                            last_seq = lease.id().last_seq,
                            attempt = lease.attempt(),
                            table = %table_name,
                            error = %err,
                            "Iceberg transaction commit failed; nacking lease"
                        );
                        lease.nack(class).await?;
                        if is_fatal {
                            return Err(err);
                        }
                        return Ok(());
                    }
                }
            }
        } else {
            for (path, sub_batch) in sub_batches {
                info!(
                    "Simulating partition batch write (rows: {}) for partition: {}",
                    sub_batch.num_rows(),
                    path
                );
            }
            info!(
                "Simulating ACID transaction commit for table: {}",
                table_name
            );
        }

        lease.ack().await?;
        Ok(())
    }
}

async fn write_batch_to_table(
    table: &iceberg::table::Table,
    batch: RecordBatch,
) -> Result<Vec<iceberg::spec::DataFile>, PipelineError> {
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use parquet::file::properties::WriterProperties;
    use uuid::Uuid;

    let location_generator = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| PipelineError::Storage(Box::new(e)))?;
    let file_prefix = format!("part-{}", Uuid::now_v7());
    let file_name_generator =
        DefaultFileNameGenerator::new(file_prefix, None, iceberg::spec::DataFileFormat::Parquet);

    let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
        WriterProperties::default(),
        table.current_schema_ref().clone(),
        iceberg::arrow::FieldMatchMode::Name,
    );

    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);

    let mut data_file_writer = data_file_writer_builder
        .build(None)
        .await
        .map_err(|e| PipelineError::Storage(Box::new(e)))?;

    data_file_writer
        .write(batch)
        .await
        .map_err(|e| PipelineError::Storage(Box::new(e)))?;

    let data_files = data_file_writer
        .close()
        .await
        .map_err(|e| PipelineError::Storage(Box::new(e)))?;

    Ok(data_files)
}

#[async_trait]
impl Sink for IcebergSink {
    async fn run(&mut self, mut buffer: SinkBuffer) -> Result<(), PipelineError> {
        info!(
            "Starting IcebergSink task connected to catalog: {:?}, Table: {}",
            self.config.catalog_type, self.config.table_identifier
        );

        let catalog: Option<Arc<dyn Catalog>> = if let Some(ref cat) = self.catalog_override {
            Some(Arc::clone(cat))
        } else if !self.config.dry_run {
            match self.config.catalog_type {
                CatalogType::Rest => {
                    let mut props = HashMap::new();
                    props.insert("uri".to_string(), self.config.catalog_uri.clone());
                    props.insert("warehouse".to_string(), self.config.warehouse.clone());
                    for (k, v) in &self.config.properties {
                        props.insert(k.clone(), v.clone());
                    }
                    let storage_factory = iceberg_storage_opendal::OpenDalStorageFactory::S3 {
                        customized_credential_load: None,
                    };
                    let cat = RestCatalogBuilder::default()
                        .with_storage_factory(Arc::new(storage_factory))
                        .load(&self.config.table_identifier, props)
                        .await
                        .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                    Some(Arc::new(cat) as Arc<dyn Catalog>)
                }
                #[cfg(feature = "aws")]
                CatalogType::Glue => {
                    let mut props = HashMap::new();
                    props.insert("warehouse".to_string(), self.config.warehouse.clone());
                    for (k, v) in &self.config.properties {
                        props.insert(k.clone(), v.clone());
                    }
                    let cat = GlueCatalogBuilder::default()
                        .load(&self.config.table_identifier, props)
                        .await
                        .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                    Some(Arc::new(cat) as Arc<dyn Catalog>)
                }
                #[cfg(feature = "aws")]
                CatalogType::S3Tables => {
                    let mut props = HashMap::new();
                    for (k, v) in &self.config.properties {
                        props.insert(k.clone(), v.clone());
                    }
                    let cat = S3TablesCatalogBuilder::default()
                        .load(&self.config.table_identifier, props)
                        .await
                        .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                    Some(Arc::new(cat) as Arc<dyn Catalog>)
                }
            }
        } else {
            None
        };

        while let Some(lease) = buffer.next_lease().await? {
            self.commit_lease(lease, catalog.as_ref()).await?;
        }

        info!("IcebergSink task terminated gracefully.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn make_test_logs_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
        ]));

        let ts_array = Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
            1_717_891_200_000_000_000,
            1_717_894_800_000_000_000,
            1_717_891_200_000_000_000,
        ])) as ArrayRef;

        let sev_array = Arc::new(StringArray::from(vec!["INFO", "ERROR", "WARN"])) as ArrayRef;

        let service_array = Arc::new(StringArray::from(vec![
            "service-a",
            "service-b",
            "service-a",
        ])) as ArrayRef;

        let res_array = Arc::new(StringArray::from(vec![
            r#"{"service.name":"service-a"}"#,
            r#"{"service.name":"service-b"}"#,
            r#"{"service.name":"service-a"}"#,
        ])) as ArrayRef;

        RecordBatch::try_new(schema, vec![ts_array, sev_array, service_array, res_array]).unwrap()
    }

    fn make_dry_run_config(schema_mode: SchemaMode) -> IcebergSinkConfig {
        IcebergSinkConfig {
            warehouse: "s3://wh/".to_string(),
            table_identifier: "db.tbl".to_string(),
            schema_mode,
            dry_run: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_logs_sorting() {
        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let batch = make_test_logs_batch();
        let sorted = sink.sort_logs(&batch).unwrap();

        let res_col = sorted
            .column_by_name("resource_attributes")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(res_col.value(0), r#"{"service.name":"service-a"}"#);
        assert_eq!(res_col.value(1), r#"{"service.name":"service-a"}"#);
        assert_eq!(res_col.value(2), r#"{"service.name":"service-b"}"#);

        let sev_col = sorted
            .column_by_name("severity_text")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(sev_col.value(0), "INFO");
        assert_eq!(sev_col.value(1), "WARN");
        assert_eq!(sev_col.value(2), "ERROR");
    }

    #[test]
    fn test_iceberg_sink_custom_order_by() {
        let cfg = IcebergSinkConfig {
            order_by: Some(pipeline_core::sort::SortConfig {
                logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                    "timestamp DESC".to_string(),
                )],
                metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                    "timestamp DESC".to_string(),
                )],
                traces: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                    "timestamp DESC".to_string(),
                )],
                ..Default::default()
            }),
            ..Default::default()
        };
        let sink = IcebergSink::new(cfg);

        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["svc", "svc"])),
                Arc::new(StringArray::from(vec!["op", "op"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        // Verify custom order_by applied to logs
        let sorted_logs = sink.sort_logs(&batch).expect("should sort logs");
        let ts_logs = sorted_logs
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_logs.value(0), 20);
        assert_eq!(ts_logs.value(1), 10);

        // Verify custom order_by applied to metrics
        let sorted_metrics = sink.sort_metrics(&batch).expect("should sort metrics");
        let ts_metrics = sorted_metrics
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_metrics.value(0), 20);
        assert_eq!(ts_metrics.value(1), 10);

        // Verify custom order_by applied to traces
        let sorted_traces = sink.sort_traces(&batch).expect("should sort traces");
        let ts_traces = sorted_traces
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_traces.value(0), 20);
        assert_eq!(ts_traces.value(1), 10);
    }

    #[test]
    fn test_iceberg_sink_invalid_order_by_fallback() {
        let cfg = IcebergSinkConfig {
            order_by: Some(pipeline_core::sort::SortConfig {
                logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                    "invalid syntax here tokens".to_string(),
                )],
                ..Default::default()
            }),
            ..Default::default()
        };
        let sink = IcebergSink::new(cfg);

        // Fallback sorter should use legacy sort keys which require service_name, severity_text, timestamp
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["svc", "svc"])),
                Arc::new(StringArray::from(vec!["INFO", "WARN"])),
                Arc::new(Int64Array::from(vec![20, 10])),
            ],
        )
        .unwrap();

        let sorted = sink.sort_logs(&batch).expect("should sort using fallback");
        let ts = sorted
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 20); // Sorted by service_name then severity_text (INFO then WARN)
        assert_eq!(ts.value(1), 10);
    }

    #[test]
    fn test_logs_partitioning() {
        let batch = make_test_logs_batch();
        let partitions =
            partition_batch(&batch, "timestamp", PartitionGranularity::Hourly).unwrap();
        assert_eq!(partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_iceberg_sink_run() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = SinkBuffer::passthrough_pair();

        let batch = make_test_logs_batch();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();
    }

    #[test]
    fn test_schema_mode_auto_validation() {
        let metadata_json = r#"{
          "format-version": 3,
          "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
          "location": "s3://bucket/test/location",
          "last-sequence-number": 0,
          "last-updated-ms": 1602638573000,
          "last-column-id": 4,
          "current-schema-id": 0,
          "next-row-id": 0,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                { "id": 1, "name": "timestamp", "required": true, "type": "timestamp_ns" },
                { "id": 2, "name": "severity_text", "required": false, "type": "string" },
                { "id": 4, "name": "service_name", "required": false, "type": "string" },
                { "id": 3, "name": "resource_attributes", "required": false, "type": "string" }
              ]
            }
          ],
          "default-spec-id": 0,
          "partition-specs": [{ "spec-id": 0, "fields": [] }],
          "last-partition-id": 999,
          "default-sort-order-id": 0,
          "sort-orders": [{ "order-id": 0, "fields": [] }],
          "properties": {},
          "current-snapshot-id": -1,
          "snapshots": [],
          "snapshot-log": [],
          "metadata-log": []
        }"#;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(metadata_json).unwrap();
        let tokio_rt = tokio::runtime::Runtime::new().unwrap();
        let iceberg_rt = iceberg::Runtime::new(&tokio_rt);
        let table = iceberg::table::Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_memory())
            .runtime(iceberg_rt)
            .build()
            .unwrap();

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Auto));

        // 1. Matching schema should succeed
        let res = sink.apply_schema_mode(make_test_logs_batch(), Some(&table));
        assert!(res.is_ok());

        // 2. Extra column should fail
        let extra_schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("new_field", DataType::Int32, true),
        ]));
        let extra_batch = RecordBatch::try_new(
            extra_schema,
            vec![
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![0])),
                Arc::new(arrow::array::StringArray::from(vec!["INFO"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    r#"{"service.name":"service-a"}"#,
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![42])),
            ],
        )
        .unwrap();
        let err = sink
            .apply_schema_mode(extra_batch, Some(&table))
            .unwrap_err()
            .to_string();
        assert!(err.contains("new column 'new_field' detected"));

        // 3. Type mismatch should fail
        let mismatch_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let mismatch_batch = RecordBatch::try_new(
            mismatch_schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["not-a-timestamp"])),
                Arc::new(arrow::array::StringArray::from(vec!["INFO"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    r#"{"service.name":"service-a"}"#,
                ])),
            ],
        )
        .unwrap();
        let err_mismatch = sink
            .apply_schema_mode(mismatch_batch, Some(&table))
            .unwrap_err()
            .to_string();
        assert!(err_mismatch.contains("Schema mismatch for field 'timestamp'"));
    }

    #[test]
    fn test_schema_mode_auto_validation_v2_type_mismatch() {
        // In Iceberg v2, timestamps are microsecond precision ("timestamp").
        let metadata_v2_json = r#"{
          "format-version": 2,
          "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
          "location": "s3://bucket/test/location",
          "last-sequence-number": 0,
          "last-updated-ms": 1602638573000,
          "last-column-id": 4,
          "current-schema-id": 0,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                { "id": 1, "name": "timestamp", "required": true, "type": "timestamp" },
                { "id": 2, "name": "severity_text", "required": false, "type": "string" },
                { "id": 4, "name": "service_name", "required": false, "type": "string" },
                { "id": 3, "name": "resource_attributes", "required": false, "type": "string" }
              ]
            }
          ],
          "default-spec-id": 0,
          "partition-specs": [{ "spec-id": 0, "fields": [] }],
          "last-partition-id": 999,
          "default-sort-order-id": 0,
          "sort-orders": [{ "order-id": 0, "fields": [] }],
          "properties": {},
          "current-snapshot-id": -1,
          "snapshots": [],
          "snapshot-log": [],
          "metadata-log": []
        }"#;

        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(metadata_v2_json).unwrap();
        let tokio_rt = tokio::runtime::Runtime::new().unwrap();
        let iceberg_rt = iceberg::Runtime::new(&tokio_rt);
        let table = iceberg::table::Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_memory())
            .runtime(iceberg_rt)
            .build()
            .unwrap();

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Auto));
        // make_test_logs_batch() has Timestamp(Nanosecond), which mismatches v2 table's Timestamp(Microsecond)
        let err = sink
            .apply_schema_mode(make_test_logs_batch(), Some(&table))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Schema mismatch for field 'timestamp'"));
    }

    #[test]
    fn test_schema_mode_catalog_v2_downcast() {
        // In Iceberg v2 with SchemaMode::Catalog, nanosecond batches are downcast to microseconds.
        let metadata_v2_json = r#"{
          "format-version": 2,
          "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
          "location": "s3://bucket/test/location",
          "last-sequence-number": 0,
          "last-updated-ms": 1602638573000,
          "last-column-id": 4,
          "current-schema-id": 0,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                { "id": 1, "name": "timestamp", "required": true, "type": "timestamp" },
                { "id": 2, "name": "severity_text", "required": false, "type": "string" },
                { "id": 4, "name": "service_name", "required": false, "type": "string" },
                { "id": 3, "name": "resource_attributes", "required": false, "type": "string" }
              ]
            }
          ],
          "default-spec-id": 0,
          "partition-specs": [{ "spec-id": 0, "fields": [] }],
          "last-partition-id": 999,
          "default-sort-order-id": 0,
          "sort-orders": [{ "order-id": 0, "fields": [] }],
          "properties": {},
          "current-snapshot-id": -1,
          "snapshots": [],
          "snapshot-log": [],
          "metadata-log": []
        }"#;

        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(metadata_v2_json).unwrap();
        let tokio_rt = tokio::runtime::Runtime::new().unwrap();
        let iceberg_rt = iceberg::Runtime::new(&tokio_rt);
        let table = iceberg::table::Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_memory())
            .runtime(iceberg_rt)
            .build()
            .unwrap();

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Catalog));
        let res = sink
            .apply_schema_mode(make_test_logs_batch(), Some(&table))
            .unwrap();
        let ts_field = res.schema().field_with_name("timestamp").unwrap().clone();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    /// `get_partition_path` must produce correct hourly partition paths.
    #[test]
    fn test_partition_path_hourly() {
        // 2024-06-09T00:00:00Z in nanos
        let ts_nanos: i64 = 1_717_891_200_000_000_000;
        let path = get_partition_path(ts_nanos, PartitionGranularity::Hourly).unwrap();
        assert_eq!(path, "year=2024/month=06/day=09/hour=00/");
    }

    /// `get_partition_path` must produce correct daily partition paths.
    #[test]
    fn test_partition_path_daily() {
        let ts_nanos: i64 = 1_717_891_200_000_000_000;
        let path = get_partition_path(ts_nanos, PartitionGranularity::Daily).unwrap();
        assert_eq!(path, "year=2024/month=06/day=09/");
    }

    /// `partition_batch` with microsecond timestamps should correctly convert
    /// and produce valid partitions.
    #[test]
    fn test_partition_batch_microsecond_timestamps() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("data", DataType::Utf8, false),
        ]));

        // Two timestamps in different hours
        let ts_array = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
            1_717_891_200_000_000, // 2024-06-09T00:00:00Z
            1_717_894_800_000_000, // 2024-06-09T01:00:00Z
        ])) as ArrayRef;
        let data_array = Arc::new(arrow::array::StringArray::from(vec!["a", "b"])) as ArrayRef;

        let batch = RecordBatch::try_new(schema, vec![ts_array, data_array]).unwrap();
        let partitions =
            partition_batch(&batch, "timestamp", PartitionGranularity::Hourly).unwrap();
        assert_eq!(
            partitions.len(),
            2,
            "Two distinct hours should produce two partitions"
        );
    }

    /// `partition_batch` must fail gracefully when the timestamp column
    /// uses an unsupported data type.
    #[test]
    fn test_partition_batch_unsupported_timestamp_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let ts_array =
            Arc::new(arrow::array::StringArray::from(vec!["not-a-timestamp"])) as ArrayRef;
        let data_array = Arc::new(arrow::array::StringArray::from(vec!["a"])) as ArrayRef;

        let batch = RecordBatch::try_new(schema, vec![ts_array, data_array]).unwrap();
        let result = partition_batch(&batch, "timestamp", PartitionGranularity::Hourly);
        assert!(
            result.is_err(),
            "Unsupported timestamp type should produce an error"
        );
    }

    /// `partition_batch` must fail when the timestamp column doesn't exist.
    #[test]
    fn test_partition_batch_missing_timestamp_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
        let data_array = Arc::new(arrow::array::StringArray::from(vec!["a"])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![data_array]).unwrap();

        let result = partition_batch(&batch, "timestamp", PartitionGranularity::Daily);
        assert!(
            result.is_err(),
            "Missing timestamp column should produce an error"
        );
    }

    /// Running the sink on an empty buffer or zero-row batch in `dry_run` mode
    /// must succeed without panicking or producing errors.
    #[tokio::test]
    async fn test_iceberg_sink_empty_flush() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = SinkBuffer::passthrough_pair();
        let empty_batch = RecordBatch::new_empty(make_test_logs_batch().schema());
        tx.send(SignalBatch::Logs(empty_batch)).await.unwrap();
        drop(tx);
        let result = sink.run(rx).await;
        assert!(result.is_ok(), "Empty flush should succeed gracefully");
    }

    /// An empty `SinkBuffer` must yield `Ok(None)` when closed without data.
    #[tokio::test]
    async fn test_should_flush_empty_buffer() {
        let config = make_dry_run_config(SchemaMode::Fixed);
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let [(logs_tx, mut logs_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();
        drop(logs_tx);
        let lease = logs_rx.next_lease().await.unwrap();
        assert!(
            lease.is_none(),
            "Closed empty SinkBuffer must not produce a lease"
        );
    }

    /// `SinkBuffer` configured from `IcebergSinkConfig::resolved_buffer_config`
    /// must flush when the record count reaches `max_batch_records`.
    #[tokio::test]
    async fn test_should_flush_record_threshold() {
        let mut config = make_dry_run_config(SchemaMode::Fixed);
        config.batching = Some(BatchingConfig {
            max_batch_records: Some(2),
            max_batch_size_bytes: 134_217_728,
            max_batch_interval_sec: 3600,
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        assert_eq!(buf_cfg.max_batch_records, Some(2));

        let [(logs_tx, mut logs_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();
        let batch = make_test_logs_batch(); // 3 rows >= max_batch_records (2)
        logs_tx.send(SignalBatch::Logs(batch)).await.unwrap();

        let lease = logs_rx
            .next_lease()
            .await
            .unwrap()
            .expect("lease must flush when record threshold is reached");
        assert_eq!(lease.num_rows(), 3);
        lease.ack().await.unwrap();
    }

    /// Multiple batches accumulating in `SinkBuffer` must all be flushed
    /// together and drained cleanly in `dry_run` mode.
    #[tokio::test]
    async fn test_iceberg_sink_multi_batch_accumulation() {
        let mut config = make_dry_run_config(SchemaMode::Fixed);
        config.buffer = Some(BufferConfig {
            max_batch_records: Some(9),
            ..IcebergSink::default_buffer_config()
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = IcebergSink::new(config);
        let [(logs_tx, logs_rx), _, _] = SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();

        // Send 3 batches (3 rows each = 9 rows total)
        for _ in 0..3 {
            logs_tx
                .send(SignalBatch::Logs(make_test_logs_batch()))
                .await
                .unwrap();
        }
        drop(logs_tx);

        let result = sink.run(logs_rx).await;
        assert!(result.is_ok(), "Multi-batch dry_run should succeed");
    }

    // ── Sorting ────────────────────────────────────────────────────────────────

    fn make_test_metrics_batch() -> RecordBatch {
        use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            2_000_000_000_i64,
            1_000_000_000_i64,
        ]));
        let svc = Arc::new(StringArray::from(vec!["svc-b", "svc-a"]));
        let name = Arc::new(StringArray::from(vec!["cpu", "mem"]));
        let attrs = Arc::new(StringArray::from(vec!["{}", "{}"])) as ArrayRef;
        let value = Arc::new(Float64Array::from(vec![2.0, 1.0]));

        RecordBatch::try_new(schema, vec![ts, svc, name, attrs, value]).unwrap()
    }

    fn make_test_traces_batch() -> RecordBatch {
        use arrow::array::{StringArray, TimestampNanosecondArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ts = Arc::new(TimestampNanosecondArray::from(vec![
            2_000_000_000_i64,
            1_000_000_000_i64,
        ]));
        let svc = Arc::new(StringArray::from(vec!["svc-b", "svc-a"]));
        let name = Arc::new(StringArray::from(vec!["span-b", "span-a"]));

        RecordBatch::try_new(schema, vec![ts, svc, name]).unwrap()
    }

    /// `sort_metrics` must order rows by (`service_name`, name, attributes, timestamp).
    #[test]
    fn test_sort_metrics_produces_correct_order() {
        use arrow::array::AsArray;
        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let batch = make_test_metrics_batch();
        let sorted = sink.sort_metrics(&batch).unwrap();

        // After sorting by (service_name ASC, name ASC, attributes ASC, ts ASC)
        // svc-a/mem should come first
        let svc_col = sorted
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(svc_col.value(0), "svc-a");
        assert_eq!(svc_col.value(1), "svc-b");
    }

    /// `sort_traces` must order rows by (`service_name`, `span_name`, timestamp).
    #[test]
    fn test_sort_traces_produces_correct_order() {
        use arrow::array::AsArray;
        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let batch = make_test_traces_batch();
        let sorted = sink.sort_traces(&batch).unwrap();

        let svc_col = sorted
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>();
        assert_eq!(svc_col.value(0), "svc-a");
    }

    /// `sort_logs` must return `PipelineError::Internal` when the batch lacks
    /// the required `service_name` column.
    #[test]
    fn test_sort_logs_missing_column_returns_error() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new(
            "severity_text",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec!["INFO", "WARN"])) as ArrayRef],
        )
        .unwrap();

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let result = sink.sort_logs(&batch);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("service_name"),
            "Error must name the missing column: {msg}"
        );
    }

    /// `sort_metrics` must return `PipelineError::Internal` when the `name` column is absent.
    #[test]
    fn test_sort_metrics_missing_column_returns_error() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec!["svc", "svc2"])) as ArrayRef],
        )
        .unwrap();

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let result = sink.sort_metrics(&batch);
        assert!(result.is_err());
    }

    /// `apply_schema_mode` with `SchemaMode::Fixed` must return the batch unchanged.
    #[test]
    fn test_apply_schema_mode_fixed_is_passthrough() {
        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let batch = make_test_logs_batch();
        let original_schema = batch.schema();
        let result = sink.apply_schema_mode(batch, None).unwrap();
        assert_eq!(result.schema(), original_schema);
    }

    /// `get_partition_path` must handle timestamp 0 (Unix epoch) correctly.
    #[test]
    fn test_get_partition_path_zero_timestamp() {
        let path = get_partition_path(0, PartitionGranularity::Hourly).unwrap();
        assert_eq!(path, "year=1970/month=01/day=01/hour=00/");
    }

    /// `should_flush` must trigger when `total_bytes` exceeds `max_batch_size_bytes`.
    #[tokio::test]
    async fn test_should_flush_byte_threshold() {
        let mut config = make_dry_run_config(SchemaMode::Fixed);
        config.batching = Some(BatchingConfig {
            max_batch_records: None,
            max_batch_size_bytes: 1,
            max_batch_interval_sec: 3600,
        });

        let buf_cfg = config.resolved_buffer_config().unwrap();
        assert_eq!(buf_cfg.max_batch_size_bytes, 1);

        let [(logs_tx, mut logs_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();
        let batch = make_test_logs_batch();
        logs_tx.send(SignalBatch::Logs(batch)).await.unwrap();

        let lease = logs_rx
            .next_lease()
            .await
            .unwrap()
            .expect("lease must flush when byte threshold is reached");
        assert!(lease.byte_size() >= 1);
        lease.ack().await.unwrap();
    }

    #[test]
    fn test_iceberg_resolved_buffer_config_prefers_buffer_and_rejects_both() {
        let config = make_dry_run_config(SchemaMode::Fixed);
        let default_buf = config.resolved_buffer_config().unwrap();
        assert_eq!(default_buf, IcebergSink::default_buffer_config());

        let mut both_cfg = make_dry_run_config(SchemaMode::Fixed);
        both_cfg.buffer = Some(BufferConfig::default());
        both_cfg.batching = Some(BatchingConfig::default());
        let err = both_cfg.resolved_buffer_config().unwrap_err().to_string();
        assert!(
            err.contains("[iceberg.buffer]") && err.contains("[iceberg.batching]"),
            "Expected V4 conflict error, got: {err}"
        );
    }

    /// `dry_run` mode with a Metrics signal must complete without error.
    #[tokio::test]
    async fn test_iceberg_sink_run_with_metrics_batch() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Metrics(make_test_metrics_batch()))
            .await
            .unwrap();
        drop(tx);
        let result = sink.run(rx).await;
        assert!(
            result.is_ok(),
            "dry_run with Metrics must succeed: {result:?}"
        );
    }

    /// `dry_run` mode with a Traces signal must complete without error.
    #[tokio::test]
    async fn test_iceberg_sink_run_with_traces_batch() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Traces(make_test_traces_batch()))
            .await
            .unwrap();
        drop(tx);
        let result = sink.run(rx).await;
        assert!(
            result.is_ok(),
            "dry_run with Traces must succeed: {result:?}"
        );
    }

    #[test]
    fn test_format_and_parse_lease_token_roundtrip() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        assert_eq!(
            snapshot_property_key(SignalKind::Logs),
            "datalake.lease.logs"
        );
        assert_eq!(
            snapshot_property_key(SignalKind::Metrics),
            "datalake.lease.metrics"
        );
        assert_eq!(
            snapshot_property_key(SignalKind::Traces),
            "datalake.lease.traces"
        );
        assert_eq!(
            lease_snapshot_property_key(SignalKind::Logs),
            "datalake.lease.logs"
        );

        let id = LeaseId {
            instance_id: 0x0123_4567_89ab_cdef,
            signal: SignalKind::Logs,
            first_seq: 12,
            last_seq: 34,
        };
        let token = format_lease_token(id);
        assert_eq!(token, "0123456789abcdef:12:34");
        assert_eq!(
            parse_lease_token(&token),
            Some((0x0123_4567_89ab_cdef, 12, 34))
        );

        let range = format_lease_range(id);
        assert_eq!(range, "0123456789abcdef:12-34");
        assert_eq!(
            parse_lease_token(&range),
            Some((0x0123_4567_89ab_cdef, 12, 34))
        );

        assert_eq!(parse_lease_token(""), None);
        assert_eq!(parse_lease_token("abc"), None);
        assert_eq!(parse_lease_token("0000000000000000:1:2"), None);
        assert_eq!(parse_lease_token("0123456789abcdef:20:10"), None);
    }

    #[test]
    fn test_iceberg_lease_already_committed_recognizes_matching_or_newer_last_seq() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        let metadata_json = r#"{
          "format-version": 3,
          "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
          "location": "memory:///bucket/test/location",
          "last-sequence-number": 1,
          "last-updated-ms": 1602638573000,
          "last-column-id": 4,
          "current-schema-id": 0,
          "next-row-id": 10,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                { "id": 1, "name": "timestamp", "required": true, "type": "timestamp_ns" },
                { "id": 2, "name": "severity_text", "required": false, "type": "string" },
                { "id": 4, "name": "service_name", "required": false, "type": "string" },
                { "id": 3, "name": "resource_attributes", "required": false, "type": "string" }
              ]
            }
          ],
          "default-spec-id": 0,
          "partition-specs": [{ "spec-id": 0, "fields": [] }],
          "last-partition-id": 999,
          "default-sort-order-id": 0,
          "sort-orders": [{ "order-id": 0, "fields": [] }],
          "properties": {},
          "current-snapshot-id": 101,
          "snapshots": [
            {
              "snapshot-id": 101,
              "sequence-number": 1,
              "timestamp-ms": 1602638573000,
              "manifest-list": "memory:///bucket/test/location/metadata/snap-101.avro",
              "summary": {
                "operation": "append",
                "datalake.lease.logs": "0123456789abcdef:1:5"
              },
              "schema-id": 0,
              "first-row-id": 0,
              "added-rows": 10
            }
          ],
          "snapshot-log": [],
          "metadata-log": []
        }"#;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(metadata_json).unwrap();
        let tokio_rt = tokio::runtime::Runtime::new().unwrap();
        let iceberg_rt = iceberg::Runtime::new(&tokio_rt);
        let table = iceberg::table::Table::builder()
            .metadata(metadata)
            .metadata_location("memory:///bucket/test/location/metadata/v1.json".to_string())
            .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_memory())
            .runtime(iceberg_rt)
            .build()
            .unwrap();

        assert!(is_lease_already_committed(
            &table,
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 5,
            }
        ));
        assert!(is_lease_already_committed(
            &table,
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 3,
            }
        ));
        // A different buffer instance (e.g. restarted memory buffer or second replica) with seq 1..5 must NOT be treated as committed
        assert!(!is_lease_already_committed(
            &table,
            LeaseId {
                instance_id: 0xfedc_ba98_7654_3210,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 5,
            }
        ));
        assert!(!is_lease_already_committed(
            &table,
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Logs,
                first_seq: 6,
                last_seq: 6,
            }
        ));
        assert!(!is_lease_already_committed(
            &table,
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Metrics,
                first_seq: 1,
                last_seq: 5,
            }
        ));
    }

    #[tokio::test]
    async fn test_iceberg_prepare_runs_schema_mode_sort_and_partition_in_spawn_blocking() {
        use pipeline_core::buffer::{BufferConfig, SignalKind};

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let buf_cfg = BufferConfig {
            max_batch_size_bytes: 1_048_576,
            max_batch_records: Some(10),
            max_batch_interval_sec: 60,
            ..BufferConfig::default()
        };
        let [(logs_tx, mut logs_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();

        logs_tx
            .send(SignalBatch::Logs(make_test_logs_batch()))
            .await
            .unwrap();
        logs_tx
            .send(SignalBatch::Logs(make_test_logs_batch()))
            .await
            .unwrap();
        drop(logs_tx);

        let lease = logs_rx.next_lease().await.unwrap().expect("expected lease");
        assert_eq!(lease.batches().len(), 2);
        assert_eq!(lease.num_rows(), 6);

        let sorter = sink.sorter.clone();
        let granularity = sink.config.partition_granularity;
        let schema_mode = sink.config.schema_mode;
        let log_dropped_fields = sink.config.log_dropped_fields;

        let sub_batches = lease
            .prepare(move |batch| {
                prepare_iceberg_batches(
                    batch,
                    SignalKind::Logs,
                    schema_mode,
                    log_dropped_fields,
                    None,
                    &sorter,
                    granularity,
                )
            })
            .await
            .unwrap();

        assert_eq!(sub_batches.len(), 2);
        let total_rows: usize = sub_batches.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 6);
        lease.ack().await.unwrap();
    }

    #[derive(Debug)]
    struct MockIcebergCatalog {
        inner: iceberg::memory::MemoryCatalog,
        update_calls: std::sync::atomic::AtomicUsize,
        fail_first_update_before_commit: bool,
        fail_first_update_after_commit: bool,
    }

    #[async_trait]
    impl Catalog for MockIcebergCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&iceberg::NamespaceIdent>,
        ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &iceberg::NamespaceIdent,
            creation: iceberg::TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(
            &self,
            table: &iceberg::TableIdent,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &iceberg::TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(
            &self,
            src: &iceberg::TableIdent,
            dest: &iceberg::TableIdent,
        ) -> iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &iceberg::TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(
            &self,
            commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            let call_idx = self
                .update_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call_idx == 0 && self.fail_first_update_before_commit {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::CatalogCommitConflicts,
                    "simulated transient catalog commit conflict",
                ));
            }
            let updated = self.inner.update_table(commit).await?;
            if call_idx == 0 && self.fail_first_update_after_commit {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "simulated network disconnect after catalog persisted snapshot",
                ));
            }
            Ok(updated)
        }
    }

    async fn make_mock_logs_catalog(
        fail_first_update_before_commit: bool,
        fail_first_update_after_commit: bool,
    ) -> Arc<MockIcebergCatalog> {
        use iceberg::CatalogBuilder;
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

        let inner = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:///warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();

        let ns = iceberg::NamespaceIdent::new("otel".to_string());
        inner.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::optional(2, "severity_text", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::optional(3, "service_name", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::optional(
                    4,
                    "resource_attributes",
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
            ])
            .build()
            .unwrap();

        let creation = iceberg::TableCreation::builder()
            .name("logs".to_string())
            .schema(schema)
            .properties(HashMap::from([(
                "commit.retry.num-retries".to_string(),
                "0".to_string(),
            )]))
            .build();
        inner.create_table(&ns, creation).await.unwrap();

        Arc::new(MockIcebergCatalog {
            inner,
            update_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first_update_before_commit,
            fail_first_update_after_commit,
        })
    }

    #[tokio::test]
    async fn test_iceberg_snapshot_token_skips_already_committed_lease_on_retry() {
        use pipeline_core::buffer::{BufferConfig, BufferMode, LeaseId, SignalKind};

        let snapshot_summary_props = HashMap::from([(
            "datalake.lease.logs".to_string(),
            "0123456789abcdef:1-5".to_string(),
        )]);
        assert!(is_lease_already_committed(
            &snapshot_summary_props,
            LeaseId {
                instance_id: 0x0123_4567_89ab_cdef,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 5,
            }
        ));
        assert!(!is_lease_already_committed(
            &snapshot_summary_props,
            LeaseId {
                instance_id: 0x9999_8888_7777_6666,
                signal: SignalKind::Logs,
                first_seq: 1,
                last_seq: 5,
            }
        ));

        // Verify against a live catalog:
        // 1. A disk-backed buffer commits lease 1:1 where the first commit persists the
        //    snapshot token before returning a disconnect error; post-error reload detects
        //    the token and acks without a second `update_table`.
        // 2. If the disk buffer crashed before persisting its watermark and is reopened on
        //    the same `disk_path`, it recovers the same `instance_id` and skips duplicate
        //    commit (`update_calls == 1`).
        // 3. A fresh memory-mode buffer (process restart in memory mode or second replica)
        //    gets a distinct `instance_id` and commits its own `1:1` lease (`update_calls == 2`).
        let disk_dir =
            std::env::temp_dir().join(format!("otel-iceberg-inst-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&disk_dir);

        let catalog = make_mock_logs_catalog(false, true).await;
        let mut config = make_dry_run_config(SchemaMode::Catalog);
        config.dry_run = false;
        config.table_identifier = "otel.logs".to_string();

        let mut sink = IcebergSink::with_catalog(config, catalog.clone());
        let disk_buf_cfg = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(disk_dir.clone()),
            sync_on_write: true,
            max_batch_size_bytes: 1_048_576,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            initial_backoff_ms: 5,
            max_backoff_sec: 1,
            ..BufferConfig::default()
        };
        let [(logs_tx, logs_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", disk_buf_cfg.clone()).unwrap();
        let disk_inst_id = logs_rx.instance_id();
        logs_tx
            .send(SignalBatch::Logs(make_test_logs_batch()))
            .await
            .unwrap();
        drop(logs_tx);

        sink.run(logs_rx).await.unwrap();

        assert_eq!(
            catalog
                .update_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let expected_token = format!("{disk_inst_id:016x}:1:1");
        let table = catalog
            .load_table(&iceberg::TableIdent::from_strs(["otel", "logs"]).unwrap())
            .await
            .unwrap();
        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snap.summary()
                .additional_properties
                .get("datalake.lease.logs")
                .map(String::as_str),
            Some(expected_token.as_str())
        );

        // Simulate crash before watermark update by resetting watermark counts to 0 while keeping instance_id
        let wm_path = disk_dir.join("watermark.json");
        let mut wm: pipeline_core::buffer::DiskWatermark =
            serde_json::from_slice(&std::fs::read(&wm_path).unwrap()).unwrap();
        assert_eq!(wm.instance_id, disk_inst_id);
        wm.logs = 0;
        std::fs::write(&wm_path, serde_json::to_vec(&wm).unwrap()).unwrap();

        // Reopen disk buffer on same disk_path -> replays seq 1:1 with same instance_id -> skipped!
        let [(reopen_tx, reopen_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", disk_buf_cfg).unwrap();
        assert_eq!(reopen_rx.instance_id(), disk_inst_id);
        drop(reopen_tx);
        sink.run(reopen_rx).await.unwrap();
        assert_eq!(
            catalog
                .update_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "Recovered disk lease with matching instance_id must be skipped without calling update_table again"
        );

        // Now start a fresh memory-mode buffer whose first lease ALSO has seq 1:1 -> distinct instance_id -> MUST commit!
        let mem_buf_cfg = BufferConfig {
            max_batch_size_bytes: 1_048_576,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            initial_backoff_ms: 5,
            max_backoff_sec: 1,
            ..BufferConfig::default()
        };
        let [(mem_tx, mem_rx), _, _] =
            SinkBuffer::new_signal_triad("iceberg", mem_buf_cfg).unwrap();
        assert_ne!(mem_rx.instance_id(), disk_inst_id);
        mem_tx
            .send(SignalBatch::Logs(make_test_logs_batch()))
            .await
            .unwrap();
        drop(mem_tx);

        sink.run(mem_rx).await.unwrap();
        assert_eq!(
            catalog
                .update_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "Fresh memory buffer with distinct instance_id must commit its own seq 1:1 lease instead of dropping it"
        );

        let _ = std::fs::remove_dir_all(&disk_dir);
    }

    #[tokio::test]
    async fn test_iceberg_catalog_commit_failure_nacks_transient_and_retains_lease() {
        use pipeline_core::buffer::BufferConfig;

        let catalog = make_mock_logs_catalog(true, false).await;
        let mut config = make_dry_run_config(SchemaMode::Catalog);
        config.dry_run = false;
        config.table_identifier = "otel.logs".to_string();

        let mut sink = IcebergSink::with_catalog(config, catalog.clone());
        let buf_cfg = BufferConfig {
            max_batch_size_bytes: 1_048_576,
            max_batch_records: Some(1),
            max_batch_interval_sec: 60,
            initial_backoff_ms: 5,
            max_backoff_sec: 1,
            ..BufferConfig::default()
        };
        let [(logs_tx, logs_rx), _, _] = SinkBuffer::new_signal_triad("iceberg", buf_cfg).unwrap();
        let expected_token = format!("{:016x}:1:1", logs_rx.instance_id());
        logs_tx
            .send(SignalBatch::Logs(make_test_logs_batch()))
            .await
            .unwrap();
        drop(logs_tx);

        sink.run(logs_rx).await.unwrap();

        // First attempt failed with CatalogCommitConflicts -> nack(Transient) retained
        // the lease -> second attempt succeeded.
        assert_eq!(
            catalog
                .update_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "Transient commit failure must nack and retry the retained lease"
        );

        let table = catalog
            .load_table(&iceberg::TableIdent::from_strs(["otel", "logs"]).unwrap())
            .await
            .unwrap();
        let snap = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            snap.summary()
                .additional_properties
                .get("datalake.lease.logs")
                .map(String::as_str),
            Some(expected_token.as_str())
        );
    }

    #[test]
    fn test_iceberg_lease_tokens_and_error_classification_variants() {
        use pipeline_core::buffer::{LeaseId, SignalKind};

        assert_eq!(
            lease_snapshot_property_key(SignalKind::Logs),
            "datalake.lease.logs"
        );
        assert_eq!(
            lease_snapshot_property_key(SignalKind::Metrics),
            "datalake.lease.metrics"
        );
        assert_eq!(
            lease_snapshot_property_key(SignalKind::Traces),
            "datalake.lease.traces"
        );
        assert_eq!(
            instance_snapshot_property_key(SignalKind::Logs, 1),
            "datalake.lease.logs.0000000000000001"
        );

        let lease_id = LeaseId {
            instance_id: 1,
            signal: SignalKind::Logs,
            first_seq: 2,
            last_seq: 5,
        };
        assert_eq!(format_lease_range(lease_id), "0000000000000001:2-5");
        assert_eq!(parse_lease_token("0000000000000000:1:2"), None);
        assert_eq!(parse_lease_token("0000000000000001:5:2"), None);
        assert_eq!(parse_lease_token("0000000000000001:2-5"), Some((1, 2, 5)));

        let mut props = HashMap::new();
        props.insert(
            "datalake.lease.logs.0000000000000001".to_string(),
            "0000000000000001:1-10".to_string(),
        );
        assert!(!props.has_committed_lease(LeaseId {
            instance_id: 0,
            signal: SignalKind::Logs,
            first_seq: 1,
            last_seq: 1,
        }));
        let props_ref = &props;
        assert!(!(&props_ref).has_committed_lease(LeaseId {
            instance_id: 0,
            signal: SignalKind::Logs,
            first_seq: 1,
            last_seq: 1,
        }));
        assert!((&props_ref).has_committed_lease(lease_id));
        assert_eq!(
            (&props_ref).lease_property("datalake.lease.logs.0000000000000001"),
            Some("0000000000000001:1-10")
        );

        assert!(matches!(
            classify_iceberg_error(&PipelineError::Internal("bad row".into())),
            FailureClass::Rejected { .. }
        ));
        assert!(matches!(
            classify_iceberg_error(&PipelineError::Arrow(
                arrow::error::ArrowError::InvalidArgumentError("bad col".into())
            )),
            FailureClass::Rejected { .. }
        ));
        let cfg_err = BufferConfig {
            max_attempts: 0,
            ..BufferConfig::default()
        }
        .validate("iceberg")
        .unwrap_err();
        assert!(matches!(
            classify_iceberg_error(&cfg_err),
            FailureClass::Fatal { .. }
        ));
        assert!(matches!(
            classify_iceberg_error(&PipelineError::Storage("401 Unauthorized".into())),
            FailureClass::Fatal { .. }
        ));
        assert!(matches!(
            classify_iceberg_error(&PipelineError::Storage("TableNotFound".into())),
            FailureClass::Fatal { .. }
        ));
        assert!(matches!(
            classify_iceberg_error(&PipelineError::Storage("DataInvalid: bad field".into())),
            FailureClass::Rejected { .. }
        ));
        assert!(matches!(
            classify_iceberg_error(&PipelineError::Storage("503 Service Unavailable".into())),
            FailureClass::Transient
        ));

        let default_buf = IcebergSink::default_buffer_config();
        assert_eq!(default_buf.max_batch_size_bytes, 134_217_728);
        assert_eq!(default_buf.max_batch_interval_sec, 60);

        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let empty_batch = make_test_logs_batch().slice(0, 0);
        let empty_res = prepare_iceberg_batches(
            empty_batch,
            SignalKind::Logs,
            SchemaMode::Fixed,
            true,
            None,
            &sink.sorter,
            PartitionGranularity::Daily,
        )
        .unwrap();
        assert!(empty_res.is_empty());
    }

    #[test]
    fn test_partition_batch_microsecond_and_error_branches() {
        use arrow::array::{Int64Array, TimestampMicrosecondArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        // 1. Valid TimestampMicrosecondArray with PartitionGranularity::Daily
        let micro_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let micro_batch = RecordBatch::try_new(
            Arc::clone(&micro_schema),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000i64),
                Some(1_700_000_001_000_000i64),
            ]))],
        )
        .unwrap();
        let parts =
            partition_batch(&micro_batch, "timestamp", PartitionGranularity::Daily).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, "year=2023/month=11/day=14/");
        assert_eq!(parts[0].1.num_rows(), 2);

        // 2. Null timestamp in TimestampMicrosecondArray -> Err
        let null_micro_batch = RecordBatch::try_new(
            micro_schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000i64),
                None,
            ]))],
        )
        .unwrap();
        let null_err = partition_batch(&null_micro_batch, "timestamp", PartitionGranularity::Daily)
            .unwrap_err();
        assert!(null_err.to_string().contains("Null timestamp"));

        // 3. Null timestamp in TimestampNanosecondArray -> Err
        let nano_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let null_nano_batch = RecordBatch::try_new(
            nano_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![None]))],
        )
        .unwrap();
        let null_nano_err =
            partition_batch(&null_nano_batch, "timestamp", PartitionGranularity::Hourly)
                .unwrap_err();
        assert!(null_nano_err.to_string().contains("Null timestamp"));

        // 4. Missing timestamp column -> Err
        let missing_err =
            partition_batch(&micro_batch, "missing_ts", PartitionGranularity::Daily).unwrap_err();
        assert!(missing_err.to_string().contains("Missing missing_ts"));

        // 5. Unsupported timestamp datatype -> Err
        let bad_type_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let bad_type_batch = RecordBatch::try_new(
            bad_type_schema,
            vec![Arc::new(Int64Array::from(vec![1_700_000_000i64]))],
        )
        .unwrap();
        let bad_type_err =
            partition_batch(&bad_type_batch, "timestamp", PartitionGranularity::Daily).unwrap_err();
        assert!(
            bad_type_err
                .to_string()
                .contains("Unexpected timestamp column datatype")
        );
    }
}
