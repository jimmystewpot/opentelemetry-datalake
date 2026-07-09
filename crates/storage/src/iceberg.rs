use crate::{CatalogType, PartitionGranularity, SchemaMode};
#[cfg(test)]
use arrow::array::ArrayRef;
use arrow::array::{Array, AsArray};
use arrow::compute::{SortColumn, lexsort_to_indices};
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
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, SignalBatch, Sink};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

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

fn default_max_batch_size_bytes() -> usize {
    134_217_728 // 128MB
}

fn default_max_batch_interval_sec() -> u64 {
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
    #[serde(default)]
    pub batching: Option<BatchingConfig>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// When true, catalog operations are skipped and transaction commits
    /// are simulated. Used for testing without a live catalog.
    #[serde(default)]
    pub dry_run: bool,
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
            batching: None,
            properties: HashMap::new(),
            dry_run: false,
        }
    }
}

fn default_schema_mode() -> SchemaMode {
    SchemaMode::Fixed
}

fn default_partition_granularity() -> PartitionGranularity {
    PartitionGranularity::Hourly
}

fn default_log_dropped_fields() -> bool {
    true
}

struct TableBuffer {
    signal_type: &'static str,
    batches: Vec<RecordBatch>,
    total_rows: usize,
    total_bytes: usize,
    last_flush: std::time::Instant,
}

impl TableBuffer {
    fn new(signal_type: &'static str) -> Self {
        Self {
            signal_type,
            batches: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            last_flush: std::time::Instant::now(),
        }
    }
}

/// Apache Iceberg storage sink.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    buffers: std::collections::HashMap<String, TableBuffer>,
}

impl IcebergSink {
    /// Creates a new `IcebergSink` from configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig) -> Self {
        Self {
            config,
            buffers: std::collections::HashMap::new(),
        }
    }

    /// Sorts a log batch based on the (`ServiceName`, `SeverityText`, `Timestamp`) tuple.
    pub fn sort_logs(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let service_name_col = batch
            .column_by_name("service_name")
            .ok_or_else(|| PipelineError::Internal("Missing service_name".to_string()))?
            .clone();

        let severity_col = batch
            .column_by_name("severity_text")
            .ok_or_else(|| PipelineError::Internal("Missing severity_text".to_string()))?
            .clone();

        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| PipelineError::Internal("Missing timestamp".to_string()))?
            .clone();

        let sort_cols = vec![
            SortColumn {
                values: service_name_col,
                options: None,
            },
            SortColumn {
                values: severity_col,
                options: None,
            },
            SortColumn {
                values: timestamp_col,
                options: None,
            },
        ];

        sort_batch(batch, &sort_cols)
    }

    /// Sorts a metrics batch based on the (`ServiceName`, `MetricName`, `Attributes`, `Timestamp`) tuple.
    pub fn sort_metrics(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let service_name_col = batch
            .column_by_name("service_name")
            .ok_or_else(|| PipelineError::Internal("Missing service_name".to_string()))?
            .clone();

        let name_col = batch
            .column_by_name("name")
            .ok_or_else(|| PipelineError::Internal("Missing name".to_string()))?
            .clone();

        let attributes_col = batch
            .column_by_name("attributes")
            .ok_or_else(|| PipelineError::Internal("Missing attributes".to_string()))?
            .clone();

        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| PipelineError::Internal("Missing timestamp".to_string()))?
            .clone();

        let sort_cols = vec![
            SortColumn {
                values: service_name_col,
                options: None,
            },
            SortColumn {
                values: name_col,
                options: None,
            },
            SortColumn {
                values: attributes_col,
                options: None,
            },
            SortColumn {
                values: timestamp_col,
                options: None,
            },
        ];

        sort_batch(batch, &sort_cols)
    }

    /// Sorts a traces batch based on the (`ServiceName`, `SpanName`, `Timestamp`) tuple.
    pub fn sort_traces(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let service_name_col = batch
            .column_by_name("service_name")
            .ok_or_else(|| PipelineError::Internal("Missing service_name".to_string()))?
            .clone();

        let name_col = batch
            .column_by_name("name")
            .ok_or_else(|| PipelineError::Internal("Missing name (span name)".to_string()))?
            .clone();

        let timestamp_col = batch
            .column_by_name("timestamp")
            .ok_or_else(|| PipelineError::Internal("Missing timestamp".to_string()))?
            .clone();

        let sort_cols = vec![
            SortColumn {
                values: service_name_col,
                options: None,
            },
            SortColumn {
                values: name_col,
                options: None,
            },
            SortColumn {
                values: timestamp_col,
                options: None,
            },
        ];

        sort_batch(batch, &sort_cols)
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
        match self.config.schema_mode {
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

                if !dropped_fields.is_empty() && self.config.log_dropped_fields {
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
}

/// Helper function to lexically sort a `RecordBatch` by sort columns.
fn sort_batch(batch: &RecordBatch, sort_cols: &[SortColumn]) -> Result<RecordBatch, PipelineError> {
    let indices = lexsort_to_indices(sort_cols, None).map_err(PipelineError::Arrow)?;

    let columns = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(PipelineError::Arrow)?;

    RecordBatch::try_new(batch.schema(), columns).map_err(PipelineError::Arrow)
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
            let ts_array = ts_col.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
            for i in 0..batch.num_rows() {
                timestamps_nanos.push(ts_array.value(i));
            }
        }
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let ts_array = ts_col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
            for i in 0..batch.num_rows() {
                timestamps_nanos.push(ts_array.value(i) * 1000);
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

    /// Flushes the buffered data for the specified table.
    async fn flush_table(
        &mut self,
        table_name: &str,
        catalog: Option<&Arc<dyn Catalog>>,
    ) -> Result<(), PipelineError> {
        let Some(buffer) = self.buffers.get_mut(table_name) else {
            return Ok(());
        };

        if buffer.batches.is_empty() {
            buffer.last_flush = std::time::Instant::now();
            return Ok(());
        }

        let batches_to_flush = std::mem::take(&mut buffer.batches);
        let signal_type = buffer.signal_type;
        buffer.total_rows = 0;
        buffer.total_bytes = 0;
        buffer.last_flush = std::time::Instant::now();

        // Concatenate batches
        let schema = batches_to_flush[0].schema();
        let combined_batch = arrow::compute::concat_batches(&schema, &batches_to_flush)
            .map_err(PipelineError::Arrow)?;

        info!(
            "Flushing {} accumulated records for {} to table {}",
            combined_batch.num_rows(),
            signal_type,
            table_name
        );

        let loaded_table = self.load_table(catalog, table_name).await?;
        let final_batch = self.apply_schema_mode(combined_batch, loaded_table.as_ref())?;

        // Sort batch based on signal type
        let sorted_batch = match signal_type {
            "logs" => self.sort_logs(&final_batch)?,
            "metrics" => self.sort_metrics(&final_batch)?,
            "traces" => self.sort_traces(&final_batch)?,
            _ => final_batch,
        };

        let timestamp_col = "timestamp";

        // Partition the batch
        let sub_batches = partition_batch(
            &sorted_batch,
            timestamp_col,
            self.config.partition_granularity,
        )?;

        if let Some(ref table) = loaded_table {
            let mut all_data_files = Vec::new();
            for (path, sub_batch) in sub_batches {
                info!(
                    "Writing sorted {} batch (rows: {}) to Iceberg partition: {}{}",
                    signal_type,
                    sub_batch.num_rows(),
                    self.config.warehouse,
                    path
                );
                let data_files = write_batch_to_table(table, sub_batch).await?;
                all_data_files.extend(data_files);
            }

            if let (Some(cat), false) = (catalog, all_data_files.is_empty()) {
                let tx = Transaction::new(table);
                let action = tx.fast_append().add_data_files(all_data_files);
                let tx = action
                    .apply(tx)
                    .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                tx.commit(cat.as_ref())
                    .await
                    .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                info!(
                    "Committed ACID transaction with data files for table: {}",
                    table_name
                );
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

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.current_schema_ref().clone(),
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

impl IcebergSink {
    fn should_flush(&self, table_name: &str) -> bool {
        let Some(buffer) = self.buffers.get(table_name) else {
            return false;
        };
        if buffer.batches.is_empty() {
            return false;
        }
        let batching = self.config.batching.as_ref();
        let max_records = batching.and_then(|b| b.max_batch_records);
        let max_size =
            batching.map_or_else(default_max_batch_size_bytes, |b| b.max_batch_size_bytes);
        let max_interval =
            batching.map_or_else(default_max_batch_interval_sec, |b| b.max_batch_interval_sec);

        if max_records.is_some_and(|m| buffer.total_rows >= m) {
            return true;
        }
        if buffer.total_bytes >= max_size {
            return true;
        }
        if buffer.last_flush.elapsed().as_secs() >= max_interval {
            return true;
        }
        false
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        info!(
            "Starting IcebergSink task connected to catalog: {:?}, Table: {}",
            self.config.catalog_type, self.config.table_identifier
        );

        let mut catalog: Option<Arc<dyn Catalog>> = None;

        if !self.config.dry_run {
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
                    catalog = Some(Arc::new(cat) as Arc<dyn Catalog>);
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
                    catalog = Some(Arc::new(cat) as Arc<dyn Catalog>);
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
                    catalog = Some(Arc::new(cat) as Arc<dyn Catalog>);
                }
            }
        }

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        let table_name = self.config.table_identifier.clone();

        loop {
            tokio::select! {
                maybe_signal = input.recv() => {
                    match maybe_signal {
                        Some(signal) => {
                            let signal_type = match &signal {
                                SignalBatch::Logs(_) => "logs",
                                SignalBatch::Metrics(_) => "metrics",
                                SignalBatch::Traces(_) => "traces",
                            };

                            let buffer = self.buffers.entry(table_name.clone()).or_insert_with(|| TableBuffer::new(signal_type));
                            let batch = match signal {
                                SignalBatch::Logs(b) | SignalBatch::Metrics(b) | SignalBatch::Traces(b) => b,
                            };

                            buffer.total_rows += batch.num_rows();
                            buffer.total_bytes += batch.get_array_memory_size();
                            buffer.batches.push(batch);

                            if self.should_flush(&table_name) {
                                self.flush_table(&table_name, catalog.as_ref()).await?;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if self.should_flush(&table_name) {
                        self.flush_table(&table_name, catalog.as_ref()).await?;
                    }
                }
            }
        }

        info!("Performing final flush of buffered data before termination...");
        self.flush_table(&table_name, catalog.as_ref()).await?;

        info!("IcebergSink task terminated gracefully.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tokio::sync::mpsc;

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
        println!("Test batch schema: {:?}", batch.schema());
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
    fn test_logs_partitioning() {
        let batch = make_test_logs_batch();
        let partitions =
            partition_batch(&batch, "timestamp", PartitionGranularity::Hourly).unwrap();
        assert_eq!(partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_iceberg_sink_run() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = mpsc::channel(10);

        let batch = make_test_logs_batch();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();
    }

    #[test]
    fn test_schema_mode_auto_validation() {
        let metadata_json = r#"{
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

    /// get_partition_path must produce correct hourly partition paths.
    #[test]
    fn test_partition_path_hourly() {
        // 2024-06-09T00:00:00Z in nanos
        let ts_nanos: i64 = 1_717_891_200_000_000_000;
        let path = get_partition_path(ts_nanos, PartitionGranularity::Hourly).unwrap();
        assert_eq!(path, "year=2024/month=06/day=09/hour=00/");
    }

    /// get_partition_path must produce correct daily partition paths.
    #[test]
    fn test_partition_path_daily() {
        let ts_nanos: i64 = 1_717_891_200_000_000_000;
        let path = get_partition_path(ts_nanos, PartitionGranularity::Daily).unwrap();
        assert_eq!(path, "year=2024/month=06/day=09/");
    }

    /// partition_batch with microsecond timestamps should correctly convert
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

    /// partition_batch must fail gracefully when the timestamp column
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

    /// partition_batch must fail when the timestamp column doesn't exist.
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

    /// Flushing an empty buffer in dry_run mode must succeed without
    /// panicking or producing errors.
    #[tokio::test]
    async fn test_iceberg_sink_empty_flush() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        // Flush without any buffered data should be a no-op
        let result = sink.flush_table(&"db.tbl".to_string(), None).await;
        assert!(result.is_ok(), "Empty flush should succeed gracefully");
    }

    /// should_flush must return false when the buffer is empty,
    /// even if other thresholds are exceeded.
    #[test]
    fn test_should_flush_empty_buffer() {
        let sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        assert!(
            !sink.should_flush("db.tbl"),
            "should_flush on non-existent buffer must return false"
        );
    }

    /// should_flush must trigger when the record count exceeds the
    /// configured max_batch_records threshold.
    #[test]
    fn test_should_flush_record_threshold() {
        let mut config = make_dry_run_config(SchemaMode::Fixed);
        config.batching = Some(BatchingConfig {
            max_batch_records: Some(2),
            max_batch_size_bytes: usize::MAX,
            max_batch_interval_sec: u64::MAX,
        });

        let mut sink = IcebergSink::new(config);
        let batch = make_test_logs_batch(); // 3 rows
        let table_name = "db.tbl".to_string();

        let buffer = sink
            .buffers
            .entry(table_name.clone())
            .or_insert_with(|| TableBuffer::new("logs"));
        buffer.total_rows = batch.num_rows();
        buffer.total_bytes = batch.get_array_memory_size();
        buffer.batches.push(batch);

        assert!(
            sink.should_flush(&table_name),
            "should_flush must trigger when total_rows >= max_batch_records"
        );
    }

    /// Multiple batches accumulating in the buffer must all be flushed
    /// together and the buffer must be empty afterwards in dry_run mode.
    #[tokio::test]
    async fn test_iceberg_sink_multi_batch_accumulation() {
        let mut sink = IcebergSink::new(make_dry_run_config(SchemaMode::Fixed));
        let (tx, rx) = mpsc::channel(10);

        // Send multiple batches
        for _ in 0..3 {
            tx.send(SignalBatch::Logs(make_test_logs_batch()))
                .await
                .unwrap();
        }
        drop(tx);

        let result = sink.run(rx).await;
        assert!(result.is_ok(), "Multi-batch dry_run should succeed");

        // After run completes, all buffers should have been flushed
        for buffer in sink.buffers.values() {
            assert!(
                buffer.batches.is_empty(),
                "Buffer should be empty after final flush"
            );
            assert_eq!(
                buffer.total_rows, 0,
                "total_rows should be zero after flush"
            );
        }
    }
}
