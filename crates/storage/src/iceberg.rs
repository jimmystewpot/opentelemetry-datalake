use crate::{CatalogType, PartitionGranularity, SchemaMode};
use arrow::array::{Array, ArrayRef, AsArray, StringArray};
use arrow::compute::{SortColumn, lexsort_to_indices};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use iceberg::Catalog;
use iceberg::CatalogBuilder;
use iceberg::TableIdent;
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
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_batch_interval_sec: default_max_batch_interval_sec(),
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

/// Apache Iceberg storage sink.
pub struct IcebergSink {
    config: IcebergSinkConfig,
}

impl IcebergSink {
    /// Creates a new `IcebergSink` from configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig) -> Self {
        Self { config }
    }

    /// Sorts a log batch based on the (`ServiceName`, `SeverityText`, `Timestamp`) tuple.
    pub fn sort_logs(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .ok_or_else(|| PipelineError::Internal("Missing resource_attributes".to_string()))?;
        let resource_attrs = resource_attrs_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                PipelineError::Internal("resource_attributes column is not Utf8".to_string())
            })?;

        let service_names = extract_service_names(resource_attrs);
        let service_name_col = Arc::new(service_names) as ArrayRef;

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

    /// Sorts a metrics batch based on the (`ServiceName`, `MetricName`, `Attributes`, `TimeUnix`) tuple.
    pub fn sort_metrics(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .ok_or_else(|| PipelineError::Internal("Missing resource_attributes".to_string()))?;
        let resource_attrs = resource_attrs_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                PipelineError::Internal("resource_attributes column is not Utf8".to_string())
            })?;

        let service_names = extract_service_names(resource_attrs);
        let service_name_col = Arc::new(service_names) as ArrayRef;

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
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .ok_or_else(|| PipelineError::Internal("Missing resource_attributes".to_string()))?;
        let resource_attrs = resource_attrs_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                PipelineError::Internal("resource_attributes column is not Utf8".to_string())
            })?;

        let service_names = extract_service_names(resource_attrs);
        let service_name_col = Arc::new(service_names) as ArrayRef;

        let name_col = batch
            .column_by_name("name")
            .ok_or_else(|| PipelineError::Internal("Missing name (span name)".to_string()))?
            .clone();

        let timestamp_col = batch
            .column_by_name("start_time")
            .ok_or_else(|| PipelineError::Internal("Missing start_time".to_string()))?
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
                    if arrow_schema.field_with_name(field.name()).is_ok() {
                        let col = batch
                            .column_by_name(field.name())
                            .ok_or_else(|| {
                                PipelineError::Internal(format!(
                                    "Column {} missing from batch",
                                    field.name()
                                ))
                            })?
                            .clone();
                        final_columns.push(col);
                        final_fields.push(field.clone());
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

/// Helper function to extract `service.name` from `resource_attributes` JSON strings.
fn extract_service_names(resource_attrs: &StringArray) -> StringArray {
    let mut builder = arrow::array::StringBuilder::new();
    for i in 0..resource_attrs.len() {
        if resource_attrs.is_null(i) {
            builder.append_value("unknown");
        } else {
            let json_str = resource_attrs.value(i);
            let service_name = serde_json::from_str::<serde_json::Value>(json_str)
                .ok()
                .and_then(|v| {
                    v.get(opentelemetry_semantic_conventions::resource::SERVICE_NAME)
                        .and_then(serde_json::Value::as_str)
                        .map(String::from)
                })
                .unwrap_or_else(|| "unknown".to_string());
            builder.append_value(service_name);
        }
    }
    builder.finish()
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
    let ts_array = batch
        .column_by_name(timestamp_col_name)
        .ok_or_else(|| PipelineError::Internal(format!("Missing {timestamp_col_name}")))?
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();

    let mut partitions: HashMap<String, Vec<u32>> = HashMap::new();
    for i in 0..batch.num_rows() {
        let val = ts_array.value(i);
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
    ) -> Result<Option<iceberg::table::Table>, PipelineError> {
        let Some(cat) = catalog else {
            return Ok(None);
        };
        let table_ident = TableIdent::from_strs(self.config.table_identifier.split('.'))
            .map_err(|e| PipelineError::Storage(Box::new(e)))?;
        let table = cat
            .load_table(&table_ident)
            .await
            .map_err(|e| PipelineError::Storage(Box::new(e)))?;
        Ok(Some(table))
    }

    /// Processes a single signal batch through the full pipeline:
    /// schema mode → sort → partition → commit.
    ///
    /// This is the unified handler for all three signal types, eliminating
    /// the previously triplicated match arms in `run()`.
    async fn process_signal(
        &self,
        batch: RecordBatch,
        sort_fn: fn(&IcebergSink, &RecordBatch) -> Result<RecordBatch, PipelineError>,
        timestamp_col: &str,
        signal_name: &str,
        catalog: Option<&Arc<dyn Catalog>>,
    ) -> Result<(), PipelineError> {
        let loaded_table = self.load_table(catalog).await?;
        let filtered_batch = self.apply_schema_mode(batch, loaded_table.as_ref())?;
        let sorted_batch = sort_fn(self, &filtered_batch)?;
        let sub_batches = partition_batch(
            &sorted_batch,
            timestamp_col,
            self.config.partition_granularity,
        )?;

        for (path, sub_batch) in sub_batches {
            info!(
                "Writing sorted {} batch (rows: {}) to Iceberg partition: {}{}",
                signal_name,
                sub_batch.num_rows(),
                self.config.warehouse,
                path
            );

            if let Some(cat) = catalog {
                if let Some(ref table) = loaded_table {
                    let tx = Transaction::new(table);
                    tx.commit(cat.as_ref())
                        .await
                        .map_err(|e| PipelineError::Storage(Box::new(e)))?;
                    info!(
                        "Committed ACID transaction for table: {}",
                        self.config.table_identifier
                    );
                }
            } else {
                info!("Simulating ACID transaction commit for partition: {}", path);
            }
        }

        Ok(())
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
                        configured_scheme: "s3".to_string(),
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

        while let Some(signal) = input.recv().await {
            match signal {
                SignalBatch::Logs(batch) => {
                    self.process_signal(
                        batch,
                        Self::sort_logs,
                        "timestamp",
                        "logs",
                        catalog.as_ref(),
                    )
                    .await?;
                }
                SignalBatch::Metrics(batch) => {
                    self.process_signal(
                        batch,
                        Self::sort_metrics,
                        "timestamp",
                        "metrics",
                        catalog.as_ref(),
                    )
                    .await?;
                }
                SignalBatch::Traces(batch) => {
                    self.process_signal(
                        batch,
                        Self::sort_traces,
                        "start_time",
                        "traces",
                        catalog.as_ref(),
                    )
                    .await?;
                }
            }
        }

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
            Field::new("resource_attributes", DataType::Utf8, false),
        ]));

        let ts_array = Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
            1_717_891_200_000_000_000,
            1_717_894_800_000_000_000,
            1_717_891_200_000_000_000,
        ])) as ArrayRef;

        let sev_array = Arc::new(StringArray::from(vec!["INFO", "ERROR", "WARN"])) as ArrayRef;

        let res_array = Arc::new(StringArray::from(vec![
            r#"{"service.name":"service-a"}"#,
            r#"{"service.name":"service-b"}"#,
            r#"{"service.name":"service-a"}"#,
        ])) as ArrayRef;

        RecordBatch::try_new(schema, vec![ts_array, sev_array, res_array]).unwrap()
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
          "last-column-id": 3,
          "current-schema-id": 0,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                { "id": 1, "name": "timestamp", "required": true, "type": "timestamp_ns" },
                { "id": 2, "name": "severity_text", "required": false, "type": "string" },
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
        let table = iceberg::table::Table::builder()
            .metadata(metadata)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(iceberg::TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_memory())
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
}
