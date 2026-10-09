//! Signal routing and schema preparation for Parquet streaming sink.
//!
//! Routes incoming OpenTelemetry [`SignalBatch`] payloads (Logs, Metrics, Traces),
//! identifies candidate semi-structured columns, and optionally transforms them into
//! Apache Parquet / Spark Variant binary `StructArray` columns.

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use pipeline_core::pipeline::SignalBatch;
use smallvec::SmallVec;

use crate::error::ParquetSinkError;
use crate::variant::VariantTransformer;

static LOG_CANDIDATE_COLUMNS: [&str; 3] = ["attributes", "resource_attributes", "body"];
static TRACE_CANDIDATE_COLUMNS: [&str; 2] = ["attributes", "resource_attributes"];
static METRIC_CANDIDATE_COLUMNS: [&str; 3] = ["attributes", "resource_attributes", "datapoints"];

/// A record batch prepared and annotated with its signal type for partitioning and storage.
#[derive(Debug, Clone)]
pub struct PreparedBatch {
    /// Telemetry signal identifier (e.g. `"logs"`, `"metrics"`, `"traces"`).
    pub signal: &'static str,
    /// Arrow record batch, optionally transformed with Variant columns.
    pub batch: RecordBatch,
}

/// Routes incoming telemetry signals and prepares Arrow schemas for Parquet storage.
///
/// When Variant encoding is enabled, semi-structured columns (`attributes`, `resource_attributes`,
/// `body`, and `datapoints`) are converted into Parquet / Spark Variant binary `StructArray` columns.
/// Concurrency characteristics: `SignalRouter` is thread-safe (`Send + Sync`) and stateless,
/// designed for concurrent execution across worker threads.
#[derive(Debug, Clone, Default)]
pub struct SignalRouter {
    variant_enabled: bool,
    transformer: VariantTransformer,
}

impl SignalRouter {
    /// Creates a new `SignalRouter`.
    ///
    /// # Arguments
    /// * `variant_enabled` - When `true`, transforms JSON attribute/datapoint columns into Variant structs.
    #[must_use]
    pub fn new(variant_enabled: bool) -> Self {
        Self {
            variant_enabled,
            transformer: VariantTransformer::new(),
        }
    }

    /// Returns whether Variant format encoding is enabled.
    #[must_use]
    pub fn is_variant_enabled(&self) -> bool {
        self.variant_enabled
    }

    /// Routes an incoming [`SignalBatch`] and applies schema transformations.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if transforming candidate columns into Variant structs fails.
    pub fn route_and_prepare(&self, batch: SignalBatch) -> Result<PreparedBatch, ParquetSinkError> {
        match batch {
            SignalBatch::Logs(record_batch) => {
                self.prepare_batch("logs", record_batch, &LOG_CANDIDATE_COLUMNS)
            }
            SignalBatch::Traces(record_batch) => {
                self.prepare_batch("traces", record_batch, &TRACE_CANDIDATE_COLUMNS)
            }
            SignalBatch::Metrics(record_batch) => {
                self.prepare_batch("metrics", record_batch, &METRIC_CANDIDATE_COLUMNS)
            }
        }
    }

    fn prepare_batch(
        &self,
        signal: &'static str,
        batch: RecordBatch,
        candidate_columns: &[&'static str],
    ) -> Result<PreparedBatch, ParquetSinkError> {
        if !self.variant_enabled {
            return Ok(PreparedBatch { signal, batch });
        }

        let schema = batch.schema();
        let mut columns: SmallVec<[&str; 4]> = SmallVec::new();

        for &col in candidate_columns {
            if schema.field_with_name(col).is_ok_and(|field| {
                matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
            }) {
                columns.push(col);
            }
        }

        let transformed_batch = if columns.is_empty() {
            batch
        } else {
            self.transformer
                .transform_to_variant(&batch, columns.as_slice())?
        };

        Ok(PreparedBatch {
            signal,
            batch: transformed_batch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use pipeline_core::pipeline::SignalBatch;
    use std::sync::Arc;

    #[test]
    fn test_route_logs_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(true); // variant enabled
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("routing logs should succeed");
        assert_eq!(prepared.signal, "logs");
        assert!(matches!(
            prepared
                .batch
                .schema()
                .field_with_name("attributes")
                .expect("field exists")
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_route_logs_variant_disabled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(false); // variant disabled
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("routing logs should succeed");
        assert_eq!(prepared.signal, "logs");
        assert_eq!(
            prepared
                .batch
                .schema()
                .field_with_name("attributes")
                .expect("field exists")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_route_logs_with_body_utf8() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"app":"web"}"#)])),
                Arc::new(StringArray::from(vec![Some(r#"{"env":"prod"}"#)])),
                Arc::new(StringArray::from(vec![Some(r#"{"message":"started"}"#)])),
                Arc::new(StringArray::from(vec!["INFO"])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("route succeeds");
        assert_eq!(prepared.signal, "logs");

        let out_schema = prepared.batch.schema();
        assert!(matches!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            out_schema
                .field_with_name("resource_attributes")
                .expect("resource_attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            out_schema
                .field_with_name("body")
                .expect("body exists")
                .data_type(),
            DataType::Struct(_)
        ));
        // Untouched column remains Utf8
        assert_eq!(
            out_schema
                .field_with_name("severity_text")
                .expect("severity_text exists")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_route_logs_with_body_binary_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Utf8, true),
            Field::new("body", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"app":"web"}"#)])),
                Arc::new(BinaryArray::from(vec![Some(b"binary payload".as_slice())])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("route succeeds");
        assert_eq!(prepared.signal, "logs");

        let out_schema = prepared.batch.schema();
        assert!(matches!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        // Non-string body is skipped gracefully
        assert_eq!(
            out_schema
                .field_with_name("body")
                .expect("body exists")
                .data_type(),
            &DataType::Binary
        );
    }

    #[test]
    fn test_route_logs_missing_columns_graceful() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("severity_number", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                    1_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![9])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("route succeeds even when candidate columns are absent");
        assert_eq!(prepared.signal, "logs");
        assert_eq!(prepared.batch.num_columns(), 2);
    }

    #[test]
    fn test_route_traces_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["trace-01"])),
                Arc::new(StringArray::from(vec!["span-01"])),
                Arc::new(StringArray::from(vec![Some(r#"{"http.status":200}"#)])),
                Arc::new(StringArray::from(vec![Some(r#"{"service":"api"}"#)])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Traces(batch))
            .expect("route traces succeeds");
        assert_eq!(prepared.signal, "traces");

        let out_schema = prepared.batch.schema();
        assert_eq!(
            out_schema
                .field_with_name("trace_id")
                .expect("trace_id exists")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            out_schema
                .field_with_name("span_id")
                .expect("span_id exists")
                .data_type(),
            &DataType::Utf8
        );
        assert!(matches!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            out_schema
                .field_with_name("resource_attributes")
                .expect("resource_attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_route_traces_variant_disabled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(false);
        let prepared = router
            .route_and_prepare(SignalBatch::Traces(batch))
            .expect("route traces succeeds");
        assert_eq!(prepared.signal, "traces");
        assert_eq!(
            prepared
                .batch
                .schema()
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_route_metrics_polymorphic_datapoints() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("datapoints", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "cpu_usage",
                    "request_count",
                    "latency_hist",
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"host":"node-1"}"#),
                    Some(r#"{"endpoint":"/health"}"#),
                    Some(r#"{"method":"GET"}"#),
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"cluster":"us-east"}"#),
                    Some(r#"{"cluster":"us-east"}"#),
                    Some(r#"{"cluster":"us-east"}"#),
                ])),
                // Polymorphic datapoints across 3 different metric types:
                Arc::new(StringArray::from(vec![
                    Some(r#"{"type":"gauge","as_double":42.5}"#),
                    Some(r#"{"type":"sum","as_int":1000}"#),
                    Some(r#"{"type":"histogram","bucket_counts":[10,20,5],"explicit_bounds":[0.1,0.5]}"#),
                ])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Metrics(batch))
            .expect("route metrics succeeds");
        assert_eq!(prepared.signal, "metrics");

        let out_schema = prepared.batch.schema();
        assert_eq!(
            out_schema
                .field_with_name("name")
                .expect("name exists")
                .data_type(),
            &DataType::Utf8
        );
        assert!(matches!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            out_schema
                .field_with_name("resource_attributes")
                .expect("resource_attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            out_schema
                .field_with_name("datapoints")
                .expect("datapoints exists")
                .data_type(),
            DataType::Struct(_)
        ));

        // Verify that the datapoints column has 3 rows and valid Variant struct fields
        let dp_col = prepared
            .batch
            .column_by_name("datapoints")
            .expect("datapoints column exists");
        assert_eq!(dp_col.len(), 3);
        let struct_col = dp_col
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("downcasts to StructArray");
        assert_eq!(struct_col.num_columns(), 2);
    }

    #[test]
    fn test_route_metrics_without_datapoints() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Metrics(batch))
            .expect("route metrics succeeds without datapoints column");
        assert_eq!(prepared.signal, "metrics");
        assert!(matches!(
            prepared
                .batch
                .schema()
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_route_metrics_variant_disabled() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("datapoints", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let router = SignalRouter::new(false);
        let prepared = router
            .route_and_prepare(SignalBatch::Metrics(batch))
            .expect("route metrics succeeds");
        assert_eq!(prepared.signal, "metrics");
        assert_eq!(
            prepared
                .batch
                .schema()
                .field_with_name("datapoints")
                .expect("datapoints exists")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_route_logs_non_string_attributes_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Int64, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec![Some(r#"{"cluster":"us-west"}"#)])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Logs(batch))
            .expect("route logs succeeds");
        assert_eq!(prepared.signal, "logs");

        let out_schema = prepared.batch.schema();
        // Non-string attributes column is gracefully skipped and left unchanged
        assert_eq!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            &DataType::Int64
        );
        // Utf8 resource_attributes is converted to Variant Struct
        assert!(matches!(
            out_schema
                .field_with_name("resource_attributes")
                .expect("resource_attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_route_traces_non_string_candidate_columns_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Binary, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![Some(b"binary-attrs".as_slice())])),
                Arc::new(StringArray::from(vec![Some(r#"{"service":"billing"}"#)])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Traces(batch))
            .expect("route traces succeeds");
        assert_eq!(prepared.signal, "traces");

        let out_schema = prepared.batch.schema();
        assert_eq!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            &DataType::Binary
        );
        assert!(matches!(
            out_schema
                .field_with_name("resource_attributes")
                .expect("resource_attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_route_metrics_non_string_datapoints_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("datapoints", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["cpu"])),
                Arc::new(StringArray::from(vec![Some(r#"{"host":"node-1"}"#)])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .expect("batch creation succeeds");

        let router = SignalRouter::new(true);
        let prepared = router
            .route_and_prepare(SignalBatch::Metrics(batch))
            .expect("route metrics succeeds");
        assert_eq!(prepared.signal, "metrics");

        let out_schema = prepared.batch.schema();
        assert!(matches!(
            out_schema
                .field_with_name("attributes")
                .expect("attributes exists")
                .data_type(),
            DataType::Struct(_)
        ));
        // Non-string datapoints column is gracefully skipped and left unchanged
        assert_eq!(
            out_schema
                .field_with_name("datapoints")
                .expect("datapoints exists")
                .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn test_is_variant_enabled() {
        let router_enabled = SignalRouter::new(true);
        assert!(router_enabled.is_variant_enabled());

        let router_disabled = SignalRouter::new(false);
        assert!(!router_disabled.is_variant_enabled());

        let default_router = SignalRouter::default();
        assert!(!default_router.is_variant_enabled());
    }
}
