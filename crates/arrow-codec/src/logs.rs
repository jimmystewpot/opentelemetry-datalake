use arrow::array::{Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

use crate::common::{any_value_to_string, convert_attributes, timestamp_to_i64, to_hex_string};

/// Decodes OTLP Logs requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
pub fn decode_logs(req: &ExportLogsServiceRequest) -> Result<RecordBatch, PipelineError> {
    let mut total_records = 0;
    for r_log in &req.resource_logs {
        for s_log in &r_log.scope_logs {
            total_records += s_log.log_records.len();
        }
    }

    let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut observed_timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut severity_num_builder = Int32Builder::with_capacity(total_records);
    let mut severity_text_builder = StringBuilder::new();
    let mut body_builder = StringBuilder::new();
    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();
    let mut flags_builder = UInt32Builder::with_capacity(total_records);
    let mut attributes_builder = StringBuilder::new();
    let mut resource_attributes_builder = StringBuilder::new();
    let mut scope_name_builder = StringBuilder::new();
    let mut scope_version_builder = StringBuilder::new();

    for r_log in &req.resource_logs {
        let resource_attrs_json = if let Some(ref res) = r_log.resource {
            convert_attributes(&res.attributes)
        } else {
            "{}".to_string()
        };

        for s_log in &r_log.scope_logs {
            let (scope_name, scope_version) = if let Some(ref scope) = s_log.scope {
                (scope.name.as_str(), scope.version.as_str())
            } else {
                ("", "")
            };

            for log in &s_log.log_records {
                timestamp_builder.append_value(timestamp_to_i64(log.time_unix_nano)?);
                observed_timestamp_builder
                    .append_value(timestamp_to_i64(log.observed_time_unix_nano)?);
                severity_num_builder.append_value(log.severity_number);
                severity_text_builder.append_value(&log.severity_text);

                let body_str = log
                    .body
                    .as_ref()
                    .map(any_value_to_string)
                    .unwrap_or_default();
                body_builder.append_value(&body_str);

                trace_id_builder.append_value(to_hex_string(&log.trace_id));
                span_id_builder.append_value(to_hex_string(&log.span_id));
                flags_builder.append_value(log.flags);

                let log_attrs_json = convert_attributes(&log.attributes);
                attributes_builder.append_value(&log_attrs_json);

                resource_attributes_builder.append_value(&resource_attrs_json);
                scope_name_builder.append_value(scope_name);
                scope_version_builder.append_value(scope_version);
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_builder.finish()),
            Arc::new(observed_timestamp_builder.finish()),
            Arc::new(severity_num_builder.finish()),
            Arc::new(severity_text_builder.finish()),
            Arc::new(body_builder.finish()),
            Arc::new(trace_id_builder.finish()),
            Arc::new(span_id_builder.finish()),
            Arc::new(flags_builder.finish()),
            Arc::new(attributes_builder.finish()),
            Arc::new(resource_attributes_builder.finish()),
            Arc::new(scope_name_builder.finish()),
            Arc::new(scope_version_builder.finish()),
        ],
    )
    .map_err(PipelineError::Arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_decode_logs_empty() {
        let req = ExportLogsServiceRequest::default();
        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_decode_logs_non_empty() {
        let r_log = ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                    ..Default::default()
                }],
                dropped_attributes_count: 0,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000_000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test log body".to_string())),
                    }),
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let req = ExportLogsServiceRequest {
            resource_logs: vec![r_log],
        };

        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 12);
    }
}
