use arrow::array::{Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use pipeline_core::error::PipelineError;
use std::fmt::Write;
use std::sync::Arc;

fn to_hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

fn convert_attributes(attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue]) -> String {
    let mut map = std::collections::HashMap::new();
    for attr in attrs {
        if let Some(ref val) = attr.value {
            let val_str = match &val.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    s.clone()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                    i.to_string()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                    d.to_string()
                }
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                    b.to_string()
                }
                _ => "Unsupported".to_string(),
            };
            map.insert(attr.key.clone(), val_str);
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

fn any_value_to_string(val: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    match &val.value {
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
            i.to_string()
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
            d.to_string()
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
            b.to_string()
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kvlist)) => {
            convert_attributes(&kvlist.values)
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(arr)) => {
            let elements: Vec<String> = arr.values.iter().map(any_value_to_string).collect();
            format!("[{}]", elements.join(","))
        }
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(bytes)) => {
            to_hex_string(bytes)
        }
        _ => String::new(),
    }
}

/// Decodes OTLP Logs requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
#[allow(clippy::cast_possible_wrap)]
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
                (scope.name.clone(), scope.version.clone())
            } else {
                (String::new(), String::new())
            };

            for log in &s_log.log_records {
                timestamp_builder.append_value(log.time_unix_nano as i64);
                observed_timestamp_builder.append_value(log.observed_time_unix_nano as i64);
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
                scope_name_builder.append_value(&scope_name);
                scope_version_builder.append_value(&scope_version);
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
    .map_err(|e| PipelineError::Internal(e.to_string()))
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
        let mut req = ExportLogsServiceRequest::default();
        let mut r_log = ResourceLogs::default();
        r_log.resource = Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test-service".to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        });

        let mut s_log = ScopeLogs::default();
        let mut record = LogRecord::default();
        record.time_unix_nano = 1_000_000_000;
        record.severity_number = 9;
        record.severity_text = "INFO".to_string();
        record.body = Some(AnyValue {
            value: Some(any_value::Value::StringValue("test log body".to_string())),
        });
        record.trace_id = vec![1; 16];
        record.span_id = vec![2; 8];
        s_log.log_records.push(record);
        r_log.scope_logs.push(s_log);
        req.resource_logs.push(r_log);

        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 12);
    }
}
