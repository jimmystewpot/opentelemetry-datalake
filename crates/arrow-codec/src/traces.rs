use arrow::array::{Int32Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
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

/// Decodes OTLP Trace requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
#[allow(clippy::cast_possible_wrap)]
pub fn decode_traces(req: &ExportTraceServiceRequest) -> Result<RecordBatch, PipelineError> {
    let mut total_records = 0;
    for r_span in &req.resource_spans {
        for s_span in &r_span.scope_spans {
            total_records += s_span.spans.len();
        }
    }

    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();
    let mut trace_state_builder = StringBuilder::new();
    let mut parent_span_id_builder = StringBuilder::new();
    let mut name_builder = StringBuilder::new();
    let mut kind_builder = Int32Builder::with_capacity(total_records);
    let mut start_time_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut end_time_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut attributes_builder = StringBuilder::new();
    let mut resource_attributes_builder = StringBuilder::new();
    let mut scope_name_builder = StringBuilder::new();
    let mut scope_version_builder = StringBuilder::new();
    let mut status_code_builder = Int32Builder::with_capacity(total_records);
    let mut status_message_builder = StringBuilder::new();

    for r_span in &req.resource_spans {
        let resource_attrs_json = if let Some(ref res) = r_span.resource {
            convert_attributes(&res.attributes)
        } else {
            "{}".to_string()
        };

        for s_span in &r_span.scope_spans {
            let (scope_name, scope_version) = if let Some(ref scope) = s_span.scope {
                (scope.name.clone(), scope.version.clone())
            } else {
                (String::new(), String::new())
            };

            for span in &s_span.spans {
                trace_id_builder.append_value(to_hex_string(&span.trace_id));
                span_id_builder.append_value(to_hex_string(&span.span_id));
                trace_state_builder.append_value(&span.trace_state);
                parent_span_id_builder.append_value(to_hex_string(&span.parent_span_id));
                name_builder.append_value(&span.name);
                kind_builder.append_value(span.kind);
                start_time_builder.append_value(span.start_time_unix_nano as i64);
                end_time_builder.append_value(span.end_time_unix_nano as i64);

                let span_attrs_json = convert_attributes(&span.attributes);
                attributes_builder.append_value(&span_attrs_json);

                resource_attributes_builder.append_value(&resource_attrs_json);
                scope_name_builder.append_value(&scope_name);
                scope_version_builder.append_value(&scope_version);

                if let Some(ref status) = span.status {
                    status_code_builder.append_value(status.code);
                    status_message_builder.append_value(&status.message);
                } else {
                    status_code_builder.append_value(0);
                    status_message_builder.append_value("");
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(trace_id_builder.finish()),
            Arc::new(span_id_builder.finish()),
            Arc::new(trace_state_builder.finish()),
            Arc::new(parent_span_id_builder.finish()),
            Arc::new(name_builder.finish()),
            Arc::new(kind_builder.finish()),
            Arc::new(start_time_builder.finish()),
            Arc::new(end_time_builder.finish()),
            Arc::new(attributes_builder.finish()),
            Arc::new(resource_attributes_builder.finish()),
            Arc::new(scope_name_builder.finish()),
            Arc::new(scope_version_builder.finish()),
            Arc::new(status_code_builder.finish()),
            Arc::new(status_message_builder.finish()),
        ],
    )
    .map_err(|e| PipelineError::Internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    #[test]
    fn test_decode_traces_empty() {
        let req = ExportTraceServiceRequest::default();
        let batch = decode_traces(&req).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_decode_traces_non_empty() {
        let mut req = ExportTraceServiceRequest::default();
        let mut r_span = ResourceSpans::default();
        r_span.resource = Some(Resource {
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

        let mut s_span = ScopeSpans::default();
        let mut span = Span::default();
        span.trace_id = vec![1; 16];
        span.span_id = vec![2; 8];
        span.name = "test-span".to_string();
        span.kind = 1;
        span.start_time_unix_nano = 1_000_000_000;
        span.end_time_unix_nano = 2_000_000_000;
        s_span.spans.push(span);
        r_span.scope_spans.push(s_span);
        req.resource_spans.push(r_span);

        let batch = decode_traces(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 14);
    }
}
