use arrow::array::{Int32Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

use crate::common::{convert_attributes, timestamp_to_i64, to_hex_string};

/// Decodes OTLP Trace requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
#[allow(clippy::too_many_lines)]
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
    let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut end_time_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut attributes_builder = StringBuilder::new();
    let mut service_name_builder = StringBuilder::new();
    let mut resource_attributes_builder = StringBuilder::new();
    let mut scope_name_builder = StringBuilder::new();
    let mut scope_version_builder = StringBuilder::new();
    let mut status_code_builder = Int32Builder::with_capacity(total_records);
    let mut status_message_builder = StringBuilder::new();

    for r_span in &req.resource_spans {
        let (resource_attrs_json, service_name) = if let Some(ref res) = r_span.resource {
            let service_name = res
                .attributes
                .iter()
                .find(|kv| kv.key == opentelemetry_semantic_conventions::resource::SERVICE_NAME)
                .and_then(|kv| kv.value.as_ref())
                .map_or_else(|| "unknown".to_string(), crate::common::any_value_to_string);
            (convert_attributes(&res.attributes), service_name)
        } else {
            ("{}".to_string(), "unknown".to_string())
        };

        for s_span in &r_span.scope_spans {
            let (scope_name, scope_version) = if let Some(ref scope) = s_span.scope {
                (scope.name.as_str(), scope.version.as_str())
            } else {
                ("", "")
            };

            for span in &s_span.spans {
                trace_id_builder.append_value(to_hex_string(&span.trace_id));
                span_id_builder.append_value(to_hex_string(&span.span_id));
                trace_state_builder.append_value(&span.trace_state);
                parent_span_id_builder.append_value(to_hex_string(&span.parent_span_id));
                name_builder.append_value(span.name.as_str());
                kind_builder.append_value(span.kind);
                timestamp_builder.append_value(timestamp_to_i64(span.start_time_unix_nano)?);
                end_time_builder.append_value(timestamp_to_i64(span.end_time_unix_nano)?);

                let span_attrs_json = convert_attributes(&span.attributes);
                attributes_builder.append_value(&span_attrs_json);

                service_name_builder.append_value(&service_name);
                resource_attributes_builder.append_value(&resource_attrs_json);
                scope_name_builder.append_value(scope_name);
                scope_version_builder.append_value(scope_version);

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
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
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
            Arc::new(timestamp_builder.finish()),
            Arc::new(end_time_builder.finish()),
            Arc::new(attributes_builder.finish()),
            Arc::new(service_name_builder.finish()),
            Arc::new(resource_attributes_builder.finish()),
            Arc::new(scope_name_builder.finish()),
            Arc::new(scope_version_builder.finish()),
            Arc::new(status_code_builder.finish()),
            Arc::new(status_message_builder.finish()),
        ],
    )
    .map_err(PipelineError::Arrow)
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
        let r_span = ResourceSpans {
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
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "test-span".to_string(),
                    kind: 1,
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let req = ExportTraceServiceRequest {
            resource_spans: vec![r_span],
        };

        let batch = decode_traces(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 15);
    }
}
