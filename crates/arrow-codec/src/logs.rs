use arrow::array::{Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

use crate::common::{
    LOGS_SCHEMA, append_any_value_to_builder, append_hex_to_builder, extract_service_name,
    timestamp_to_i64, write_attributes_json,
};

/// Decodes OTLP Logs requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
#[allow(clippy::too_many_lines)]
pub fn decode_logs(req: &ExportLogsServiceRequest) -> Result<RecordBatch, PipelineError> {
    let mut total_records: usize = 0;
    for r_log in &req.resource_logs {
        for s_log in &r_log.scope_logs {
            total_records = total_records.saturating_add(s_log.log_records.len());
        }
    }

    let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut observed_timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut severity_num_builder = Int32Builder::with_capacity(total_records);
    let mut severity_text_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(8));
    let mut body_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(64));
    let mut trace_id_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(32));
    let mut span_id_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(16));
    let mut flags_builder = UInt32Builder::with_capacity(total_records);
    let mut attributes_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(128));
    let mut service_name_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(24));
    let mut resource_attributes_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(128));
    let mut scope_name_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(24));
    let mut scope_version_builder =
        StringBuilder::with_capacity(total_records, total_records.saturating_mul(16));

    let mut resource_attrs_json = String::with_capacity(256);
    let mut scratch = String::with_capacity(256);

    for r_log in &req.resource_logs {
        let service_name = if let Some(ref res) = r_log.resource {
            write_attributes_json(&mut resource_attrs_json, &res.attributes);
            extract_service_name(&res.attributes)
        } else {
            resource_attrs_json.clear();
            resource_attrs_json.push_str("{}");
            std::borrow::Cow::Borrowed("unknown")
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

                append_any_value_to_builder(&mut body_builder, &mut scratch, log.body.as_ref());

                append_hex_to_builder(&mut trace_id_builder, &log.trace_id);
                append_hex_to_builder(&mut span_id_builder, &log.span_id);
                flags_builder.append_value(log.flags);

                write_attributes_json(&mut scratch, &log.attributes);
                attributes_builder.append_value(&scratch);

                service_name_builder.append_value(service_name.as_ref());
                resource_attributes_builder.append_value(&resource_attrs_json);
                scope_name_builder.append_value(scope_name);
                scope_version_builder.append_value(scope_version);
            }
        }
    }

    RecordBatch::try_new(
        Arc::clone(&*LOGS_SCHEMA),
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
            Arc::new(service_name_builder.finish()),
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
        assert_eq!(schema.fields().len(), 13);
    }

    /// Two `resource_logs` groups each containing one log must yield 2 rows.
    #[test]
    fn test_decode_logs_multiple_resource_logs() {
        let make_resource_log = |service: &str| ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000_000_000,
                    severity_number: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let req = ExportLogsServiceRequest {
            resource_logs: vec![make_resource_log("svc-a"), make_resource_log("svc-b")],
        };
        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    /// When resource is absent, `service_name` must fall back to "unknown".
    #[test]
    fn test_decode_logs_missing_resource_uses_unknown() {
        use arrow::array::AsArray;
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let svc = batch
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(svc, "unknown");
    }

    /// A log record without a body must result in an empty string body column.
    #[test]
    fn test_decode_logs_missing_body_is_empty_string() {
        use arrow::array::AsArray;
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        body: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let batch = decode_logs(&req).unwrap();
        let body = batch
            .column_by_name("body")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(body, "", "Missing log body must produce an empty string");
    }

    /// Two `scope_logs` under the same resource must both contribute rows,
    /// and their `scope_name` values must be recorded correctly.
    #[test]
    fn test_decode_logs_multiple_scopes() {
        use arrow::array::AsArray;
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "scope-a".to_string(),
                            ..Default::default()
                        }),
                        log_records: vec![LogRecord {
                            time_unix_nano: 1_000,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: "scope-b".to_string(),
                            ..Default::default()
                        }),
                        log_records: vec![LogRecord {
                            time_unix_nano: 2_000,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        };
        let batch = decode_logs(&req).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let scope_col = batch
            .column_by_name("scope_name")
            .unwrap()
            .as_string::<i32>();
        let names: std::collections::HashSet<&str> = (0..arrow::array::Array::len(scope_col))
            .map(|i| scope_col.value(i))
            .collect();
        assert!(names.contains("scope-a"));
        assert!(names.contains("scope-b"));
    }

    /// A log record with a timestamp exceeding `i64::MAX` must propagate an error.
    #[test]
    fn test_decode_logs_timestamp_overflow_returns_error() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        // u64::MAX far exceeds i64::MAX
                        time_unix_nano: u64::MAX,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let result = decode_logs(&req);
        assert!(
            result.is_err(),
            "Timestamp overflow must return an error, not silently wrap"
        );
    }

    #[test]
    fn test_decode_logs_static_schema_identity_and_resource_reset() {
        use arrow::array::AsArray;
        let b1 = decode_logs(&ExportLogsServiceRequest::default()).unwrap();
        let b2 = decode_logs(&ExportLogsServiceRequest::default()).unwrap();
        assert!(Arc::ptr_eq(&b1.schema(), &b2.schema()));

        let req = ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: opentelemetry_semantic_conventions::resource::SERVICE_NAME
                                .to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("svc-1".to_string())),
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: 100,
                            attributes: vec![KeyValue {
                                key: "status".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(200)),
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: None,
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: 200,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };
        let batch = decode_logs(&req).unwrap();
        let attrs_col = batch
            .column_by_name("attributes")
            .unwrap()
            .as_string::<i32>();
        let res_attrs_col = batch
            .column_by_name("resource_attributes")
            .unwrap()
            .as_string::<i32>();
        let svc_col = batch
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>();

        assert_eq!(attrs_col.value(0), r#"{"status":200}"#);
        assert_eq!(attrs_col.value(1), "{}");
        assert_eq!(res_attrs_col.value(0), r#"{"service.name":"svc-1"}"#);
        assert_eq!(res_attrs_col.value(1), "{}");
        assert_eq!(svc_col.value(0), "svc-1");
        assert_eq!(svc_col.value(1), "unknown");
    }
}
