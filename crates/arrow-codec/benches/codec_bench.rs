use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_codec::compliance::{ComplianceEngine, ComplianceMode};
use arrow_codec::{decode_logs, decode_metrics, decode_traces};
use criterion::{Criterion, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use std::collections::HashMap;
use std::hint::black_box as bbox;
use std::sync::Arc;

fn make_populated_logs_request(n: usize) -> ExportLogsServiceRequest {
    let mut log_records = Vec::with_capacity(n);
    for i in 0..n {
        log_records.push(LogRecord {
            time_unix_nano: 1_717_891_200_000_000_000 + (i as u64 * 100_000_000),
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(format!("test log body {i}"))),
            }),
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            attributes: vec![KeyValue {
                key: "http.status".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(200)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
    }

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
            log_records,
            ..Default::default()
        }],
        ..Default::default()
    };

    ExportLogsServiceRequest {
        resource_logs: vec![r_log],
    }
}

fn make_populated_traces_request(n: usize) -> ExportTraceServiceRequest {
    let mut spans = Vec::with_capacity(n);
    for i in 0..n {
        spans.push(Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            name: format!("span-{i}"),
            kind: 1,
            start_time_unix_nano: 1_717_891_200_000_000_000 + (i as u64 * 100_000_000),
            end_time_unix_nano: 1_717_891_200_000_000_000 + (i as u64 * 100_000_000) + 50_000,
            attributes: vec![KeyValue {
                key: "http.status".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(200)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
    }

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
            spans,
            ..Default::default()
        }],
        ..Default::default()
    };

    ExportTraceServiceRequest {
        resource_spans: vec![r_span],
    }
}

fn make_populated_metrics_request(n: usize) -> ExportMetricsServiceRequest {
    let mut data_points = Vec::with_capacity(n);
    for i in 0..n {
        data_points.push(NumberDataPoint {
            time_unix_nano: 1_717_891_200_000_000_000 + (i as u64 * 100_000_000),
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                    42.5 + i as f64,
                ),
            ),
            attributes: vec![KeyValue {
                key: "http.status".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(200)),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
    }

    let r_metric = ResourceMetrics {
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
        scope_metrics: vec![ScopeMetrics {
            metrics: vec![Metric {
                name: "test_gauge".to_string(),
                description: "A test gauge".to_string(),
                unit: "1".to_string(),
                data: Some(metric::Data::Gauge(Gauge { data_points })),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    ExportMetricsServiceRequest {
        resource_metrics: vec![r_metric],
    }
}

fn make_test_batch_size(
    n: usize,
    service_name: Option<&str>,
    legacy_key: Option<&str>,
) -> RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("attributes", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new(
            "resource_attributes",
            arrow::datatypes::DataType::Utf8,
            false,
        ),
    ]));

    let mut attrs_vec = Vec::with_capacity(n);
    let mut res_vec = Vec::with_capacity(n);

    for _ in 0..n {
        let resource_attrs = match service_name {
            Some(name) => format!(r#"{{"service.name":"{}"}}"#, name),
            None => r#"{}"#.to_string(),
        };

        let attrs = match legacy_key {
            Some(key) => format!(r#"{{"{}":"GET"}}"#, key),
            None => r#"{}"#.to_string(),
        };
        attrs_vec.push(attrs);
        res_vec.push(resource_attrs);
    }

    let attrs_array = Arc::new(arrow::array::StringArray::from(attrs_vec)) as ArrayRef;
    let res_array = Arc::new(arrow::array::StringArray::from(res_vec)) as ArrayRef;

    RecordBatch::try_new(schema, vec![attrs_array, res_array]).unwrap()
}

fn bench_decoders(c: &mut Criterion) {
    // 1. Empty request benchmarks
    let req_logs_empty = ExportLogsServiceRequest::default();
    let req_traces_empty = ExportTraceServiceRequest::default();
    let req_metrics_empty = ExportMetricsServiceRequest::default();

    c.bench_function("decode_logs_empty", |b| {
        b.iter(|| decode_logs(bbox(&req_logs_empty)).unwrap());
    });

    c.bench_function("decode_traces_empty", |b| {
        b.iter(|| decode_traces(bbox(&req_traces_empty)).unwrap());
    });

    c.bench_function("decode_metrics_empty", |b| {
        b.iter(|| decode_metrics(bbox(&req_metrics_empty)).unwrap());
    });

    // 2. Populated request benchmarks (size: 100 records)
    let req_logs_populated = make_populated_logs_request(100);
    let req_traces_populated = make_populated_traces_request(100);
    let req_metrics_populated = make_populated_metrics_request(100);

    c.bench_function("decode_logs_populated_100", |b| {
        b.iter(|| decode_logs(bbox(&req_logs_populated)).unwrap());
    });

    c.bench_function("decode_traces_populated_100", |b| {
        b.iter(|| decode_traces(bbox(&req_traces_populated)).unwrap());
    });

    c.bench_function("decode_metrics_populated_100", |b| {
        b.iter(|| decode_metrics(bbox(&req_metrics_populated)).unwrap());
    });

    // 3. Compliance engine benchmarks (size: 100 records)
    let batch_compliant = make_test_batch_size(100, Some("my-service"), None);
    let batch_non_compliant = make_test_batch_size(100, None, Some("legacy_key"));

    let engine_strict = ComplianceEngine::new(ComplianceMode::Strict, HashMap::new());
    let engine_quarantine = ComplianceEngine::new(ComplianceMode::Quarantine, HashMap::new());
    let mut mappings = HashMap::new();
    mappings.insert("legacy_key".to_string(), "http.request.method".to_string());
    let engine_remap = ComplianceEngine::new(ComplianceMode::Remap, mappings);

    c.bench_function("compliance_strict_compliant_batch_100", |b| {
        b.iter(|| {
            engine_strict
                .assess_and_remap(bbox(batch_compliant.clone()))
                .unwrap()
        });
    });

    c.bench_function("compliance_quarantine_non_compliant_batch_100", |b| {
        b.iter(|| {
            engine_quarantine
                .assess_and_remap(bbox(batch_non_compliant.clone()))
                .unwrap()
        });
    });

    c.bench_function("compliance_remap_non_compliant_batch_100", |b| {
        b.iter(|| {
            engine_remap
                .assess_and_remap(bbox(batch_non_compliant.clone()))
                .unwrap()
        });
    });
}

criterion_group!(benches, bench_decoders);
criterion_main!(benches);
