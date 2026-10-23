use arrow::array::{Float64Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

use crate::common::{
    METRICS_SCHEMA, extract_service_name, timestamp_to_i64, write_attributes_json,
};

/// Decodes OTLP Metrics requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
///
/// # Precision Limitation
///
/// Casting `AsInt(i)` values to `f64` in `decode_metrics` can lead to a loss of
/// precision for integers whose absolute values exceed 2^53.
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
struct MetricsRecordBuilder {
    name: StringBuilder,
    desc: StringBuilder,
    unit: StringBuilder,
    timestamp: TimestampNanosecondBuilder,
    value: Float64Builder,
    attributes: StringBuilder,
    service_name: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
}

struct MetricContext<'a> {
    service_name: &'a str,
    resource_attributes: &'a str,
    scope_name: &'a str,
    scope_version: &'a str,
}

struct MetricMeta<'a> {
    name: &'a str,
    desc: &'a str,
    unit: &'a str,
}

impl MetricsRecordBuilder {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            name: StringBuilder::with_capacity(capacity, capacity.saturating_mul(32)),
            desc: StringBuilder::with_capacity(capacity, capacity.saturating_mul(64)),
            unit: StringBuilder::with_capacity(capacity, capacity.saturating_mul(8)),
            timestamp: TimestampNanosecondBuilder::with_capacity(capacity),
            value: Float64Builder::with_capacity(capacity),
            attributes: StringBuilder::with_capacity(capacity, capacity.saturating_mul(128)),
            service_name: StringBuilder::with_capacity(capacity, capacity.saturating_mul(24)),
            resource_attributes: StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(128),
            ),
            scope_name: StringBuilder::with_capacity(capacity, capacity.saturating_mul(24)),
            scope_version: StringBuilder::with_capacity(capacity, capacity.saturating_mul(16)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_record(
        &mut self,
        name: &str,
        desc: &str,
        unit: &str,
        timestamp_unix_nano: u64,
        value: f64,
        attributes: &str,
        service_name: &str,
        resource_attributes: &str,
        scope_name: &str,
        scope_version: &str,
    ) -> Result<(), PipelineError> {
        self.name.append_value(name);
        self.desc.append_value(desc);
        self.unit.append_value(unit);
        self.timestamp
            .append_value(timestamp_to_i64(timestamp_unix_nano)?);
        self.value.append_value(value);
        self.attributes.append_value(attributes);
        self.service_name.append_value(service_name);
        self.resource_attributes.append_value(resource_attributes);
        self.scope_name.append_value(scope_name);
        self.scope_version.append_value(scope_version);
        Ok(())
    }
}

#[allow(clippy::cast_precision_loss)]
fn process_gauge(
    builder: &mut MetricsRecordBuilder,
    gauge: &opentelemetry_proto::tonic::metrics::v1::Gauge,
    meta: &MetricMeta<'_>,
    ctx: &MetricContext<'_>,
    scratch: &mut String,
) -> Result<(), PipelineError> {
    for dp in &gauge.data_points {
        let val = match dp.value {
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                d,
            )) => d,
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i)) => {
                i as f64
            }
            None => 0.0,
        };
        write_attributes_json(scratch, &dp.attributes);
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            scratch.as_str(),
            ctx.service_name,
            ctx.resource_attributes,
            ctx.scope_name,
            ctx.scope_version,
        )?;
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn process_sum(
    builder: &mut MetricsRecordBuilder,
    sum: &opentelemetry_proto::tonic::metrics::v1::Sum,
    meta: &MetricMeta<'_>,
    ctx: &MetricContext<'_>,
    scratch: &mut String,
) -> Result<(), PipelineError> {
    for dp in &sum.data_points {
        let val = match dp.value {
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                d,
            )) => d,
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i)) => {
                i as f64
            }
            None => 0.0,
        };
        write_attributes_json(scratch, &dp.attributes);
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            scratch.as_str(),
            ctx.service_name,
            ctx.resource_attributes,
            ctx.scope_name,
            ctx.scope_version,
        )?;
    }
    Ok(())
}

fn process_histogram(
    builder: &mut MetricsRecordBuilder,
    hist: &opentelemetry_proto::tonic::metrics::v1::Histogram,
    meta: &MetricMeta<'_>,
    ctx: &MetricContext<'_>,
    scratch: &mut String,
) -> Result<(), PipelineError> {
    for dp in &hist.data_points {
        #[allow(clippy::cast_precision_loss)]
        let val = dp.sum.unwrap_or(dp.count as f64);
        write_attributes_json(scratch, &dp.attributes);
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            scratch.as_str(),
            ctx.service_name,
            ctx.resource_attributes,
            ctx.scope_name,
            ctx.scope_version,
        )?;
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
pub fn decode_metrics(req: &ExportMetricsServiceRequest) -> Result<RecordBatch, PipelineError> {
    // Count total data points first to set builder capacity
    let mut total_records: usize = 0;
    for r_metric in &req.resource_metrics {
        for s_metric in &r_metric.scope_metrics {
            for metric in &s_metric.metrics {
                if let Some(ref data) = metric.data {
                    match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            total_records = total_records.saturating_add(gauge.data_points.len());
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            total_records = total_records.saturating_add(sum.data_points.len());
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            total_records = total_records.saturating_add(hist.data_points.len());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let mut builder = MetricsRecordBuilder::with_capacity(total_records);
    let mut resource_attrs_json = String::with_capacity(256);
    let mut scratch = String::with_capacity(256);

    for r_metric in &req.resource_metrics {
        let service_name = if let Some(ref res) = r_metric.resource {
            write_attributes_json(&mut resource_attrs_json, &res.attributes);
            extract_service_name(&res.attributes)
        } else {
            resource_attrs_json.clear();
            resource_attrs_json.push_str("{}");
            std::borrow::Cow::Borrowed("unknown")
        };

        for s_metric in &r_metric.scope_metrics {
            let (scope_name, scope_version) = if let Some(ref scope) = s_metric.scope {
                (scope.name.as_str(), scope.version.as_str())
            } else {
                ("", "")
            };

            let ctx = MetricContext {
                service_name: service_name.as_ref(),
                resource_attributes: &resource_attrs_json,
                scope_name,
                scope_version,
            };

            for metric in &s_metric.metrics {
                let meta = MetricMeta {
                    name: metric.name.as_str(),
                    desc: metric.description.as_str(),
                    unit: metric.unit.as_str(),
                };

                if let Some(ref data) = metric.data {
                    match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            process_gauge(&mut builder, gauge, &meta, &ctx, &mut scratch)?;
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            process_sum(&mut builder, sum, &meta, &ctx, &mut scratch)?;
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            process_histogram(&mut builder, hist, &meta, &ctx, &mut scratch)?;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    RecordBatch::try_new(
        Arc::clone(&*METRICS_SCHEMA),
        vec![
            Arc::new(builder.name.finish()),
            Arc::new(builder.desc.finish()),
            Arc::new(builder.unit.finish()),
            Arc::new(builder.timestamp.finish()),
            Arc::new(builder.value.finish()),
            Arc::new(builder.attributes.finish()),
            Arc::new(builder.service_name.finish()),
            Arc::new(builder.resource_attributes.finish()),
            Arc::new(builder.scope_name.finish()),
            Arc::new(builder.scope_version.finish()),
        ],
    )
    .map_err(PipelineError::Arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_decode_metrics_static_schema_identity_and_typed_attributes() {
        let b1 = decode_metrics(&ExportMetricsServiceRequest::default()).unwrap();
        let b2 = decode_metrics(&ExportMetricsServiceRequest::default()).unwrap();
        assert!(Arc::ptr_eq(&b1.schema(), &b2.schema()));

        let r_metric = ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "req_count".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(5),
                            ),
                            attributes: vec![KeyValue {
                                key: "worker".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(3)),
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        let attrs = batch
            .column_by_name("attributes")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(attrs, r#"{"worker":3}"#);
    }

    #[test]
    fn test_decode_metrics_empty() {
        let req = ExportMetricsServiceRequest::default();
        let batch = decode_metrics(&req).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_decode_metrics_non_empty() {
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
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            value: Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.5)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        };

        let batch = decode_metrics(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 10);
    }

    /// A Sum metric must produce one row per data point.
    #[test]
    fn test_decode_metrics_sum_type() {
        use opentelemetry_proto::tonic::metrics::v1::{NumberDataPoint, Sum, metric};
        let r_metric = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "request_total".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(100.0),
                            ),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    /// A Histogram metric must produce one row per data point using the
    /// `sum` field (or count as fallback).
    #[test]
    fn test_decode_metrics_histogram_type() {
        use opentelemetry_proto::tonic::metrics::v1::{Histogram, HistogramDataPoint, metric};
        let r_metric = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "latency_ms".to_string(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            time_unix_nano: 2_000_000_000,
                            count: 5,
                            sum: Some(250.0),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        assert_eq!(batch.num_rows(), 1);
        // The value column must contain the sum (250.0)
        let val = batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0);
        assert!((val - 250.0).abs() < f64::EPSILON);
    }

    /// A Gauge data point with an integer value must be stored as f64.
    #[test]
    fn test_decode_metrics_gauge_as_int_value() {
        let r_metric = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "cpu_count".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(42),
                            ),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        let val = batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0);
        assert!(
            (val - 42.0).abs() < f64::EPSILON,
            "AsInt must cast to f64: {val}"
        );
    }

    /// A Gauge data point with no value set must default to 0.0.
    #[test]
    fn test_decode_metrics_gauge_none_value_defaults_to_zero() {
        let r_metric = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "empty_gauge".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            value: None,
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        let val = batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0);
        assert!(
            (val - 0.0).abs() < f64::EPSILON,
            "None value must default to 0.0: {val}"
        );
    }

    /// When no resource is set, `service_name` must default to "unknown".
    #[test]
    fn test_decode_metrics_missing_resource_uses_unknown() {
        let r_metric = ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "no_resource_gauge".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000_000_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(1.0),
                            ),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        let svc = batch
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(svc, "unknown");
    }
}
