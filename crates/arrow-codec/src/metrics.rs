use arrow::array::{Float64Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

use crate::common::{any_value_to_string, convert_attributes, timestamp_to_i64};

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
            name: StringBuilder::new(),
            desc: StringBuilder::new(),
            unit: StringBuilder::new(),
            timestamp: TimestampNanosecondBuilder::with_capacity(capacity),
            value: Float64Builder::with_capacity(capacity),
            attributes: StringBuilder::new(),
            service_name: StringBuilder::new(),
            resource_attributes: StringBuilder::new(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
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
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            &convert_attributes(&dp.attributes),
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
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            &convert_attributes(&dp.attributes),
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
) -> Result<(), PipelineError> {
    for dp in &hist.data_points {
        #[allow(clippy::cast_precision_loss)]
        let val = dp.sum.unwrap_or(dp.count as f64);
        builder.append_record(
            meta.name,
            meta.desc,
            meta.unit,
            dp.time_unix_nano,
            val,
            &convert_attributes(&dp.attributes),
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
    let mut total_records = 0;
    for r_metric in &req.resource_metrics {
        for s_metric in &r_metric.scope_metrics {
            for metric in &s_metric.metrics {
                if let Some(ref data) = metric.data {
                    match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            total_records += gauge.data_points.len();
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            total_records += sum.data_points.len();
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            total_records += hist.data_points.len();
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let mut builder = MetricsRecordBuilder::with_capacity(total_records);

    for r_metric in &req.resource_metrics {
        let (resource_attrs_json, service_name) = if let Some(ref res) = r_metric.resource {
            let service_name = res
                .attributes
                .iter()
                .find(|kv| kv.key == opentelemetry_semantic_conventions::resource::SERVICE_NAME)
                .and_then(|kv| kv.value.as_ref())
                .map_or_else(|| "unknown".to_string(), any_value_to_string);
            (convert_attributes(&res.attributes), service_name)
        } else {
            ("{}".to_string(), "unknown".to_string())
        };

        for s_metric in &r_metric.scope_metrics {
            let (scope_name, scope_version) = if let Some(ref scope) = s_metric.scope {
                (scope.name.as_str(), scope.version.as_str())
            } else {
                ("", "")
            };

            let ctx = MetricContext {
                service_name: &service_name,
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
                            process_gauge(&mut builder, gauge, &meta, &ctx)?;
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            process_sum(&mut builder, sum, &meta, &ctx)?;
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            process_histogram(&mut builder, hist, &meta, &ctx)?;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
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
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

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
}
