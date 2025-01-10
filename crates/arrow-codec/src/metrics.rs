use arrow::array::{Float64Builder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline_core::error::PipelineError;
use std::sync::Arc;

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
            map.insert(attr.key.as_str(), val_str);
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// Decodes OTLP Metrics requests into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if schema matching or record creation fails.
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]
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

    let mut name_builder = StringBuilder::new();
    let mut desc_builder = StringBuilder::new();
    let mut unit_builder = StringBuilder::new();
    let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(total_records);
    let mut value_builder = Float64Builder::with_capacity(total_records);
    let mut attributes_builder = StringBuilder::new();
    let mut resource_attributes_builder = StringBuilder::new();
    let mut scope_name_builder = StringBuilder::new();
    let mut scope_version_builder = StringBuilder::new();

    for r_metric in &req.resource_metrics {
        let resource_attrs_json = if let Some(ref res) = r_metric.resource {
            convert_attributes(&res.attributes)
        } else {
            "{}".to_string()
        };

        for s_metric in &r_metric.scope_metrics {
            let (scope_name, scope_version) = if let Some(ref scope) = s_metric.scope {
                (scope.name.as_str(), scope.version.as_str())
            } else {
                ("", "")
            };

            for metric in &s_metric.metrics {
                let name = metric.name.as_str();
                let desc = metric.description.as_str();
                let unit = metric.unit.as_str();

                if let Some(ref data) = metric.data {
                    match data {
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) => {
                            for dp in &gauge.data_points {
                                name_builder.append_value(name);
                                desc_builder.append_value(desc);
                                unit_builder.append_value(unit);
                                timestamp_builder.append_value(dp.time_unix_nano as i64);

                                let val = match dp.value {
                                    Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(d)) => d,
                                    Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i)) => i as f64,
                                    None => 0.0,
                                };
                                value_builder.append_value(val);

                                attributes_builder.append_value(convert_attributes(&dp.attributes));
                                resource_attributes_builder.append_value(&resource_attrs_json);
                                scope_name_builder.append_value(scope_name);
                                scope_version_builder.append_value(scope_version);
                            }
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
                            for dp in &sum.data_points {
                                name_builder.append_value(name);
                                desc_builder.append_value(desc);
                                unit_builder.append_value(unit);
                                timestamp_builder.append_value(dp.time_unix_nano as i64);

                                let val = match dp.value {
                                    Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(d)) => d,
                                    Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i)) => i as f64,
                                    None => 0.0,
                                };
                                value_builder.append_value(val);

                                attributes_builder.append_value(convert_attributes(&dp.attributes));
                                resource_attributes_builder.append_value(&resource_attrs_json);
                                scope_name_builder.append_value(scope_name);
                                scope_version_builder.append_value(scope_version);
                            }
                        }
                        opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(hist) => {
                            for dp in &hist.data_points {
                                name_builder.append_value(name);
                                desc_builder.append_value(desc);
                                unit_builder.append_value(unit);
                                timestamp_builder.append_value(dp.time_unix_nano as i64);

                                // Use sum as value if available, otherwise fallback to count
                                let val = dp.sum.unwrap_or(dp.count as f64);
                                value_builder.append_value(val);

                                attributes_builder.append_value(convert_attributes(&dp.attributes));
                                resource_attributes_builder.append_value(&resource_attrs_json);
                                scope_name_builder.append_value(scope_name);
                                scope_version_builder.append_value(scope_version);
                            }
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
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(name_builder.finish()),
            Arc::new(desc_builder.finish()),
            Arc::new(unit_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(value_builder.finish()),
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
        let mut req = ExportMetricsServiceRequest::default();
        let mut r_metric = ResourceMetrics::default();
        r_metric.resource = Some(Resource {
            attributes: vec![KeyValue {
                key: opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test-service".to_string())),
                }),
                ..Default::default()
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        });

        let mut s_metric = ScopeMetrics::default();
        let mut metric = Metric::default();
        metric.name = "test_gauge".to_string();
        metric.description = "A test gauge".to_string();
        metric.unit = "1".to_string();

        let mut gauge = Gauge::default();
        let mut dp = NumberDataPoint::default();
        dp.time_unix_nano = 1_000_000_000;
        dp.value =
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.5));
        gauge.data_points.push(dp);
        metric.data = Some(metric::Data::Gauge(gauge));

        s_metric.metrics.push(metric);
        r_metric.scope_metrics.push(s_metric);
        req.resource_metrics.push(r_metric);

        let batch = decode_metrics(&req).unwrap();
        assert_eq!(batch.num_rows(), 1);

        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 9);
    }
}
