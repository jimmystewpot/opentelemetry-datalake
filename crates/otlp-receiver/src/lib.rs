use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::post,
};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};

use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{SignalBatch, Source};

/// Maximum HTTP payload byte size processed inline on the async worker.
/// Payloads exceeding this threshold are offloaded to `tokio::task::spawn_blocking`.
pub const HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES: usize = 64 * 1024;

/// Maximum gRPC record count processed inline on the async worker.
/// Batches exceeding this threshold are offloaded to `tokio::task::spawn_blocking`.
pub const GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD: usize = 512;

/// Estimates the total number of log records in an export request.
#[must_use]
pub fn estimate_logs_records(req: &ExportLogsServiceRequest) -> usize {
    req.resource_logs.iter().fold(0usize, |acc, r| {
        acc.saturating_add(
            r.scope_logs
                .iter()
                .fold(0usize, |s_acc, s| s_acc.saturating_add(s.log_records.len())),
        )
    })
}

/// Estimates the total number of spans in an export request.
#[must_use]
pub fn estimate_traces_records(req: &ExportTraceServiceRequest) -> usize {
    req.resource_spans.iter().fold(0usize, |acc, r| {
        acc.saturating_add(
            r.scope_spans
                .iter()
                .fold(0usize, |s_acc, s| s_acc.saturating_add(s.spans.len())),
        )
    })
}

/// Estimates the total number of metric data points in an export request.
#[must_use]
pub fn estimate_metrics_records(req: &ExportMetricsServiceRequest) -> usize {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;

    req.resource_metrics.iter().fold(0usize, |acc, r| {
        acc.saturating_add(r.scope_metrics.iter().fold(0usize, |s_acc, s| {
            s_acc.saturating_add(s.metrics.iter().fold(0usize, |m_acc, m| {
                let dp_count = match m.data {
                    Some(Data::Gauge(ref g)) => g.data_points.len(),
                    Some(Data::Sum(ref sum)) => sum.data_points.len(),
                    Some(Data::Histogram(ref h)) => h.data_points.len(),
                    Some(Data::ExponentialHistogram(ref eh)) => eh.data_points.len(),
                    Some(Data::Summary(ref s)) => s.data_points.len(),
                    None => 0,
                };
                m_acc.saturating_add(dp_count.max(1))
            }))
        }))
    })
}

#[derive(Clone)]
struct GrpcLogsService {
    tx: mpsc::Sender<SignalBatch>,
}

#[tonic::async_trait]
impl LogsService for GrpcLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let batch_res = if estimate_logs_records(&req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD {
            tokio::task::spawn_blocking(move || arrow_codec::decode_logs(&req))
                .await
                .map_err(|e| Status::internal(format!("codec task join error: {e}")))?
        } else {
            arrow_codec::decode_logs(&req)
        };

        match batch_res {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    self.tx
                        .send(SignalBatch::Logs(batch))
                        .await
                        .map_err(|_| Status::unavailable("pipeline downstream closed"))?;
                }
                Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[derive(Clone)]
struct GrpcTraceService {
    tx: mpsc::Sender<SignalBatch>,
}

#[tonic::async_trait]
impl TraceService for GrpcTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        let batch_res = if estimate_traces_records(&req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD {
            tokio::task::spawn_blocking(move || arrow_codec::decode_traces(&req))
                .await
                .map_err(|e| Status::internal(format!("codec task join error: {e}")))?
        } else {
            arrow_codec::decode_traces(&req)
        };

        match batch_res {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    self.tx
                        .send(SignalBatch::Traces(batch))
                        .await
                        .map_err(|_| Status::unavailable("pipeline downstream closed"))?;
                }
                Ok(Response::new(ExportTraceServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[derive(Clone)]
struct GrpcMetricsService {
    tx: mpsc::Sender<SignalBatch>,
}

#[tonic::async_trait]
impl MetricsService for GrpcMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();
        let batch_res = if estimate_metrics_records(&req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD {
            tokio::task::spawn_blocking(move || arrow_codec::decode_metrics(&req))
                .await
                .map_err(|e| Status::internal(format!("codec task join error: {e}")))?
        } else {
            arrow_codec::decode_metrics(&req)
        };

        match batch_res {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    self.tx
                        .send(SignalBatch::Metrics(batch))
                        .await
                        .map_err(|_| Status::unavailable("pipeline downstream closed"))?;
                }
                Ok(Response::new(ExportMetricsServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

fn decode_http_body<T: prost::Message + Default + serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: Bytes,
) -> Result<T, String> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    if content_type.contains("application/x-protobuf") {
        T::decode(body).map_err(|e| format!("Failed to decode protobuf: {e}"))
    } else {
        serde_json::from_slice(&body).map_err(|e| format!("Failed to decode JSON: {e}"))
    }
}

async fn decode_and_convert_http<T, F>(
    headers: HeaderMap,
    body: Bytes,
    signal_name: &'static str,
    decode_fn: F,
) -> Result<RecordBatch, StatusCode>
where
    T: prost::Message + Default + serde::de::DeserializeOwned + Send + 'static,
    F: FnOnce(&T) -> Result<RecordBatch, PipelineError> + Send + 'static,
{
    if body.len() > HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES {
        tokio::task::spawn_blocking(move || {
            let req: T = decode_http_body(&headers, body).map_err(|e| {
                tracing::error!("{signal_name} HTTP decode error: {e}");
                StatusCode::BAD_REQUEST
            })?;
            decode_fn(&req).map_err(|e| {
                tracing::error!("{signal_name} arrow-codec error: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })
        })
        .await
        .map_err(|e| {
            tracing::error!("{signal_name} spawn_blocking join error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
    } else {
        let req: T = decode_http_body(&headers, body).map_err(|e| {
            tracing::error!("{signal_name} HTTP decode error: {e}");
            StatusCode::BAD_REQUEST
        })?;
        decode_fn(&req).map_err(|e| {
            tracing::error!("{signal_name} arrow-codec error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }
}

#[derive(Clone)]
#[allow(clippy::struct_field_names)]
struct HttpState {
    logs_tx: mpsc::Sender<SignalBatch>,
    traces_tx: mpsc::Sender<SignalBatch>,
    metrics_tx: mpsc::Sender<SignalBatch>,
}

async fn handle_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let batch = decode_and_convert_http::<ExportLogsServiceRequest, _>(
        headers,
        body,
        "Logs",
        arrow_codec::decode_logs,
    )
    .await?;

    if batch.num_rows() > 0 {
        state
            .logs_tx
            .send(SignalBatch::Logs(batch))
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    }
    Ok((
        StatusCode::OK,
        serde_json::json!({ "partial_success": null }).to_string(),
    ))
}

async fn handle_traces(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let batch = decode_and_convert_http::<ExportTraceServiceRequest, _>(
        headers,
        body,
        "Traces",
        arrow_codec::decode_traces,
    )
    .await?;

    if batch.num_rows() > 0 {
        state
            .traces_tx
            .send(SignalBatch::Traces(batch))
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    }
    Ok((
        StatusCode::OK,
        serde_json::json!({ "partial_success": null }).to_string(),
    ))
}

async fn handle_metrics(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let batch = decode_and_convert_http::<ExportMetricsServiceRequest, _>(
        headers,
        body,
        "Metrics",
        arrow_codec::decode_metrics,
    )
    .await?;

    if batch.num_rows() > 0 {
        state
            .metrics_tx
            .send(SignalBatch::Metrics(batch))
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    }
    Ok((
        StatusCode::OK,
        serde_json::json!({ "partial_success": null }).to_string(),
    ))
}

/// OTLP Receiver source supporting gRPC and HTTP/JSON endpoints.
pub struct OtlpReceiverSource {
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    logs_tx: mpsc::Sender<SignalBatch>,
    traces_tx: mpsc::Sender<SignalBatch>,
    metrics_tx: mpsc::Sender<SignalBatch>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl OtlpReceiverSource {
    /// Creates a new `OtlpReceiverSource`.
    #[must_use]
    pub fn new(
        grpc_addr: SocketAddr,
        http_addr: SocketAddr,
        logs_tx: mpsc::Sender<SignalBatch>,
        traces_tx: mpsc::Sender<SignalBatch>,
        metrics_tx: mpsc::Sender<SignalBatch>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
            shutdown_rx,
        }
    }
}

#[async_trait]
impl Source for OtlpReceiverSource {
    async fn run(&mut self) -> Result<(), PipelineError> {
        let grpc_service_logs = GrpcLogsService {
            tx: self.logs_tx.clone(),
        };
        let grpc_service_traces = GrpcTraceService {
            tx: self.traces_tx.clone(),
        };
        let grpc_service_metrics = GrpcMetricsService {
            tx: self.metrics_tx.clone(),
        };

        let mut shutdown_rx_grpc = self.shutdown_rx.clone();
        let grpc_shutdown = async move {
            let _ = shutdown_rx_grpc
                .wait_for(|shutting_down| *shutting_down)
                .await;
            tracing::info!("gRPC server shutting down gracefully");
        };

        let grpc_server = tonic::transport::Server::builder()
            .add_service(LogsServiceServer::new(grpc_service_logs))
            .add_service(TraceServiceServer::new(grpc_service_traces))
            .add_service(MetricsServiceServer::new(grpc_service_metrics))
            .serve_with_shutdown(self.grpc_addr, grpc_shutdown);

        let http_state = HttpState {
            logs_tx: self.logs_tx.clone(),
            traces_tx: self.traces_tx.clone(),
            metrics_tx: self.metrics_tx.clone(),
        };

        let app = Router::new()
            .route("/v1/logs", post(handle_logs))
            .route("/v1/traces", post(handle_traces))
            .route("/v1/metrics", post(handle_metrics))
            .with_state(http_state);

        let listener = tokio::net::TcpListener::bind(self.http_addr)
            .await
            .map_err(|e| PipelineError::Internal(format!("Failed to bind HTTP: {e}")))?;

        let mut shutdown_rx_http = self.shutdown_rx.clone();
        let http_shutdown = async move {
            let _ = shutdown_rx_http
                .wait_for(|shutting_down| *shutting_down)
                .await;
            tracing::info!("HTTP server shutting down gracefully");
        };

        let http_server = axum::serve(listener, app).with_graceful_shutdown(http_shutdown);

        tracing::info!("gRPC server listening on {}", self.grpc_addr);
        tracing::info!("HTTP server listening on {}", self.http_addr);

        let grpc_fut = async {
            grpc_server
                .await
                .map_err(|e| PipelineError::Internal(format!("gRPC server error: {e}")))
        };
        let http_fut = async {
            http_server
                .await
                .map_err(|e| PipelineError::Internal(format!("HTTP server error: {e}")))
        };

        tokio::try_join!(grpc_fut, http_fut)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use std::net::IpAddr;

    #[tokio::test]
    async fn test_source_bind() {
        let (logs_tx, _logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);

        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let grpc_addr = SocketAddr::new(loopback, 0);
        let http_addr = SocketAddr::new(loopback, 0);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let mut source = OtlpReceiverSource::new(
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
            shutdown_rx,
        );

        // Run source with a timeout to verify it can bind successfully
        let handle = tokio::spawn(async move {
            let _ = source.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), handle).await;
    }

    /// Sending an empty `ExportLogsServiceRequest` must succeed with zero rows
    /// and NOT push anything to the channel.
    #[tokio::test]
    async fn test_grpc_logs_empty_request() {
        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcLogsService { tx };

        let req = Request::new(ExportLogsServiceRequest::default());
        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Empty logs request should succeed");

        // Channel should be empty since zero-row batches are skipped
        assert!(
            rx.try_recv().is_err(),
            "No signal should be sent for empty payload"
        );
    }

    /// Sending an empty `ExportTraceServiceRequest` must succeed with zero rows.
    #[tokio::test]
    async fn test_grpc_traces_empty_request() {
        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcTraceService { tx };

        let req = Request::new(ExportTraceServiceRequest::default());
        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Empty traces request should succeed");

        assert!(
            rx.try_recv().is_err(),
            "No signal should be sent for empty payload"
        );
    }

    /// Sending an empty `ExportMetricsServiceRequest` must succeed with zero rows.
    #[tokio::test]
    async fn test_grpc_metrics_empty_request() {
        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcMetricsService { tx };

        let req = Request::new(ExportMetricsServiceRequest::default());
        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Empty metrics request should succeed");

        assert!(
            rx.try_recv().is_err(),
            "No signal should be sent for empty payload"
        );
    }

    /// When the downstream channel is dropped, the gRPC service must
    /// return UNAVAILABLE status to propagate backpressure to the caller.
    #[tokio::test]
    async fn test_grpc_logs_backpressure_on_closed_channel() {
        let (tx, rx) = mpsc::channel(10);
        let svc = GrpcLogsService { tx };

        // Drop the receiver to simulate a failed downstream.
        drop(rx);

        let req = Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let result = svc.export(req).await;
        assert!(result.is_err(), "Should fail when downstream is closed");
        let status = result.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::Unavailable,
            "Expected UNAVAILABLE status, got: {:?}",
            status.code()
        );
    }

    /// `decode_http_body` with application/json content-type and valid JSON
    /// must successfully parse the request.
    #[test]
    fn test_decode_http_body_json() {
        #[allow(clippy::items_after_statements)]
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let body = Bytes::from(r#"{"resourceLogs":[]}"#);
        let result: Result<ExportLogsServiceRequest, _> = decode_http_body(&headers, body);
        assert!(result.is_ok(), "Valid JSON should decode successfully");
    }

    /// `decode_http_body` must default to JSON when no Content-Type header is set.
    #[test]
    fn test_decode_http_body_defaults_to_json() {
        let headers = HeaderMap::new();
        let body = Bytes::from(r#"{"resourceLogs":[]}"#);
        let result: Result<ExportLogsServiceRequest, _> = decode_http_body(&headers, body);
        assert!(
            result.is_ok(),
            "Missing content-type should default to JSON"
        );
    }

    /// `decode_http_body` with application/x-protobuf content-type and
    /// garbage bytes must return a decode error.
    #[test]
    fn test_decode_http_body_invalid_protobuf() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        let body = Bytes::from_static(b"this is not valid protobuf");
        let result: Result<ExportLogsServiceRequest, _> = decode_http_body(&headers, body);
        assert!(
            result.is_err(),
            "Invalid protobuf bytes should fail decoding"
        );
    }

    /// `decode_http_body` with application/json content-type and
    /// malformed JSON must return a decode error.
    #[test]
    fn test_decode_http_body_invalid_json() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        let body = Bytes::from(r"{ not valid json }");
        let result: Result<ExportLogsServiceRequest, _> = decode_http_body(&headers, body);
        assert!(result.is_err(), "Invalid JSON should fail decoding");
    }

    /// A non-empty traces request on a closed channel must return UNAVAILABLE.
    #[tokio::test]
    async fn test_grpc_traces_backpressure_on_closed_channel() {
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let (tx, rx) = mpsc::channel(10);
        let svc = GrpcTraceService { tx };
        drop(rx);

        let req = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let result = svc.export(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    /// A non-empty metrics request on a closed channel must return UNAVAILABLE.
    #[tokio::test]
    async fn test_grpc_metrics_backpressure_on_closed_channel() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let (tx, rx) = mpsc::channel(10);
        let svc = GrpcMetricsService { tx };
        drop(rx);

        let req = Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("svc".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                                        1.0,
                                    ),
                                ),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let result = svc.export(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    }

    /// A non-empty gRPC logs request must push a `SignalBatch::Logs` onto the channel.
    #[tokio::test]
    async fn test_grpc_logs_non_empty_sends_to_channel() {
        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcLogsService { tx };

        let req = Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        svc.export(req).await.unwrap();
        let msg = rx.try_recv();
        assert!(
            msg.is_ok(),
            "Non-empty logs request must push a batch to the channel"
        );
        assert!(
            matches!(msg.unwrap(), SignalBatch::Logs(_)),
            "Signal must be Logs variant"
        );
    }

    /// A non-empty gRPC traces request must push a `SignalBatch::Traces` onto the channel.
    #[tokio::test]
    async fn test_grpc_traces_non_empty_sends_to_channel() {
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcTraceService { tx };

        let req = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        svc.export(req).await.unwrap();
        let msg = rx.try_recv();
        assert!(
            msg.is_ok(),
            "Non-empty traces request must push a batch to the channel"
        );
        assert!(
            matches!(msg.unwrap(), SignalBatch::Traces(_)),
            "Signal must be Traces variant"
        );
    }

    #[test]
    fn test_estimate_record_counts_sums_across_resources_and_scopes() {
        use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        // Construct ExportLogsServiceRequest with 2 resources x 2 scopes x 200 records = 800 records
        let logs_req = ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    scope_logs: vec![
                        ScopeLogs {
                            log_records: vec![LogRecord::default(); 200],
                            ..Default::default()
                        },
                        ScopeLogs {
                            log_records: vec![LogRecord::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                ResourceLogs {
                    scope_logs: vec![
                        ScopeLogs {
                            log_records: vec![LogRecord::default(); 200],
                            ..Default::default()
                        },
                        ScopeLogs {
                            log_records: vec![LogRecord::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };
        assert_eq!(estimate_logs_records(&logs_req), 800);
        assert!(estimate_logs_records(&logs_req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD);

        // Construct ExportTraceServiceRequest with 2 resources x 2 scopes x 200 records = 800 records
        let traces_req = ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    scope_spans: vec![
                        ScopeSpans {
                            spans: vec![Span::default(); 200],
                            ..Default::default()
                        },
                        ScopeSpans {
                            spans: vec![Span::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                ResourceSpans {
                    scope_spans: vec![
                        ScopeSpans {
                            spans: vec![Span::default(); 200],
                            ..Default::default()
                        },
                        ScopeSpans {
                            spans: vec![Span::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };
        assert_eq!(estimate_traces_records(&traces_req), 800);
        assert!(estimate_traces_records(&traces_req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD);

        // Construct ExportMetricsServiceRequest with 2 resources x 2 scopes x 200 records = 800 records
        let metrics_req = ExportMetricsServiceRequest {
            resource_metrics: vec![
                ResourceMetrics {
                    scope_metrics: vec![
                        ScopeMetrics {
                            metrics: vec![Metric::default(); 200],
                            ..Default::default()
                        },
                        ScopeMetrics {
                            metrics: vec![Metric::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                ResourceMetrics {
                    scope_metrics: vec![
                        ScopeMetrics {
                            metrics: vec![Metric::default(); 200],
                            ..Default::default()
                        },
                        ScopeMetrics {
                            metrics: vec![Metric::default(); 200],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };
        assert_eq!(estimate_metrics_records(&metrics_req), 800);
        assert!(estimate_metrics_records(&metrics_req) >= GRPC_SPAWN_BLOCKING_RECORD_THRESHOLD);
    }

    #[tokio::test]
    async fn test_http_logs_over_64kib_decodes_via_spawn_blocking() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
        use prost::Message;

        let (logs_tx, mut logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);

        let state = HttpState {
            logs_tx,
            traces_tx,
            metrics_tx,
        };

        // Construct a protobuf ExportLogsServiceRequest body > 65_536 bytes (64 KiB)
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("x".repeat(70_000))),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let mut body_bytes = Vec::new();
        req.encode(&mut body_bytes).unwrap();
        assert!(
            body_bytes.len() > HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES,
            "Payload must be larger than threshold: {} <= {}",
            body_bytes.len(),
            HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        let res = handle_logs(State(state), headers, Bytes::from(body_bytes)).await;
        assert!(res.is_ok(), "handle_logs should succeed for >64KiB payload");
        let resp = res.unwrap().into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let received = logs_rx.try_recv();
        assert!(received.is_ok(), "Expected batch in logs channel");
        if let SignalBatch::Logs(batch) = received.unwrap() {
            assert_eq!(batch.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Logs variant");
        }
    }

    #[tokio::test]
    async fn test_grpc_logs_over_threshold_decodes_via_spawn_blocking() {
        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcLogsService { tx };

        let req = Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1_000_000_000,
                            ..Default::default()
                        };
                        600
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Large logs export should succeed");

        let msg = rx.try_recv();
        assert!(msg.is_ok(), "Expected batch in channel");
        if let SignalBatch::Logs(batch) = msg.unwrap() {
            assert_eq!(batch.num_rows(), 600);
        } else {
            panic!("Expected SignalBatch::Logs");
        }
    }

    #[tokio::test]
    async fn test_grpc_traces_over_threshold_decodes_via_spawn_blocking() {
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcTraceService { tx };

        let req = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![
                        Span {
                            start_time_unix_nano: 1_000_000_000,
                            end_time_unix_nano: 2_000_000_000,
                            ..Default::default()
                        };
                        600
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Large traces export should succeed");

        let msg = rx.try_recv();
        assert!(msg.is_ok(), "Expected batch in channel");
        if let SignalBatch::Traces(batch) = msg.unwrap() {
            assert_eq!(batch.num_rows(), 600);
        } else {
            panic!("Expected SignalBatch::Traces");
        }
    }

    #[tokio::test]
    async fn test_grpc_metrics_over_threshold_decodes_via_spawn_blocking() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
        };

        let (tx, mut rx) = mpsc::channel(10);
        let svc = GrpcMetricsService { tx };

        let req = Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: (0..600)
                        .map(|i| Metric {
                            name: format!("metric_{i}"),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 1_000_000_000,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                                            1.0,
                                        ),
                                    ),
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let resp = svc.export(req).await;
        assert!(resp.is_ok(), "Large metrics export should succeed");

        let msg = rx.try_recv();
        assert!(msg.is_ok(), "Expected batch in channel");
        if let SignalBatch::Metrics(batch) = msg.unwrap() {
            assert_eq!(batch.num_rows(), 600);
        } else {
            panic!("Expected SignalBatch::Metrics");
        }

        // Also verify a single Metric container holding 600 data points is estimated as 600 records
        let single_metric_many_dps = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "single_gauge".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![
                                NumberDataPoint {
                                    time_unix_nano: 1_000_000_000,
                                    ..Default::default()
                                };
                                600
                            ],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_eq!(estimate_metrics_records(&single_metric_many_dps), 600);
    }

    #[tokio::test]
    async fn test_http_traces_over_64kib_decodes_via_spawn_blocking() {
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        use prost::Message;

        let (logs_tx, _logs_rx) = mpsc::channel(10);
        let (traces_tx, mut traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);

        let state = HttpState {
            logs_tx,
            traces_tx,
            metrics_tx,
        };

        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        name: "s".repeat(70_000),
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let mut body_bytes = Vec::new();
        req.encode(&mut body_bytes).unwrap();
        assert!(body_bytes.len() > HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        let res = handle_traces(State(state), headers, Bytes::from(body_bytes)).await;
        assert!(res.is_ok());
        let resp = res.unwrap().into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let received = traces_rx.try_recv();
        assert!(received.is_ok());
        if let SignalBatch::Traces(batch) = received.unwrap() {
            assert_eq!(batch.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Traces");
        }
    }

    #[tokio::test]
    async fn test_http_metrics_over_64kib_decodes_via_spawn_blocking() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
        };
        use prost::Message;

        let (logs_tx, _logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, mut metrics_rx) = mpsc::channel(10);

        let state = HttpState {
            logs_tx,
            traces_tx,
            metrics_tx,
        };

        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".repeat(70_000),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                                        1.0,
                                    ),
                                ),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let mut body_bytes = Vec::new();
        req.encode(&mut body_bytes).unwrap();
        assert!(body_bytes.len() > HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        let res = handle_metrics(State(state), headers, Bytes::from(body_bytes)).await;
        assert!(res.is_ok());
        let resp = res.unwrap().into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let received = metrics_rx.try_recv();
        assert!(received.is_ok());
        if let SignalBatch::Metrics(batch) = received.unwrap() {
            assert_eq!(batch.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Metrics");
        }
    }

    #[tokio::test]
    async fn test_http_logs_under_threshold_decodes_inline() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
        use prost::Message;

        let (logs_tx, mut logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);

        let state = HttpState {
            logs_tx,
            traces_tx,
            metrics_tx,
        };

        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("small".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let mut body_bytes = Vec::new();
        req.encode(&mut body_bytes).unwrap();
        assert!(body_bytes.len() <= HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        let res = handle_logs(State(state), headers, Bytes::from(body_bytes)).await;
        assert!(res.is_ok());
        let resp = res.unwrap().into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let received = logs_rx.try_recv();
        assert!(received.is_ok());
        if let SignalBatch::Logs(batch) = received.unwrap() {
            assert_eq!(batch.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Logs");
        }
    }
}
