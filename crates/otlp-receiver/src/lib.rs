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
        match arrow_codec::decode_logs(&req) {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    let _ = self.tx.send(SignalBatch::Logs(batch)).await;
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
        match arrow_codec::decode_traces(&req) {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    let _ = self.tx.send(SignalBatch::Traces(batch)).await;
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
        match arrow_codec::decode_metrics(&req) {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    let _ = self.tx.send(SignalBatch::Metrics(batch)).await;
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
    let req: ExportLogsServiceRequest = decode_http_body(&headers, body).map_err(|e| {
        tracing::error!("Logs HTTP decode error: {e}");
        StatusCode::BAD_REQUEST
    })?;

    match arrow_codec::decode_logs(&req) {
        Ok(batch) => {
            if batch.num_rows() > 0 {
                let _ = state.logs_tx.send(SignalBatch::Logs(batch)).await;
            }
            Ok((
                StatusCode::OK,
                serde_json::json!({ "partial_success": null }).to_string(),
            ))
        }
        Err(e) => {
            tracing::error!("Logs arrow-codec error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_traces(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let req: ExportTraceServiceRequest = decode_http_body(&headers, body).map_err(|e| {
        tracing::error!("Traces HTTP decode error: {e}");
        StatusCode::BAD_REQUEST
    })?;

    match arrow_codec::decode_traces(&req) {
        Ok(batch) => {
            if batch.num_rows() > 0 {
                let _ = state.traces_tx.send(SignalBatch::Traces(batch)).await;
            }
            Ok((
                StatusCode::OK,
                serde_json::json!({ "partial_success": null }).to_string(),
            ))
        }
        Err(e) => {
            tracing::error!("Traces arrow-codec error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_metrics(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    let req: ExportMetricsServiceRequest = decode_http_body(&headers, body).map_err(|e| {
        tracing::error!("Metrics HTTP decode error: {e}");
        StatusCode::BAD_REQUEST
    })?;

    match arrow_codec::decode_metrics(&req) {
        Ok(batch) => {
            if batch.num_rows() > 0 {
                let _ = state.metrics_tx.send(SignalBatch::Metrics(batch)).await;
            }
            Ok((
                StatusCode::OK,
                serde_json::json!({ "partial_success": null }).to_string(),
            ))
        }
        Err(e) => {
            tracing::error!("Metrics arrow-codec error: {e}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// OTLP Receiver source supporting gRPC and HTTP/JSON endpoints.
pub struct OtlpReceiverSource {
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    logs_tx: mpsc::Sender<SignalBatch>,
    traces_tx: mpsc::Sender<SignalBatch>,
    metrics_tx: mpsc::Sender<SignalBatch>,
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
    ) -> Self {
        Self {
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
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

        let grpc_server = tonic::transport::Server::builder()
            .add_service(LogsServiceServer::new(grpc_service_logs))
            .add_service(TraceServiceServer::new(grpc_service_traces))
            .add_service(MetricsServiceServer::new(grpc_service_metrics))
            .serve(self.grpc_addr);

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

        let http_server = axum::serve(listener, app);

        tracing::info!("gRPC server listening on {}", self.grpc_addr);
        tracing::info!("HTTP server listening on {}", self.http_addr);

        tokio::select! {
            res = grpc_server => {
                res.map_err(|e| PipelineError::Internal(format!("gRPC server error: {e}")))?;
            }
            res = http_server => {
                res.map_err(|e| PipelineError::Internal(format!("HTTP server error: {e}")))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    #[tokio::test]
    async fn test_source_bind() {
        let (logs_tx, _logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);

        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let grpc_addr = SocketAddr::new(loopback, 0);
        let http_addr = SocketAddr::new(loopback, 0);

        let mut source =
            OtlpReceiverSource::new(grpc_addr, http_addr, logs_tx, traces_tx, metrics_tx);

        // Run source with a timeout to verify it can bind successfully
        let handle = tokio::spawn(async move {
            let _ = source.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        handle.abort();
    }
}
