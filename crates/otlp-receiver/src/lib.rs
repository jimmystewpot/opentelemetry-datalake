use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, Request as AxumRequest, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER},
    },
    middleware::{self, Next},
    response::IntoResponse,
    routing::post,
};
use opentelemetry::{KeyValue, metrics::Counter};
use std::io::Read;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tonic::{Request, Response, Status, codec::CompressionEncoding};

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

/// Default maximum decompressed request payload size (16 MiB).
pub const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Minimum permitted `max_body_bytes` value (1 KiB).
pub const MIN_MAX_BODY_BYTES: usize = 1024;

/// Default maximum concurrent in-flight requests per protocol listener.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 256;

/// Default maximum duration in milliseconds to wait on a full downstream channel before shedding.
pub const DEFAULT_INGEST_SEND_TIMEOUT_MS: u64 = 500;

/// Resource bounds and overload shedding configuration for [`OtlpReceiverSource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtlpReceiverConfig {
    /// Maximum decompressed request payload size in bytes (`>= 1024`).
    pub max_body_bytes: usize,
    /// Maximum concurrent in-flight requests per protocol listener (`>= 1`).
    pub max_concurrent_requests: usize,
    /// Maximum time in milliseconds to wait on a semaphore permit or full channel before shedding (`>= 1`).
    pub ingest_send_timeout_ms: u64,
}

impl Default for OtlpReceiverConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            ingest_send_timeout_ms: DEFAULT_INGEST_SEND_TIMEOUT_MS,
        }
    }
}

impl OtlpReceiverConfig {
    /// Validates receiver bounds before starting the servers.
    ///
    /// # Errors
    /// Returns [`PipelineError::Configuration`] if any bound is below its minimum threshold.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if self.max_body_bytes < MIN_MAX_BODY_BYTES {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from(format!(
                    "server.max_body_bytes must be >= {MIN_MAX_BODY_BYTES} (got {})",
                    self.max_body_bytes
                )),
            )));
        }
        if self.max_concurrent_requests == 0 {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from("server.max_concurrent_requests must be >= 1 (got 0)"),
            )));
        }
        if self.ingest_send_timeout_ms == 0 {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from("server.ingest_send_timeout_ms must be >= 1 (got 0)"),
            )));
        }
        Ok(())
    }
}

static SHED_LOGS_GRPC: AtomicU64 = AtomicU64::new(0);
static SHED_TRACES_GRPC: AtomicU64 = AtomicU64::new(0);
static SHED_METRICS_GRPC: AtomicU64 = AtomicU64::new(0);
static SHED_LOGS_HTTP: AtomicU64 = AtomicU64::new(0);
static SHED_TRACES_HTTP: AtomicU64 = AtomicU64::new(0);
static SHED_METRICS_HTTP: AtomicU64 = AtomicU64::new(0);

/// Returns the cumulative number of shed requests for the given `(signal, protocol)` pair.
#[must_use]
pub fn shed_requests_total(signal: &str, protocol: &str) -> u64 {
    match (signal, protocol) {
        ("logs", "grpc") => SHED_LOGS_GRPC.load(Ordering::Relaxed),
        ("traces", "grpc") => SHED_TRACES_GRPC.load(Ordering::Relaxed),
        ("metrics", "grpc") => SHED_METRICS_GRPC.load(Ordering::Relaxed),
        ("logs", "http") => SHED_LOGS_HTTP.load(Ordering::Relaxed),
        ("traces", "http") => SHED_TRACES_HTTP.load(Ordering::Relaxed),
        ("metrics", "http") => SHED_METRICS_HTTP.load(Ordering::Relaxed),
        _ => 0,
    }
}

fn build_requests_shed_counter() -> Counter<u64> {
    opentelemetry::global::meter("otel-datalake")
        .u64_counter("datalake.receiver.requests_shed_total")
        .with_unit("{request}")
        .with_description(
            "Requests rejected with gRPC UNAVAILABLE / HTTP 503 when the receiver concurrency limit or downstream channel send timeout is reached",
        )
        .build()
}

fn record_request_shed(shed_counter: &Counter<u64>, signal: &'static str, protocol: &'static str) {
    match (signal, protocol) {
        ("logs", "grpc") => {
            SHED_LOGS_GRPC.fetch_add(1, Ordering::Relaxed);
        }
        ("traces", "grpc") => {
            SHED_TRACES_GRPC.fetch_add(1, Ordering::Relaxed);
        }
        ("metrics", "grpc") => {
            SHED_METRICS_GRPC.fetch_add(1, Ordering::Relaxed);
        }
        ("logs", "http") => {
            SHED_LOGS_HTTP.fetch_add(1, Ordering::Relaxed);
        }
        ("traces", "http") => {
            SHED_TRACES_HTTP.fetch_add(1, Ordering::Relaxed);
        }
        ("metrics", "http") => {
            SHED_METRICS_HTTP.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }

    shed_counter.add(
        1,
        &[
            KeyValue::new("signal", signal),
            KeyValue::new("protocol", protocol),
        ],
    );
}

fn remaining_timeout(deadline: tokio::time::Instant) -> Duration {
    deadline.saturating_duration_since(tokio::time::Instant::now())
}

struct ConcurrencyPermit {
    _permit: OwnedSemaphorePermit,
}

type GrpcGateState = (Arc<Semaphore>, Duration, Counter<u64>);

async fn grpc_concurrency_gate(
    State((semaphore, send_timeout, shed_counter)): State<GrpcGateState>,
    mut req: AxumRequest,
    next: Next,
) -> axum::response::Response {
    let signal = match req.uri().path() {
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export" => Some("logs"),
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export" => Some("traces"),
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export" => Some("metrics"),
        _ => None,
    };

    let mut _gate_permit: Option<Arc<ConcurrencyPermit>> = None;
    if let Some(sig) = signal {
        let deadline = tokio::time::Instant::now() + send_timeout;
        let Some(permit) = acquire_concurrency_permit(&semaphore, send_timeout).await else {
            record_request_shed(&shed_counter, sig, "grpc");
            return Status::unavailable("receiver concurrency limit reached: request shed")
                .into_http();
        };
        let shared_permit = Arc::new(ConcurrencyPermit { _permit: permit });
        req.extensions_mut().insert(Arc::clone(&shared_permit));
        req.extensions_mut().insert(deadline);
        _gate_permit = Some(shared_permit);
    }

    next.run(req).await
}

async fn acquire_concurrency_permit(
    semaphore: &Arc<Semaphore>,
    timeout: Duration,
) -> Option<OwnedSemaphorePermit> {
    match tokio::time::timeout(timeout, Arc::clone(semaphore).acquire_owned()).await {
        Ok(Ok(permit)) => Some(permit),
        _ => None,
    }
}

async fn send_batch_grpc(
    tx: &mpsc::Sender<SignalBatch>,
    batch: SignalBatch,
    timeout: Duration,
    signal: &'static str,
    shed_counter: &Counter<u64>,
) -> Result<(), Status> {
    match tx.send_timeout(batch, timeout).await {
        Ok(()) => Ok(()),
        Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
            record_request_shed(shed_counter, signal, "grpc");
            Err(Status::unavailable(
                "pipeline channel full: request shed after send timeout",
            ))
        }
        Err(mpsc::error::SendTimeoutError::Closed(_)) => {
            Err(Status::unavailable("pipeline downstream closed"))
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HttpReject {
    status: StatusCode,
    retry_after: Option<&'static str>,
    body: &'static str,
}

impl HttpReject {
    const fn status_only(status: StatusCode) -> Self {
        Self {
            status,
            retry_after: None,
            body: "",
        }
    }

    const fn service_unavailable(body: &'static str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            retry_after: Some("1"),
            body,
        }
    }
}

impl IntoResponse for HttpReject {
    fn into_response(self) -> axum::response::Response {
        match self.retry_after {
            Some(retry) => (self.status, [(RETRY_AFTER, retry)], self.body).into_response(),
            None => self.status.into_response(),
        }
    }
}

async fn send_batch_http(
    tx: &mpsc::Sender<SignalBatch>,
    batch: SignalBatch,
    timeout: Duration,
    signal: &'static str,
    shed_counter: &Counter<u64>,
) -> Result<(), HttpReject> {
    match tx.send_timeout(batch, timeout).await {
        Ok(()) => Ok(()),
        Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
            record_request_shed(shed_counter, signal, "http");
            Err(HttpReject::service_unavailable(
                "pipeline channel full: request shed after send timeout",
            ))
        }
        Err(mpsc::error::SendTimeoutError::Closed(_)) => Err(HttpReject::service_unavailable(
            "pipeline downstream closed",
        )),
    }
}

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

async fn retain_or_acquire_grpc_permit<T>(
    request: &Request<T>,
    semaphore: &Arc<Semaphore>,
    send_timeout: Duration,
    shed_counter: &Counter<u64>,
    signal: &'static str,
) -> Result<(Arc<ConcurrencyPermit>, tokio::time::Instant), Status> {
    if let Some(ext_permit) = request
        .extensions()
        .get::<Arc<ConcurrencyPermit>>()
        .cloned()
    {
        let dl = request
            .extensions()
            .get::<tokio::time::Instant>()
            .copied()
            .unwrap_or_else(|| tokio::time::Instant::now() + send_timeout);
        Ok((ext_permit, dl))
    } else {
        let dl = tokio::time::Instant::now() + send_timeout;
        let Some(permit) = acquire_concurrency_permit(semaphore, send_timeout).await else {
            record_request_shed(shed_counter, signal, "grpc");
            return Err(Status::unavailable(
                "receiver concurrency limit reached: request shed",
            ));
        };
        Ok((Arc::new(ConcurrencyPermit { _permit: permit }), dl))
    }
}

#[derive(Clone)]
struct GrpcLogsService {
    tx: mpsc::Sender<SignalBatch>,
    send_timeout: Duration,
    semaphore: Arc<Semaphore>,
    shed_counter: Counter<u64>,
}

impl GrpcLogsService {
    fn new(
        tx: mpsc::Sender<SignalBatch>,
        config: OtlpReceiverConfig,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            tx,
            send_timeout: Duration::from_millis(config.ingest_send_timeout_ms.max(1)),
            semaphore,
            shed_counter: build_requests_shed_counter(),
        }
    }
}

#[tonic::async_trait]
impl LogsService for GrpcLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let (_permit, deadline) = retain_or_acquire_grpc_permit(
            &request,
            &self.semaphore,
            self.send_timeout,
            &self.shed_counter,
            "logs",
        )
        .await?;

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
                    send_batch_grpc(
                        &self.tx,
                        SignalBatch::Logs(batch),
                        remaining_timeout(deadline),
                        "logs",
                        &self.shed_counter,
                    )
                    .await?;
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
    send_timeout: Duration,
    semaphore: Arc<Semaphore>,
    shed_counter: Counter<u64>,
}

impl GrpcTraceService {
    fn new(
        tx: mpsc::Sender<SignalBatch>,
        config: OtlpReceiverConfig,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            tx,
            send_timeout: Duration::from_millis(config.ingest_send_timeout_ms.max(1)),
            semaphore,
            shed_counter: build_requests_shed_counter(),
        }
    }
}

#[tonic::async_trait]
impl TraceService for GrpcTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let (_permit, deadline) = retain_or_acquire_grpc_permit(
            &request,
            &self.semaphore,
            self.send_timeout,
            &self.shed_counter,
            "traces",
        )
        .await?;

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
                    send_batch_grpc(
                        &self.tx,
                        SignalBatch::Traces(batch),
                        remaining_timeout(deadline),
                        "traces",
                        &self.shed_counter,
                    )
                    .await?;
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
    send_timeout: Duration,
    semaphore: Arc<Semaphore>,
    shed_counter: Counter<u64>,
}

impl GrpcMetricsService {
    fn new(
        tx: mpsc::Sender<SignalBatch>,
        config: OtlpReceiverConfig,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            tx,
            send_timeout: Duration::from_millis(config.ingest_send_timeout_ms.max(1)),
            semaphore,
            shed_counter: build_requests_shed_counter(),
        }
    }
}

#[tonic::async_trait]
impl MetricsService for GrpcMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let (_permit, deadline) = retain_or_acquire_grpc_permit(
            &request,
            &self.semaphore,
            self.send_timeout,
            &self.shed_counter,
            "metrics",
        )
        .await?;

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
                    send_batch_grpc(
                        &self.tx,
                        SignalBatch::Metrics(batch),
                        remaining_timeout(deadline),
                        "metrics",
                        &self.shed_counter,
                    )
                    .await?;
                }
                Ok(Response::new(ExportMetricsServiceResponse {
                    partial_success: None,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressedEncoding {
    Gzip,
    Zstd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentEncoding {
    Identity,
    Compressed(CompressedEncoding),
}

impl ContentEncoding {
    fn from_headers(headers: &HeaderMap) -> Result<Self, StatusCode> {
        let mut values = headers.get_all(CONTENT_ENCODING).iter();
        let Some(raw_val) = values.next() else {
            return Ok(Self::Identity);
        };
        if values.next().is_some() {
            return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        let raw_str = raw_val
            .to_str()
            .map_err(|_| StatusCode::UNSUPPORTED_MEDIA_TYPE)?;
        let trimmed = raw_str.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("identity") {
            Ok(Self::Identity)
        } else if trimmed.eq_ignore_ascii_case("gzip") || trimmed.eq_ignore_ascii_case("x-gzip") {
            Ok(Self::Compressed(CompressedEncoding::Gzip))
        } else if trimmed.eq_ignore_ascii_case("zstd") {
            Ok(Self::Compressed(CompressedEncoding::Zstd))
        } else {
            Err(StatusCode::UNSUPPORTED_MEDIA_TYPE)
        }
    }
}

fn read_bounded<R: Read>(
    mut reader: R,
    max_body_bytes: usize,
    initial_cap: usize,
) -> Result<Vec<u8>, StatusCode> {
    let mut buf = Vec::with_capacity(initial_cap.min(max_body_bytes));
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        if n == 0 {
            break;
        }
        let needed = buf.len().saturating_add(n);
        if needed > max_body_bytes {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        if needed > buf.capacity() {
            let target_cap = buf
                .capacity()
                .saturating_mul(2)
                .max(needed)
                .min(max_body_bytes);
            buf.reserve_exact(target_cap.saturating_sub(buf.len()));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

fn decompress_bounded(
    raw: &[u8],
    encoding: CompressedEncoding,
    max_body_bytes: usize,
) -> Result<Vec<u8>, StatusCode> {
    match encoding {
        CompressedEncoding::Gzip => {
            let decoder = flate2::bufread::GzDecoder::new(raw);
            read_bounded(decoder, max_body_bytes, raw.len())
        }
        CompressedEncoding::Zstd => {
            let decoder = zstd::stream::read::Decoder::with_buffer(raw)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            read_bounded(decoder, max_body_bytes, raw.len())
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
    max_body_bytes: usize,
    signal_name: &'static str,
    decode_fn: F,
) -> Result<RecordBatch, StatusCode>
where
    T: prost::Message + Default + serde::de::DeserializeOwned + Send + 'static,
    F: FnOnce(&T) -> Result<RecordBatch, PipelineError> + Send + 'static,
{
    let encoding = ContentEncoding::from_headers(&headers)?;
    if body.len() > max_body_bytes {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    if encoding != ContentEncoding::Identity || body.len() > HTTP_SPAWN_BLOCKING_THRESHOLD_BYTES {
        tokio::task::spawn_blocking(move || {
            let payload = match encoding {
                ContentEncoding::Identity => body,
                ContentEncoding::Compressed(compressed) => {
                    let decompressed = decompress_bounded(&body, compressed, max_body_bytes)?;
                    Bytes::from(decompressed)
                }
            };
            let req: T = decode_http_body(&headers, payload).map_err(|e| {
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
    config: OtlpReceiverConfig,
    semaphore: Arc<Semaphore>,
    shed_counter: Counter<u64>,
}

impl HttpState {
    fn new(
        logs_tx: mpsc::Sender<SignalBatch>,
        traces_tx: mpsc::Sender<SignalBatch>,
        metrics_tx: mpsc::Sender<SignalBatch>,
        config: OtlpReceiverConfig,
    ) -> Self {
        Self {
            logs_tx,
            traces_tx,
            metrics_tx,
            config,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_requests.max(1))),
            shed_counter: build_requests_shed_counter(),
        }
    }
}

async fn handle_http_signal<T, F, W>(
    state: HttpState,
    req: AxumRequest,
    signal: &'static str,
    signal_name: &'static str,
    tx_fn: impl FnOnce(&HttpState) -> &mpsc::Sender<SignalBatch>,
    decode_fn: F,
    wrap_batch: W,
) -> Result<impl IntoResponse, HttpReject>
where
    T: prost::Message + Default + serde::de::DeserializeOwned + Send + 'static,
    F: FnOnce(&T) -> Result<RecordBatch, PipelineError> + Send + 'static,
    W: FnOnce(RecordBatch) -> SignalBatch,
{
    let _ = ContentEncoding::from_headers(req.headers()).map_err(HttpReject::status_only)?;

    let send_timeout = Duration::from_millis(state.config.ingest_send_timeout_ms.max(1));
    let deadline = tokio::time::Instant::now() + send_timeout;
    let Some(_permit) = acquire_concurrency_permit(&state.semaphore, send_timeout).await else {
        record_request_shed(&state.shed_counter, signal, "http");
        return Err(HttpReject::service_unavailable(
            "receiver concurrency limit reached: request shed",
        ));
    };

    let headers = req.headers().clone();
    let body = Bytes::from_request(req, &state)
        .await
        .map_err(|rej| HttpReject::status_only(rej.status()))?;

    let batch = decode_and_convert_http::<T, _>(
        headers,
        body,
        state.config.max_body_bytes,
        signal_name,
        decode_fn,
    )
    .await
    .map_err(HttpReject::status_only)?;

    if batch.num_rows() > 0 {
        send_batch_http(
            tx_fn(&state),
            wrap_batch(batch),
            remaining_timeout(deadline),
            signal,
            &state.shed_counter,
        )
        .await?;
    }
    Ok((
        StatusCode::OK,
        serde_json::json!({ "partial_success": null }).to_string(),
    ))
}

async fn handle_logs(
    State(state): State<HttpState>,
    req: AxumRequest,
) -> Result<impl IntoResponse, HttpReject> {
    handle_http_signal::<ExportLogsServiceRequest, _, _>(
        state,
        req,
        "logs",
        "Logs",
        |s| &s.logs_tx,
        arrow_codec::decode_logs,
        SignalBatch::Logs,
    )
    .await
}

async fn handle_traces(
    State(state): State<HttpState>,
    req: AxumRequest,
) -> Result<impl IntoResponse, HttpReject> {
    handle_http_signal::<ExportTraceServiceRequest, _, _>(
        state,
        req,
        "traces",
        "Traces",
        |s| &s.traces_tx,
        arrow_codec::decode_traces,
        SignalBatch::Traces,
    )
    .await
}

async fn handle_metrics(
    State(state): State<HttpState>,
    req: AxumRequest,
) -> Result<impl IntoResponse, HttpReject> {
    handle_http_signal::<ExportMetricsServiceRequest, _, _>(
        state,
        req,
        "metrics",
        "Metrics",
        |s| &s.metrics_tx,
        arrow_codec::decode_metrics,
        SignalBatch::Metrics,
    )
    .await
}

fn build_http_router(http_state: HttpState) -> Router {
    let max_body_bytes = http_state.config.max_body_bytes;
    Router::new()
        .route("/v1/logs", post(handle_logs))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(http_state)
}

/// OTLP Receiver source supporting gRPC and HTTP/JSON endpoints.
pub struct OtlpReceiverSource {
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    logs_tx: mpsc::Sender<SignalBatch>,
    traces_tx: mpsc::Sender<SignalBatch>,
    metrics_tx: mpsc::Sender<SignalBatch>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    config: OtlpReceiverConfig,
}

impl OtlpReceiverSource {
    /// Creates a new `OtlpReceiverSource` with default resource bounds.
    #[must_use]
    pub fn new(
        grpc_addr: SocketAddr,
        http_addr: SocketAddr,
        logs_tx: mpsc::Sender<SignalBatch>,
        traces_tx: mpsc::Sender<SignalBatch>,
        metrics_tx: mpsc::Sender<SignalBatch>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self::with_config(
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
            shutdown_rx,
            OtlpReceiverConfig::default(),
        )
    }

    /// Creates a new `OtlpReceiverSource` with explicit resource bounds.
    #[must_use]
    pub fn with_config(
        grpc_addr: SocketAddr,
        http_addr: SocketAddr,
        logs_tx: mpsc::Sender<SignalBatch>,
        traces_tx: mpsc::Sender<SignalBatch>,
        metrics_tx: mpsc::Sender<SignalBatch>,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        config: OtlpReceiverConfig,
    ) -> Self {
        Self {
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
            shutdown_rx,
            config,
        }
    }
}

#[async_trait]
impl Source for OtlpReceiverSource {
    async fn run(&mut self) -> Result<(), PipelineError> {
        self.config.validate()?;

        let grpc_semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_requests));
        let grpc_service_logs = GrpcLogsService::new(
            self.logs_tx.clone(),
            self.config,
            Arc::clone(&grpc_semaphore),
        );
        let grpc_service_traces = GrpcTraceService::new(
            self.traces_tx.clone(),
            self.config,
            Arc::clone(&grpc_semaphore),
        );
        let grpc_service_metrics = GrpcMetricsService::new(
            self.metrics_tx.clone(),
            self.config,
            Arc::clone(&grpc_semaphore),
        );

        let mut shutdown_rx_grpc = self.shutdown_rx.clone();
        let grpc_shutdown = async move {
            let _ = shutdown_rx_grpc
                .wait_for(|shutting_down| *shutting_down)
                .await;
            tracing::info!("gRPC server shutting down gracefully");
        };

        let max_body_bytes = self.config.max_body_bytes;
        let send_timeout = Duration::from_millis(self.config.ingest_send_timeout_ms.max(1));
        let grpc_shed_counter = build_requests_shed_counter();
        let grpc_routes = tonic::service::Routes::new(
            LogsServiceServer::new(grpc_service_logs)
                .max_decoding_message_size(max_body_bytes)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
        )
        .add_service(
            TraceServiceServer::new(grpc_service_traces)
                .max_decoding_message_size(max_body_bytes)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
        )
        .add_service(
            MetricsServiceServer::new(grpc_service_metrics)
                .max_decoding_message_size(max_body_bytes)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
        )
        .into_axum_router()
        .layer(middleware::from_fn_with_state(
            (grpc_semaphore, send_timeout, grpc_shed_counter),
            grpc_concurrency_gate,
        ));

        let grpc_server = tonic::transport::Server::builder()
            .concurrency_limit_per_connection(self.config.max_concurrent_requests)
            .add_routes(tonic::service::Routes::from(grpc_routes))
            .serve_with_shutdown(self.grpc_addr, grpc_shutdown);

        let http_state = HttpState::new(
            self.logs_tx.clone(),
            self.traces_tx.clone(),
            self.metrics_tx.clone(),
            self.config,
        );

        let app = build_http_router(http_state);

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
        let svc = GrpcLogsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcTraceService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcMetricsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcLogsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcTraceService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );
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
        let svc = GrpcMetricsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );
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
        let svc = GrpcLogsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcTraceService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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

        let state = HttpState::new(
            logs_tx,
            traces_tx,
            metrics_tx,
            OtlpReceiverConfig::default(),
        );

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

        let res = handle_logs(
            State(state),
            test_http_request(headers, Bytes::from(body_bytes)),
        )
        .await;
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
        let svc = GrpcLogsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcTraceService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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
        let svc = GrpcMetricsService::new(
            tx,
            OtlpReceiverConfig::default(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_REQUESTS)),
        );

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

        let state = HttpState::new(
            logs_tx,
            traces_tx,
            metrics_tx,
            OtlpReceiverConfig::default(),
        );

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

        let res = handle_traces(
            State(state),
            test_http_request(headers, Bytes::from(body_bytes)),
        )
        .await;
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

        let state = HttpState::new(
            logs_tx,
            traces_tx,
            metrics_tx,
            OtlpReceiverConfig::default(),
        );

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

        let res = handle_metrics(
            State(state),
            test_http_request(headers, Bytes::from(body_bytes)),
        )
        .await;
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

        let state = HttpState::new(
            logs_tx,
            traces_tx,
            metrics_tx,
            OtlpReceiverConfig::default(),
        );

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

        let res = handle_logs(
            State(state),
            test_http_request(headers, Bytes::from(body_bytes)),
        )
        .await;
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

    fn test_http_request(headers: HeaderMap, body: Bytes) -> AxumRequest {
        let mut req = AxumRequest::new(axum::body::Body::from(body));
        *req.headers_mut() = headers;
        req
    }

    static SHED_METRIC_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[test]
    fn test_otlp_receiver_config_defaults_and_validation() {
        let cfg = OtlpReceiverConfig::default();
        assert_eq!(cfg.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(cfg.max_body_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.max_concurrent_requests, DEFAULT_MAX_CONCURRENT_REQUESTS);
        assert_eq!(cfg.max_concurrent_requests, 256);
        assert_eq!(cfg.ingest_send_timeout_ms, DEFAULT_INGEST_SEND_TIMEOUT_MS);
        assert_eq!(cfg.ingest_send_timeout_ms, 500);
        assert!(cfg.validate().is_ok());

        let too_small_body = OtlpReceiverConfig {
            max_body_bytes: 512,
            ..Default::default()
        };
        assert!(matches!(
            too_small_body.validate(),
            Err(PipelineError::Configuration(_))
        ));

        let zero_concurrency = OtlpReceiverConfig {
            max_concurrent_requests: 0,
            ..Default::default()
        };
        assert!(matches!(
            zero_concurrency.validate(),
            Err(PipelineError::Configuration(_))
        ));

        let zero_timeout = OtlpReceiverConfig {
            ingest_send_timeout_ms: 0,
            ..Default::default()
        };
        assert!(matches!(
            zero_timeout.validate(),
            Err(PipelineError::Configuration(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_grpc_full_channel_sheds_after_timeout_and_increments_metric() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let _guard = SHED_METRIC_TEST_LOCK.lock().await;

        let cfg = OtlpReceiverConfig {
            ingest_send_timeout_ms: 250,
            ..Default::default()
        };
        let sem = Arc::new(Semaphore::new(cfg.max_concurrent_requests));

        // 1. Logs
        let (logs_tx, _logs_rx) = mpsc::channel(1);
        let logs_svc = GrpcLogsService::new(logs_tx, cfg, Arc::clone(&sem));
        let log_req = ExportLogsServiceRequest {
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
        };
        logs_svc
            .export(Request::new(log_req.clone()))
            .await
            .unwrap();
        let before_logs = shed_requests_total("logs", "grpc");
        let err = logs_svc
            .export(Request::new(log_req))
            .await
            .expect_err("full logs channel should shed after timeout");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(
            err.message().contains("shed"),
            "Expected message containing 'shed', got: {}",
            err.message()
        );
        assert_eq!(shed_requests_total("logs", "grpc"), before_logs + 1);

        // 2. Traces
        let (traces_tx, _traces_rx) = mpsc::channel(1);
        let traces_svc = GrpcTraceService::new(traces_tx, cfg, Arc::clone(&sem));
        let trace_req = ExportTraceServiceRequest {
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
        };
        traces_svc
            .export(Request::new(trace_req.clone()))
            .await
            .unwrap();
        let before_traces = shed_requests_total("traces", "grpc");
        let err = traces_svc
            .export(Request::new(trace_req))
            .await
            .expect_err("full traces channel should shed after timeout");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("shed"));
        assert_eq!(shed_requests_total("traces", "grpc"), before_traces + 1);

        // 3. Metrics
        let (metrics_tx, _metrics_rx) = mpsc::channel(1);
        let metrics_svc = GrpcMetricsService::new(metrics_tx, cfg, Arc::clone(&sem));
        let metrics_req = ExportMetricsServiceRequest {
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
        };
        metrics_svc
            .export(Request::new(metrics_req.clone()))
            .await
            .unwrap();
        let before_metrics = shed_requests_total("metrics", "grpc");
        let err = metrics_svc
            .export(Request::new(metrics_req))
            .await
            .expect_err("full metrics channel should shed after timeout");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("shed"));
        assert_eq!(shed_requests_total("metrics", "grpc"), before_metrics + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_http_full_channel_sheds_with_503_and_retry_after_header() {
        let _guard = SHED_METRIC_TEST_LOCK.lock().await;

        let (logs_tx, logs_rx) = mpsc::channel(1);
        let (traces_tx, _traces_rx) = mpsc::channel(1);
        let (metrics_tx, _metrics_rx) = mpsc::channel(1);

        let cfg = OtlpReceiverConfig {
            ingest_send_timeout_ms: 200,
            ..Default::default()
        };
        let state = HttpState::new(logs_tx, traces_tx, metrics_tx, cfg);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let json_body = Bytes::from_static(
            br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1000000000"}]}]}]}"#,
        );

        // First request fills the capacity-1 channel
        let first_res = handle_logs(
            State(state.clone()),
            test_http_request(headers.clone(), json_body.clone()),
        )
        .await;
        assert!(first_res.is_ok());

        // Second request blocks until timeout and sheds with 503 + Retry-After: 1
        let before_http_logs = shed_requests_total("logs", "http");
        let second_res = handle_logs(
            State(state.clone()),
            test_http_request(headers.clone(), json_body.clone()),
        )
        .await;
        assert!(second_res.is_err());
        let resp = second_res.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert_eq!(shed_requests_total("logs", "http"), before_http_logs + 1);

        // Closed channel returns 503 + Retry-After: 1 WITHOUT incrementing shed counter
        drop(logs_rx);
        let before_closed = shed_requests_total("logs", "http");
        let closed_res = handle_logs(State(state), test_http_request(headers, json_body)).await;
        assert!(closed_res.is_err());
        let closed_resp = closed_res.into_response();
        assert_eq!(closed_resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            closed_resp
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert_eq!(shed_requests_total("logs", "http"), before_closed);
    }

    #[tokio::test]
    async fn test_http_gzip_and_zstd_decompression_roundtrip() {
        use axum::http::header::CONTENT_ENCODING;
        use flate2::{Compression, write::GzEncoder};
        use prost::Message;
        use std::io::Write;

        let json_payload =
            br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1000000000"}]}]}]}"#;

        let proto_req = ExportLogsServiceRequest {
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
        };
        let mut proto_payload = Vec::new();
        proto_req.encode(&mut proto_payload).unwrap();

        // 1. Gzip JSON ("gzip")
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(json_payload).unwrap();
        let gz_json = gz.finish().unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let batch = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            headers,
            Bytes::from(gz_json),
            DEFAULT_MAX_BODY_BYTES,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await
        .expect("gzip JSON should decode");
        assert_eq!(batch.num_rows(), 1);

        // 2. Gzip Protobuf ("GZIP" uppercase)
        let mut gz_proto = GzEncoder::new(Vec::new(), Compression::default());
        gz_proto.write_all(&proto_payload).unwrap();
        let gz_proto_bytes = gz_proto.finish().unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "GZIP".parse().unwrap());
        let batch = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            headers,
            Bytes::from(gz_proto_bytes),
            DEFAULT_MAX_BODY_BYTES,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await
        .expect("GZIP protobuf should decode");
        assert_eq!(batch.num_rows(), 1);

        // 3. Zstd JSON ("zstd")
        let zstd_json = zstd::stream::encode_all(&json_payload[..], 3).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
        let batch = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            headers,
            Bytes::from(zstd_json),
            DEFAULT_MAX_BODY_BYTES,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await
        .expect("zstd JSON should decode");
        assert_eq!(batch.num_rows(), 1);

        // 4. Zstd Protobuf through HTTP handler
        let zstd_proto = zstd::stream::encode_all(&proto_payload[..], 3).unwrap();
        let (logs_tx, mut logs_rx) = mpsc::channel(4);
        let (traces_tx, _traces_rx) = mpsc::channel(4);
        let (metrics_tx, _metrics_rx) = mpsc::channel(4);
        let state = HttpState::new(
            logs_tx,
            traces_tx,
            metrics_tx,
            OtlpReceiverConfig::default(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
        let resp = handle_logs(
            State(state),
            test_http_request(headers, Bytes::from(zstd_proto)),
        )
        .await
        .expect("zstd protobuf handler should succeed")
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        if let SignalBatch::Logs(b) = logs_rx.try_recv().unwrap() {
            assert_eq!(b.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Logs");
        }
    }

    #[tokio::test]
    async fn test_http_decompression_bomb_and_corrupt_encoding_and_unsupported_encoding() {
        use axum::http::header::CONTENT_ENCODING;
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;

        // 1. Decompression bomb: 64 KiB of zeros compressed with gzip (< 200 bytes on wire) vs max_body_bytes = 4096
        let zeros = vec![0u8; 64 * 1024];
        let mut gz = GzEncoder::new(Vec::new(), Compression::best());
        gz.write_all(&zeros).unwrap();
        let bomb_bytes = gz.finish().unwrap();
        assert!(bomb_bytes.len() < 1024);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            headers,
            Bytes::from(bomb_bytes),
            4096,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(res.unwrap_err(), StatusCode::PAYLOAD_TOO_LARGE);

        // 2. Corrupt gzip and corrupt zstd -> 400 Bad Request
        let mut bad_gz_headers = HeaderMap::new();
        bad_gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            bad_gz_headers,
            Bytes::from_static(b"not a valid gzip stream"),
            4096,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(res.unwrap_err(), StatusCode::BAD_REQUEST);

        let mut bad_zstd_headers = HeaderMap::new();
        bad_zstd_headers.insert(CONTENT_ENCODING, "zstd".parse().unwrap());
        let res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            bad_zstd_headers,
            Bytes::from_static(b"not a valid zstd stream"),
            4096,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(res.unwrap_err(), StatusCode::BAD_REQUEST);

        // 3. Unsupported Content-Encoding ("br" and comma-chained "gzip, deflate") -> 415 Unsupported Media Type
        let mut br_headers = HeaderMap::new();
        br_headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
        let res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            br_headers,
            Bytes::from_static(b"{}"),
            4096,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(res.unwrap_err(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let mut chained_headers = HeaderMap::new();
        chained_headers.insert(CONTENT_ENCODING, "gzip, deflate".parse().unwrap());
        let res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            chained_headers,
            Bytes::from_static(b"{}"),
            4096,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(res.unwrap_err(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn test_grpc_compressed_requests_and_max_decoding_message_size() {
        use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
        use tonic::codec::CompressionEncoding;

        // Reserve two free loopback ports
        let grpc_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let grpc_addr = grpc_listener.local_addr().unwrap();
        drop(grpc_listener);

        let http_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        drop(http_listener);

        let (logs_tx, mut logs_rx) = mpsc::channel(10);
        let (traces_tx, _traces_rx) = mpsc::channel(10);
        let (metrics_tx, _metrics_rx) = mpsc::channel(10);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let cfg = OtlpReceiverConfig {
            max_body_bytes: 2048,
            max_concurrent_requests: 16,
            ingest_send_timeout_ms: 500,
        };
        let mut source = OtlpReceiverSource::with_config(
            grpc_addr,
            http_addr,
            logs_tx,
            traces_tx,
            metrics_tx,
            shutdown_rx,
            cfg,
        );

        let server_handle = tokio::spawn(async move {
            let _ = source.run().await;
        });

        // Connect gRPC client with retry while server binds
        let endpoint = format!("http://{grpc_addr}");
        let mut client = None;
        for _ in 0..25 {
            if let Ok(c) = LogsServiceClient::connect(endpoint.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        let mut client = client
            .expect("gRPC client should connect to OtlpReceiverSource")
            .send_compressed(CompressionEncoding::Gzip);

        // 1. Small Gzip-compressed request succeeds
        let small_req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("hello gzip".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        client
            .export(Request::new(small_req))
            .await
            .expect("gzip compressed gRPC logs request should succeed");
        let received = logs_rx.recv().await.expect("should receive logs batch");
        if let SignalBatch::Logs(batch) = received {
            assert_eq!(batch.num_rows(), 1);
        } else {
            panic!("Expected SignalBatch::Logs");
        }

        // 2. Oversized request (decoded > 2048 bytes, even when gzip compressed) is rejected with ResourceExhausted
        let oversized_req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("A".repeat(8192))),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let err = client
            .export(Request::new(oversized_req))
            .await
            .expect_err("oversized decoded gRPC message must fail");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        let _ = shutdown_tx.send(true);
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_receiver_concurrency_limit_sheds_when_permits_exhausted() {
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let _guard = SHED_METRIC_TEST_LOCK.lock().await;

        let cfg = OtlpReceiverConfig {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent_requests: 1,
            ingest_send_timeout_ms: 100,
        };

        // 1. gRPC concurrency limit exhaustion
        let grpc_sem = Arc::new(Semaphore::new(1));
        let _held_grpc_permit = grpc_sem.acquire().await.unwrap();
        let (logs_tx, _logs_rx) = mpsc::channel(4);
        let grpc_svc = GrpcLogsService::new(logs_tx, cfg, Arc::clone(&grpc_sem));

        let before_grpc = shed_requests_total("logs", "grpc");
        let err = grpc_svc
            .export(Request::new(ExportLogsServiceRequest::default()))
            .await
            .expect_err("gRPC export should shed when semaphore permits are exhausted");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(shed_requests_total("logs", "grpc"), before_grpc + 1);

        // 2. HTTP concurrency limit exhaustion
        let (logs_tx, _logs_rx) = mpsc::channel(4);
        let (traces_tx, _traces_rx) = mpsc::channel(4);
        let (metrics_tx, _metrics_rx) = mpsc::channel(4);
        let state = HttpState::new(logs_tx, traces_tx, metrics_tx, cfg);
        let _held_http_permit = state.semaphore.acquire().await.unwrap();

        let before_http = shed_requests_total("logs", "http");
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        let res = handle_logs(
            State(state.clone()),
            test_http_request(headers, Bytes::from_static(br#"{"resourceLogs":[]}"#)),
        )
        .await;
        assert!(res.is_err());
        let resp = res.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        assert_eq!(shed_requests_total("logs", "http"), before_http + 1);
    }

    #[tokio::test]
    async fn test_http_raw_body_exceeding_max_body_bytes_rejected_with_413() {
        use axum::http::header::CONTENT_ENCODING;

        let cfg = OtlpReceiverConfig {
            max_body_bytes: 1024,
            max_concurrent_requests: 16,
            ingest_send_timeout_ms: 500,
        };
        let (logs_tx, _logs_rx) = mpsc::channel(4);
        let (traces_tx, _traces_rx) = mpsc::channel(4);
        let (metrics_tx, _metrics_rx) = mpsc::channel(4);
        let state = HttpState::new(logs_tx, traces_tx, metrics_tx, cfg);

        let oversized_body = Bytes::from(vec![b' '; 2048]);
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        // 1. Direct decode_and_convert_http rejects uncompressed payload > max_body_bytes with 413
        let direct_res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            headers.clone(),
            oversized_body.clone(),
            cfg.max_body_bytes,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(direct_res.unwrap_err(), StatusCode::PAYLOAD_TOO_LARGE);

        // 2. HTTP router with DefaultBodyLimit::max(max_body_bytes) rejects > max_body_bytes with 413 before buffering
        let mut app = build_http_router(state);
        let mut req = AxumRequest::builder()
            .method("POST")
            .uri("/v1/logs")
            .body(axum::body::Body::from(oversized_body))
            .unwrap();
        *req.headers_mut() = headers;
        let resp = tonic::codegen::Service::call(&mut app, req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

        // 3. Multiple Content-Encoding headers rejected with 415 Unsupported Media Type
        let mut multi_enc_headers = HeaderMap::new();
        multi_enc_headers.append(CONTENT_ENCODING, "gzip".parse().unwrap());
        multi_enc_headers.append(CONTENT_ENCODING, "gzip".parse().unwrap());
        let multi_res = decode_and_convert_http::<ExportLogsServiceRequest, _>(
            multi_enc_headers,
            Bytes::from_static(b"{}"),
            cfg.max_body_bytes,
            "Logs",
            arrow_codec::decode_logs,
        )
        .await;
        assert_eq!(multi_res.unwrap_err(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test(start_paused = true)]
    async fn test_grpc_extension_permit_retained_across_into_inner_and_channel_send() {
        let cfg = OtlpReceiverConfig {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent_requests: 1,
            ingest_send_timeout_ms: 500,
        };
        let grpc_sem = Arc::new(Semaphore::new(1));
        let non_empty_req = ExportLogsServiceRequest {
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
        };
        let (logs_tx, _logs_rx) = mpsc::channel(1);
        // Pre-fill the channel so export() parks on send_batch_grpc after request.into_inner().
        logs_tx
            .send(SignalBatch::Logs(
                arrow_codec::decode_logs(&non_empty_req).unwrap(),
            ))
            .await
            .unwrap();

        let grpc_svc = GrpcLogsService::new(logs_tx, cfg, Arc::clone(&grpc_sem));

        // Simulate a request admitted by grpc_concurrency_gate carrying the sole Arc<ConcurrencyPermit>
        // in its extensions.
        let gate_permit = grpc_sem.clone().acquire_owned().await.unwrap();
        assert_eq!(grpc_sem.available_permits(), 0);

        let mut req1 = Request::new(non_empty_req);
        req1.extensions_mut().insert(Arc::new(ConcurrencyPermit {
            _permit: gate_permit,
        }));
        req1.extensions_mut()
            .insert(tokio::time::Instant::now() + Duration::from_millis(500));

        let svc_clone = grpc_svc.clone();
        let in_flight = tokio::spawn(async move { svc_clone.export(req1).await });

        // Yield so in_flight enters export(), calls request.into_inner(), decodes the batch,
        // and parks on send_batch_grpc.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        // The concurrency permit MUST still be held while export() is parked on send_batch_grpc.
        assert_eq!(
            grpc_sem.available_permits(),
            0,
            "concurrency permit was dropped prematurely by request.into_inner()"
        );

        let err = in_flight.await.unwrap().unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert_eq!(grpc_sem.available_permits(), 1);
    }
}
