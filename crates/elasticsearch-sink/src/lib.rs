//! Elasticsearch and `OpenSearch` sink for the `opentelemetry-datalake` pipeline.

// Compile-time guard: at least one TLS root store feature must be enabled.
#[cfg(not(any(feature = "tls-native-roots", feature = "tls-webpki-roots")))]
compile_error!(
    "At least one TLS root store feature must be enabled in `elasticsearch-sink`: \
     enable `tls-native-roots` and/or `tls-webpki-roots`."
);

pub mod client;
pub mod config;
pub mod error;
pub mod serializer;
pub mod tls;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use pipeline_core::buffer::{FailureClass, Lease, SignalKind, SinkBuffer};
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::Sink;
use pipeline_core::sort::{BatchSorter, SignalType};

pub use client::{BulkItem, BulkItemError, BulkItemWrapper, BulkResponse, HttpClient};
pub use config::{
    DataStreamMapping, ElasticsearchAuthConfig, ElasticsearchBatchingConfig,
    ElasticsearchSinkConfig,
};
pub use error::ElasticsearchError;
pub use serializer::{
    BulkChunk, serialize_batch, serialize_batch_chunks, serialize_batch_chunks_with_ranges,
};
pub use tls::TlsConfig;

/// Result of a single dispatched [`BulkChunk`] task: `(start_row, row_count, response)`.
type ChunkTaskOutput = (usize, usize, Result<BulkResponse, ElasticsearchError>);

/// Elasticsearch and `OpenSearch` sink for the `opentelemetry-datalake` pipeline.
///
/// Acquires coalesced [`Lease`]s from [`SinkBuffer`], offloads chronological sorting
/// ([`BatchSorter`]) and NDJSON chunk serialization ([`serialize_batch_chunks_with_ranges`])
/// onto [`Lease::prepare`], and streams all chunks concurrently into Elasticsearch/`OpenSearch`
/// Data Streams via HTTP/2 using [`HttpClient`].
///
/// # Concurrency & Delivery Guarantees
///
/// Implements [`Sink`], driving up to `max_inflight_leases` concurrent lease tasks in a
/// [`tokio::task::JoinSet`]. Within each lease, bulk chunks are bounded by the shared
/// [`tokio::sync::Semaphore`] enforcing `max_concurrent_requests`. A lease is acknowledged
/// (`lease.ack()`) only after all of its bulk chunks succeed; unrecoverable per-item errors
/// (e.g. HTTP `400`) are sliced by exact row index and quarantined via
/// [`Lease::quarantine_batches`].
#[derive(Debug)]
pub struct ElasticsearchSink {
    config: ElasticsearchSinkConfig,
    client: Arc<HttpClient>,
    sorter: BatchSorter,
    semaphore: Arc<tokio::sync::Semaphore>,
    validated: Arc<std::sync::atomic::AtomicBool>,
}

impl ElasticsearchSink {
    /// Constructs a new `ElasticsearchSink` from configuration.
    ///
    /// Initializes the HTTP connection pool, pre-sorting engine, and concurrency semaphore.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError`] if:
    /// - `max_concurrent_requests` is 0.
    /// - Endpoint configuration is invalid.
    /// - TLS certificate loading fails.
    /// - Sorter configuration parsing fails.
    pub fn try_new(config: ElasticsearchSinkConfig) -> Result<Self, PipelineError> {
        config
            .validate()
            .map_err(|e| PipelineError::Internal(e.to_string()))?;

        let client = HttpClient::try_new(&config)?;
        let sorter = if let Some(ref order_by) = config.order_by {
            BatchSorter::from_config(order_by)?
        } else {
            BatchSorter::default()
        };
        let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_requests));

        Ok(Self {
            config,
            client: Arc::new(client),
            sorter,
            semaphore,
            validated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Shares the validation status handle with another sink instance to prevent redundant checks.
    pub fn share_validation_from(&mut self, other: &Self) {
        self.validated = Arc::clone(&other.validated);
    }

    /// Shares the HTTP client, concurrency limiter semaphore, and validation status handle
    /// with another sink instance to ensure unified cluster concurrency limits and connection reuse.
    pub fn share_state_from(&mut self, other: &Self) {
        self.client = Arc::clone(&other.client);
        self.semaphore = Arc::clone(&other.semaphore);
        self.validated = Arc::clone(&other.validated);
    }

    /// Shares the concurrency limiter semaphore with another sink instance.
    pub fn share_concurrency_from(&mut self, other: &Self) {
        self.semaphore = Arc::clone(&other.semaphore);
    }

    /// Returns a clone of the internal concurrency limiter semaphore.
    #[must_use]
    pub fn semaphore(&self) -> Arc<tokio::sync::Semaphore> {
        Arc::clone(&self.semaphore)
    }

    /// Returns a reference to the active sink configuration.
    #[must_use]
    pub fn config(&self) -> &ElasticsearchSinkConfig {
        &self.config
    }

    /// Returns a reference to the internal [`HttpClient`].
    #[must_use]
    pub fn client(&self) -> &HttpClient {
        &self.client
    }

    /// Returns a reference to the internal [`BatchSorter`].
    #[must_use]
    pub fn sorter(&self) -> &BatchSorter {
        &self.sorter
    }

    /// Returns the configured data stream name for the given signal type.
    #[must_use]
    pub fn data_stream_for(&self, signal_type: SignalType) -> &str {
        match signal_type {
            SignalType::Logs => &self.config.data_streams.logs,
            SignalType::Metrics => &self.config.data_streams.metrics,
            SignalType::Traces => &self.config.data_streams.traces,
        }
    }

    /// Performs startup cluster health check and index template validation if configured.
    ///
    /// If `validate_on_startup` is enabled, verifies cluster health and data stream templates.
    /// Idempotent: once successfully validated, subsequent calls are no-ops.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError`] if the cluster health check fails or any required index template is missing.
    pub async fn validate_startup(&self) -> Result<(), PipelineError> {
        if !self.config.validate_on_startup
            || self.validated.load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }

        tracing::info!("Executing startup validation checks");
        self.client.health_check().await?;
        self.client
            .validate_index_template(&self.config.data_streams.logs)
            .await?;
        self.client
            .validate_index_template(&self.config.data_streams.metrics)
            .await?;
        self.client
            .validate_index_template(&self.config.data_streams.traces)
            .await?;
        self.validated
            .store(true, std::sync::atomic::Ordering::Release);
        tracing::info!("Elasticsearch sink startup validation passed");
        Ok(())
    }

    /// Classifies an [`ElasticsearchError`] into a [`FailureClass`] for [`Lease::nack`].
    fn classify_elasticsearch_error(err: &ElasticsearchError) -> FailureClass {
        match err {
            ElasticsearchError::AuthenticationFailed(_) => FailureClass::Fatal {
                reason: err.to_string(),
            },
            ElasticsearchError::Http(_) => FailureClass::Transient,
            ElasticsearchError::BulkFailed { message, .. } => {
                if message.starts_with("HTTP 429")
                    || message.starts_with("HTTP 500")
                    || message.starts_with("HTTP 502")
                    || message.starts_with("HTTP 503")
                    || message.starts_with("HTTP 504")
                    || message.starts_with("network error")
                    || message.contains("recoverable status (429/500/503)")
                {
                    FailureClass::Transient
                } else {
                    FailureClass::Rejected {
                        reason: err.to_string(),
                    }
                }
            }
            ElasticsearchError::Serialization(_)
            | ElasticsearchError::PayloadTooLarge { .. }
            | ElasticsearchError::StartupValidation(_) => FailureClass::Rejected {
                reason: err.to_string(),
            },
        }
    }

    /// Records the result of an in-flight chunk task into the outcomes or error accumulators.
    fn record_task_result(
        res: Result<ChunkTaskOutput, tokio::task::JoinError>,
        outcomes: &mut Vec<(usize, usize, BulkResponse)>,
        first_es_err: &mut Option<ElasticsearchError>,
        first_join_err: &mut Option<PipelineError>,
    ) {
        match res {
            Ok((start_row, row_count, Ok(resp))) => {
                outcomes.push((start_row, row_count, resp));
            }
            Ok((_start_row, _row_count, Err(es_err))) => {
                if first_es_err.is_none() {
                    *first_es_err = Some(es_err);
                }
            }
            Err(join_err) => {
                if first_join_err.is_none() {
                    *first_join_err = Some(PipelineError::Internal(format!(
                        "Bulk dispatch task failed: {join_err}"
                    )));
                }
            }
        }
    }

    /// Dispatches a single [`BulkChunk`] under the concurrency semaphore.
    ///
    /// If permits are exhausted, awaits the next completed task in `join_set`. Any error
    /// from previously dispatched tasks is retained in `first_es_err` / `first_join_err`
    /// without aborting the submission of the current chunk once a permit is freed.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch(
        client: Arc<HttpClient>,
        semaphore: Arc<tokio::sync::Semaphore>,
        join_set: &mut tokio::task::JoinSet<ChunkTaskOutput>,
        data_stream: String,
        chunk: BulkChunk,
        outcomes: &mut Vec<(usize, usize, BulkResponse)>,
        first_es_err: &mut Option<ElasticsearchError>,
        first_join_err: &mut Option<PipelineError>,
    ) -> Result<(), PipelineError> {
        if chunk.payload.is_empty() || chunk.row_count == 0 {
            return Ok(());
        }

        while let Some(res) = join_set.try_join_next() {
            Self::record_task_result(res, outcomes, first_es_err, first_join_err);
        }

        while semaphore.available_permits() == 0 && !join_set.is_empty() {
            if let Some(res) = join_set.join_next().await {
                Self::record_task_result(res, outcomes, first_es_err, first_join_err);
            }
        }

        let permit = semaphore.acquire_owned().await.map_err(|e| {
            PipelineError::Internal(format!("Failed to acquire semaphore permit: {e}"))
        })?;

        join_set.spawn(async move {
            let _permit = permit;
            let res = client.send_bulk(&data_stream, chunk.payload).await;
            (chunk.start_row, chunk.row_count, res)
        });

        Ok(())
    }

    /// Awaits all remaining in-flight chunk tasks in `join_set`.
    ///
    /// Drains all tasks to completion before returning to ensure no in-flight requests are
    /// abruptly aborted on the first failure.
    async fn drain_join_set(
        join_set: &mut tokio::task::JoinSet<ChunkTaskOutput>,
        outcomes: &mut Vec<(usize, usize, BulkResponse)>,
        first_es_err: &mut Option<ElasticsearchError>,
        first_join_err: &mut Option<PipelineError>,
    ) {
        while let Some(res) = join_set.join_next().await {
            Self::record_task_result(res, outcomes, first_es_err, first_join_err);
        }
    }

    /// Drains all remaining in-flight lease tasks in `lease_tasks` without overwriting an existing error.
    async fn drain_lease_tasks(lease_tasks: &mut tokio::task::JoinSet<Result<(), PipelineError>>) {
        while lease_tasks.join_next().await.is_some() {}
    }

    /// Drains all remaining in-flight lease tasks in `lease_tasks`, propagating the first error encountered.
    async fn drain_lease_tasks_propagating(
        lease_tasks: &mut tokio::task::JoinSet<Result<(), PipelineError>>,
    ) -> Result<(), PipelineError> {
        let mut first_err: Option<PipelineError> = None;
        while let Some(join_res) = lease_tasks.join_next().await {
            match join_res {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(join_err) => {
                    if first_err.is_none() {
                        first_err = Some(PipelineError::Internal(format!(
                            "Bulk dispatch task failed: {join_err}"
                        )));
                    }
                }
            }
        }
        if let Some(err) = first_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Processes a single [`Lease`]: sorts and serializes chunks via [`Lease::prepare`],
    /// dispatches all chunks under `semaphore`, joins all chunk tasks, quarantines any
    /// unrecoverable item-level `400` rows, and acknowledges or nacks the lease.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn process_lease(
        lease: Lease,
        signal_type: SignalType,
        data_stream: String,
        client: Arc<HttpClient>,
        semaphore: Arc<tokio::sync::Semaphore>,
        sorter: BatchSorter,
        unpack_attributes: bool,
        max_payload_bytes: usize,
    ) -> Result<(), PipelineError> {
        if lease.num_rows() == 0 {
            lease.ack().await?;
            return Ok(());
        }

        let prep_res = lease
            .prepare(move |batch| {
                if batch.num_rows() == 0 {
                    return Ok((batch, Vec::new()));
                }
                let sorted_batch = sorter.sort(&batch, signal_type)?;
                let chunks = crate::serializer::serialize_batch_chunks_with_ranges(
                    &sorted_batch,
                    unpack_attributes,
                    max_payload_bytes,
                )?;
                Ok((sorted_batch, chunks))
            })
            .await;

        let (sorted_batch, chunks) = match prep_res {
            Ok(pair) => pair,
            Err(err) => {
                let _ = lease
                    .nack(FailureClass::Fatal {
                        reason: err.to_string(),
                    })
                    .await;
                return Err(err);
            }
        };

        if chunks.is_empty() {
            lease.ack().await?;
            return Ok(());
        }

        let mut join_set: tokio::task::JoinSet<ChunkTaskOutput> = tokio::task::JoinSet::new();
        let mut outcomes: Vec<(usize, usize, BulkResponse)> = Vec::with_capacity(chunks.len());
        let mut first_es_err: Option<ElasticsearchError> = None;
        let mut first_join_err: Option<PipelineError> = None;

        for chunk in chunks {
            if let Err(dispatch_err) = Self::dispatch(
                Arc::clone(&client),
                Arc::clone(&semaphore),
                &mut join_set,
                data_stream.clone(),
                chunk,
                &mut outcomes,
                &mut first_es_err,
                &mut first_join_err,
            )
            .await
            {
                Self::drain_join_set(
                    &mut join_set,
                    &mut outcomes,
                    &mut first_es_err,
                    &mut first_join_err,
                )
                .await;
                let _ = lease
                    .nack(FailureClass::Fatal {
                        reason: dispatch_err.to_string(),
                    })
                    .await;
                return Err(dispatch_err);
            }
        }

        Self::drain_join_set(
            &mut join_set,
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;

        if let Some(join_err) = first_join_err {
            let _ = lease
                .nack(FailureClass::Fatal {
                    reason: join_err.to_string(),
                })
                .await;
            return Err(join_err);
        }

        if let Some(es_err) = first_es_err {
            let class = Self::classify_elasticsearch_error(&es_err);
            let is_fatal = matches!(class, FailureClass::Fatal { .. });
            lease.nack(class).await?;
            if is_fatal {
                return Err(es_err.into());
            }
            return Ok(());
        }

        outcomes.sort_unstable_by_key(|(start_row, _, _)| *start_row);

        if outcomes
            .iter()
            .any(|(_, _, resp)| resp.recoverable_count() > 0)
        {
            lease.nack(FailureClass::Transient).await?;
            return Ok(());
        }

        let mut rejected_slices: Vec<RecordBatch> = Vec::new();
        let mut first_reason: Option<String> = None;

        for (start_row, row_count, resp) in &outcomes {
            for (item_idx, item_wrapper) in resp.items.iter().enumerate() {
                if item_wrapper.is_unrecoverable() {
                    let status = item_wrapper.status().unwrap_or(400);
                    let err_type = item_wrapper
                        .error()
                        .map_or("unknown", |e| e.error_type.as_str());
                    let err_reason = item_wrapper
                        .error()
                        .map_or("unrecoverable bulk item error", |e| e.reason.as_str());
                    if first_reason.is_none() {
                        first_reason = Some(format!("item {status}: {err_type}: {err_reason}"));
                    }
                    if item_idx < *row_count {
                        let row_idx = start_row.saturating_add(item_idx);
                        if row_idx < sorted_batch.num_rows() {
                            rejected_slices.push(sorted_batch.slice(row_idx, 1));
                        }
                    }
                }
            }
        }

        if !rejected_slices.is_empty() {
            let reason = first_reason
                .unwrap_or_else(|| "item 400: unrecoverable bulk item error".to_string());
            lease.quarantine_batches(reason, rejected_slices).await?;
        }

        lease.ack().await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    async fn run(&mut self, mut buffer: SinkBuffer) -> Result<(), PipelineError> {
        tracing::info!(
            endpoints = ?self.config.endpoints,
            logs_stream = %self.config.data_streams.logs,
            metrics_stream = %self.config.data_streams.metrics,
            traces_stream = %self.config.data_streams.traces,
            "ElasticsearchSink started"
        );

        // 1. Startup validation
        self.validate_startup().await?;

        let mut lease_tasks: tokio::task::JoinSet<Result<(), PipelineError>> =
            tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                Some(join_res) = lease_tasks.join_next(), if !lease_tasks.is_empty() => {
                    match join_res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            Self::drain_lease_tasks(&mut lease_tasks).await;
                            return Err(err);
                        }
                        Err(join_err) => {
                            Self::drain_lease_tasks(&mut lease_tasks).await;
                            return Err(PipelineError::Internal(format!(
                                "Bulk dispatch task failed: {join_err}"
                            )));
                        }
                    }
                }
                next = buffer.next_lease() => {
                    match next {
                        Ok(Some(lease)) => {
                            let client = Arc::clone(&self.client);
                            let semaphore = Arc::clone(&self.semaphore);
                            let sorter = self.sorter.clone();
                            let unpack_attributes = self.config.unpack_attributes;
                            let max_payload_bytes = self.config.max_payload_bytes;
                            let signal_type = match lease.signal() {
                                SignalKind::Logs => SignalType::Logs,
                                SignalKind::Metrics => SignalType::Metrics,
                                SignalKind::Traces => SignalType::Traces,
                            };
                            let data_stream = self.data_stream_for(signal_type).to_string();

                            lease_tasks.spawn(async move {
                                Self::process_lease(
                                    lease,
                                    signal_type,
                                    data_stream,
                                    client,
                                    semaphore,
                                    sorter,
                                    unpack_attributes,
                                    max_payload_bytes,
                                )
                                .await
                            });
                        }
                        Ok(None) => {
                            Self::drain_lease_tasks_propagating(&mut lease_tasks).await?;
                            break;
                        }
                        Err(err) => {
                            Self::drain_lease_tasks(&mut lease_tasks).await;
                            return Err(err);
                        }
                    }
                }
            }
        }

        tracing::info!("ElasticsearchSink channel closed; shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use flate2::read::GzDecoder;
    use pipeline_core::buffer::{BufferConfig, DeadLetterManifest};
    use pipeline_core::pipeline::SignalBatch;
    use std::io::Read;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_test_config(endpoint: String, validate_on_startup: bool) -> ElasticsearchSinkConfig {
        ElasticsearchSinkConfig {
            endpoints: vec![endpoint],
            auth: ElasticsearchAuthConfig::None,
            data_streams: DataStreamMapping {
                logs: "logs-otel-default".to_string(),
                metrics: "metrics-otel-default".to_string(),
                traces: "traces-otel-default".to_string(),
            },
            tls: TlsConfig::default(),
            unpack_attributes: true,
            gzip_compression: false,
            max_concurrent_requests: 4,
            max_payload_bytes: 20_971_520,
            connect_timeout_secs: 5,
            request_timeout_secs: 5,
            max_retries: 2,
            retry_interval_secs: 0,
            validate_on_startup,
            buffer: None,
            batching: None,
            order_by: None,
        }
    }

    fn make_test_config_with_batching(endpoint: String) -> ElasticsearchSinkConfig {
        let mut config = make_test_config(endpoint, false);
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 1,
        });
        config
    }

    async fn setup_mock_client_and_semaphore() -> (Arc<HttpClient>, Arc<tokio::sync::Semaphore>) {
        let server = MockServer::start().await;
        let config = make_test_config(server.uri(), false);
        let client = Arc::new(HttpClient::try_new(&config).unwrap());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        (client, semaphore)
    }

    async fn setup_startup_validation_mocks(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "version": { "number": "8.12.0" }
            })))
            .mount(server)
            .await;

        let all_templates = [
            "logs-otel-default",
            "metrics-otel-default",
            "traces-otel-default",
        ]
        .into_iter()
        .map(|stream| {
            serde_json::json!({
                "name": stream,
                "index_template": {
                    "index_patterns": [format!("{stream}*")],
                    "data_stream": {}
                }
            })
        })
        .collect::<Vec<_>>();

        Mock::given(method("GET"))
            .and(path("/_index_template"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "index_templates": all_templates
            })))
            .mount(server)
            .await;

        for stream in [
            "logs-otel-default",
            "metrics-otel-default",
            "traces-otel-default",
        ] {
            Mock::given(method("POST"))
                .and(path(format!("/_index_template/_simulate_index/{stream}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "template": {
                        "data_stream": {}
                    }
                })))
                .mount(server)
                .await;
        }
    }

    fn make_log_batch_with_timestamps(nanos: Vec<i64>) -> RecordBatch {
        let count = nanos.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(nanos)),
                Arc::new(StringArray::from(vec!["frontend"; count])),
                Arc::new(Int32Array::from(vec![9; count])),
                Arc::new(StringArray::from(vec!["Request processed"; count])),
                Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#; count])),
            ],
        )
        .unwrap()
    }

    fn make_log_batch() -> RecordBatch {
        make_log_batch_with_timestamps(vec![1_726_500_000_000_000_000])
    }

    fn make_test_log_batch(count: usize) -> RecordBatch {
        make_log_batch_with_timestamps(vec![1_726_500_000_000_000_000; count])
    }

    fn make_log_batch_with_service(service_name: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000,
                ])),
                Arc::new(StringArray::from(vec![service_name])),
                Arc::new(Int32Array::from(vec![9])),
                Arc::new(StringArray::from(vec!["Request processed"])),
                Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#])),
            ],
        )
        .unwrap()
    }

    fn make_multi_row_log_batch(count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        let services: Vec<String> = (0..count).map(|i| format!("service_{i}")).collect();
        let service_refs: Vec<&str> = services.iter().map(String::as_str).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000;
                    count
                ])),
                Arc::new(StringArray::from(service_refs)),
                Arc::new(Int32Array::from(vec![9; count])),
                Arc::new(StringArray::from(vec!["Request processed"; count])),
                Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#; count])),
            ],
        )
        .unwrap()
    }

    fn make_metric_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000,
                    1_726_500_000_000_000_001,
                ])),
                Arc::new(StringArray::from(vec![
                    "system.cpu.utilization",
                    "system.memory.usage",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![0.42, 0.73])),
            ],
        )
        .unwrap()
    }

    fn make_trace_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["4bf92f3577b34da6a3ce929d0e0e4736"])),
                Arc::new(StringArray::from(vec!["00f067aa0ba902b7"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sink_sends_valid_ndjson_to_mock_server() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(header("content-type", "application/x-ndjson"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 12,
                "errors": false,
                "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        let batch = make_log_batch();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_req = requests
            .iter()
            .find(|r| r.url.path() == "/logs-otel-default/_bulk")
            .expect("bulk request must be received");

        let body_str = std::str::from_utf8(&bulk_req.body).unwrap();
        let lines: Vec<&str> = body_str.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"create":{}}"#);
        assert!(lines[1].contains(r#""@timestamp":"#));
        assert!(lines[1].contains(r#""service_name":"frontend""#));
        assert!(lines[1].contains(r#""attributes":{"http.method":"GET"}"#));
    }

    #[tokio::test]
    async fn test_sink_429_triggers_retries() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        // First bulk attempt returns 429
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Too Many Requests"))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Second bulk attempt returns 200 OK
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 15,
                "errors": false,
                "items": []
            })))
            .mount(&server)
            .await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_req_count = requests
            .iter()
            .filter(|r| r.url.path() == "/logs-otel-default/_bulk")
            .count();
        assert_eq!(
            bulk_req_count, 2,
            "Bulk request should be retried after 429"
        );
    }

    #[tokio::test]
    async fn test_sink_exhausted_retries_produces_storage_error() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Too Many Requests"))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.max_retries = 1;
        config.buffer = Some(BufferConfig {
            max_transient_duration_sec: 1,
            initial_backoff_ms: 5,
            ..BufferConfig::passthrough()
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        drop(tx);

        let err = sink.run(rx).await.unwrap_err();
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Exhausted transient 429 retries must produce PipelineError::Storage, got: {err:?}"
        );
        assert!(
            err.to_string()
                .contains("transient retry duration exceeded")
        );
    }

    #[tokio::test]
    async fn test_sink_graceful_shutdown_drains_all_in_flight_requests() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/metrics-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/traces-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        tx.send(SignalBatch::Metrics(make_metric_batch()))
            .await
            .unwrap();
        tx.send(SignalBatch::Traces(make_trace_batch()))
            .await
            .unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_reqs = requests
            .iter()
            .filter(|r| r.url.path().ends_with("/_bulk"))
            .count();
        assert_eq!(
            bulk_reqs, 3,
            "All 3 in-flight requests must be drained on shutdown"
        );
    }

    #[tokio::test]
    async fn test_sink_batching_accumulation_and_record_limit_flush() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .expect(2)
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.max_concurrent_requests = 1;
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 4,
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();

        // Send batch 1 with 2 rows
        let b1 = make_log_batch_with_timestamps(vec![1_000_000_000, 2_000_000_000]);
        tx.send(SignalBatch::Logs(b1)).await.unwrap();

        // Send batch 2 with 2 rows -> total 4 rows reaches max_batch_records!
        let b2 = make_log_batch_with_timestamps(vec![3_000_000_000, 4_000_000_000]);
        tx.send(SignalBatch::Logs(b2)).await.unwrap();

        // Send batch 3 with 1 row -> stays buffered until shutdown flush
        let b3 = make_log_batch_with_timestamps(vec![5_000_000_000]);
        tx.send(SignalBatch::Logs(b3)).await.unwrap();

        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_requests: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path() == "/logs-otel-default/_bulk")
            .collect();
        assert_eq!(
            bulk_requests.len(),
            2,
            "Expected 2 bulk requests (threshold + shutdown flush)"
        );

        // First bulk request had 4 rows = 8 lines (action + doc)
        let body1 = std::str::from_utf8(&bulk_requests[0].body).unwrap();
        let lines1: Vec<&str> = body1.lines().collect();
        assert_eq!(lines1.len(), 8);

        // Second bulk request had 1 row = 2 lines
        let body2 = std::str::from_utf8(&bulk_requests[1].body).unwrap();
        let lines2: Vec<&str> = body2.lines().collect();
        assert_eq!(lines2.len(), 2);
    }

    #[tokio::test]
    async fn test_sink_presorts_chronologically() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "timestamp ASC".to_string(),
            )],
            metrics: vec![],
            traces: vec![],
        });
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        // Unordered timestamps: 3s, 1s, 2s
        let batch =
            make_log_batch_with_timestamps(vec![3_000_000_000, 1_000_000_000, 2_000_000_000]);
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_req = requests
            .iter()
            .find(|r| r.url.path() == "/logs-otel-default/_bulk")
            .unwrap();

        let body_str = std::str::from_utf8(&bulk_req.body).unwrap();
        let lines: Vec<&str> = body_str.lines().collect();
        assert_eq!(lines.len(), 6); // 3 docs * 2 lines

        assert!(lines[1].contains("1970-01-01T00:00:01"));
        assert!(lines[3].contains("1970-01-01T00:00:02"));
        assert!(lines[5].contains("1970-01-01T00:00:03"));
    }

    #[tokio::test]
    async fn test_sink_batching_interval_timer_flush() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 1, // 1 second interval
            max_batch_records: 1000,
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let run_handle = tokio::spawn(async move { sink.run(rx).await });

        // Send 1 batch (well below record limit of 1000)
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();

        // Sleep 1.5 seconds to let interval timer fire and flush
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // Verify request was dispatched before channel was closed
        let requests = server.received_requests().await.unwrap();
        let bulk_count = requests
            .iter()
            .filter(|r| r.url.path() == "/logs-otel-default/_bulk")
            .count();
        assert_eq!(bulk_count, 1, "Interval timer must flush buffered batch");

        // Clean up channel and wait for sink task to finish
        drop(tx);
        run_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_sink_startup_validation_fails_on_missing_index_template() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "version": { "number": "8.12.0" }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/_index_template"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/_index_template/_simulate_index/logs-otel-default"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (_tx, rx) = SinkBuffer::passthrough_pair();
        let err = sink.run(rx).await.unwrap_err();
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Missing index template must produce PipelineError::Storage, got: {err:?}"
        );
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_sink_startup_validation_fails_on_unauthorized() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (_tx, rx) = SinkBuffer::passthrough_pair();
        let err = sink.run(rx).await.unwrap_err();
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "401 during startup health check must produce PipelineError::Storage, got: {err:?}"
        );
        assert!(err.to_string().contains("401 Unauthorized"));
    }

    #[tokio::test]
    async fn test_sink_startup_validation_succeeds_under_restricted_rbac_role() {
        let server = MockServer::start().await;

        // Health check
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "version": { "number": "8.12.0" }
            })))
            .mount(&server)
            .await;

        // Tier 1 returns 403 Forbidden (restricted service account lacks cluster manage_index_templates)
        Mock::given(method("GET"))
            .and(path("/_index_template"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;

        for stream in &[
            "logs-otel-default",
            "metrics-otel-default",
            "traces-otel-default",
        ] {
            // Tier 2 returns 403 Forbidden
            Mock::given(method("POST"))
                .and(path(format!("/_index_template/_simulate_index/{stream}")))
                .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
                .mount(&server)
                .await;

            // Tier 3 returns 200 OK because data stream was pre-provisioned
            Mock::given(method("GET"))
                .and(path(format!("/_data_stream/{stream}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "data_streams": [{
                        "name": *stream,
                        "status": "GREEN"
                    }]
                })))
                .mount(&server)
                .await;
        }

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).expect("sink creation failed");

        let (tx, rx) = SinkBuffer::passthrough_pair();
        let run_handle = tokio::spawn(async move { sink.run(rx).await });

        // Drop sender immediately so run() completes cleanly after startup validation
        drop(tx);
        let run_result = run_handle.await.expect("join failed");
        assert!(
            run_result.is_ok(),
            "sink must start and shut down cleanly under restricted RBAC role with pre-created data stream, got: {run_result:?}"
        );
    }

    #[tokio::test]
    async fn test_sink_with_gzip_compression() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(header("content-encoding", "gzip"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 5, "errors": false, "items": []
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.gzip_compression = true;
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_req = requests
            .iter()
            .find(|r| r.url.path() == "/logs-otel-default/_bulk")
            .unwrap();

        let mut decoder = GzDecoder::new(&bulk_req.body[..]);
        let mut decompressed = String::new();
        decoder.read_to_string(&mut decompressed).unwrap();
        assert!(decompressed.contains("@timestamp"));
        assert!(decompressed.contains("frontend"));
    }

    #[tokio::test]
    async fn test_sink_empty_batches_ignored() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        let config = make_test_config(server.uri(), true);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        let empty_batch = make_log_batch().slice(0, 0);
        tx.send(SignalBatch::Logs(empty_batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let bulk_count = requests
            .iter()
            .filter(|r| r.url.path().ends_with("/_bulk"))
            .count();
        assert_eq!(
            bulk_count, 0,
            "Empty batches should not trigger bulk requests"
        );
    }

    #[test]
    fn test_sink_try_new_rejects_zero_max_concurrent_requests() {
        let mut config = make_test_config("http://localhost:9200".to_string(), false);
        config.max_concurrent_requests = 0;
        let err = ElasticsearchSink::try_new(config).unwrap_err();
        assert!(matches!(err, PipelineError::Internal(_)));
        assert!(
            err.to_string()
                .contains("max_concurrent_requests must be greater than 0")
        );
    }

    #[tokio::test]
    async fn test_drain_join_set_awaits_all_tasks_on_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let completed_tasks = Arc::new(AtomicUsize::new(0));

        let mut join_set = tokio::task::JoinSet::new();

        // Task 1: Fails immediately
        join_set.spawn(async move {
            (
                0usize,
                1usize,
                Err(ElasticsearchError::StartupValidation(
                    "task 1 failed".to_string(),
                )),
            )
        });

        // Task 2: Sleeps a bit then succeeds, recording completion
        let counter2 = Arc::clone(&completed_tasks);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            counter2.fetch_add(1, Ordering::SeqCst);
            (
                1usize,
                1usize,
                Ok(BulkResponse {
                    took: 1,
                    errors: false,
                    items: vec![],
                }),
            )
        });

        // Task 3: Sleeps a bit then succeeds, recording completion
        let counter3 = Arc::clone(&completed_tasks);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            counter3.fetch_add(1, Ordering::SeqCst);
            (
                2usize,
                1usize,
                Ok(BulkResponse {
                    took: 1,
                    errors: false,
                    items: vec![],
                }),
            )
        });

        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;
        ElasticsearchSink::drain_join_set(
            &mut join_set,
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;
        assert!(first_join_err.is_none());
        let err = first_es_err.expect("expected error from task 1");
        assert!(err.to_string().contains("task 1 failed"));
        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            completed_tasks.load(Ordering::SeqCst),
            2,
            "All remaining in-flight tasks must run to completion despite earlier task failure"
        );
        assert!(join_set.is_empty(), "JoinSet must be empty after drain");
    }

    #[tokio::test]
    async fn test_sink_share_state_shares_semaphore_and_validation() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        let mut config = make_test_config(server.uri(), true);
        config.max_concurrent_requests = 5;

        let logs_sink = ElasticsearchSink::try_new(config.clone()).unwrap();
        let mut traces_sink = ElasticsearchSink::try_new(config.clone()).unwrap();
        let mut metrics_sink = ElasticsearchSink::try_new(config).unwrap();

        traces_sink.share_state_from(&logs_sink);
        metrics_sink.share_state_from(&logs_sink);

        // Verify all three share the same underlying semaphore with 5 permits
        assert_eq!(logs_sink.semaphore().available_permits(), 5);
        assert_eq!(traces_sink.semaphore().available_permits(), 5);
        assert_eq!(metrics_sink.semaphore().available_permits(), 5);

        // Acquiring a permit on logs_sink decreases available permits on traces and metrics
        let _permit = logs_sink.semaphore().acquire_owned().await.unwrap();
        assert_eq!(logs_sink.semaphore().available_permits(), 4);
        assert_eq!(traces_sink.semaphore().available_permits(), 4);
        assert_eq!(metrics_sink.semaphore().available_permits(), 4);

        // Validate startup on logs_sink
        logs_sink.validate_startup().await.unwrap();
        assert!(
            logs_sink
                .validated
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(
            traces_sink
                .validated
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(
            metrics_sink
                .validated
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn test_sink_run_non_batching_offloads_serialization() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.batching = None;
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::passthrough_pair();
        let batch = make_log_batch();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        let res = sink.run(rx).await;
        assert!(res.is_ok());
    }

    struct SlowSuccessResponder {
        completed: Arc<std::sync::atomic::AtomicBool>,
        delay: std::time::Duration,
    }

    impl wiremock::Respond for SlowSuccessResponder {
        fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
            let completed = Arc::clone(&self.completed);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                completed.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            ResponseTemplate::new(200)
                .set_delay(self.delay)
                .set_body_json(serde_json::json!({
                    "took": 1,
                    "errors": false,
                    "items": []
                }))
        }
    }

    #[tokio::test]
    async fn test_sink_run_drains_all_inflight_tasks_on_task_failure() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        // Mock 1: Fails after 20ms with 401 Unauthorized (fatal)
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service-fail")
            })
            .respond_with(
                ResponseTemplate::new(401)
                    .set_delay(std::time::Duration::from_millis(20))
                    .set_body_string("Unauthorized"),
            )
            .mount(&server)
            .await;

        // Mock 2: Succeeds after delay (150ms)
        let task2_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service-slow")
            })
            .respond_with(SlowSuccessResponder {
                completed: Arc::clone(&task2_completed),
                delay: std::time::Duration::from_millis(150),
            })
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.batching = None;
        config.max_retries = 0;
        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(1);
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        tx.send(SignalBatch::Logs(make_log_batch_with_service(
            "service-fail",
        )))
        .await
        .unwrap();
        tx.send(SignalBatch::Logs(make_log_batch_with_service(
            "service-slow",
        )))
        .await
        .unwrap();
        drop(tx);

        let res = sink.run(rx).await;
        assert!(res.is_err(), "sink.run should return error on task failure");
        assert!(
            task2_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task must be drained and allowed to complete before sink.run returns on failure"
        );
    }

    #[tokio::test]
    async fn test_flush_error_drains_join_set() {
        let server = MockServer::start().await;

        let in_flight_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(SlowSuccessResponder {
                completed: Arc::clone(&in_flight_completed),
                delay: std::time::Duration::from_millis(150),
            })
            .mount(&server)
            .await;

        let mut config = make_test_config_with_batching(server.uri());
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 1,
        });
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![],
            metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "nonexistent_sort_col ASC".to_string(),
            )],
            traces: vec![],
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let run_handle = tokio::spawn(async move { sink.run(rx).await });

        let batch1 = make_log_batch();
        tx.send(SignalBatch::Logs(batch1)).await.unwrap();

        // Allow batch 1 lease to be dispatched into lease_tasks and reach wiremock
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Batch 2 causes lease preparation to error due to missing required sort column
        let batch2 = make_metric_batch();
        tx.send(SignalBatch::Metrics(batch2)).await.unwrap();
        drop(tx);

        let result = run_handle.await.unwrap();
        assert!(result.is_err(), "expected flush error to propagate");
        assert!(
            in_flight_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task must be drained and allowed to complete when another lease fails"
        );
    }

    #[tokio::test]
    async fn test_shutdown_flush_error_drains_join_set() {
        let server = MockServer::start().await;

        let in_flight_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(SlowSuccessResponder {
                completed: Arc::clone(&in_flight_completed),
                delay: std::time::Duration::from_millis(150),
            })
            .mount(&server)
            .await;

        let mut config = make_test_config_with_batching(server.uri());
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 10,
        });
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![],
            metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "nonexistent_sort_col ASC".to_string(),
            )],
            traces: vec![],
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let run_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.send(SignalBatch::Metrics(make_metric_batch()))
            .await
            .unwrap();
        drop(tx);

        let result = run_handle.await.unwrap();
        assert!(
            result.is_err(),
            "expected shutdown flush error to propagate"
        );
        assert!(
            in_flight_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task from earlier buffer must be drained when shutdown lease preparation fails"
        );
    }

    #[tokio::test]
    async fn test_interval_tick_flush_error_drains_join_set() {
        let server = MockServer::start().await;

        let in_flight_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(SlowSuccessResponder {
                completed: Arc::clone(&in_flight_completed),
                delay: std::time::Duration::from_millis(150),
            })
            .mount(&server)
            .await;

        let mut config = make_test_config_with_batching(server.uri());
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 1,
            max_batch_records: 10,
        });
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![],
            metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "nonexistent_sort_col ASC".to_string(),
            )],
            traces: vec![],
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let run_handle = tokio::spawn(async move { sink.run(rx).await });

        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.send(SignalBatch::Metrics(make_metric_batch()))
            .await
            .unwrap();

        let result = run_handle.await.unwrap();
        assert!(
            result.is_err(),
            "expected interval flush error to propagate"
        );
        assert!(
            in_flight_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task from earlier buffer must be drained when interval tick lease preparation fails"
        );
    }

    #[tokio::test]
    async fn test_dispatch_drains_all_inflight_tasks_on_task_failure() {
        let server = MockServer::start().await;
        let config = make_test_config(server.uri(), false);
        let client = Arc::new(HttpClient::try_new(&config).unwrap());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));

        let mut join_set = tokio::task::JoinSet::new();
        let slow_task_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // In-flight task 1: fails immediately
        join_set.spawn(async move {
            (
                0usize,
                1usize,
                Err(ElasticsearchError::StartupValidation(
                    "fast failure".to_string(),
                )),
            )
        });

        // In-flight task 2: slow task
        let flag = Arc::clone(&slow_task_completed);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            (
                1usize,
                1usize,
                Ok(BulkResponse {
                    took: 1,
                    errors: false,
                    items: vec![],
                }),
            )
        });

        // Yield so task 1 finishes and task 2 starts sleeping
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            semaphore,
            &mut join_set,
            "logs-otel-default".to_string(),
            BulkChunk {
                start_row: 2,
                row_count: 1,
                payload: bytes::Bytes::from_static(b"dummy payload"),
            },
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should succeed without aborting chunk submission"
        );
        assert!(
            first_es_err.is_some(),
            "first_es_err should capture the earlier failure"
        );

        // Sinks drain remaining tasks after all chunks are dispatched
        ElasticsearchSink::drain_join_set(
            &mut join_set,
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;
        assert!(
            slow_task_completed.load(std::sync::atomic::Ordering::SeqCst),
            "drain_join_set must drain all remaining in-flight tasks"
        );
        assert!(join_set.is_empty(), "JoinSet must be drained and empty");
    }

    #[tokio::test]
    async fn test_dispatch_drains_all_inflight_tasks_on_semaphore_exhaustion_failure() {
        let server = MockServer::start().await;
        let config = make_test_config(server.uri(), false);
        let client = Arc::new(HttpClient::try_new(&config).unwrap());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));

        let mut join_set = tokio::task::JoinSet::new();
        let slow_task_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // In-flight task 1 holds permit and fails after 10ms
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        join_set.spawn(async move {
            let _permit = permit;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            (
                0usize,
                1usize,
                Err(ElasticsearchError::StartupValidation(
                    "failure while waiting for permit".to_string(),
                )),
            )
        });

        // Task 2: slow task finishes after 50ms
        let flag = Arc::clone(&slow_task_completed);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            (
                1usize,
                1usize,
                Ok(BulkResponse {
                    took: 1,
                    errors: false,
                    items: vec![],
                }),
            )
        });

        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            Arc::clone(&semaphore),
            &mut join_set,
            "logs-otel-default".to_string(),
            BulkChunk {
                start_row: 2,
                row_count: 1,
                payload: bytes::Bytes::from_static(b"dummy payload"),
            },
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should succeed once permit is freed by failed task"
        );
        assert!(
            first_es_err.is_some(),
            "first_es_err should capture the failure from task 1"
        );

        // Sinks drain remaining tasks after all chunks are dispatched
        ElasticsearchSink::drain_join_set(
            &mut join_set,
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;
        assert!(
            slow_task_completed.load(std::sync::atomic::Ordering::SeqCst),
            "drain_join_set must drain all remaining in-flight tasks"
        );
        assert!(join_set.is_empty(), "JoinSet must be drained and empty");
    }

    #[test]
    fn test_sink_getters() {
        let config = make_test_config("http://localhost:9200".to_string(), false);
        let sink = ElasticsearchSink::try_new(config).unwrap();
        assert_eq!(sink.config().endpoints, vec!["http://localhost:9200"]);
        assert_eq!(sink.client().endpoints(), &["http://localhost:9200"]);
        assert_eq!(sink.data_stream_for(SignalType::Logs), "logs-otel-default");
        assert_eq!(
            sink.data_stream_for(SignalType::Metrics),
            "metrics-otel-default"
        );
        assert_eq!(
            sink.data_stream_for(SignalType::Traces),
            "traces-otel-default"
        );
    }

    #[tokio::test]
    async fn test_buffer_accumulation_and_shutdown_flush_via_sink_buffer() {
        let mock_server = wiremock::MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .mount(&mock_server)
            .await;

        let config = make_test_config_with_batching(mock_server.uri());
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let batch = make_test_log_batch(1); // below threshold, flushed on shutdown
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();
        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn test_buffer_record_threshold_coalesces_batches_via_sink_buffer() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_test_config_with_batching(mock_server.uri());
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 2,
        });
        let buf_cfg = config.resolved_buffer_config().unwrap();
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        tx.send(SignalBatch::Logs(make_test_log_batch(1)))
            .await
            .unwrap();
        tx.send(SignalBatch::Logs(make_test_log_batch(1)))
            .await
            .unwrap();
        drop(tx);

        let res = sink.run(rx).await;
        assert!(res.is_ok());
        mock_server.verify().await;
    }

    #[tokio::test]
    async fn test_sink_non_batching_splits_oversized_batch_into_multiple_requests() {
        let server = MockServer::start().await;

        // Server expects 2 distinct POST bulk requests
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .expect(2)
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.batching = None; // Non-batching mode
        config.max_payload_bytes = 200; // Small limit to force chunking (2 rows = 176 bytes)

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::passthrough_pair();

        // Create a batch with 4 rows that will exceed 200 bytes
        let timestamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000i64; 4]);
        let bodies = StringArray::from(vec!["hello-world-data"; 4]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(bodies)]).unwrap();

        let handle = tokio::spawn(async move {
            sink.run(rx).await.unwrap();
        });

        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        handle.await.unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn test_sink_batching_splits_oversized_batch_into_multiple_requests() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .expect(2)
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.batching = Some(crate::config::ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_000_000,
            max_batch_interval_sec: 10,
            max_batch_records: 100,
        });
        config.max_payload_bytes = 200;

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::passthrough_pair();

        let timestamps = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000i64; 4]);
        let bodies = StringArray::from(vec!["hello-world-data"; 4]);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(timestamps), Arc::new(bodies)]).unwrap();

        let handle = tokio::spawn(async move {
            sink.run(rx).await.unwrap();
        });

        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        handle.await.unwrap();
        server.verify().await;
    }

    #[tokio::test]
    async fn test_sink_dispatches_all_chunks_despite_earlier_chunk_failure() {
        let server = MockServer::start().await;
        setup_startup_validation_mocks(&server).await;

        // First bulk request fails with 401 (fatal, no lease retry)
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service_0")
            })
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        // Second bulk request succeeds with 200
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service_1")
            })
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"took": 5, "errors": false, "items": []}"#),
            )
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), true);
        config.max_concurrent_requests = 1; // Strict serial concurrency
        config.max_retries = 0;
        config.max_payload_bytes = 200; // Force splitting into multiple chunks

        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(2);
        buf_cfg.max_attempts = 1;
        let mut sink = ElasticsearchSink::try_new(config).expect("sink creation failed");

        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();
        let batch = make_multi_row_log_batch(2); // 2 rows, each serialized NDJSON is ~150 bytes
        tx.send(SignalBatch::Logs(batch))
            .await
            .expect("send failed");
        drop(tx);

        let result = sink.run(rx).await;
        assert!(result.is_err(), "expected error due to first chunk failure");

        let requests = server
            .received_requests()
            .await
            .expect("failed to get received requests");
        let bulk_requests: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path().ends_with("/_bulk"))
            .collect();

        assert_eq!(
            bulk_requests.len(),
            2,
            "both chunks must be dispatched even when the first chunk encounters an error"
        );
    }

    #[tokio::test]
    async fn test_dispatch_handles_task_join_error_panic() {
        let (client, semaphore) = setup_mock_client_and_semaphore().await;
        let mut join_set = tokio::task::JoinSet::new();

        // Spawn a task that panics
        join_set.spawn(async move {
            panic!("simulated worker task panic");
        });

        // Sleep briefly so the task completes panicked
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            semaphore,
            &mut join_set,
            "logs-otel-default".to_string(),
            BulkChunk {
                start_row: 0,
                row_count: 1,
                payload: bytes::Bytes::from_static(b"dummy payload"),
            },
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should submit chunk even if earlier task panicked"
        );
        assert!(
            first_join_err.is_some(),
            "first_join_err must record task panic JoinError"
        );
        let err_msg = first_join_err.unwrap().to_string();
        assert!(
            err_msg.contains("Bulk dispatch task failed"),
            "error message must describe task join failure, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_drain_join_set_handles_join_error_panic() {
        let mut join_set = tokio::task::JoinSet::new();
        join_set.spawn(async move {
            panic!("simulated worker panic for drain");
        });

        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;
        ElasticsearchSink::drain_join_set(
            &mut join_set,
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        )
        .await;
        assert!(
            first_join_err.is_some(),
            "drain_join_set must return error when task panicked"
        );
        let err_msg = first_join_err.unwrap().to_string();
        assert!(
            err_msg.contains("Bulk dispatch task failed"),
            "error must describe join error, got: {err_msg}"
        );
    }

    #[test]
    fn test_record_task_result_unit() {
        let mut outcomes = Vec::new();
        let mut first_es_err = None;
        let mut first_join_err = None;

        // Ok(Ok) does not record error
        ElasticsearchSink::record_task_result(
            Ok((
                0usize,
                2usize,
                Ok(BulkResponse {
                    took: 1,
                    errors: false,
                    items: vec![],
                }),
            )),
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        );
        assert!(first_es_err.is_none());
        assert!(first_join_err.is_none());
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].0, 0);
        assert_eq!(outcomes[0].1, 2);

        // Ok(Err) records first error
        ElasticsearchSink::record_task_result(
            Ok((
                2usize,
                1usize,
                Err(ElasticsearchError::StartupValidation(
                    "first error".to_string(),
                )),
            )),
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        );
        assert!(first_es_err.is_some());
        assert!(
            first_es_err
                .as_ref()
                .unwrap()
                .to_string()
                .contains("first error")
        );

        // Subsequent error does not overwrite first_es_err
        ElasticsearchSink::record_task_result(
            Ok((
                3usize,
                1usize,
                Err(ElasticsearchError::StartupValidation(
                    "second error".to_string(),
                )),
            )),
            &mut outcomes,
            &mut first_es_err,
            &mut first_join_err,
        );
        assert!(
            first_es_err
                .as_ref()
                .unwrap()
                .to_string()
                .contains("first error")
        );
    }

    #[tokio::test]
    async fn test_elasticsearch_retains_lease_when_any_bulk_chunk_returns_503() {
        let server = MockServer::start().await;

        // Chunk 0 (service_0) always succeeds with 200
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": false,
                "items": [{"create": {"_index": "logs-otel-default", "_id": "1", "status": 201}}]
            })))
            .mount(&server)
            .await;

        // Chunk 1 (service_1) fails with 503 on first lease attempt, then succeeds with 200 on lease retry
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_1"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": false,
                "items": [{"create": {"_index": "logs-otel-default", "_id": "2", "status": 201}}]
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.max_retries = 0; // Let SinkBuffer lease retry handle the 503 nack
        config.max_payload_bytes = 200; // Split 2 rows into 2 chunks

        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(2);
        buf_cfg.initial_backoff_ms = 10;
        buf_cfg.max_backoff_sec = 1;
        buf_cfg.max_attempts = 4;

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();

        tx.send(SignalBatch::Logs(make_multi_row_log_batch(2)))
            .await
            .unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let requests = server.received_requests().await.unwrap();
        let svc1_attempts = requests
            .iter()
            .filter(|r| {
                r.url.path() == "/logs-otel-default/_bulk"
                    && String::from_utf8_lossy(&r.body).contains("service_1")
            })
            .count();
        assert_eq!(
            svc1_attempts, 2,
            "Lease must be retained on 503 chunk failure and retried until all chunks succeed"
        );
    }

    #[tokio::test]
    async fn test_elasticsearch_sink_retains_lease_when_any_chunk_fails_503() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": false,
                "items": [{"create": {"_index": "logs-otel-default", "_id": "1", "status": 201}}]
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_1"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("service_1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": false,
                "items": [{"create": {"_index": "logs-otel-default", "_id": "2", "status": 201}}]
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.max_retries = 0;
        config.max_payload_bytes = 200;

        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(2);
        buf_cfg.initial_backoff_ms = 10;
        buf_cfg.max_backoff_sec = 1;
        buf_cfg.max_attempts = 4;

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();

        tx.send(SignalBatch::Logs(make_multi_row_log_batch(2)))
            .await
            .unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 4);
    }

    #[tokio::test]
    async fn test_elasticsearch_bulk_item_400_quarantines_exact_sorted_row_across_chunks() {
        let server = MockServer::start().await;
        let dlq_dir = std::env::temp_dir().join(format!(
            "otel-es-dlq-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dlq_dir);

        // Build an unsorted 4-row log batch:
        // Unsorted rows: ts=30 (svc-ts30), ts=10 (svc-ts10), ts=40 (svc-ts40), ts=20 (svc-ts20)
        // After sorting by `timestamp ASC`:
        //   Sorted row 0: ts=10 (svc-ts10) -> Chunk 0, item 0
        //   Sorted row 1: ts=20 (svc-ts20) -> Chunk 0, item 1
        //   Sorted row 2: ts=30 (svc-ts30) -> Chunk 1, item 0
        //   Sorted row 3: ts=40 (svc-ts40) -> Chunk 1, item 1 (returns 400 mapper_parsing_exception)
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
        ]));
        let unsorted_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_030i64,
                    1_700_000_000_000_000_010i64,
                    1_700_000_000_000_000_040i64,
                    1_700_000_000_000_000_020i64,
                ])),
                Arc::new(StringArray::from(vec![
                    "svc-ts30", "svc-ts10", "svc-ts40", "svc-ts20",
                ])),
                Arc::new(StringArray::from(vec![
                    "body-30", "body-10", "body-40", "body-20",
                ])),
            ],
        )
        .unwrap();

        // Measure 2-row serialized size so max_payload_bytes splits 4 sorted rows into 2 chunks of 2 rows
        let two_row_chunks = crate::serializer::serialize_batch_chunks_with_ranges(
            &unsorted_batch.slice(0, 2),
            true,
            10_485_760,
        )
        .unwrap();
        let two_row_bytes = two_row_chunks[0].payload.len();

        // Chunk 0 (contains svc-ts10 and svc-ts20): both items succeed with 201
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("svc-ts10"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": false,
                "items": [
                    {"create": {"_index": "logs-otel-default", "_id": "1", "status": 201}},
                    {"create": {"_index": "logs-otel-default", "_id": "2", "status": 201}}
                ]
            })))
            .mount(&server)
            .await;

        // Chunk 1 (contains svc-ts30 and svc-ts40): item 0 succeeds (201), item 1 fails with 400
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| String::from_utf8_lossy(&req.body).contains("svc-ts30"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 3,
                "errors": true,
                "items": [
                    {"create": {"_index": "logs-otel-default", "_id": "3", "status": 201}},
                    {"create": {
                        "_index": "logs-otel-default",
                        "_id": "4",
                        "status": 400,
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse field [body] of type [long]"
                        }
                    }}
                ]
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.max_retries = 0;
        config.max_payload_bytes = two_row_bytes;
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "timestamp ASC".to_string(),
            )],
            metrics: vec![],
            traces: vec![],
        });

        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(4);
        buf_cfg.dead_letter_path = Some(dlq_dir.clone());

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();

        tx.send(SignalBatch::Logs(unsorted_batch)).await.unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let es_dlq = dlq_dir.join("elasticsearch");
        assert!(
            es_dlq.exists(),
            "Expected elasticsearch DLQ directory to exist"
        );
        let mut json_files = Vec::new();
        let mut arrow_files = Vec::new();
        for entry in std::fs::read_dir(&es_dlq).unwrap() {
            let p = entry.unwrap().path();
            match p.extension().and_then(|e| e.to_str()) {
                Some("json") => json_files.push(p),
                Some("arrow") => arrow_files.push(p),
                _ => {}
            }
        }
        assert_eq!(
            json_files.len(),
            1,
            "Exactly 1 DLQ manifest must be written for the rejected row"
        );
        assert_eq!(
            arrow_files.len(),
            1,
            "Exactly 1 DLQ Arrow IPC file must be written for the rejected row"
        );

        let manifest: DeadLetterManifest =
            serde_json::from_slice(&std::fs::read(&json_files[0]).unwrap()).unwrap();
        assert_eq!(manifest.sink, "elasticsearch");
        assert_eq!(manifest.row_count, 1);
        assert_eq!(manifest.signal, pipeline_core::buffer::SignalKind::Logs);
        assert!(
            manifest.reason.contains("mapper_parsing_exception"),
            "DLQ reason must include item error details, got: {}",
            manifest.reason
        );

        let file = std::fs::File::open(&arrow_files[0]).unwrap();
        let mut reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let dlq_batch = reader.next().unwrap().unwrap();
        assert_eq!(dlq_batch.num_rows(), 1);
        let svc_col = dlq_batch
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            svc_col.value(0),
            "svc-ts40",
            "Quarantined row must be sorted_batch row (chunk.start_row + item_idx) = 2 + 1 = 3 (svc-ts40)"
        );

        let _ = std::fs::remove_dir_all(&dlq_dir);
    }

    #[tokio::test]
    async fn test_elasticsearch_sink_quarantines_item_400_to_dead_letter_and_acks_lease() {
        let server = MockServer::start().await;
        let dlq_dir = std::env::temp_dir().join(format!(
            "otel-es-dlq-alias-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dlq_dir);

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 2,
                "errors": true,
                "items": [
                    {"create": {"_index": "logs-otel-default", "_id": "1", "status": 201}},
                    {"create": {
                        "_index": "logs-otel-default",
                        "_id": "2",
                        "status": 400,
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse field"
                        }
                    }}
                ]
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.max_retries = 0;

        let mut buf_cfg = config.resolved_buffer_config().unwrap();
        buf_cfg.max_batch_records = Some(2);
        buf_cfg.dead_letter_path = Some(dlq_dir.clone());

        let mut sink = ElasticsearchSink::try_new(config).unwrap();
        let (tx, rx) = SinkBuffer::new("elasticsearch", buf_cfg).unwrap();

        tx.send(SignalBatch::Logs(make_multi_row_log_batch(2)))
            .await
            .unwrap();
        drop(tx);

        sink.run(rx).await.unwrap();

        let es_dlq = dlq_dir.join("elasticsearch");
        assert!(es_dlq.exists());
        let mut json_files = Vec::new();
        for entry in std::fs::read_dir(&es_dlq).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) == Some("json") {
                json_files.push(p);
            }
        }
        assert_eq!(json_files.len(), 1);
        let manifest: DeadLetterManifest =
            serde_json::from_slice(&std::fs::read(&json_files[0]).unwrap()).unwrap();
        assert_eq!(manifest.row_count, 1);

        let _ = std::fs::remove_dir_all(&dlq_dir);
    }
}
