//! Elasticsearch and `OpenSearch` sink for the `opentelemetry-datalake` pipeline.

pub mod client;
pub mod config;
pub mod error;
pub mod serializer;
pub mod tls;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, SignalBatch, Sink};
use pipeline_core::sort::{BatchSorter, SignalType};

pub use client::{BulkItem, BulkItemError, BulkItemWrapper, BulkResponse, HttpClient};
pub use config::{
    DataStreamMapping, ElasticsearchAuthConfig, ElasticsearchBatchingConfig,
    ElasticsearchSinkConfig,
};
pub use error::ElasticsearchError;
pub use serializer::{serialize_batch, serialize_batch_chunks};
pub use tls::TlsConfig;

/// Internal accumulator for micro-batch buffering per signal type.
#[derive(Debug, Default)]
struct BufferState {
    batches: Vec<RecordBatch>,
    bytes: usize,
    records: usize,
}

/// Elasticsearch and `OpenSearch` sink for the `opentelemetry-datalake` pipeline.
///
/// Ingests [`SignalBatch`] events (Logs, Metrics, Traces), accumulates them in micro-buffers
/// per signal type, sorts them chronologically using [`BatchSorter`], serializes them into
/// NDJSON bulk format via [`serialize_batch`], and streams them concurrently into
/// Elasticsearch/`OpenSearch` Data Streams via HTTP/2 using [`HttpClient`].
///
/// # Concurrency
///
/// Implements [`Sink`], processing incoming batches on an asynchronous Tokio runtime.
/// HTTP bulk requests are dispatched asynchronously into a [`tokio::task::JoinSet`], bounded
/// by a [`tokio::sync::Semaphore`] enforcing `max_concurrent_requests`.
/// The sink is [`Send`] and [`Sync`].
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

    /// Records the result of an in-flight bulk task into the first encountered error accumulator.
    fn record_task_result(
        res: Result<Result<BulkResponse, ElasticsearchError>, tokio::task::JoinError>,
        first_err: &mut Option<PipelineError>,
    ) {
        match res {
            Ok(Ok(_resp)) => {}
            Ok(Err(es_err)) => {
                if first_err.is_none() {
                    *first_err = Some(es_err.into());
                }
            }
            Err(join_err) => {
                if first_err.is_none() {
                    *first_err = Some(PipelineError::Internal(format!(
                        "Bulk dispatch task failed: {join_err}"
                    )));
                }
            }
        }
    }

    /// Dispatches a single bulk payload chunk under the concurrency semaphore.
    ///
    /// If permits are exhausted, awaits the next completed task in `join_set`. Any error
    /// from previously dispatched tasks is retained in `first_err` without aborting the
    /// submission of the current chunk once a permit is freed.
    async fn dispatch(
        client: Arc<HttpClient>,
        semaphore: Arc<tokio::sync::Semaphore>,
        join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
        data_stream: String,
        payload: Bytes,
        first_err: &mut Option<PipelineError>,
    ) -> Result<(), PipelineError> {
        if payload.is_empty() {
            return Ok(());
        }

        // Drain any already completed tasks in join_set to detect completed tasks
        while let Some(res) = join_set.try_join_next() {
            Self::record_task_result(res, first_err);
        }

        // If semaphore permits are exhausted, await until at least one task completes
        while semaphore.available_permits() == 0 && !join_set.is_empty() {
            if let Some(res) = join_set.join_next().await {
                Self::record_task_result(res, first_err);
            }
        }

        let permit = semaphore.acquire_owned().await.map_err(|e| {
            PipelineError::Internal(format!("Failed to acquire semaphore permit: {e}"))
        })?;

        join_set.spawn(async move {
            let _permit = permit;
            client.send_bulk(&data_stream, payload).await
        });

        Ok(())
    }

    /// Awaits all remaining in-flight tasks in the join set.
    ///
    /// Drains all tasks to completion before returning to ensure no in-flight requests are
    /// abruptly aborted on the first failure. Retains the first encountered error.
    async fn drain_join_set(
        join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
    ) -> Result<(), PipelineError> {
        let mut first_err = None;
        while let Some(res) = join_set.join_next().await {
            Self::record_task_result(res, &mut first_err);
        }
        if let Some(err) = first_err {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Concatenates accumulated batches, sorts them, serializes to NDJSON chunks, and dispatches.
    async fn flush_buffer(
        &self,
        buf: &mut BufferState,
        signal_type: SignalType,
        target_data_stream: &str,
        join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
    ) -> Result<(), PipelineError> {
        if buf.batches.is_empty() {
            return Ok(());
        }

        let signal_label = match signal_type {
            SignalType::Logs => "logs",
            SignalType::Metrics => "metrics",
            SignalType::Traces => "traces",
        };

        tracing::debug!(
            signal = signal_label,
            bytes = buf.bytes,
            records = buf.records,
            data_stream = target_data_stream,
            "Flushing buffer"
        );

        let mut batches = std::mem::take(&mut buf.batches);
        buf.bytes = 0;
        buf.records = 0;

        let sorter = self.sorter.clone();
        let unpack_attributes = self.config.unpack_attributes;
        let max_payload_bytes = self.config.max_payload_bytes;

        let chunks = tokio::task::spawn_blocking(move || -> Result<Vec<Bytes>, PipelineError> {
            let combined = if batches.len() == 1 {
                match batches.pop() {
                    Some(b) => b,
                    None => return Ok(Vec::new()),
                }
            } else {
                let schema = match batches.first() {
                    Some(b) => b.schema(),
                    None => return Ok(Vec::new()),
                };
                let refs: Vec<&RecordBatch> = batches.iter().collect();
                arrow::compute::concat_batches(&schema, refs).map_err(PipelineError::Arrow)?
            };

            if combined.num_rows() == 0 {
                return Ok(Vec::new());
            }

            let sorted_batch = sorter.sort(&combined, signal_type)?;
            let chunks = crate::serializer::serialize_batch_chunks(
                &sorted_batch,
                unpack_attributes,
                max_payload_bytes,
            )?;
            Ok(chunks)
        })
        .await
        .map_err(|e| PipelineError::Internal(format!("Serialization task panicked: {e}")))??;

        let stream_name = target_data_stream.to_string();
        let mut first_dispatch_error: Option<PipelineError> = None;
        for chunk in chunks {
            Self::dispatch(
                Arc::clone(&self.client),
                Arc::clone(&self.semaphore),
                join_set,
                stream_name.clone(),
                chunk,
                &mut first_dispatch_error,
            )
            .await?;
        }

        if let Some(err) = first_dispatch_error {
            let _ = Self::drain_join_set(join_set).await;
            return Err(err);
        }

        tracing::debug!(
            signal = signal_label,
            data_stream = target_data_stream,
            "Buffer flushed and reset"
        );

        Ok(())
    }

    /// Flushes all active buffers across logs, metrics, and traces.
    async fn flush_all_buffers(
        &self,
        logs_buf: &mut BufferState,
        metrics_buf: &mut BufferState,
        traces_buf: &mut BufferState,
        join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
    ) -> Result<(), PipelineError> {
        self.flush_buffer(
            logs_buf,
            SignalType::Logs,
            self.data_stream_for(SignalType::Logs),
            join_set,
        )
        .await?;

        self.flush_buffer(
            metrics_buf,
            SignalType::Metrics,
            self.data_stream_for(SignalType::Metrics),
            join_set,
        )
        .await?;

        self.flush_buffer(
            traces_buf,
            SignalType::Traces,
            self.data_stream_for(SignalType::Traces),
            join_set,
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        tracing::info!(
            endpoints = ?self.config.endpoints,
            logs_stream = %self.config.data_streams.logs,
            metrics_stream = %self.config.data_streams.metrics,
            traces_stream = %self.config.data_streams.traces,
            "ElasticsearchSink started"
        );

        // 1. Startup validation
        self.validate_startup().await?;

        let batching = self.config.batching.clone();
        let max_interval = batching
            .as_ref()
            .map_or(86_400, |b| b.max_batch_interval_sec);

        let mut logs_buf = BufferState::default();
        let mut metrics_buf = BufferState::default();
        let mut traces_buf = BufferState::default();

        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(max_interval.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        let mut join_set: tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>> =
            tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                Some(res) = join_set.join_next(), if !join_set.is_empty() => {
                    match res {
                        Ok(Ok(_resp)) => {}
                        Ok(Err(es_err)) => {
                            let _ = Self::drain_join_set(&mut join_set).await;
                            return Err(es_err.into());
                        }
                        Err(join_err) => {
                            let _ = Self::drain_join_set(&mut join_set).await;
                            return Err(PipelineError::Internal(format!(
                                "Bulk dispatch task failed: {join_err}"
                            )));
                        }
                    }
                }
                _ = interval.tick(), if batching.is_some() && (!logs_buf.batches.is_empty() || !metrics_buf.batches.is_empty() || !traces_buf.batches.is_empty()) => {
                    if let Err(e) = self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await {
                        let _ = Self::drain_join_set(&mut join_set).await;
                        return Err(e);
                    }
                }
                msg = input.recv() => {
                    if let Some(signal) = msg {
                        let (signal_type, batch) = match signal {
                            SignalBatch::Logs(b) => (SignalType::Logs, b),
                            SignalBatch::Metrics(b) => (SignalType::Metrics, b),
                            SignalBatch::Traces(b) => (SignalType::Traces, b),
                        };

                        if batch.num_rows() == 0 {
                            continue;
                        }

                        let buf = match signal_type {
                            SignalType::Logs => &mut logs_buf,
                            SignalType::Metrics => &mut metrics_buf,
                            SignalType::Traces => &mut traces_buf,
                        };

                        let signal_label = match signal_type {
                            SignalType::Logs => "logs",
                            SignalType::Metrics => "metrics",
                            SignalType::Traces => "traces",
                        };

                        let batch_bytes = batch.get_array_memory_size();
                        let batch_rows = batch.num_rows();

                        buf.bytes = buf.bytes.saturating_add(batch_bytes);
                        buf.records = buf.records.saturating_add(batch_rows);
                        buf.batches.push(batch);

                        tracing::debug!(
                            signal = signal_label,
                            bytes = buf.bytes,
                            records = buf.records,
                            data_stream = self.data_stream_for(signal_type),
                            "Buffer accumulated batch"
                        );

                        let should_flush = match batching.as_ref() {
                            Some(cfg) => {
                                buf.bytes >= cfg.max_batch_size_bytes
                                    || buf.records >= cfg.max_batch_records
                            }
                            None => true,
                        };

                        if should_flush {
                            if batching.is_some() {
                                tracing::debug!(
                                    signal = signal_label,
                                    bytes = buf.bytes,
                                    records = buf.records,
                                    data_stream = self.data_stream_for(signal_type),
                                    reason = "threshold",
                                    "Buffer threshold reached, flushing"
                                );
                            }
                            if let Err(e) = self
                                .flush_buffer(
                                    buf,
                                    signal_type,
                                    self.data_stream_for(signal_type),
                                    &mut join_set,
                                )
                                .await
                            {
                                let _ = Self::drain_join_set(&mut join_set).await;
                                return Err(e);
                            }
                        }
                    } else {
                        if let Err(e) = self
                            .flush_all_buffers(
                                &mut logs_buf,
                                &mut metrics_buf,
                                &mut traces_buf,
                                &mut join_set,
                            )
                            .await
                        {
                            let _ = Self::drain_join_set(&mut join_set).await;
                            return Err(e);
                        }

                        Self::drain_join_set(&mut join_set).await?;
                        break;
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
                ])),
                Arc::new(StringArray::from(vec!["system.cpu.utilization"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.42])),
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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
        config.max_retries = 2;
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        drop(tx);

        let err = sink.run(rx).await.unwrap_err();
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Exhausted 429 retries must produce PipelineError::Storage, got: {err:?}"
        );
        assert!(err.to_string().contains("HTTP 429"));
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 4,
        });
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(10);

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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (_tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (_tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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
            Err(ElasticsearchError::StartupValidation(
                "task 1 failed".to_string(),
            ))
        });

        // Task 2: Sleeps a bit then succeeds, recording completion
        let counter2 = Arc::clone(&completed_tasks);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            counter2.fetch_add(1, Ordering::SeqCst);
            Ok(BulkResponse {
                took: 1,
                errors: false,
                items: vec![],
            })
        });

        // Task 3: Sleeps a bit then succeeds, recording completion
        let counter3 = Arc::clone(&completed_tasks);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            counter3.fetch_add(1, Ordering::SeqCst);
            Ok(BulkResponse {
                took: 1,
                errors: false,
                items: vec![],
            })
        });

        let err = ElasticsearchSink::drain_join_set(&mut join_set)
            .await
            .unwrap_err();
        assert!(matches!(err, PipelineError::Storage(_)));
        assert!(err.to_string().contains("task 1 failed"));
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

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        // Mock 1: Fails immediately with 400 Bad Request
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service-fail")
            })
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": {
                    "root_cause": [{"type": "illegal_argument_exception", "reason": "bad payload"}],
                    "type": "illegal_argument_exception",
                    "reason": "bad payload"
                },
                "status": 400
            })))
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
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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
        config.order_by = Some(pipeline_core::sort::SortConfig {
            on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
            logs: vec![],
            metrics: vec![pipeline_core::sort::SortColumnDef::Shorthand(
                "nonexistent_sort_col ASC".to_string(),
            )],
            traces: vec![],
        });
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let batch1 = make_log_batch();
        tx.send(SignalBatch::Logs(batch1)).await.unwrap();

        // Allow batch 1 to be dispatched into join_set and reach wiremock
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Batch 2 causes flush_buffer to error due to missing required sort column
        let batch2 = make_metric_batch();
        tx.send(SignalBatch::Metrics(batch2)).await.unwrap();
        drop(tx);

        let result = sink
            .run(pipeline_core::pipeline::PipelineReceiver::from(rx))
            .await;
        assert!(result.is_err(), "expected flush error to propagate");
        assert!(
            in_flight_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task must be drained and allowed to complete when flush_buffer fails"
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
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
        tx.send(SignalBatch::Metrics(make_metric_batch()))
            .await
            .unwrap();
        drop(tx);

        let result = sink
            .run(pipeline_core::pipeline::PipelineReceiver::from(rx))
            .await;
        assert!(
            result.is_err(),
            "expected shutdown flush error to propagate"
        );
        assert!(
            in_flight_completed.load(std::sync::atomic::Ordering::SeqCst),
            "In-flight task from earlier buffer must be drained when shutdown flush_all_buffers fails"
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
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let run_handle = tokio::spawn(async move {
            sink.run(pipeline_core::pipeline::PipelineReceiver::from(rx))
                .await
        });

        tx.send(SignalBatch::Logs(make_log_batch())).await.unwrap();
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
            "In-flight task from earlier buffer must be drained when interval tick flush_all_buffers fails"
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
            Err(ElasticsearchError::StartupValidation(
                "fast failure".to_string(),
            ))
        });

        // In-flight task 2: slow task
        let flag = Arc::clone(&slow_task_completed);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(BulkResponse {
                took: 1,
                errors: false,
                items: vec![],
            })
        });

        // Yield so task 1 finishes and task 2 starts sleeping
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let mut first_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            semaphore,
            &mut join_set,
            "logs-otel-default".to_string(),
            bytes::Bytes::from_static(b"dummy payload"),
            &mut first_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should succeed without aborting chunk submission"
        );
        assert!(
            first_err.is_some(),
            "first_err should capture the earlier failure"
        );

        // Sinks drain remaining tasks after all chunks are dispatched
        let _ = ElasticsearchSink::drain_join_set(&mut join_set).await;
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
            Err(ElasticsearchError::StartupValidation(
                "failure while waiting for permit".to_string(),
            ))
        });

        // Task 2: slow task finishes after 50ms
        let flag = Arc::clone(&slow_task_completed);
        join_set.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(BulkResponse {
                took: 1,
                errors: false,
                items: vec![],
            })
        });

        let mut first_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            Arc::clone(&semaphore),
            &mut join_set,
            "logs-otel-default".to_string(),
            bytes::Bytes::from_static(b"dummy payload"),
            &mut first_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should succeed once permit is freed by failed task"
        );
        assert!(
            first_err.is_some(),
            "first_err should capture the failure from task 1"
        );

        // Sinks drain remaining tasks after all chunks are dispatched
        let _ = ElasticsearchSink::drain_join_set(&mut join_set).await;
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
    async fn test_buffer_accumulation_emits_debug_event() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default, Clone)]
        struct EventCapture(Arc<Mutex<Vec<String>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct Visitor(String);
                impl tracing::field::Visit for Visitor {
                    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                        if field.name() == "message" {
                            self.0 = value.to_string();
                        }
                    }
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        if field.name() == "message" {
                            self.0 = format!("{value:?}");
                        }
                    }
                }
                let mut v = Visitor(String::new());
                event.record(&mut v);
                if !v.0.is_empty() {
                    self.0.lock().unwrap().push(v.0);
                }
            }
        }

        let captured = EventCapture::default();
        let events = Arc::clone(&captured.0);
        let subscriber = tracing_subscriber::registry().with(captured);
        let _guard = tracing::subscriber::set_default(subscriber);

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
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let batch = make_test_log_batch(1); // below threshold
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        let _ = sink
            .run(pipeline_core::pipeline::PipelineReceiver::from(rx))
            .await;

        let msgs = events.lock().unwrap();
        assert!(
            msgs.iter().any(|m| m.contains("Buffer accumulated batch")),
            "expected 'Buffer accumulated batch' debug event; got: {msgs:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_buffer_all_telemetry_events_and_fields() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Default, Clone, Debug)]
        struct EventRecord {
            message: String,
            signal: String,
            bytes: u64,
            records: u64,
            data_stream: String,
            reason: String,
        }

        #[derive(Default, Clone)]
        struct EventCapture(Arc<Mutex<Vec<EventRecord>>>);

        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                struct Visitor(EventRecord);
                impl tracing::field::Visit for Visitor {
                    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                        match field.name() {
                            "message" => self.0.message = value.to_string(),
                            "signal" => self.0.signal = value.to_string(),
                            "data_stream" => self.0.data_stream = value.to_string(),
                            "reason" => self.0.reason = value.to_string(),
                            _ => {}
                        }
                    }
                    fn record_debug(
                        &mut self,
                        field: &tracing::field::Field,
                        value: &dyn std::fmt::Debug,
                    ) {
                        match field.name() {
                            "message" => self.0.message = format!("{value:?}"),
                            "signal" => self.0.signal = format!("{value:?}"),
                            "data_stream" => self.0.data_stream = format!("{value:?}"),
                            "reason" => self.0.reason = format!("{value:?}"),
                            _ => {}
                        }
                    }
                    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                        match field.name() {
                            "bytes" => self.0.bytes = value,
                            "records" => self.0.records = value,
                            _ => {}
                        }
                    }
                }
                let mut v = Visitor(EventRecord::default());
                event.record(&mut v);
                if !v.0.message.is_empty() {
                    self.0.lock().unwrap().push(v.0);
                }
            }
        }

        let captured = EventCapture::default();
        let records = Arc::clone(&captured.0);
        let subscriber = tracing_subscriber::registry().with(captured);
        let _guard = tracing::subscriber::set_default(subscriber);

        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .mount(&mock_server)
            .await;

        let mut config = make_test_config_with_batching(mock_server.uri());
        config.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 10_485_760,
            max_batch_interval_sec: 60,
            max_batch_records: 2,
        });
        let mut sink = ElasticsearchSink::try_new(config).expect("sink construction");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        // Batch 1: 1 record -> accumulated (below threshold)
        tx.send(SignalBatch::Logs(make_test_log_batch(1)))
            .await
            .unwrap();
        // Batch 2: 1 record -> accumulated, then threshold reached (records >= 2) -> flush
        tx.send(SignalBatch::Logs(make_test_log_batch(1)))
            .await
            .unwrap();
        drop(tx);

        let res = sink
            .run(pipeline_core::pipeline::PipelineReceiver::from(rx))
            .await;
        assert!(res.is_ok());

        let events = records.lock().unwrap();
        // 1. Buffer accumulated batch
        let acc_events: Vec<_> = events
            .iter()
            .filter(|e| e.message == "Buffer accumulated batch")
            .collect();
        assert_eq!(acc_events.len(), 2, "Expected 2 accumulation events");
        assert_eq!(acc_events[0].signal, "logs");
        assert_eq!(acc_events[0].data_stream, "logs-otel-default");
        assert_eq!(acc_events[0].records, 1);
        assert!(acc_events[0].bytes > 0);

        assert_eq!(acc_events[1].signal, "logs");
        assert_eq!(acc_events[1].data_stream, "logs-otel-default");
        assert_eq!(acc_events[1].records, 2);
        assert!(acc_events[1].bytes > acc_events[0].bytes);

        // 2. Buffer threshold reached, flushing
        let thresh_events: Vec<_> = events
            .iter()
            .filter(|e| e.message == "Buffer threshold reached, flushing")
            .collect();
        assert_eq!(thresh_events.len(), 1, "Expected 1 threshold event");
        assert_eq!(thresh_events[0].signal, "logs");
        assert_eq!(thresh_events[0].data_stream, "logs-otel-default");
        assert_eq!(thresh_events[0].records, 2);
        assert_eq!(thresh_events[0].reason, "threshold");

        // 3. Flushing buffer
        let flush_events: Vec<_> = events
            .iter()
            .filter(|e| e.message == "Flushing buffer")
            .collect();
        assert_eq!(flush_events.len(), 1, "Expected 1 flush event");
        assert_eq!(flush_events[0].signal, "logs");
        assert_eq!(flush_events[0].data_stream, "logs-otel-default");
        assert_eq!(flush_events[0].records, 2);

        // 4. Buffer flushed and reset
        let reset_events: Vec<_> = events
            .iter()
            .filter(|e| e.message == "Buffer flushed and reset")
            .collect();
        assert_eq!(reset_events.len(), 1, "Expected 1 reset event");
        assert_eq!(reset_events[0].signal, "logs");
        assert_eq!(reset_events[0].data_stream, "logs-otel-default");
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
        let (tx, rx) = tokio::sync::mpsc::channel(10);

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
        let (tx, rx) = tokio::sync::mpsc::channel(10);

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

        // First bulk request fails with 500 (exhausting retries if retried)
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(|req: &wiremock::Request| {
                let body_str = String::from_utf8_lossy(&req.body);
                body_str.contains("service_0")
            })
            .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
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

        let mut sink = ElasticsearchSink::try_new(config).expect("sink creation failed");

        let (tx, rx) = tokio::sync::mpsc::channel(10);
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

        let mut first_err = None;
        let res = ElasticsearchSink::dispatch(
            client,
            semaphore,
            &mut join_set,
            "logs-otel-default".to_string(),
            bytes::Bytes::from_static(b"dummy payload"),
            &mut first_err,
        )
        .await;

        assert!(
            res.is_ok(),
            "dispatch should submit chunk even if earlier task panicked"
        );
        assert!(
            first_err.is_some(),
            "first_err must record task panic JoinError"
        );
        let err_msg = first_err.unwrap().to_string();
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

        let res = ElasticsearchSink::drain_join_set(&mut join_set).await;
        assert!(
            res.is_err(),
            "drain_join_set must return error when task panicked"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("Bulk dispatch task failed"),
            "error must describe join error, got: {err_msg}"
        );
    }

    #[test]
    fn test_record_task_result_unit() {
        let mut first_err = None;

        // Ok(Ok) does not record error
        ElasticsearchSink::record_task_result(
            Ok(Ok(BulkResponse {
                took: 1,
                errors: false,
                items: vec![],
            })),
            &mut first_err,
        );
        assert!(first_err.is_none());

        // Ok(Err) records first error
        ElasticsearchSink::record_task_result(
            Ok(Err(ElasticsearchError::StartupValidation(
                "first error".to_string(),
            ))),
            &mut first_err,
        );
        assert!(first_err.is_some());
        assert!(
            first_err
                .as_ref()
                .unwrap()
                .to_string()
                .contains("first error")
        );

        // Subsequent error does not overwrite first_err
        ElasticsearchSink::record_task_result(
            Ok(Err(ElasticsearchError::StartupValidation(
                "second error".to_string(),
            ))),
            &mut first_err,
        );
        assert!(
            first_err
                .as_ref()
                .unwrap()
                .to_string()
                .contains("first error")
        );
    }
}
