use clap::Parser;
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use pipeline_core::pipeline::Sink;
use pipeline_core::pipeline::Source;
use pipeline_core::pipeline::Transform;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Clone)]
struct AppConfig {
    #[serde(flatten)]
    pipeline: pipeline_core::config::PipelineConfig,
    server: ServerConfig,
    kafka: Option<KafkaConfig>,
    iceberg: Option<storage::iceberg::IcebergSinkConfig>,
    starrocks: Option<starrocks_sink::StarRocksSinkConfig>,
    elasticsearch: Option<elasticsearch_sink::ElasticsearchSinkConfig>,
    #[serde(default)]
    pub wasm_transformer: Option<pipeline_core::config::WasmTransformerConfig>,
}

#[derive(Debug, Deserialize, Clone)]
#[allow(clippy::struct_field_names)]
struct ServerConfig {
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    #[serde(default)]
    admin_addr: Option<SocketAddr>,
}

#[derive(Debug, Deserialize, Clone)]
struct KafkaConfig {
    brokers: String,
    logs_topic: String,
    traces_topic: String,
    metrics_topic: String,
    logs_format: String,
    traces_format: String,
    metrics_format: String,
    logs_partition_key: Option<String>,
    metrics_partition_key: Option<String>,
    traces_partition_key: Option<String>,
    #[serde(default)]
    order_by: Option<pipeline_core::sort::SortConfig>,
    #[serde(default)]
    options: HashMap<String, String>,
}

#[derive(Parser, Debug)]
#[command(name = "opentelemetry-datalake")]
#[command(version, about = "High-performance OTLP data lakehouse receiver", long_about = None)]
struct Cli {
    /// Path to the configuration file (TOML format)
    #[arg(short, long, value_name = "FILE")]
    config: Option<std::path::PathBuf>,

    /// Set the level of logging verbosity (can be specified multiple times, e.g. -v, -vv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Check the configuration file for validity and exit
    #[arg(long)]
    check: bool,

    /// Override gRPC bind address (e.g. 0.0.0.0:4317)
    #[arg(long, value_name = "ADDR")]
    grpc_addr: Option<SocketAddr>,

    /// Override HTTP bind address (e.g. 0.0.0.0:4318)
    #[arg(long, value_name = "ADDR")]
    http_addr: Option<SocketAddr>,

    /// Override admin HTTP bind address (e.g. 127.0.0.1:9090)
    #[arg(long, value_name = "ADDR")]
    admin_addr: Option<SocketAddr>,

    /// Override dry-run mode for Iceberg sink
    #[arg(long)]
    dry_run: Option<bool>,
}

/// Validates that required sink configuration constraints are satisfied.
fn validate_config(config: &AppConfig) -> anyhow::Result<()> {
    if let Some(ref wasm_cfg) = config.wasm_transformer {
        wasm_transformer::WasmTransformer::validate_config(wasm_cfg)
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {e}"))?;
    }

    if config.kafka.is_none()
        && config.iceberg.is_none()
        && config.starrocks.is_none()
        && config.elasticsearch.is_none()
    {
        anyhow::bail!(
            "Configuration validation failed: one of [kafka], [iceberg], [starrocks], or [elasticsearch] configuration must be provided"
        );
    }

    if let Some(ref iceberg_cfg) = config.iceberg {
        let logs_table = iceberg_cfg
            .logs_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);
        let traces_table = iceberg_cfg
            .traces_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);
        let metrics_table = iceberg_cfg
            .metrics_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);

        if logs_table == traces_table
            || logs_table == metrics_table
            || traces_table == metrics_table
        {
            anyhow::bail!(
                "Configuration validation failed: logs, traces, and metrics Iceberg table identifiers must be distinct. Got: logs='{logs_table}', traces='{traces_table}', metrics='{metrics_table}'"
            );
        }
    } else if let Some(ref es_cfg) = config.elasticsearch {
        es_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {e}"))?;
    } else if let Some(ref sr_cfg) = config.starrocks {
        sr_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {e}"))?;
    } else if let Some(sort_cfg) = config.kafka.as_ref().and_then(|k| k.order_by.as_ref()) {
        pipeline_core::sort::BatchSorter::from_config(sort_cfg)
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {e}"))?;
    }

    if let Some(admin_addr) = config.server.admin_addr {
        if !is_non_public_ip(admin_addr.ip()) {
            anyhow::bail!(
                "Configuration validation failed: server.admin_addr ({admin_addr}) cannot bind to wildcard/unspecified address or public address; loopback or private interface required to prevent public exposure"
            );
        }
        if admin_addr.port() == config.server.http_addr.port()
            || admin_addr.port() == config.server.grpc_addr.port()
        {
            anyhow::bail!(
                "Configuration validation failed: server.admin_addr port ({}) conflicts with http_addr port ({}) or grpc_addr port ({}) (port isolation required)",
                admin_addr.port(),
                config.server.http_addr.port(),
                config.server.grpc_addr.port()
            );
        }
    }

    Ok(())
}

/// Returns `true` if `ip` is a non-public address (loopback, RFC 1918 private, or link-local).
///
/// Unspecified/wildcard addresses (`0.0.0.0`, `::`) and publicly routable IP addresses return `false`.
fn is_non_public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ipv4) => {
            !ipv4.is_unspecified()
                && (ipv4.is_loopback() || ipv4.is_private() || ipv4.is_link_local())
        }
        std::net::IpAddr::V6(ipv6) => {
            if ipv6.is_unspecified() {
                return false;
            }
            if ipv6.is_loopback() {
                return true;
            }
            let octets = ipv6.octets();
            // IPv4-mapped IPv6 (::ffff:x.x.x.x)
            if octets[..10].iter().all(|&b| b == 0) && octets[10] == 0xff && octets[11] == 0xff {
                let v4 = std::net::Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]);
                return v4.is_loopback() || v4.is_private() || v4.is_link_local();
            }
            // Unique local address (fc00::/7)
            if (octets[0] & 0xfe) == 0xfc {
                return true;
            }
            // Unicast link-local address (fe80::/10)
            if octets[0] == 0xfe && (octets[1] & 0xc0) == 0x80 {
                return true;
            }
            false
        }
    }
}

static DLQ_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DLQ_TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Returns `true` if `val` is a safe, single relative path component without path traversal,
/// separators, or null bytes (e.g. neither empty, `"."`, `".."`, nor containing `/` or `\`).
fn is_safe_path_component(val: &str) -> bool {
    if val.is_empty() || val.contains('/') || val.contains('\\') || val.contains('\0') {
        return false;
    }
    let mut components = std::path::Path::new(val).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

/// Durable filesystem Dead Letter Queue (DLQ) sink writing Arrow IPC batches directly in the routed send path.
#[derive(Debug)]
pub struct FileDlqSink {
    transformer_id: String,
    signal: String,
    role: String,
    dlq_dir: std::path::PathBuf,
}

impl FileDlqSink {
    /// Creates a new `FileDlqSink` targeting the directory `dlq/{transformer_id}/{signal}/{role}`.
    ///
    /// # Errors
    ///
    /// Returns an error if `transformer_id`, `signal`, or `role` contain path separators
    /// or traversal components (e.g., `..`, `/`, `\`), which would escape the intended DLQ hierarchy.
    pub fn new(transformer_id: &str, signal: &str, role: &str) -> anyhow::Result<Self> {
        for (label, value) in [
            ("transformer_id", transformer_id),
            ("signal", signal),
            ("role", role),
        ] {
            if !is_safe_path_component(value) {
                anyhow::bail!("DLQ {label} contains unsafe path components: {value:?}");
            }
        }
        let dlq_dir = std::path::PathBuf::from("dlq")
            .join(transformer_id)
            .join(signal)
            .join(role);
        Ok(Self {
            transformer_id: transformer_id.to_string(),
            signal: signal.to_string(),
            role: role.to_string(),
            dlq_dir,
        })
    }

    /// Atomically persists an Arrow IPC payload to disk via a temporary file and sync rename.
    async fn persist_ipc_payload(
        &self,
        file_path: &std::path::Path,
        temp_file_path: &std::path::Path,
        buf: &[u8],
    ) -> Result<(), std::io::Error> {
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temp_file_path)
            .await?;

        if let Err(e) = file.write_all(buf).await {
            drop(file);
            let _ = tokio::fs::remove_file(temp_file_path).await;
            return Err(e);
        }

        if let Err(e) = file.sync_all().await {
            drop(file);
            let _ = tokio::fs::remove_file(temp_file_path).await;
            return Err(e);
        }

        drop(file);

        if let Err(e) = tokio::fs::rename(temp_file_path, file_path).await {
            let _ = tokio::fs::remove_file(temp_file_path).await;
            return Err(e);
        }

        #[cfg(unix)]
        {
            let dir = tokio::fs::File::open(&self.dlq_dir).await.map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to open DLQ directory {} for sync: {e}",
                        self.dlq_dir.display()
                    ),
                )
            })?;
            dir.sync_all().await.map_err(|e| {
                std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to sync DLQ directory {}: {e}",
                        self.dlq_dir.display()
                    ),
                )
            })?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl wasm_transformer::DlqSink for FileDlqSink {
    async fn send(
        &self,
        batch: pipeline_core::pipeline::SignalBatch,
    ) -> Result<(), pipeline_core::error::PipelineError> {
        if let Err(e) = tokio::fs::create_dir_all(&self.dlq_dir).await {
            tracing::error!(
                "Failed to create DLQ directory {}: {e}. Terminating DLQ task to propagate backpressure.",
                self.dlq_dir.display()
            );
            return Err(pipeline_core::error::PipelineError::Internal(format!(
                "Failed to create DLQ directory {}: {e}",
                self.dlq_dir.display()
            )));
        }

        let (batch_signal, record_batch) = match &batch {
            pipeline_core::pipeline::SignalBatch::Logs(rb) => ("logs", rb),
            pipeline_core::pipeline::SignalBatch::Metrics(rb) => ("metrics", rb),
            pipeline_core::pipeline::SignalBatch::Traces(rb) => ("traces", rb),
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let seq = DLQ_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let file_path = self
            .dlq_dir
            .join(format!("{batch_signal}_{timestamp}_{seq}.arrow"));

        let encode_res = tokio::task::spawn_blocking({
            let rb = record_batch.clone();
            move || -> Result<Vec<u8>, arrow::error::ArrowError> {
                let mut buf = Vec::new();
                let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &rb.schema())?;
                writer.write(&rb)?;
                writer.finish()?;
                Ok(buf)
            }
        })
        .await;

        let buf = match encode_res {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(e)) => {
                tracing::error!(
                    "Failed to serialize DLQ batch to Arrow IPC: {e}. Terminating DLQ task to propagate backpressure."
                );
                return Err(pipeline_core::error::PipelineError::Internal(format!(
                    "Failed to serialize DLQ batch to Arrow IPC: {e}"
                )));
            }
            Err(join_err) => {
                tracing::error!(
                    "DLQ serialization blocking task failed: {join_err}. Terminating DLQ task to propagate backpressure."
                );
                return Err(pipeline_core::error::PipelineError::Internal(format!(
                    "DLQ serialization blocking task failed: {join_err}"
                )));
            }
        };

        let tmp_seq = DLQ_TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp_file_path = self.dlq_dir.join(format!(
            ".tmp_{batch_signal}_{timestamp}_{seq}_{}_{tmp_seq}.arrow",
            std::process::id()
        ));

        let write_res = self
            .persist_ipc_payload(&file_path, &temp_file_path, &buf)
            .await;

        if let Err(e) = write_res {
            tracing::error!(
                "Failed to persist DLQ batch to {}: {e}. Terminating DLQ task to propagate backpressure.",
                file_path.display()
            );
            return Err(pipeline_core::error::PipelineError::Internal(format!(
                "Failed to persist DLQ batch to {}: {e}",
                file_path.display()
            )));
        }

        tracing::warn!(
            transformer_id = %self.transformer_id,
            signal = %self.signal,
            role = %self.role,
            rows = record_batch.num_rows(),
            path = %file_path.display(),
            "DLQ: Persisted diverted batch to disk"
        );

        Ok(())
    }
}

/// Tuple containing the signal-isolated transformers for logs, traces, and metrics.
type SignalTransformers = (Box<dyn Transform>, Box<dyn Transform>, Box<dyn Transform>);

/// Instantiates a signal-isolated [`wasm_transformer::WasmTransformer`], setting up durable DLQ sinks as needed.
fn instantiate_signal_wasm_transformer(
    cfg: &pipeline_core::config::WasmTransformerConfig,
    signal: &str,
    engine: &std::sync::Arc<wasm_transformer::engine::EngineCache>,
    metric_bridges: &mut Vec<wasm_transformer::MetricBridgeHandle>,
) -> anyhow::Result<Box<dyn Transform>> {
    let mut signal_cfg = cfg.clone();
    signal_cfg
        .env
        .insert("signal".to_string(), signal.to_string());

    let reroute_error = if signal_cfg.on_error == pipeline_core::config::OnErrorPolicy::Reroute {
        Some(wasm_transformer::DlqOutput::Sink(std::sync::Arc::new(
            FileDlqSink::new(&signal_cfg.id, signal, "error")?,
        )))
    } else {
        None
    };

    let reroute_reject = if signal_cfg.on_reject == pipeline_core::config::OnRejectPolicy::Reroute {
        Some(wasm_transformer::DlqOutput::Sink(std::sync::Arc::new(
            FileDlqSink::new(&signal_cfg.id, signal, "reject")?,
        )))
    } else {
        None
    };

    let transformer = wasm_transformer::WasmTransformer::with_engine(
        signal_cfg,
        reroute_error,
        reroute_reject,
        std::sync::Arc::clone(engine),
    )?;

    // Bridge the guest MetricRegistry into OpenTelemetry before erasing the concrete transformer
    let bridge =
        wasm_transformer::bridge_metrics_to_opentelemetry(transformer.metric_registry(), signal);
    metric_bridges.push(bridge);

    Ok(Box::new(transformer))
}

/// Initializes the pipeline transformers based on the provided application configuration.
/// If `wasm_transformer` is configured, it instantiates three signal-isolated instances
/// sharing a common [`wasm_transformer::engine::EngineCache`].
/// Otherwise, it falls back to No-op transformers.
fn initialize_transformers(
    config: &AppConfig,
    metric_bridges: &mut Vec<wasm_transformer::MetricBridgeHandle>,
) -> anyhow::Result<(
    SignalTransformers,
    Option<std::sync::Arc<wasm_transformer::engine::EngineCache>>,
)> {
    if let Some(ref wasm_cfg) = config.wasm_transformer {
        tracing::info!(
            transformer_id = %wasm_cfg.id,
            module_path = %wasm_cfg.module_path,
            "Initializing 3x signal-isolated WasmTransformer instances with shared EngineCache"
        );
        let max_memory_bytes = wasm_transformer::worker::parse_byte_size(&wasm_cfg.max_memory)
            .unwrap_or(64 * 1024 * 1024);
        let pool_capacity = (wasm_cfg.concurrency.max(1) * 6).saturating_add(3);
        let shared_engine = std::sync::Arc::new(
            wasm_transformer::engine::EngineCache::new_pooling(pool_capacity, max_memory_bytes)?,
        );

        let transformers = (
            instantiate_signal_wasm_transformer(wasm_cfg, "logs", &shared_engine, metric_bridges)?,
            instantiate_signal_wasm_transformer(
                wasm_cfg,
                "traces",
                &shared_engine,
                metric_bridges,
            )?,
            instantiate_signal_wasm_transformer(
                wasm_cfg,
                "metrics",
                &shared_engine,
                metric_bridges,
            )?,
        );
        Ok((transformers, Some(shared_engine)))
    } else {
        Ok((
            (
                Box::new(noop_transformer::NoopTransformer::new()),
                Box::new(noop_transformer::NoopTransformer::new()),
                Box::new(noop_transformer::NoopTransformer::new()),
            ),
            None,
        ))
    }
}

#[cfg(unix)]
fn spawn_sighup_reload_task(
    wasm_cfg: pipeline_core::config::WasmTransformerConfig,
    engine: std::sync::Arc<wasm_transformer::engine::EngineCache>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut signal_stream =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to register SIGHUP handler: {e}");
                    return;
                }
            };

        tracing::info!(
            transformer_id = %wasm_cfg.id,
            module_path = %wasm_cfg.module_path,
            "SIGHUP reload handler installed"
        );

        loop {
            tokio::select! {
                biased;
                res = shutdown_rx.changed() => {
                    if res.is_err() || *shutdown_rx.borrow() {
                        tracing::debug!("SIGHUP listener shutting down");
                        break;
                    }
                }
                Some(()) = signal_stream.recv() => {
                    tracing::info!(
                        transformer_id = %wasm_cfg.id,
                        module_path = %wasm_cfg.module_path,
                        "Received SIGHUP signal. Reloading WASM module..."
                    );

                    let wasm_cfg_clone = wasm_cfg.clone();
                    let engine_clone = std::sync::Arc::clone(&engine);

                    let reload_res = tokio::task::spawn_blocking(move || {
                        wasm_transformer::WasmTransformer::reload_module(&wasm_cfg_clone, &engine_clone)
                    })
                    .await;

                    match reload_res {
                        Ok(Ok(generation)) => {
                            tracing::info!(
                                transformer_id = %wasm_cfg.id,
                                generation = generation,
                                "Successfully recompiled WASM module and advanced generation on SIGHUP"
                            );
                        }
                        Ok(Err(e)) => {
                            tracing::error!(
                                transformer_id = %wasm_cfg.id,
                                "Failed to reload WASM module on SIGHUP: {e}"
                            );
                        }
                        Err(e) => {
                            tracing::error!("Blocking reload task panicked on SIGHUP: {e}");
                        }
                    }
                }
            }
        }
    })
}

#[cfg(not(unix))]
fn spawn_sighup_reload_task(
    _wasm_cfg: pipeline_core::config::WasmTransformerConfig,
    _engine: std::sync::Arc<wasm_transformer::engine::EngineCache>,
    _shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {})
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    let cli_args = Cli::parse();

    // Set logging verbosity based on the flag
    if cli_args.verbose > 0 {
        let level = match cli_args.verbose {
            1 => "debug",
            _ => "trace",
        };
        // SAFETY: This is executed at the very beginning of the program's main function
        // before any other threads are spawned, ensuring no concurrent environment mutation occurs.
        unsafe {
            std::env::set_var("RUST_LOG", level);
        }
    }

    // Load configuration using Figment with fallback defaults
    let mut figment = Figment::new().merge(Toml::string(
        r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        "#,
    ));

    if let Some(ref path) = cli_args.config {
        if !path.exists() {
            anyhow::bail!("Configuration file not found: {}", path.display());
        }
        figment = figment.merge(Toml::file(path));
    } else if std::path::Path::new("config.toml").exists() {
        figment = figment.merge(Toml::file("config.toml"));
    }

    let mut config: AppConfig = figment.merge(Env::prefixed("OTEL_DATALAKE_")).extract()?;

    // Apply CLI overrides to configuration
    if let Some(grpc_addr) = cli_args.grpc_addr {
        config.server.grpc_addr = grpc_addr;
    }
    if let Some(http_addr) = cli_args.http_addr {
        config.server.http_addr = http_addr;
    }
    if let Some(admin_addr) = cli_args.admin_addr {
        config.server.admin_addr = Some(admin_addr);
    }
    if let (Some(dry_run), Some(iceberg)) = (cli_args.dry_run, &mut config.iceberg) {
        iceberg.dry_run = dry_run;
    }

    // Perform early validation checks
    validate_config(&config)?;

    if cli_args.check {
        #[allow(clippy::print_stdout)]
        {
            println!("Configuration is valid.");
        }
        return Ok(());
    }

    // Initialize telemetry
    let _telemetry = pipeline_core::telemetry::init_telemetry(&config.pipeline.telemetry)?;

    tracing::info!("Starting opentelemetry-datalake service");

    // Channels for the OTLP source demultiplexer
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(1000);
    let (traces_tx, traces_rx) = tokio::sync::mpsc::channel(1000);
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::channel(1000);

    // Channels between Transformers and Sinks
    let (logs_sink_tx, logs_sink_rx) = tokio::sync::mpsc::channel(1000);
    let (traces_sink_tx, traces_sink_rx) = tokio::sync::mpsc::channel(1000);
    let (metrics_sink_tx, metrics_sink_rx) = tokio::sync::mpsc::channel(1000);

    // Create shutdown watch channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Create OTLP Receiver Source
    let mut source = otlp_receiver::OtlpReceiverSource::new(
        config.server.grpc_addr,
        config.server.http_addr,
        logs_tx,
        traces_tx,
        metrics_tx,
        shutdown_rx.clone(),
    );

    // Create Transformers (WASM if configured, otherwise Noop)
    let mut metric_bridges = Vec::new();
    let ((mut logs_transformer, mut traces_transformer, mut metrics_transformer), shared_engine) =
        initialize_transformers(&config, &mut metric_bridges)?;

    let admin_handle = if let Some(admin_addr) = config.server.admin_addr {
        let expected_sha = config
            .wasm_transformer
            .as_ref()
            .and_then(|w| w.sha256.clone());
        let admin_router = if let Some(ref engine) = shared_engine {
            wasm_transformer::reload::build_admin_router_with_sha(
                std::sync::Arc::clone(engine),
                expected_sha,
            )
        } else {
            axum::Router::new()
        };
        let listener = tokio::net::TcpListener::bind(admin_addr)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to bind admin HTTP listener to {admin_addr}: {e}")
            })?;
        tracing::info!("Admin HTTP server listening on {admin_addr}");
        let mut shutdown_rx_admin = shutdown_tx.subscribe();
        let admin_shutdown = async move {
            while !*shutdown_rx_admin.borrow_and_update() {
                if shutdown_rx_admin.changed().await.is_err() {
                    break;
                }
            }
            tracing::info!("Admin HTTP server shutting down gracefully");
        };
        Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, admin_router)
                .with_graceful_shutdown(admin_shutdown)
                .await
            {
                tracing::error!("Admin HTTP server error: {e}");
            }
        }))
    } else {
        None
    };

    let sighup_handle =
        if let (Some(wasm_cfg), Some(engine)) = (&config.wasm_transformer, &shared_engine) {
            if wasm_cfg.enable_sighup {
                Some(spawn_sighup_reload_task(
                    wasm_cfg.clone(),
                    std::sync::Arc::clone(engine),
                    shutdown_rx.clone(),
                ))
            } else {
                None
            }
        } else {
            None
        };

    // Spawn transformers
    let mut logs_trans_handle = tokio::spawn(async move {
        if let Err(e) = logs_transformer.transform(logs_rx, logs_sink_tx).await {
            tracing::error!("Logs transformer error: {}", e);
            Err(e)
        } else {
            Ok(())
        }
    });

    let mut traces_trans_handle = tokio::spawn(async move {
        if let Err(e) = traces_transformer
            .transform(traces_rx, traces_sink_tx)
            .await
        {
            tracing::error!("Traces transformer error: {}", e);
            Err(e)
        } else {
            Ok(())
        }
    });

    let mut metrics_trans_handle = tokio::spawn(async move {
        if let Err(e) = metrics_transformer
            .transform(metrics_rx, metrics_sink_tx)
            .await
        {
            tracing::error!("Metrics transformer error: {}", e);
            Err(e)
        } else {
            Ok(())
        }
    });

    // Spawn sinks
    let logs_sink_handle;
    let traces_sink_handle;
    let metrics_sink_handle;

    if let Some(ref iceberg_cfg) = config.iceberg {
        let logs_table = iceberg_cfg
            .logs_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);
        let traces_table = iceberg_cfg
            .traces_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);
        let metrics_table = iceberg_cfg
            .metrics_table_identifier
            .as_ref()
            .unwrap_or(&iceberg_cfg.table_identifier);

        tracing::info!(
            "Initializing Iceberg sinks. logs='{}', traces='{}', metrics='{}'",
            logs_table,
            traces_table,
            metrics_table
        );

        let mut logs_cfg = iceberg_cfg.clone();
        logs_cfg.table_identifier.clone_from(logs_table);
        let mut logs_sink = storage::iceberg::IcebergSink::new(logs_cfg);

        let mut traces_cfg = iceberg_cfg.clone();
        traces_cfg.table_identifier.clone_from(traces_table);
        let mut traces_sink = storage::iceberg::IcebergSink::new(traces_cfg);

        let mut metrics_cfg = iceberg_cfg.clone();
        metrics_cfg.table_identifier.clone_from(metrics_table);
        let mut metrics_sink = storage::iceberg::IcebergSink::new(metrics_cfg);

        logs_sink_handle = tokio::spawn(async move {
            if let Err(e) = logs_sink.run(logs_sink_rx).await {
                tracing::error!("Logs Iceberg sink error: {}", e);
            }
        });

        traces_sink_handle = tokio::spawn(async move {
            if let Err(e) = traces_sink.run(traces_sink_rx).await {
                tracing::error!("Traces Iceberg sink error: {}", e);
            }
        });

        metrics_sink_handle = tokio::spawn(async move {
            if let Err(e) = metrics_sink.run(metrics_sink_rx).await {
                tracing::error!("Metrics Iceberg sink error: {}", e);
            }
        });
    } else if let Some(es_cfg) = config.elasticsearch {
        tracing::info!(
            endpoints = ?es_cfg.endpoints,
            "Initializing Elasticsearch sinks"
        );

        let mut logs_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
        let mut traces_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
        let mut metrics_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg)?;

        // Validate cluster health and data stream templates before starting receiver
        logs_sink
            .validate_startup()
            .await
            .map_err(|e| anyhow::anyhow!("Elasticsearch startup validation failed: {e}"))?;

        // Share the client, concurrency limiter, and validated status across sink instances
        traces_sink.share_state_from(&logs_sink);
        metrics_sink.share_state_from(&logs_sink);

        logs_sink_handle = tokio::spawn(async move {
            if let Err(e) = logs_sink.run(logs_sink_rx).await {
                tracing::error!("Logs Elasticsearch sink error: {}", e);
            }
        });

        traces_sink_handle = tokio::spawn(async move {
            if let Err(e) = traces_sink.run(traces_sink_rx).await {
                tracing::error!("Traces Elasticsearch sink error: {}", e);
            }
        });

        metrics_sink_handle = tokio::spawn(async move {
            if let Err(e) = metrics_sink.run(metrics_sink_rx).await {
                tracing::error!("Metrics Elasticsearch sink error: {}", e);
            }
        });
    } else if let Some(starrocks_cfg) = config.starrocks {
        tracing::info!(
            database = %starrocks_cfg.database,
            format = %starrocks_cfg.format,
            mode = ?starrocks_cfg.transaction_mode,
            "Initializing StarRocks sinks"
        );

        let primary_sink = starrocks_sink::StarRocksSink::try_new(starrocks_cfg.clone())?;
        let shared_manager = primary_sink.manager();

        let mut logs_sink = primary_sink;
        let mut traces_sink = starrocks_sink::StarRocksSink::with_manager(
            starrocks_cfg.clone(),
            std::sync::Arc::clone(&shared_manager),
        )?;
        let mut metrics_sink =
            starrocks_sink::StarRocksSink::with_manager(starrocks_cfg, shared_manager)?;

        logs_sink_handle = tokio::spawn(async move {
            if let Err(e) = logs_sink.run(logs_sink_rx).await {
                tracing::error!("Logs StarRocks sink error: {}", e);
            }
        });

        traces_sink_handle = tokio::spawn(async move {
            if let Err(e) = traces_sink.run(traces_sink_rx).await {
                tracing::error!("Traces StarRocks sink error: {}", e);
            }
        });

        metrics_sink_handle = tokio::spawn(async move {
            if let Err(e) = metrics_sink.run(metrics_sink_rx).await {
                tracing::error!("Metrics StarRocks sink error: {}", e);
            }
        });
    } else if let Some(ref kafka_cfg) = config.kafka {
        tracing::info!("Initializing Kafka sinks");

        let sorter = if let Some(ref sort_cfg) = kafka_cfg.order_by {
            pipeline_core::sort::BatchSorter::from_config(sort_cfg)?
        } else {
            pipeline_core::sort::BatchSorter::default()
        };

        let mut logs_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.logs_topic,
            kafka_cfg.logs_format.parse()?,
            &kafka_cfg.options,
        )?
        .with_sorting(sorter.clone(), kafka_cfg.logs_partition_key.clone());

        let mut traces_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.traces_topic,
            kafka_cfg.traces_format.parse()?,
            &kafka_cfg.options,
        )?
        .with_sorting(sorter.clone(), kafka_cfg.traces_partition_key.clone());

        let mut metrics_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.metrics_topic,
            kafka_cfg.metrics_format.parse()?,
            &kafka_cfg.options,
        )?
        .with_sorting(sorter, kafka_cfg.metrics_partition_key.clone());

        logs_sink_handle = tokio::spawn(async move {
            if let Err(e) = logs_sink.run(logs_sink_rx).await {
                tracing::error!("Logs Kafka sink error: {}", e);
            }
        });

        traces_sink_handle = tokio::spawn(async move {
            if let Err(e) = traces_sink.run(traces_sink_rx).await {
                tracing::error!("Traces Kafka sink error: {}", e);
            }
        });

        metrics_sink_handle = tokio::spawn(async move {
            if let Err(e) = metrics_sink.run(metrics_sink_rx).await {
                tracing::error!("Metrics Kafka sink error: {}", e);
            }
        });
    } else {
        return Err(anyhow::anyhow!(
            "One of [iceberg], [elasticsearch], [starrocks], or [kafka] configuration must be provided"
        ));
    }

    // Spawn source
    let mut source_handle = tokio::spawn(async move { source.run().await });

    let mut exit_err: Option<anyhow::Error> = None;

    // Handle shutdown
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received shutdown signal. Starting graceful shutdown...");
            let _ = shutdown_tx.send(true);
            match source_handle.await {
                Ok(Err(e)) => tracing::error!("Source receiver stopped with error: {}", e),
                Ok(Ok(())) => tracing::info!("Source receiver shut down gracefully."),
                Err(e) => tracing::error!("Source receiver task failed/panicked: {}", e),
            }
        }
        res = &mut source_handle => {
            match res {
                Ok(Err(e)) => {
                    tracing::error!("Source receiver stopped unexpectedly with error: {}", e);
                    exit_err = Some(anyhow::anyhow!("Source receiver stopped unexpectedly with error: {e}"));
                }
                Ok(Ok(())) => {
                    tracing::error!("Source receiver stopped unexpectedly.");
                    exit_err = Some(anyhow::anyhow!("Source receiver stopped unexpectedly"));
                }
                Err(e) => {
                    tracing::error!("Source receiver task panicked: {}", e);
                    exit_err = Some(anyhow::anyhow!("Source receiver task panicked: {e}"));
                }
            }
            let _ = shutdown_tx.send(true);
        }
        res = &mut logs_trans_handle => {
            match res {
                Ok(Err(e)) => {
                    tracing::error!("Logs transformer failed: {e}");
                    exit_err = Some(anyhow::anyhow!("Logs transformer failed: {e}"));
                }
                Ok(Ok(())) => {
                    tracing::error!("Logs transformer exited prematurely");
                    exit_err = Some(anyhow::anyhow!("Logs transformer exited prematurely"));
                }
                Err(e) => {
                    tracing::error!("Logs transformer task panicked: {e}");
                    exit_err = Some(anyhow::anyhow!("Logs transformer task panicked: {e}"));
                }
            }
            let _ = shutdown_tx.send(true);
        }
        res = &mut traces_trans_handle => {
            match res {
                Ok(Err(e)) => {
                    tracing::error!("Traces transformer failed: {e}");
                    exit_err = Some(anyhow::anyhow!("Traces transformer failed: {e}"));
                }
                Ok(Ok(())) => {
                    tracing::error!("Traces transformer exited prematurely");
                    exit_err = Some(anyhow::anyhow!("Traces transformer exited prematurely"));
                }
                Err(e) => {
                    tracing::error!("Traces transformer task panicked: {e}");
                    exit_err = Some(anyhow::anyhow!("Traces transformer task panicked: {e}"));
                }
            }
            let _ = shutdown_tx.send(true);
        }
        res = &mut metrics_trans_handle => {
            match res {
                Ok(Err(e)) => {
                    tracing::error!("Metrics transformer failed: {e}");
                    exit_err = Some(anyhow::anyhow!("Metrics transformer failed: {e}"));
                }
                Ok(Ok(())) => {
                    tracing::error!("Metrics transformer exited prematurely");
                    exit_err = Some(anyhow::anyhow!("Metrics transformer exited prematurely"));
                }
                Err(e) => {
                    tracing::error!("Metrics transformer task panicked: {e}");
                    exit_err = Some(anyhow::anyhow!("Metrics transformer task panicked: {e}"));
                }
            }
            let _ = shutdown_tx.send(true);
        }
    }

    // Wait for pipeline to drain
    let _ = tokio::join!(
        async {
            if !logs_trans_handle.is_finished() {
                let _ = logs_trans_handle.await;
            }
        },
        async {
            if !traces_trans_handle.is_finished() {
                let _ = traces_trans_handle.await;
            }
        },
        async {
            if !metrics_trans_handle.is_finished() {
                let _ = metrics_trans_handle.await;
            }
        },
        logs_sink_handle,
        traces_sink_handle,
        metrics_sink_handle
    );

    if let Some(h) = sighup_handle {
        let _ = h.await;
    }

    if let Some(mut h) = admin_handle {
        tokio::select! {
            res = &mut h => {
                if let Err(e) = res {
                    tracing::warn!("Admin HTTP server task exited with error: {e}");
                }
            }
            () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                tracing::warn!("Admin HTTP server did not drain within 5 seconds; aborting");
                h.abort();
                let _ = h.await;
            }
        }
    }

    drop(metric_bridges);

    if let Some(err) = exit_err {
        return Err(err);
    }

    tracing::info!("Shutdown complete.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserializes_and_validates_with_elasticsearch() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [elasticsearch]
        endpoints = ["http://localhost:9200"]
        [elasticsearch.data_streams]
        logs = "logs-otel-default"
        metrics = "metrics-otel-default"
        traces = "traces-otel-default"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Elasticsearch config should deserialize into AppConfig");

        assert!(config.elasticsearch.is_some());
        let es = config.elasticsearch.as_ref().unwrap();
        assert_eq!(es.endpoints, vec!["http://localhost:9200"]);
        assert_eq!(es.data_streams.logs, "logs-otel-default");
        assert_eq!(es.data_streams.metrics, "metrics-otel-default");
        assert_eq!(es.data_streams.traces, "traces-otel-default");
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_config_deserialization_with_elasticsearch_batching_and_auth() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [elasticsearch]
        endpoints = ["http://node1:9200", "http://node2:9200"]
        [elasticsearch.auth]
        type = "api_key"
        api_key = "secret-token"
        [elasticsearch.data_streams]
        logs = "custom-logs"
        metrics = "custom-metrics"
        traces = "custom-traces"
        [elasticsearch.batching]
        max_batch_size_bytes = 5242880
        max_batch_interval_sec = 5
        max_batch_records = 10000
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Elasticsearch full config should deserialize");

        let es = config
            .elasticsearch
            .as_ref()
            .expect("elasticsearch config must be present");
        assert_eq!(es.endpoints.len(), 2);
        assert!(matches!(
            es.auth,
            elasticsearch_sink::ElasticsearchAuthConfig::ApiKey { ref api_key } if api_key == "secret-token"
        ));
        let batching = es.batching.as_ref().expect("batching must be present");
        assert_eq!(batching.max_batch_size_bytes, 5_242_880);
        assert_eq!(batching.max_batch_interval_sec, 5);
        assert_eq!(batching.max_batch_records, 10_000);
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_config_validation_fails_with_no_sink() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Minimal config without sinks should deserialize");

        assert!(config.kafka.is_none());
        assert!(config.iceberg.is_none());
        assert!(config.starrocks.is_none());
        assert!(config.elasticsearch.is_none());

        let err = validate_config(&config)
            .expect_err("Validation should fail when no sink is configured");
        assert!(
            err.to_string()
                .contains("one of [kafka], [iceberg], [starrocks], or [elasticsearch]"),
            "Error message should mention all four sinks: {err}"
        );
    }

    #[test]
    fn test_config_validation_succeeds_with_kafka() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "telemetry-logs"
        traces_topic = "telemetry-traces"
        metrics_topic = "telemetry-metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Kafka config should deserialize");

        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_config_validation_succeeds_with_starrocks() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [starrocks]
        frontend_urls = ["http://localhost:8030"]
        username = "root"
        database = "telemetry"
        [starrocks.table_mapping]
        type = "unified"
        table = "telemetry"
        signal_type_column = "signal_type"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("StarRocks config should deserialize");

        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_config_validation_iceberg_duplicate_identifiers() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [iceberg]
        catalog_name = "test_catalog"
        catalog_type = "Rest"
        catalog_uri = "http://localhost:8181"
        warehouse = "s3://warehouse"
        table_identifier = "db.telemetry"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Iceberg config should deserialize");

        let err = validate_config(&config)
            .expect_err("Duplicate table identifiers should fail validation");
        assert!(
            err.to_string()
                .contains("Iceberg table identifiers must be distinct"),
            "Error message should indicate duplicate table identifiers: {err}"
        );
    }

    #[test]
    fn test_config_with_only_elasticsearch_leaves_kafka_none() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [elasticsearch]
        endpoints = ["http://localhost:9200"]
        [elasticsearch.data_streams]
        logs = "logs-otel-default"
        metrics = "metrics-otel-default"
        traces = "traces-otel-default"
        "#;

        let figment = Figment::new()
            .merge(Toml::string(
                r#"
                [server]
                grpc_addr = "127.0.0.1:4317"
                http_addr = "127.0.0.1:4318"
                "#,
            ))
            .merge(Toml::string(toml_str));

        let config: AppConfig = figment.extract().expect("Config should deserialize");
        assert!(config.elasticsearch.is_some());
        assert!(config.kafka.is_none());
        assert!(config.starrocks.is_none());
        assert!(config.iceberg.is_none());
    }

    #[test]
    fn test_config_defaults_has_no_sinks() {
        let figment = Figment::new().merge(Toml::string(
            r#"
            [server]
            grpc_addr = "127.0.0.1:4317"
            http_addr = "127.0.0.1:4318"
            "#,
        ));

        let config: AppConfig = figment.extract().expect("Config should deserialize");
        assert!(config.kafka.is_none());
        assert!(config.elasticsearch.is_none());
        assert!(config.starrocks.is_none());
        assert!(config.iceberg.is_none());
    }

    #[test]
    fn test_config_validation_fails_with_invalid_elasticsearch_config() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [elasticsearch]
        endpoints = []
        [elasticsearch.data_streams]
        logs = "logs-otel-default"
        metrics = "metrics-otel-default"
        traces = "traces-otel-default"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize");

        let err = validate_config(&config).expect_err("Validation should fail for empty endpoints");
        assert!(
            err.to_string()
                .contains("at least one endpoint must be configured")
        );
    }

    #[test]
    fn test_app_config_deserializes_wasm_transformer() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [wasm_transformer]
        id = "audit_pipeline"
        type = "wasm"
        module_path = "transforms/audit.wasm"
        on_error = "passthrough"
        on_reject = "drop"
        concurrency = 8
        worker_channel_capacity = 2
        max_memory = "128MiB"
        rejuvenate_threshold = "32MiB"
        rejuvenate_batches = 50000
        init_timeout = "5s"
        allow_unmasked_passthrough = true
        schema_guard = "strict"
        env_whitelist = ["REGION", "ENV"]
        enable_sighup = true
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize with wasm_transformer");

        assert!(config.wasm_transformer.is_some());
        let wasm = config.wasm_transformer.unwrap();
        assert_eq!(wasm.id, "audit_pipeline");
        assert_eq!(wasm.r#type, "wasm");
        assert_eq!(wasm.module_path, "transforms/audit.wasm");
        assert_eq!(
            wasm.on_error,
            pipeline_core::config::OnErrorPolicy::Passthrough
        );
        assert_eq!(wasm.on_reject, pipeline_core::config::OnRejectPolicy::Drop);
        assert_eq!(wasm.concurrency, 8);
        assert_eq!(wasm.worker_channel_capacity, 2);
        assert_eq!(wasm.max_memory, "128MiB");
        assert_eq!(wasm.rejuvenate_threshold, "32MiB");
        assert_eq!(wasm.rejuvenate_batches, 50000);
        assert_eq!(wasm.init_timeout, "5s");
        assert!(wasm.allow_unmasked_passthrough);
        assert_eq!(
            wasm.schema_guard,
            pipeline_core::config::SchemaGuardMode::Strict
        );
        assert_eq!(wasm.env_whitelist, vec!["REGION", "ENV"]);
        assert!(wasm.enable_sighup);
    }

    #[tokio::test]
    async fn test_build_admin_router_integration() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use std::sync::Arc;
        use tower::ServiceExt;
        use wasm_transformer::engine::EngineCache;
        use wasm_transformer::reload::build_admin_router;

        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        let router = build_admin_router(engine);
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/transforms/wasm/reload")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"module_path": "/tmp/non_existent.wasm"}"#))
            .expect("Request should be created successfully");

        let response = router
            .oneshot(req)
            .await
            .expect("Router should handle request");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_admin_server_runtime_listener_serves_reload_endpoint() {
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use wasm_transformer::engine::EngineCache;
        use wasm_transformer::reload::build_admin_router_multi;

        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        let router = build_admin_router_multi(vec![engine], None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while !*shutdown_rx.borrow_and_update() {
                        if shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
        });

        // Test sending request to runtime listener over loopback TCP
        let mut stream = tokio::net::TcpStream::connect(bound_addr).await.unwrap();
        let body = r#"{"module_path": ""}"#;
        let req = format!(
            "POST /api/v1/transforms/wasm/reload HTTP/1.1\r\nHost: {bound_addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let resp_str = String::from_utf8_lossy(&buf[..n]);
        assert!(resp_str.starts_with("HTTP/1.1 400 Bad Request"));

        let _ = shutdown_tx.send(true);
        server_handle.await.unwrap();
    }

    #[test]
    fn test_config_validation_fails_when_admin_addr_matches_http_addr() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        admin_addr = "127.0.0.1:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        "#;
        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("port isolation required"));
    }

    #[test]
    fn test_config_validation_fails_when_admin_addr_port_conflicts_across_different_ips() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        admin_addr = "127.0.0.2:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        "#;
        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("port isolation required"));
    }

    #[test]
    fn test_config_validation_fails_when_admin_addr_is_wildcard() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        admin_addr = "0.0.0.0:9090"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        "#;
        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot bind to wildcard/unspecified address")
        );
    }

    #[test]
    fn test_config_validation_fails_when_admin_addr_is_public() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        admin_addr = "8.8.8.8:9090"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        "#;
        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot bind to wildcard/unspecified address or public address")
        );
    }

    #[test]
    fn test_is_non_public_ip() {
        use std::net::IpAddr;
        use std::str::FromStr;

        // Loopback addresses
        assert!(is_non_public_ip(IpAddr::from_str("127.0.0.1").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("127.0.0.2").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("::1").unwrap()));

        // RFC 1918 private addresses
        assert!(is_non_public_ip(IpAddr::from_str("10.0.0.1").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("172.16.0.1").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("192.168.1.1").unwrap()));

        // Link-local addresses
        assert!(is_non_public_ip(IpAddr::from_str("169.254.1.1").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("fe80::1").unwrap()));

        // IPv6 Unique Local Addresses (fc00::/7)
        assert!(is_non_public_ip(IpAddr::from_str("fd00::1").unwrap()));
        assert!(is_non_public_ip(IpAddr::from_str("fc00::1").unwrap()));

        // IPv4-mapped IPv6
        assert!(is_non_public_ip(
            IpAddr::from_str("::ffff:127.0.0.1").unwrap()
        ));
        assert!(is_non_public_ip(
            IpAddr::from_str("::ffff:10.0.0.1").unwrap()
        ));
        assert!(!is_non_public_ip(
            IpAddr::from_str("::ffff:8.8.8.8").unwrap()
        ));

        // Unspecified / wildcard (must be rejected)
        assert!(!is_non_public_ip(IpAddr::from_str("0.0.0.0").unwrap()));
        assert!(!is_non_public_ip(IpAddr::from_str("::").unwrap()));

        // Public IPs (must be rejected)
        assert!(!is_non_public_ip(IpAddr::from_str("8.8.8.8").unwrap()));
        assert!(!is_non_public_ip(IpAddr::from_str("1.1.1.1").unwrap()));
        assert!(!is_non_public_ip(IpAddr::from_str("2001:db8::1").unwrap()));
    }

    #[test]
    fn test_config_validation_fails_with_insecure_starrocks_tls() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [starrocks]
        frontend_urls = ["http://localhost:8030"]
        username = "root"
        database = "telemetry"
        [starrocks.table_mapping]
        type = "unified"
        table = "telemetry"
        signal_type_column = "signal_type"
        [starrocks.tls]
        verification = "disabled"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("StarRocks config should deserialize");

        let err = validate_config(&config)
            .expect_err("Validation should fail when StarRocks TLS verification is disabled");
        assert!(
            err.to_string()
                .contains("disabling TLS certificate verification is prohibited"),
            "Error message should indicate disabled verification prohibited: {err}"
        );
    }

    #[test]
    fn test_config_validation_fails_with_missing_starrocks_ca_cert() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [starrocks]
        frontend_urls = ["http://localhost:8030"]
        username = "root"
        database = "telemetry"
        [starrocks.table_mapping]
        type = "unified"
        table = "telemetry"
        signal_type_column = "signal_type"
        [starrocks.tls]
        ca_cert_path = "/nonexistent/ca.pem"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("StarRocks config should deserialize");

        let err = validate_config(&config)
            .expect_err("Validation should fail when StarRocks CA file is missing");
        assert!(
            err.to_string().contains("CA certificate file not found"),
            "Error message should indicate missing CA file: {err}"
        );
    }

    #[test]
    fn test_validate_config_validates_only_selected_sink_in_priority_order() {
        // Iceberg takes precedence over StarRocks and Elasticsearch.
        // Even if an inactive StarRocks section has invalid TLS, validate_config should succeed
        // because Iceberg is the selected sink.
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [iceberg]
        catalog_name = "test_catalog"
        catalog_type = "Rest"
        catalog_uri = "http://localhost:8181"
        warehouse = "s3://warehouse"
        table_identifier = "db.telemetry"
        logs_table_identifier = "db.logs"
        traces_table_identifier = "db.traces"
        metrics_table_identifier = "db.metrics"

        [starrocks]
        frontend_urls = ["http://localhost:8030"]
        username = "root"
        database = "telemetry"
        [starrocks.table_mapping]
        type = "unified"
        table = "telemetry"
        signal_type_column = "signal_type"
        [starrocks.tls]
        verification = "disabled"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize");

        assert!(
            validate_config(&config).is_ok(),
            "validate_config should succeed because Iceberg is selected and valid, ignoring inactive StarRocks"
        );
    }

    #[test]
    fn test_initialize_transformers_noop() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"
        "#;
        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize");

        let mut metric_bridges = Vec::new();
        let res = initialize_transformers(&config, &mut metric_bridges);
        assert!(res.is_ok());
        let ((_logs, _traces, _metrics), engine) = res.unwrap();
        assert!(engine.is_none());
        assert!(metric_bridges.is_empty());
    }

    #[tokio::test]
    async fn test_initialize_transformers_wasm() {
        let wasm_bytes = wat::parse_str(
            r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )"#,
        )
        .expect("wat parse");
        let path = std::env::temp_dir().join(format!("test_wasm_{}.wasm", std::process::id()));
        std::fs::write(&path, wasm_bytes).unwrap();

        let toml_str = format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:4317"
            http_addr = "127.0.0.1:4318"

            [wasm_transformer]
            id = "test_wasm"
            type = "wasm"
            module_path = "{}"
            on_error = "drop"
            on_reject = "drop"
            concurrency = 1
            worker_channel_capacity = 1
            max_memory = "16MiB"
            rejuvenate_threshold = "8MiB"
            rejuvenate_batches = 1000
            init_timeout = "1s"
            allow_unmasked_passthrough = true
            schema_guard = "defensive"
            env_whitelist = []
            enable_sighup = true
            "#,
            path.display()
        );

        let config: AppConfig = Figment::new()
            .merge(Toml::string(&toml_str))
            .extract()
            .expect("Config should deserialize");

        let mut metric_bridges = Vec::new();
        let ((_logs, _traces, _metrics), engine) =
            initialize_transformers(&config, &mut metric_bridges).unwrap();

        assert!(engine.is_some());
        assert_eq!(engine.unwrap().module_generation(), 1);
        assert_eq!(metric_bridges.len(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_initialize_transformers_wasm_error() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [wasm_transformer]
        id = "test_wasm"
        type = "wasm"
        module_path = "/nonexistent.wasm"
        on_error = "drop"
        on_reject = "drop"
        concurrency = 1
        worker_channel_capacity = 1
        max_memory = "16MiB"
        rejuvenate_threshold = "8MiB"
        rejuvenate_batches = 1000
        init_timeout = "1s"
        allow_unmasked_passthrough = true
        schema_guard = "defensive"
        env_whitelist = []
        enable_sighup = true
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize");

        let mut metric_bridges = Vec::new();
        let res = initialize_transformers(&config, &mut metric_bridges);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_initialize_transformers_wasm_with_reroute_policies() {
        let wasm_bytes = wat::parse_str(
            r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )"#,
        )
        .expect("wat parse");

        let temp_dir = std::env::temp_dir();
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let wasm_path = temp_dir.join(format!(
            "test_wasm_reroute_{}_{}.wasm",
            std::process::id(),
            now_nanos
        ));
        std::fs::write(&wasm_path, &wasm_bytes).expect("write wasm");

        let toml_str = format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:4317"
            http_addr = "127.0.0.1:4318"

            [wasm_transformer]
            id = "test_reroute_wasm"
            type = "wasm"
            module_path = "{}"
            on_error = "reroute"
            on_reject = "reroute"
            concurrency = 1
            worker_channel_capacity = 1
            max_memory = "16MiB"
            rejuvenate_threshold = "8MiB"
            rejuvenate_batches = 1000
            init_timeout = "1s"
            allow_unmasked_passthrough = true
            schema_guard = "defensive"
            env_whitelist = []
            enable_sighup = true
            "#,
            wasm_path.display()
        );

        let config: AppConfig = Figment::new()
            .merge(Toml::string(&toml_str))
            .extract()
            .expect("Config should deserialize");

        let mut metric_bridges = Vec::new();
        let res = initialize_transformers(&config, &mut metric_bridges);
        let _ = std::fs::remove_file(&wasm_path);

        assert!(
            res.is_ok(),
            "Transformers with reroute policies and DLQ wiring must initialize successfully: {:?}",
            res.err()
        );
        assert_eq!(
            metric_bridges.len(),
            3,
            "Expected 3 metric bridge handles (1 per signal)"
        );
    }

    #[tokio::test]
    async fn test_file_dlq_sink_persists_batch_durably() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use pipeline_core::pipeline::SignalBatch;
        use std::sync::Arc;
        use wasm_transformer::DlqSink;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .expect("batch creation");

        let temp_dir = std::env::temp_dir();
        let test_id = format!("test_dlq_{}", std::process::id());
        let sink = FileDlqSink {
            transformer_id: test_id.clone(),
            signal: "logs".to_string(),
            role: "error".to_string(),
            dlq_dir: temp_dir
                .join("dlq_test")
                .join(&test_id)
                .join("logs")
                .join("error"),
        };

        let res = sink.send(SignalBatch::Logs(batch.clone())).await;
        assert!(
            res.is_ok(),
            "FileDlqSink::send should succeed: {:?}",
            res.err()
        );

        // Verify the file was written and can be read back as Arrow IPC
        let mut read_entries = tokio::fs::read_dir(&sink.dlq_dir)
            .await
            .expect("read dlq dir");
        let mut found_file = None;
        while let Some(entry) = read_entries.next_entry().await.expect("entry") {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("arrow") {
                found_file = Some(path);
                break;
            }
        }
        let file_path = found_file.expect("arrow file should exist");
        let file_bytes = tokio::fs::read(&file_path).await.expect("read arrow file");
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(file_bytes), None)
                .expect("stream reader");
        let read_batch = reader
            .next()
            .expect("batch in reader")
            .expect("valid batch");
        assert_eq!(read_batch.num_rows(), 2);
        assert_eq!(read_batch.num_columns(), 2);

        // Verify no temporary files remain in the directory
        let mut check_entries = tokio::fs::read_dir(&sink.dlq_dir)
            .await
            .expect("read dlq dir for temp check");
        while let Some(entry) = check_entries.next_entry().await.expect("entry") {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            assert!(
                !name_str.starts_with(".tmp_"),
                "Temporary file was not cleaned up or renamed: {name_str}"
            );
        }

        // Cleanup
        let _ = tokio::fs::remove_dir_all(temp_dir.join("dlq_test")).await;
    }

    #[tokio::test]
    async fn test_file_dlq_sink_returns_error_on_inaccessible_path() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use pipeline_core::pipeline::SignalBatch;
        use std::sync::Arc;
        use wasm_transformer::DlqSink;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
            .expect("batch creation");

        // Target an impossible path (e.g. attempting to create a directory under a non-directory file)
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!("dlq_conflict_{}.tmp", std::process::id()));
        std::fs::write(&file_path, b"not a directory").expect("write conflict file");

        let sink = FileDlqSink {
            transformer_id: "test".to_string(),
            signal: "logs".to_string(),
            role: "error".to_string(),
            dlq_dir: file_path.join("impossible_subdir"),
        };

        let res = sink.send(SignalBatch::Logs(batch)).await;
        assert!(
            res.is_err(),
            "Sink must return error instead of panicking on inaccessible directory"
        );

        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_is_safe_path_component() {
        assert!(is_safe_path_component("wasm_transformer"));
        assert!(is_safe_path_component("my-sink-123"));
        assert!(is_safe_path_component("valid.name"));

        assert!(!is_safe_path_component(""));
        assert!(!is_safe_path_component("."));
        assert!(!is_safe_path_component(".."));
        assert!(!is_safe_path_component("../archive"));
        assert!(!is_safe_path_component("foo/bar"));
        assert!(!is_safe_path_component("foo\\bar"));
        assert!(!is_safe_path_component("/absolute"));
        assert!(!is_safe_path_component("foo\0bar"));
    }

    #[test]
    fn test_file_dlq_sink_new_rejects_path_traversal() {
        assert!(FileDlqSink::new("../archive", "logs", "error").is_err());
        assert!(FileDlqSink::new("/etc", "logs", "error").is_err());
        assert!(FileDlqSink::new("test", "../traces", "error").is_err());
        assert!(FileDlqSink::new("test", "logs", "error/extra").is_err());
        assert!(FileDlqSink::new("", "logs", "error").is_err());

        let ok_sink = FileDlqSink::new("valid_transformer", "logs", "error");
        assert!(ok_sink.is_ok());
        let sink = ok_sink.unwrap();
        assert_eq!(
            sink.dlq_dir,
            std::path::PathBuf::from("dlq/valid_transformer/logs/error")
        );
    }

    #[test]
    fn test_validate_config_with_wasm_transformer() {
        let toml_invalid_module = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"

        [wasm_transformer]
        id = "test_validate"
        type = "wasm"
        module_path = "/nonexistent/path/module.wasm"
        on_error = "drop"
        on_reject = "drop"
        concurrency = 1
        worker_channel_capacity = 1
        max_memory = "16MiB"
        rejuvenate_threshold = "8MiB"
        rejuvenate_batches = 1000
        init_timeout = "1s"
        allow_unmasked_passthrough = true
        schema_guard = "defensive"
        env_whitelist = []
        enable_sighup = true
        "#;

        let config_invalid: AppConfig = Figment::new()
            .merge(Toml::string(toml_invalid_module))
            .extract()
            .expect("Config should deserialize");

        let res_invalid = validate_config(&config_invalid);
        assert!(
            res_invalid.is_err(),
            "validate_config must fail when WASM module path does not exist"
        );
    }

    #[test]
    fn test_validate_config_rejects_module_missing_abi_version() {
        let wasm_bytes = wat::parse_str(
            r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )"#,
        )
        .expect("wat parse");

        let temp_dir = std::env::temp_dir();
        let wasm_path = temp_dir.join(format!(
            "test_validate_missing_abi_{}.wasm",
            std::process::id()
        ));
        std::fs::write(&wasm_path, &wasm_bytes).expect("write wasm");

        let toml_str = format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:4317"
            http_addr = "127.0.0.1:4318"

            [kafka]
            brokers = "localhost:9092"
            logs_topic = "logs"
            traces_topic = "traces"
            metrics_topic = "metrics"
            logs_format = "json"
            traces_format = "json"
            metrics_format = "json"

            [wasm_transformer]
            id = "test_validate_no_abi"
            type = "wasm"
            module_path = "{}"
            on_error = "drop"
            on_reject = "drop"
            concurrency = 1
            worker_channel_capacity = 1
            max_memory = "16MiB"
            rejuvenate_threshold = "8MiB"
            rejuvenate_batches = 1000
            init_timeout = "1s"
            allow_unmasked_passthrough = true
            schema_guard = "defensive"
            env_whitelist = []
            enable_sighup = false
            "#,
            wasm_path.display()
        );

        let config: AppConfig = Figment::new()
            .merge(Toml::string(&toml_str))
            .extract()
            .expect("Config should deserialize");

        let res = validate_config(&config);
        let _ = std::fs::remove_file(&wasm_path);

        assert!(
            res.is_err(),
            "validate_config must reject module missing ABI version"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("datalake_abi_version"));
    }

    #[tokio::test]
    async fn test_initialize_transformers_rejects_module_missing_abi_version() {
        let wasm_bytes = wat::parse_str(
            r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )"#,
        )
        .expect("wat parse");

        let temp_dir = std::env::temp_dir();
        let wasm_path = temp_dir.join(format!("test_init_missing_abi_{}.wasm", std::process::id()));
        std::fs::write(&wasm_path, &wasm_bytes).expect("write wasm");

        let toml_str = format!(
            r#"
            [server]
            grpc_addr = "127.0.0.1:4317"
            http_addr = "127.0.0.1:4318"

            [wasm_transformer]
            id = "test_init_no_abi"
            type = "wasm"
            module_path = "{}"
            on_error = "drop"
            on_reject = "drop"
            concurrency = 1
            worker_channel_capacity = 1
            max_memory = "16MiB"
            rejuvenate_threshold = "8MiB"
            rejuvenate_batches = 1000
            init_timeout = "1s"
            allow_unmasked_passthrough = true
            schema_guard = "defensive"
            env_whitelist = []
            enable_sighup = false
            "#,
            wasm_path.display()
        );

        let config: AppConfig = Figment::new()
            .merge(Toml::string(&toml_str))
            .extract()
            .expect("Config should deserialize");

        let mut metric_bridges = Vec::new();
        let res = initialize_transformers(&config, &mut metric_bridges);
        let _ = std::fs::remove_file(&wasm_path);

        let err_msg = match res {
            Err(e) => e.to_string(),
            Ok(_) => panic!("initialize_transformers must reject module missing ABI version"),
        };
        assert!(err_msg.contains("datalake_abi_version"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_sighup_reload_task_advances_generation_on_signal() {
        let wasm_bytes = wat::parse_str(
            r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )"#,
        )
        .expect("wat parse");

        let temp_dir = std::env::temp_dir();
        let wasm_path = temp_dir.join(format!("test_sighup_reload_{}.wasm", std::process::id()));
        std::fs::write(&wasm_path, &wasm_bytes).expect("write wasm");

        let wasm_cfg = pipeline_core::config::WasmTransformerConfig {
            id: "test_sighup".to_string(),
            r#type: "wasm".to_string(),
            module_path: wasm_path.display().to_string(),
            sha256: None,
            max_execution_duration: "1s".to_string(),
            drain_timeout: "1s".to_string(),
            max_batch_rows: 1000,
            concurrency: 1,
            worker_channel_capacity: 1,
            max_memory: "16MiB".to_string(),
            rejuvenate_threshold: "8MiB".to_string(),
            rejuvenate_batches: 1000,
            init_timeout: "1s".to_string(),
            on_error: pipeline_core::config::OnErrorPolicy::Drop,
            allow_unmasked_passthrough: true,
            on_reject: pipeline_core::config::OnRejectPolicy::Drop,
            schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
            env_whitelist: vec![],
            env: std::collections::HashMap::new(),
            config: None,
            enable_sighup: true,
        };

        let engine = std::sync::Arc::new(
            wasm_transformer::engine::EngineCache::new_pooling(2, 16 * 1024 * 1024).expect("pool"),
        );
        assert_eq!(engine.module_generation(), 0);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task_handle =
            spawn_sighup_reload_task(wasm_cfg, std::sync::Arc::clone(&engine), shutdown_rx);

        // Give the task a moment to register the SIGHUP signal listener
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send SIGHUP to the current process via kill -HUP <pid>
        let _ = std::process::Command::new("kill")
            .args(["-HUP", &std::process::id().to_string()])
            .status();

        // Wait for generation to advance
        let mut reloaded = false;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if engine.module_generation() > 0 {
                reloaded = true;
                break;
            }
        }

        assert!(
            reloaded,
            "Engine generation must advance after receiving SIGHUP"
        );

        let _ = shutdown_tx.send(true);
        let _ = task_handle.await;
        let _ = std::fs::remove_file(&wasm_path);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_spawn_sighup_reload_task_terminates_on_shutdown_channel_drop() {
        let temp_dir = std::env::temp_dir();
        let wasm_path = temp_dir.join(format!("test_sighup_drop_{}.wasm", std::process::id()));
        let wat = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32) (result i64) (i64.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat).expect("wat");
        std::fs::write(&wasm_path, wasm_bytes).expect("write wasm");

        let wasm_cfg = pipeline_core::config::WasmTransformerConfig {
            id: "test_sighup_drop".to_string(),
            r#type: "wasm".to_string(),
            module_path: wasm_path.display().to_string(),
            sha256: None,
            max_execution_duration: "1s".to_string(),
            drain_timeout: "1s".to_string(),
            max_batch_rows: 1000,
            concurrency: 1,
            worker_channel_capacity: 1,
            max_memory: "16MiB".to_string(),
            rejuvenate_threshold: "8MiB".to_string(),
            rejuvenate_batches: 1000,
            init_timeout: "1s".to_string(),
            on_error: pipeline_core::config::OnErrorPolicy::Drop,
            allow_unmasked_passthrough: true,
            on_reject: pipeline_core::config::OnRejectPolicy::Drop,
            schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
            env_whitelist: vec![],
            env: std::collections::HashMap::new(),
            config: None,
            enable_sighup: true,
        };

        let engine = std::sync::Arc::new(
            wasm_transformer::engine::EngineCache::new_pooling(2, 16 * 1024 * 1024).expect("pool"),
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task_handle =
            spawn_sighup_reload_task(wasm_cfg, std::sync::Arc::clone(&engine), shutdown_rx);

        // Drop shutdown_tx immediately without setting to true
        drop(shutdown_tx);

        let timeout_res =
            tokio::time::timeout(std::time::Duration::from_secs(2), task_handle).await;
        assert!(
            timeout_res.is_ok(),
            "SIGHUP reload task must exit cleanly on shutdown channel drop without spinning"
        );
        let _ = std::fs::remove_file(&wasm_path);
    }
}
