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
}

#[derive(Debug, Deserialize, Clone)]
struct ServerConfig {
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
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

    /// Override dry-run mode for Iceberg sink
    #[arg(long)]
    dry_run: Option<bool>,
}

/// Validates that required sink configuration constraints are satisfied.
fn validate_config(config: &AppConfig) -> anyhow::Result<()> {
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
    } else if config.kafka.is_none() && config.starrocks.is_none() && config.elasticsearch.is_none()
    {
        anyhow::bail!(
            "Configuration validation failed: one of [kafka], [iceberg], [starrocks], or [elasticsearch] configuration must be provided"
        );
    }

    Ok(())
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

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "telemetry-logs"
        traces_topic = "telemetry-traces"
        metrics_topic = "telemetry-metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
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
    pipeline_core::telemetry::init_telemetry(&config.pipeline.telemetry)?;

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
        shutdown_rx,
    );

    // Create Noop Transformers
    let mut logs_transformer = noop_transformer::NoopTransformer::new();
    let mut traces_transformer = noop_transformer::NoopTransformer::new();
    let mut metrics_transformer = noop_transformer::NoopTransformer::new();

    // Spawn transformers
    let logs_trans_handle = tokio::spawn(async move {
        if let Err(e) = logs_transformer.transform(logs_rx, logs_sink_tx).await {
            tracing::error!("Logs transformer error: {}", e);
        }
    });

    let traces_trans_handle = tokio::spawn(async move {
        if let Err(e) = traces_transformer
            .transform(traces_rx, traces_sink_tx)
            .await
        {
            tracing::error!("Traces transformer error: {}", e);
        }
    });

    let metrics_trans_handle = tokio::spawn(async move {
        if let Err(e) = metrics_transformer
            .transform(metrics_rx, metrics_sink_tx)
            .await
        {
            tracing::error!("Metrics transformer error: {}", e);
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
    } else if let Some(es_cfg) = config.elasticsearch {
        tracing::info!(
            endpoints = ?es_cfg.endpoints,
            "Initializing Elasticsearch sinks"
        );

        let mut logs_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
        let mut traces_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg.clone())?;
        let mut metrics_sink = elasticsearch_sink::ElasticsearchSink::try_new(es_cfg)?;

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
    } else {
        return Err(anyhow::anyhow!(
            "One of [kafka], [iceberg], [starrocks], or [elasticsearch] configuration must be provided"
        ));
    }

    // Spawn source
    let mut source_handle = tokio::spawn(async move { source.run().await });

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
                Ok(Err(e)) => tracing::error!("Source receiver stopped unexpectedly with error: {}", e),
                Ok(Ok(())) => tracing::error!("Source receiver stopped unexpectedly."),
                Err(e) => tracing::error!("Source receiver task panicked: {}", e),
            }
        }
    }

    // Wait for pipeline to drain
    let _ = tokio::join!(
        logs_trans_handle,
        traces_trans_handle,
        metrics_trans_handle,
        logs_sink_handle,
        traces_sink_handle,
        metrics_sink_handle
    );

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
}
