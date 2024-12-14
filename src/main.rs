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
    #[serde(default)]
    telemetry: pipeline_core::telemetry::TelemetryConfig,
    server: ServerConfig,
    kafka: Option<KafkaConfig>,
    iceberg: Option<storage::iceberg::IcebergSinkConfig>,
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
    #[serde(default)]
    options: HashMap<String, String>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
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

    if std::path::Path::new("config.toml").exists() {
        figment = figment.merge(Toml::file("config.toml"));
    }

    let config: AppConfig = figment.merge(Env::prefixed("OTEL_DATALAKE_")).extract()?;

    // Initialize telemetry
    pipeline_core::telemetry::init_telemetry(&config.telemetry)?;

    tracing::info!("Starting opentelemetry-datalake service");

    // Channels for the OTLP source demultiplexer
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(1000);
    let (traces_tx, traces_rx) = tokio::sync::mpsc::channel(1000);
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::channel(1000);

    // Channels between Transformers and Sinks
    let (logs_sink_tx, logs_sink_rx) = tokio::sync::mpsc::channel(1000);
    let (traces_sink_tx, traces_sink_rx) = tokio::sync::mpsc::channel(1000);
    let (metrics_sink_tx, metrics_sink_rx) = tokio::sync::mpsc::channel(1000);

    // Create OTLP Receiver Source
    let mut source = otlp_receiver::OtlpReceiverSource::new(
        config.server.grpc_addr,
        config.server.http_addr,
        logs_tx,
        traces_tx,
        metrics_tx,
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
        tracing::info!("Initializing Iceberg sinks");
        let mut logs_sink = storage::iceberg::IcebergSink::new(iceberg_cfg.clone());
        let mut traces_sink = storage::iceberg::IcebergSink::new(iceberg_cfg.clone());
        let mut metrics_sink = storage::iceberg::IcebergSink::new(iceberg_cfg.clone());

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
        let parse_format = |f: &str| {
            if f.eq_ignore_ascii_case("ipc") {
                kafka_sink::SerializationFormat::Ipc
            } else {
                kafka_sink::SerializationFormat::Json
            }
        };

        let mut logs_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.logs_topic,
            parse_format(&kafka_cfg.logs_format),
            &kafka_cfg.options,
        )?;

        let mut traces_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.traces_topic,
            parse_format(&kafka_cfg.traces_format),
            &kafka_cfg.options,
        )?;

        let mut metrics_sink = kafka_sink::KafkaSink::try_new(
            &kafka_cfg.brokers,
            &kafka_cfg.metrics_topic,
            parse_format(&kafka_cfg.metrics_format),
            &kafka_cfg.options,
        )?;

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
        return Err(anyhow::anyhow!("Either [kafka] or [iceberg] configuration must be provided"));
    }

    // Spawn source
    let source_handle = tokio::spawn(async move {
        if let Err(e) = source.run().await {
            tracing::error!("Source error: {}", e);
        }
    });

    // Handle shutdown
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received shutdown signal. Exiting...");
        }
        _ = source_handle => {
            tracing::error!("Source receiver stopped unexpectedly.");
        }
        _ = logs_trans_handle => {}
        _ = traces_trans_handle => {}
        _ = metrics_trans_handle => {}
        _ = logs_sink_handle => {}
        _ = traces_sink_handle => {}
        _ = metrics_sink_handle => {}
    }

    Ok(())
}
