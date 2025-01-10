use crate::error::PipelineError;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::Sampler};
use serde::Deserialize;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for local telemetry instrumentation.
#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryConfig {
    /// OTLP gRPC endpoint to send internal telemetry to.
    pub otlp_endpoint: String,
    /// Service name for local instrumentation.
    pub service_name: String,
    /// Region tag to attach to telemetry.
    pub region: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "otel-datalake".to_string(),
            region: std::env::var("REGION").ok(),
        }
    }
}

/// Initializes local tracing and OpenTelemetry emission.
///
/// # Errors
///
/// Returns `PipelineError::Configuration` if the tracer cannot be initialized.
pub fn init_telemetry(config: &TelemetryConfig) -> Result<(), PipelineError> {
    let mut attributes = vec![KeyValue::new("service.name", config.service_name.clone())];
    if let Some(region) = &config.region {
        attributes.push(KeyValue::new("cloud.region", region.clone()));
    }

    let resource = Resource::builder_empty()
        .with_attributes(attributes)
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()
        .map_err(|e| PipelineError::Configuration(e.to_string()))?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("otel-datalake");
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Set as global provider to ensure library traces are also captured
    opentelemetry::global::set_tracer_provider(provider);

    Ok(())
}
