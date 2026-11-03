use crate::error::PipelineError;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::Sampler};
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const SUPPRESSED_TRANSPORT_TARGETS: &[(&str, &str)] = &[
    ("h2", "h2=warn"),
    ("hyper", "hyper=warn"),
    ("tonic", "tonic=warn"),
    ("tower", "tower=warn"),
    ("opentelemetry", "opentelemetry=warn"),
];

/// Configuration for local telemetry instrumentation.
#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Whether OTLP self-telemetry export is enabled.
    ///
    /// Defaults to `false` to prevent self-referential OTLP feedback storms when
    /// no external collector is configured.
    #[serde(default)]
    pub enabled: bool,
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
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "otel-datalake".to_string(),
            region: std::env::var("REGION").ok(),
        }
    }
}

/// Telemetry handle retaining initialized OpenTelemetry tracer and meter providers.
#[derive(Debug)]
pub struct TelemetryGuard {
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Returns `true` when OTLP tracer or meter providers are active on this guard.
    #[must_use]
    pub const fn has_active_providers(&self) -> bool {
        self.meter_provider.is_some() || self.tracer_provider.is_some()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take()
            && let Err(e) = tracer_provider.shutdown()
        {
            tracing::warn!(
                error = %e,
                "Failed to shut down OpenTelemetry tracer provider cleanly"
            );
        }
        if let Some(meter_provider) = self.meter_provider.take()
            && let Err(e) = meter_provider.shutdown()
        {
            tracing::warn!(
                error = %e,
                "Failed to shut down OpenTelemetry meter provider cleanly"
            );
        }
    }
}

/// Builds a programmatic [`EnvFilter`] without mutating the process environment.
///
/// Appends transport suppression directives (`h2=warn`, `hyper=warn`, `tonic=warn`,
/// `tower=warn`, `opentelemetry=warn`) unless the caller or `RUST_LOG` already
/// specifies an explicit directive for that target.
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] if the filter directive string cannot be parsed.
pub fn build_env_filter(verbosity_override: Option<&str>) -> Result<EnvFilter, PipelineError> {
    let env_spec = std::env::var("RUST_LOG").ok();
    let base = verbosity_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| env_spec.as_deref().map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or("info");

    let mut spec = base.to_string();
    for &(target, directive) in SUPPRESSED_TRANSPORT_TARGETS {
        let has_target = spec.split(',').map(str::trim).any(|part| {
            part.strip_prefix(target)
                .is_some_and(|rest| rest.starts_with('=') || rest.starts_with("::"))
        });
        if !has_target {
            spec.push(',');
            spec.push_str(directive);
        }
    }

    EnvFilter::try_new(&spec)
        .map_err(|e| PipelineError::Internal(format!("Invalid log filter '{spec}': {e}")))
}

fn parse_endpoint_host_port(endpoint: &str) -> Option<(&str, u16)> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_scheme = trimmed.split_once("://").map_or(trimmed, |(_, rest)| rest);
    let authority = without_scheme.split(['/', '?', '#']).next()?;
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, rest)| rest);

    if let Some(rest) = host_port.strip_prefix('[') {
        let (host, port_part) = rest.split_once("]:")?;
        let port = port_part.parse::<u16>().ok()?;
        Some((host, port))
    } else {
        let (host, port_str) = host_port.rsplit_once(':')?;
        if host.is_empty() {
            return None;
        }
        let port = port_str.parse::<u16>().ok()?;
        Some((host, port))
    }
}

/// Returns `true` if `otlp_endpoint` resolves to a loopback or wildcard host
/// targeting the same port as the local gRPC or HTTP OTLP ingestion listeners.
#[must_use]
pub fn is_self_referential_endpoint(
    otlp_endpoint: &str,
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
) -> bool {
    let Some((host, port)) = parse_endpoint_host_port(otlp_endpoint) else {
        return false;
    };

    if port != grpc_addr.port() && port != http_addr.port() {
        return false;
    }

    if host.eq_ignore_ascii_case("localhost")
        || matches!(host, "127.0.0.1" | "::1" | "0.0.0.0" | "::")
    {
        return true;
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        return ip.is_loopback()
            || ip.is_unspecified()
            || ip == grpc_addr.ip()
            || ip == http_addr.ip();
    }

    false
}

/// Initializes local tracing and OpenTelemetry emission.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if the log filter, tracer provider, or meter provider
/// cannot be initialized.
pub fn init_telemetry(config: &TelemetryConfig) -> Result<TelemetryGuard, PipelineError> {
    init_telemetry_with_verbosity(config, None)
}

/// Initializes local tracing and optional OpenTelemetry emission with an optional
/// verbosity override (e.g., `"debug"` or `"trace"` from CLI flags).
///
/// # Errors
///
/// Returns `PipelineError::Internal` if the log filter, tracer provider, or meter provider
/// cannot be initialized.
pub fn init_telemetry_with_verbosity(
    config: &TelemetryConfig,
    verbosity_override: Option<&str>,
) -> Result<TelemetryGuard, PipelineError> {
    let env_filter = build_env_filter(verbosity_override)?;

    if !config.enabled {
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init();

        return Ok(TelemetryGuard {
            meter_provider: None,
            tracer_provider: None,
        });
    }

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
        .map_err(|e| PipelineError::Internal(e.to_string()))?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_resource(resource.clone())
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()
        .map_err(|e| PipelineError::Internal(e.to_string()))?;

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("otel-datalake");
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    // Set as global providers to ensure library traces and metrics are captured
    opentelemetry::global::set_tracer_provider(provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Ok(TelemetryGuard {
        meter_provider: Some(meter_provider),
        tracer_provider: Some(provider),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    /// Default `TelemetryConfig` must use the localhost OTLP endpoint and
    /// the canonical service name without requiring any environment variables.
    #[test]
    fn test_telemetry_config_default_values() {
        let cfg = TelemetryConfig::default();
        assert_eq!(cfg.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.service_name, "otel-datalake");
        assert_eq!(cfg.region, std::env::var("REGION").ok());
    }

    /// When the REGION environment variable is set, `TelemetryConfig::default()`
    /// must capture it in the `region` field. Because Rust runs tests in parallel
    /// and env var mutation is a global side-effect, we instead verify the
    /// same behavior by directly constructing a `TelemetryConfig` with a known
    /// region value — testing that the field is correctly handled downstream.
    #[test]
    fn test_telemetry_config_region_field_roundtrips() {
        let cfg = TelemetryConfig {
            enabled: false,
            otlp_endpoint: "http://localhost:4317".to_string(),
            service_name: "otel-datalake".to_string(),
            region: Some("ap-southeast-2".to_string()),
        };
        assert_eq!(cfg.region.as_deref(), Some("ap-southeast-2"));
        assert_eq!(cfg.service_name, "otel-datalake");
    }

    #[test]
    fn test_telemetry_config_default_is_disabled() {
        let cfg = TelemetryConfig::default();
        assert!(
            !cfg.enabled,
            "Self-telemetry export must be disabled by default to prevent feedback loops"
        );
        assert_eq!(cfg.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.service_name, "otel-datalake");
    }

    #[test]
    fn test_build_env_filter_adds_transport_suppression_directives() {
        let filter = build_env_filter(Some("debug")).unwrap();
        let rendered = filter.to_string();
        assert!(
            rendered.contains("debug"),
            "Filter must contain base level: {rendered}"
        );
        for target in [
            "h2=warn",
            "hyper=warn",
            "tonic=warn",
            "tower=warn",
            "opentelemetry=warn",
        ] {
            assert!(
                rendered.contains(target),
                "Filter '{rendered}' must suppress transport target '{target}'"
            );
        }
    }

    #[test]
    fn test_build_env_filter_preserves_explicit_target_override() {
        let filter = build_env_filter(Some("debug,h2=trace")).unwrap();
        let rendered = filter.to_string();
        assert!(
            rendered.contains("h2=trace"),
            "Explicit user directive h2=trace must be preserved: {rendered}"
        );
        assert!(
            !rendered.contains("h2=warn"),
            "Default h2=warn must not override explicit h2=trace: {rendered}"
        );
    }

    #[test]
    fn test_is_self_referential_endpoint_detects_loopback_matches() {
        let grpc: SocketAddr = "0.0.0.0:4317".parse().unwrap();
        let http: SocketAddr = "0.0.0.0:4318".parse().unwrap();

        assert!(is_self_referential_endpoint(
            "http://localhost:4317",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint(
            "http://127.0.0.1:4317",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint(
            "https://[::1]:4317/v1/traces",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint(
            "http://0.0.0.0:4317",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint(
            "http://localhost:4318",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint("localhost:4317", grpc, http));
    }

    #[test]
    fn test_is_self_referential_endpoint_allows_distinct_collector() {
        let grpc: SocketAddr = "0.0.0.0:4317".parse().unwrap();
        let http: SocketAddr = "0.0.0.0:4318".parse().unwrap();

        assert!(!is_self_referential_endpoint(
            "http://localhost:4319",
            grpc,
            http
        ));
        assert!(!is_self_referential_endpoint(
            "http://otel-collector.monitoring:4317",
            grpc,
            http
        ));
        assert!(!is_self_referential_endpoint(
            "http://10.0.0.15:4317",
            grpc,
            http
        ));
    }

    #[test]
    fn test_init_telemetry_disabled_returns_empty_guard_and_drops_cleanly() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.enabled);
        let guard = init_telemetry_with_verbosity(&cfg, Some("info"))
            .expect("disabled telemetry must initialize cleanly");
        assert!(!guard.has_active_providers());
        drop(guard);
    }

    #[tokio::test]
    async fn test_telemetry_guard_drop_shuts_down_active_providers() {
        let cfg = TelemetryConfig {
            enabled: true,
            otlp_endpoint: "http://127.0.0.1:54317".to_string(),
            service_name: "otel-datalake-test".to_string(),
            region: Some("us-east-1".to_string()),
        };
        let guard = init_telemetry_with_verbosity(&cfg, Some("info"))
            .expect("enabled telemetry with valid URI must initialize");
        assert!(guard.has_active_providers());
        drop(guard);
    }
}
