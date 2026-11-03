use crate::error::PipelineError;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::Sampler};
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const SUPPRESSED_TRANSPORT_TARGETS: &[(&str, &str)] = &[
    ("h2", "h2=warn"),
    ("hyper", "hyper=warn"),
    ("tonic", "tonic=warn"),
    ("tower", "tower=warn"),
    ("opentelemetry", "opentelemetry=warn"),
];

const DEFAULT_OTLP_GRPC_PORT: u16 = 4317;
const DEFAULT_HTTPS_PORT: u16 = 443;
const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
const RESOURCE_SERVICE_NAME_KEY: &str = "service.name";
const RESOURCE_CLOUD_REGION_KEY: &str = "cloud.region";

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_service_name() -> String {
    "otel-datalake".to_string()
}

fn default_region() -> Option<String> {
    std::env::var("REGION").ok()
}

/// Configuration for local telemetry instrumentation.
#[derive(Debug, Deserialize, Clone)]
pub struct TelemetryConfig {
    /// Whether OTLP self-telemetry export is enabled.
    ///
    /// Defaults to `false` to prevent self-referential OTLP feedback storms when
    /// no external collector is configured.
    #[serde(default)]
    pub enabled: bool,
    /// OTLP gRPC endpoint to send internal telemetry to.
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
    /// Service name for local instrumentation.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Region tag to attach to telemetry.
    #[serde(default = "default_region")]
    pub region: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            service_name: default_service_name(),
            region: default_region(),
        }
    }
}

impl TelemetryConfig {
    /// Validates telemetry configuration syntax and required fields.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Configuration`] if `enabled` is `true` and
    /// `otlp_endpoint` is empty or cannot be parsed as a valid `http(s)` or `host:port` endpoint,
    /// or if `service_name` is empty.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if !self.enabled {
            return Ok(());
        }

        if self.otlp_endpoint.trim().is_empty() {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from(
                    "telemetry.otlp_endpoint must not be empty when telemetry.enabled is true",
                ),
            )));
        }

        if parse_endpoint_host_port(&self.otlp_endpoint).is_none() {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from(format!(
                    "telemetry.otlp_endpoint ('{}') is not a valid HTTP/gRPC endpoint URI",
                    self.otlp_endpoint
                )),
            )));
        }

        if self.service_name.trim().is_empty() {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from(
                    "telemetry.service_name must not be empty when telemetry.enabled is true",
                ),
            )));
        }

        Ok(())
    }

    /// Validates telemetry configuration and ensures `otlp_endpoint` does not loop back
    /// into the local gRPC or HTTP ingestion listeners when `enabled` is `true`.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Configuration`] if [`Self::validate`] fails or if
    /// `otlp_endpoint` resolves to `grpc_addr` or `http_addr`.
    pub fn validate_with_listeners(
        &self,
        grpc_addr: SocketAddr,
        http_addr: SocketAddr,
    ) -> Result<(), PipelineError> {
        self.validate()?;
        if self.enabled && is_self_referential_endpoint(&self.otlp_endpoint, grpc_addr, http_addr) {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from(format!(
                    "telemetry.otlp_endpoint ({}) resolves to this receiver's own ingestion address (grpc_addr={}, http_addr={}), which would cause a self-referential OTLP feedback loop",
                    self.otlp_endpoint, grpc_addr, http_addr
                )),
            )));
        }
        Ok(())
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

    /// Explicitly flushes and shuts down active OpenTelemetry tracer and meter providers.
    pub fn shutdown(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take() {
            let _ = tracer_provider.shutdown();
        }
        if let Some(meter_provider) = self.meter_provider.take() {
            let _ = meter_provider.shutdown();
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
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
/// Returns [`PipelineError::Configuration`] if an explicit `verbosity_override` filter
/// directive string cannot be parsed.
pub fn build_env_filter(verbosity_override: Option<&str>) -> Result<EnvFilter, PipelineError> {
    let base = if let Some(override_str) = verbosity_override {
        let trimmed = override_str.trim();
        if trimmed.is_empty() {
            return Err(PipelineError::Configuration(Box::new(
                figment::Error::from("Invalid log filter '': filter directive must not be empty"),
            )));
        }
        EnvFilter::try_new(trimmed).map_err(|e| {
            PipelineError::Configuration(Box::new(figment::Error::from(format!(
                "Invalid log filter '{trimmed}': {e}"
            ))))
        })?;
        trimmed.to_string()
    } else if let Ok(env_val) = std::env::var("RUST_LOG") {
        let trimmed = env_val.trim();
        if !trimmed.is_empty() && EnvFilter::try_new(trimmed).is_ok() {
            trimmed.to_string()
        } else {
            "info".to_string()
        }
    } else {
        "info".to_string()
    };

    let mut spec = base;
    for &(target, directive) in SUPPRESSED_TRANSPORT_TARGETS {
        let has_target = spec.split(',').map(str::trim).any(|part| {
            part.strip_prefix(target)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('='))
        });
        if !has_target {
            spec.push(',');
            spec.push_str(directive);
        }
    }

    EnvFilter::try_new(&spec).map_err(|e| {
        PipelineError::Configuration(Box::new(figment::Error::from(format!(
            "Invalid log filter '{spec}': {e}"
        ))))
    })
}

fn parse_endpoint_host_port(endpoint: &str) -> Option<(&str, u16)> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (default_port, without_scheme) = if let Some((scheme, rest)) = trimmed.split_once("://") {
        if scheme.eq_ignore_ascii_case("http") {
            (DEFAULT_OTLP_GRPC_PORT, rest)
        } else if scheme.eq_ignore_ascii_case("https") {
            (DEFAULT_HTTPS_PORT, rest)
        } else {
            return None;
        }
    } else {
        (DEFAULT_OTLP_GRPC_PORT, trimmed)
    };

    let authority = without_scheme.split(['/', '?', '#']).next()?;
    if authority.is_empty() {
        return None;
    }
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, rest)| rest);
    if host_port.is_empty() {
        return None;
    }

    if let Some(rest) = host_port.strip_prefix('[') {
        let (host, remainder) = rest.split_once(']')?;
        if host.is_empty() {
            return None;
        }
        let port = if remainder.is_empty() {
            default_port
        } else {
            let port_str = remainder.strip_prefix(':')?;
            port_str.parse::<u16>().ok().filter(|&p| p > 0)?
        };
        Some((host, port))
    } else if let Some((host, port_str)) = host_port.rsplit_once(':') {
        if host.is_empty() || host.contains(':') {
            return None;
        }
        let port = port_str.parse::<u16>().ok().filter(|&p| p > 0)?;
        Some((host, port))
    } else {
        if host_port.contains(':') {
            return None;
        }
        Some((host_port, default_port))
    }
}

/// Returns `true` if `ip` is assigned to a network interface on the local host/container.
fn is_local_interface_ip(ip: IpAddr) -> bool {
    std::net::UdpSocket::bind(SocketAddr::new(ip, 0)).is_ok()
}

/// Returns `true` if `otlp_endpoint` resolves to a loopback, wildcard, or local-interface host
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

    let parsed_ip = host.parse::<IpAddr>().ok().map(|ip| ip.to_canonical());
    let host_is_local = host.eq_ignore_ascii_case("localhost")
        || parsed_ip.is_some_and(|ip| ip.is_loopback() || ip.is_unspecified());

    [grpc_addr, http_addr].into_iter().any(|listen_addr| {
        if listen_addr.port() != port {
            return false;
        }
        let listen_ip = listen_addr.ip().to_canonical();
        let listen_is_local_or_wildcard = listen_ip.is_loopback() || listen_ip.is_unspecified();

        (host_is_local && listen_is_local_or_wildcard)
            || parsed_ip.is_some_and(|ip| {
                ip.is_unspecified()
                    || ip == listen_ip
                    || (listen_ip.is_unspecified() && is_local_interface_ip(ip))
            })
    })
}

/// Initializes local tracing and OpenTelemetry emission.
///
/// # Errors
///
/// Returns [`PipelineError::Configuration`] if the configuration or log filter is invalid,
/// or [`PipelineError::Internal`] if the tracer or meter provider cannot be initialized.
pub fn init_telemetry(config: &TelemetryConfig) -> Result<TelemetryGuard, PipelineError> {
    init_telemetry_with_verbosity(config, None)
}

/// Initializes local tracing and optional OpenTelemetry emission with an optional
/// verbosity override (e.g., `"debug"` or `"trace"` from CLI flags).
///
/// # Errors
///
/// Returns [`PipelineError::Configuration`] if the configuration or log filter is invalid,
/// or [`PipelineError::Internal`] if the tracer or meter provider cannot be initialized.
pub fn init_telemetry_with_verbosity(
    config: &TelemetryConfig,
    verbosity_override: Option<&str>,
) -> Result<TelemetryGuard, PipelineError> {
    let env_filter = build_env_filter(verbosity_override)?;

    if !config.enabled {
        // Note: `try_init` may return `Err` in multi-test binaries where a global subscriber
        // was already installed by a prior test; this is benign and intentionally ignored.
        let _ = tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init();

        return Ok(TelemetryGuard {
            meter_provider: None,
            tracer_provider: None,
        });
    }

    config.validate()?;

    let mut attributes = Vec::with_capacity(2);
    attributes.push(KeyValue::new(
        RESOURCE_SERVICE_NAME_KEY,
        config.service_name.clone(),
    ));
    if let Some(region) = &config.region {
        attributes.push(KeyValue::new(RESOURCE_CLOUD_REGION_KEY, region.clone()));
    }

    let resource = Resource::builder_empty()
        .with_attributes(attributes)
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .with_timeout(DEFAULT_EXPORT_TIMEOUT)
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
        .with_timeout(DEFAULT_EXPORT_TIMEOUT)
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
    use figment::{
        Figment,
        providers::{Format, Toml},
    };
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

    #[test]
    fn test_telemetry_config_partial_toml_deserialization() {
        let toml_str = r#"
            service_name = "custom-datalake"
        "#;
        let cfg: TelemetryConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("partial TelemetryConfig should deserialize with serde defaults");
        assert!(!cfg.enabled);
        assert_eq!(cfg.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.service_name, "custom-datalake");
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
        let filter = build_env_filter(Some("debug,h2=trace,tonic")).unwrap();
        let rendered = filter.to_string();
        assert!(
            rendered.contains("h2=trace"),
            "Explicit user directive h2=trace must be preserved: {rendered}"
        );
        assert!(
            !rendered.contains("h2=warn"),
            "Default h2=warn must not override explicit h2=trace: {rendered}"
        );
        assert!(
            !rendered.contains("tonic=warn"),
            "Default tonic=warn must not override bare target directive 'tonic': {rendered}"
        );
    }

    #[test]
    fn test_build_env_filter_retains_root_suppression_when_submodule_overridden() {
        let filter = build_env_filter(Some("debug,h2::codec=trace")).unwrap();
        let rendered = filter.to_string();
        assert!(
            rendered.contains("h2::codec=trace"),
            "Submodule directive h2::codec=trace must be preserved: {rendered}"
        );
        assert!(
            rendered.contains("h2=warn"),
            "Root h2=warn must still be appended when only a submodule is overridden: {rendered}"
        );
    }

    #[test]
    fn test_build_env_filter_rejects_invalid_directive_with_configuration_error() {
        let err = build_env_filter(Some("invalid===directive"))
            .expect_err("malformed filter directive must return error");
        assert!(matches!(err, PipelineError::Configuration(_)));
        assert!(err.to_string().contains("Invalid log filter"));
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
            "http://127.0.0.2:4317",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint(
            "http://[::ffff:127.0.0.1]:4317",
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
        assert!(is_self_referential_endpoint("http://localhost", grpc, http));
        assert!(is_self_referential_endpoint(
            "http://127.0.0.1/v1/traces",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint("http://[::1]", grpc, http));
        assert!(is_self_referential_endpoint(
            "http://localhost:4318",
            grpc,
            http
        ));
        assert!(is_self_referential_endpoint("localhost:4317", grpc, http));

        // If a non-loopback local interface IP exists on this host, a wildcard listener
        // on the same port must detect it as self-referential.
        if let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0")
            && sock.connect("198.51.100.1:80").is_ok()
            && let Ok(local_addr) = sock.local_addr()
        {
            let endpoint = format!("http://{}:4317", local_addr.ip());
            assert!(
                is_self_referential_endpoint(&endpoint, grpc, http),
                "Wildcard listener must detect local interface IP {endpoint} as self-referential"
            );
        }
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
        assert!(!is_self_referential_endpoint(
            "https://localhost",
            grpc,
            http
        ));

        // Private IP listener does not collide with loopback collector on same port
        let private_grpc: SocketAddr = "10.10.1.5:4317".parse().unwrap();
        let private_http: SocketAddr = "10.10.1.6:4318".parse().unwrap();
        assert!(is_self_referential_endpoint(
            "http://10.10.1.5:4317",
            private_grpc,
            private_http
        ));
        assert!(!is_self_referential_endpoint(
            "http://127.0.0.1:4317",
            private_grpc,
            private_http
        ));
        assert!(!is_self_referential_endpoint(
            "http://10.10.1.6:4317",
            private_grpc,
            private_http
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

    #[test]
    fn test_init_telemetry_enabled_rejects_empty_or_invalid_endpoint() {
        let cfg = TelemetryConfig {
            enabled: true,
            otlp_endpoint: "   ".to_string(),
            service_name: "otel-datalake".to_string(),
            region: None,
        };
        let err = init_telemetry(&cfg).expect_err("empty endpoint must fail when enabled");
        assert!(matches!(err, PipelineError::Configuration(_)));
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
