use crate::error::PipelineError;
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::Deserialize;

/// Top-level configuration object.
#[derive(Debug, Deserialize, Clone)]
pub struct PipelineConfig {
    /// Local telemetry configuration.
    #[serde(default)]
    pub telemetry: crate::telemetry::TelemetryConfig,
}

impl PipelineConfig {
    /// Load configuration from file and environment variables.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Configuration` if parsing fails.
    pub fn load(path: &str) -> Result<Self, PipelineError> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("DATALAKE_").split("_"))
            .extract()
            .map_err(|e| PipelineError::Configuration(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Loading a non-existent file path must succeed because Figment
    /// treats a missing optional TOML file as an empty config; all
    /// fields with `#[serde(default)]` will take their defaults.
    #[test]
    fn test_pipeline_config_load_missing_file_uses_defaults() {
        let cfg = PipelineConfig::load("/tmp/this-file-does-not-exist-ever-12345.toml");
        // Figment's Toml::file silently ignores a missing file,
        // so config must succeed using serde defaults.
        assert!(cfg.is_ok(), "Missing TOML file must not cause a load error: {:?}", cfg);
    }

    /// A file containing invalid TOML must return PipelineError::Configuration.
    #[test]
    fn test_pipeline_config_load_invalid_toml() {
        let path = "/tmp/otel_datalake_test_invalid.toml";
        std::fs::write(path, b"[[ not valid toml ~~~").unwrap();
        let result = PipelineConfig::load(path);
        let _ = std::fs::remove_file(path);
        assert!(
            result.is_err(),
            "Invalid TOML content must produce a Configuration error"
        );
        assert!(
            matches!(result.unwrap_err(), PipelineError::Configuration(_)),
            "Error variant must be PipelineError::Configuration"
        );
    }

    /// Default telemetry configuration must expose expected endpoint and
    /// service name without requiring any file on disk.
    #[test]
    fn test_pipeline_config_telemetry_defaults() {
        let cfg = PipelineConfig::load("/nonexistent-defaults-test.toml").unwrap();
        assert_eq!(cfg.telemetry.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.telemetry.service_name, "otel-datalake");
    }
}
