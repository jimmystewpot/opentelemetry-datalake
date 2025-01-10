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
            .map_err(|e| PipelineError::Configuration(e.to_string()))
    }
}
