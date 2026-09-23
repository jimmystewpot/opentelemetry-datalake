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

/// Routing policy when a WebAssembly transformation fails or encounters an unhandled error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnErrorPolicy {
    /// Reroute the erroneous batch to the Dead Letter Queue (DLQ).
    Reroute,
    /// Silently drop the erroneous batch.
    Drop,
    /// Forward the original input batch untransformed.
    Passthrough,
}

// Explicit impl prevents variant-reordering fragility
#[allow(clippy::derivable_impls)]
impl Default for OnErrorPolicy {
    fn default() -> Self {
        Self::Reroute
    }
}

/// Routing policy when a WebAssembly transformation explicitly rejects a record or batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnRejectPolicy {
    /// Reroute the rejected batch to the Dead Letter Queue (DLQ).
    Reroute,
    /// Silently drop the rejected batch.
    Drop,
}

// Explicit impl prevents variant-reordering fragility
#[allow(clippy::derivable_impls)]
impl Default for OnRejectPolicy {
    fn default() -> Self {
        Self::Reroute
    }
}

/// Enforcement mode for Arrow schema changes produced by WebAssembly transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaGuardMode {
    /// Allow backward-compatible schema mutations (e.g. adding nullable columns).
    #[default]
    Defensive,
    /// Forbid any schema deviations from the original incoming Arrow schema.
    Strict,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ByteSizeValue {
    Integer(usize),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ByteSizeParseError {
    #[error("byte size string cannot be empty")]
    Empty,
    #[error("invalid number in byte size '{0}': {1}")]
    InvalidNumber(String, std::num::ParseIntError),
    #[error("byte size '{0}' overflows usize")]
    Overflow(String),
}

/// Parses human-readable byte sizes (e.g. "64MiB", "16MB", "1GiB", "1024B") or numeric byte strings into byte counts.
///
/// Supported units (case-insensitive): `B`/`bytes`, `KiB`/`KB`, `MiB`/`MB`, `GiB`/`GB`, `TiB`/`TB`.
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`). It operates solely on borrowed
/// string slices without mutable or global state and can be safely invoked concurrently.
///
/// # Errors
///
/// Returns an error if the string is empty, contains an invalid number, or overflows `usize`.
pub fn parse_byte_size(s: &str) -> Result<usize, ByteSizeParseError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ByteSizeParseError::Empty);
    }

    let lower = s.to_ascii_lowercase();
    let (num_len, multiplier): (usize, u64) = if let Some(stripped) = lower
        .strip_suffix("tib")
        .or_else(|| lower.strip_suffix("tb"))
    {
        (stripped.len(), 1024 * 1024 * 1024 * 1024)
    } else if let Some(stripped) = lower
        .strip_suffix("gib")
        .or_else(|| lower.strip_suffix("gb"))
    {
        (stripped.len(), 1024 * 1024 * 1024)
    } else if let Some(stripped) = lower
        .strip_suffix("mib")
        .or_else(|| lower.strip_suffix("mb"))
    {
        (stripped.len(), 1024 * 1024)
    } else if let Some(stripped) = lower
        .strip_suffix("kib")
        .or_else(|| lower.strip_suffix("kb"))
    {
        (stripped.len(), 1024)
    } else if let Some(stripped) = lower
        .strip_suffix("bytes")
        .or_else(|| lower.strip_suffix("b"))
    {
        (stripped.len(), 1)
    } else {
        (s.len(), 1)
    };

    let num_str = &s[..num_len];
    let val: u64 = num_str
        .trim()
        .parse()
        .map_err(|e| ByteSizeParseError::InvalidNumber(s.to_string(), e))?;

    let total = val
        .checked_mul(multiplier)
        .ok_or_else(|| ByteSizeParseError::Overflow(s.to_string()))?;

    usize::try_from(total).map_err(|_| ByteSizeParseError::Overflow(s.to_string()))
}

fn deserialize_bytes<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match ByteSizeValue::deserialize(deserializer)? {
        ByteSizeValue::Integer(bytes) => Ok(bytes),
        ByteSizeValue::String(s) => parse_byte_size(&s).map_err(serde::de::Error::custom),
    }
}

fn default_max_execution_duration() -> std::time::Duration {
    std::time::Duration::from_millis(500)
}

fn default_drain_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(10)
}

const DEFAULT_MAX_BATCH_ROWS: std::num::NonZeroUsize = match std::num::NonZeroUsize::new(5000) {
    Some(v) => v,
    None => std::num::NonZeroUsize::MIN,
};
const fn default_max_batch_rows() -> std::num::NonZeroUsize {
    DEFAULT_MAX_BATCH_ROWS
}

const DEFAULT_CONCURRENCY: std::num::NonZeroUsize = match std::num::NonZeroUsize::new(4) {
    Some(v) => v,
    None => std::num::NonZeroUsize::MIN,
};
const fn default_concurrency() -> std::num::NonZeroUsize {
    DEFAULT_CONCURRENCY
}

const DEFAULT_WORKER_CHANNEL_CAPACITY: std::num::NonZeroUsize = std::num::NonZeroUsize::MIN;
const fn default_worker_channel_capacity() -> std::num::NonZeroUsize {
    DEFAULT_WORKER_CHANNEL_CAPACITY
}

const fn default_max_memory() -> usize {
    64 * 1024 * 1024
}

const fn default_rejuvenate_threshold() -> usize {
    16 * 1024 * 1024
}

const fn default_rejuvenate_batches() -> u64 {
    10_000
}

fn default_init_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(2)
}

/// Configuration for a WebAssembly (WASM) transformer runtime and isolation sandbox.
///
/// **Concurrency Behavior:**
/// This configuration is instantiated once at startup and is typically cloned per-worker
/// during initialization. The `concurrency` setting directly determines the number of parallel
/// WebAssembly instances (and Tokio tasks) spawned for this transformation step. The
/// `worker_channel_capacity` sets the buffer size for the bounded MPSC channel feeding
/// each individual worker, allowing it to absorb backpressure asynchronously.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WasmTransformerConfig {
    /// Unique identifier for this transformer instance.
    pub id: String,
    /// Component type (must be "wasm").
    #[serde(rename = "type")]
    pub r#type: String,
    /// Path to the compiled `.wasm` module.
    pub module_path: String,
    /// Optional expected SHA-256 hash of the `.wasm` binary for integrity verification.
    pub sha256: Option<String>,
    /// Maximum execution duration per transform invocation before timing out.
    #[serde(default = "default_max_execution_duration")]
    #[serde(with = "humantime_serde")]
    pub max_execution_duration: std::time::Duration,
    /// Maximum duration to allow in-flight batches to drain during graceful shutdown.
    #[serde(default = "default_drain_timeout")]
    #[serde(with = "humantime_serde")]
    pub drain_timeout: std::time::Duration,
    /// Maximum number of rows to process in a single batch.
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: std::num::NonZeroUsize,
    /// Number of concurrent worker instances.
    #[serde(default = "default_concurrency")]
    pub concurrency: std::num::NonZeroUsize,
    /// Capacity of bounded worker input channels.
    #[serde(default = "default_worker_channel_capacity")]
    pub worker_channel_capacity: std::num::NonZeroUsize,
    /// Maximum linear memory allocation allowed per worker instance.
    #[serde(default = "default_max_memory")]
    #[serde(deserialize_with = "deserialize_bytes")]
    pub max_memory: usize,
    /// Memory threshold triggering worker rejuvenation (clean restart).
    #[serde(default = "default_rejuvenate_threshold")]
    #[serde(deserialize_with = "deserialize_bytes")]
    pub rejuvenate_threshold: usize,
    /// Number of batches processed after which worker rejuvenation is triggered.
    #[serde(default = "default_rejuvenate_batches")]
    pub rejuvenate_batches: u64,
    /// Timeout duration for worker instance initialization and compilation.
    #[serde(default = "default_init_timeout")]
    #[serde(with = "humantime_serde")]
    pub init_timeout: std::time::Duration,
    /// Policy governing behavior on execution failure or guest panic.
    #[serde(default)]
    pub on_error: OnErrorPolicy,
    /// Whether unmasked input batches are allowed through when passthrough is enabled.
    #[serde(default)]
    pub allow_unmasked_passthrough: bool,
    /// Policy governing behavior on explicit record rejection.
    #[serde(default)]
    pub on_reject: OnRejectPolicy,
    /// Schema validation and enforcement mode.
    #[serde(default)]
    pub schema_guard: SchemaGuardMode,
    /// Whitelist of host environment variable names forwarded to guest instances.
    #[serde(default)]
    pub env_whitelist: Vec<String>,
    /// Static key-value environment variables injected into guest instances.
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    /// Arbitrary guest configuration passed as JSON.
    #[serde(default)]
    pub config: Option<serde_json::Value>,
    /// Whether to reload the WASM module on receiving a SIGHUP signal.
    #[serde(default)]
    pub enable_sighup: bool,
}

/// Validation error returned when a [`WasmTransformerConfig`] violates invariant constraints.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WasmConfigValidationError {
    /// Instance identifier is empty.
    #[error("transformer id cannot be empty")]
    EmptyId,
    /// Component type is not "wasm".
    #[error("component type must be 'wasm', got '{0}'")]
    InvalidType(String),
    /// Path to the .wasm binary is empty.
    #[error("module_path cannot be empty")]
    EmptyModulePath,
    /// Rejuvenation memory threshold exceeds maximum linear memory limit.
    #[error(
        "rejuvenate_threshold ({rejuvenate_threshold} bytes) cannot exceed max_memory ({max_memory} bytes)"
    )]
    RejuvenateExceedsMaxMemory {
        /// Configured rejuvenation threshold in bytes.
        rejuvenate_threshold: usize,
        /// Configured maximum memory limit in bytes.
        max_memory: usize,
    },
    /// Passthrough on error requested without explicit unmasked passthrough authorization.
    #[error("on_error = 'passthrough' requires allow_unmasked_passthrough = true")]
    UnauthorizedPassthrough,
}

impl WasmTransformerConfig {
    /// Validates invariant configuration constraints.
    ///
    /// # Concurrency Characteristics
    ///
    /// This method only borrows immutable configuration state (`&self`) and is thread-safe (`Send + Sync`),
    /// allowing concurrent validation calls across threads.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmConfigValidationError`] if:
    /// - `id` is empty or only whitespace
    /// - `type` is not "wasm"
    /// - `module_path` is empty or only whitespace
    /// - `rejuvenate_threshold` is strictly greater than `max_memory`
    /// - `on_error` is "passthrough" and `allow_unmasked_passthrough` is false
    pub fn validate(&self) -> Result<(), WasmConfigValidationError> {
        if self.id.trim().is_empty() {
            return Err(WasmConfigValidationError::EmptyId);
        }
        if self.r#type != "wasm" {
            return Err(WasmConfigValidationError::InvalidType(self.r#type.clone()));
        }
        if self.module_path.trim().is_empty() {
            return Err(WasmConfigValidationError::EmptyModulePath);
        }
        if self.rejuvenate_threshold > self.max_memory {
            return Err(WasmConfigValidationError::RejuvenateExceedsMaxMemory {
                rejuvenate_threshold: self.rejuvenate_threshold,
                max_memory: self.max_memory,
            });
        }
        if self.on_error == OnErrorPolicy::Passthrough && !self.allow_unmasked_passthrough {
            return Err(WasmConfigValidationError::UnauthorizedPassthrough);
        }
        Ok(())
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
        assert!(
            cfg.is_ok(),
            "Missing TOML file must not cause a load error: {:?}",
            cfg
        );
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
