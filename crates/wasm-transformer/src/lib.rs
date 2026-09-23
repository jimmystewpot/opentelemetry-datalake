//! WebAssembly transformation engine for OpenTelemetry data lake pipelines.
//!
//! Provides an ultra-high-performance, sandboxed WASM transformation engine executing
//! whole-batch Apache Arrow transformations using Wasmtime's pooling allocator.

pub mod dispatcher;
pub mod engine;
pub mod error;
pub mod guard;
pub mod host_calls;
pub mod pool;
pub mod wasi_env;
pub mod worker;

use async_trait::async_trait;
use pipeline_core::{
    config::{OnErrorPolicy, OnRejectPolicy, WasmTransformerConfig},
    error::PipelineError,
    pipeline::{PipelineReceiver, PipelineSender, Transform},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::warn;
use wasmtime::Module;

use crate::{
    dispatcher::{DispatcherConfig, WasmDispatcher},
    engine::EngineCache,
};

pub use crate::host_calls::{
    MetricBridgeHandle, MetricHandle, MetricRegistry, MetricValue, bridge_metrics_to_opentelemetry,
};

/// Maximum supported worker concurrency per WASM transformer instance.
pub const MAX_CONCURRENCY: usize = 10_000;

/// Trait for Dead Letter Queue (DLQ) sinks that receive diverted or rejected batches.
#[async_trait]
pub trait DlqSink: Send + Sync + std::fmt::Debug {
    /// Persists or forwards a diverted [`SignalBatch`].
    ///
    /// # Errors
    ///
    /// Returns a [`PipelineError`] if the batch cannot be durably written or forwarded.
    async fn send(&self, batch: pipeline_core::pipeline::SignalBatch) -> Result<(), PipelineError>;
}

/// Destination for Dead Letter Queue (DLQ) routing, supporting channels or direct sinks.
#[derive(Clone)]
pub enum DlqOutput {
    /// Asynchronous pipeline channel.
    Sender(PipelineSender),
    /// Direct DLQ sink performing persistence in the routed send path.
    Sink(Arc<dyn DlqSink>),
}

impl std::fmt::Debug for DlqOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sender(_) => write!(f, "DlqOutput::Sender"),
            Self::Sink(sink) => write!(f, "DlqOutput::Sink({sink:?})"),
        }
    }
}

impl DlqOutput {
    /// Sends a diverted batch to the DLQ destination.
    ///
    /// # Errors
    ///
    /// Returns a [`PipelineError`] if the destination is closed or persistence fails.
    pub async fn send(
        &self,
        batch: pipeline_core::pipeline::SignalBatch,
    ) -> Result<(), PipelineError> {
        match self {
            Self::Sender(tx) => tx
                .send(batch)
                .await
                .map_err(|_| PipelineError::DownstreamClosed),
            Self::Sink(sink) => sink.send(batch).await,
        }
    }
}

impl From<PipelineSender> for DlqOutput {
    fn from(tx: PipelineSender) -> Self {
        Self::Sender(tx)
    }
}

impl From<Arc<dyn DlqSink>> for DlqOutput {
    fn from(sink: Arc<dyn DlqSink>) -> Self {
        Self::Sink(sink)
    }
}

/// WebAssembly whole-batch transformer executing in isolated sandbox workers.
///
/// Implements the [`Transform`] pipeline trait, dispatching incoming telemetry batches
/// across a pool of sandboxed Wasmtime workers, verifying DLQ routing topological integrity,
/// and executing transformations with zero-copy Arrow IPC memory interchange.
pub struct WasmTransformer {
    config: WasmTransformerConfig,
    engine: Arc<EngineCache>,
    module: Arc<Module>,
    reroute_error: Option<DlqOutput>,
    reroute_reject: Option<DlqOutput>,
    registry: Arc<crate::host_calls::MetricRegistry>,
}

impl WasmTransformer {
    /// Creates and initializes a new `WasmTransformer`.
    ///
    /// Validates topological Dead Letter Queue (DLQ) sinks when rerouting policies are enabled,
    /// emits a security audit warning if unmasked passthrough is configured on error,
    /// initializes the Wasmtime pooling allocator cache, reads the guest `.wasm` module,
    /// optionally verifies SHA-256 integrity, and pre-compiles the module.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::TopologicalSinkMissing`] if `on_error` or `on_reject` is set to
    /// `Reroute` but the corresponding DLQ sender channel is not provided (`None`).
    /// Returns [`PipelineError::Internal`] if module loading, hash verification, or engine
    /// initialization fails.
    pub fn new(
        config: WasmTransformerConfig,
        reroute_error: Option<PipelineSender>,
        reroute_reject: Option<PipelineSender>,
    ) -> Result<Self, PipelineError> {
        if config.concurrency == 0 || config.concurrency > MAX_CONCURRENCY {
            return Err(PipelineError::Internal(format!(
                "wasm_transformer.concurrency must be between 1 and {MAX_CONCURRENCY}, got {}",
                config.concurrency
            )));
        }
        let max_memory_bytes =
            crate::worker::parse_byte_size(&config.max_memory).unwrap_or(64 * 1024 * 1024);
        let pool_capacity = config
            .concurrency
            .max(1)
            .checked_mul(2)
            .and_then(|val| val.checked_add(1))
            .ok_or_else(|| {
                PipelineError::Internal(format!(
                    "wasm_transformer.concurrency {} overflows pool capacity calculation",
                    config.concurrency
                ))
            })?;
        let engine = Arc::new(
            EngineCache::new_pooling(pool_capacity, max_memory_bytes)
                .map_err(|e| PipelineError::Internal(e.to_string()))?,
        );
        Self::with_engine(
            config,
            reroute_error.map(DlqOutput::Sender),
            reroute_reject.map(DlqOutput::Sender),
            engine,
        )
    }

    /// Creates and initializes a new `WasmTransformer` with a provided [`EngineCache`].
    ///
    /// Validates topological Dead Letter Queue (DLQ) sinks when rerouting policies are enabled,
    /// emits a security audit warning if unmasked passthrough is configured on error,
    /// reads the guest `.wasm` module, verifies optional SHA-256 integrity, compiles the module,
    /// and instantiates a probe worker to validate mandatory ABI exports, ABI version handshake,
    /// and `datalake_init` execution before serving.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::TopologicalSinkMissing`] if `on_error` or `on_reject` is set to
    /// `Reroute` but the corresponding DLQ destination is not provided (`None`).
    /// Returns [`PipelineError::Internal`] if concurrency is 0 or exceeds [`MAX_CONCURRENCY`], on passthrough misconfiguration,
    /// module file read errors, SHA-256 hash mismatch, compilation failure, or probe worker initialization
    /// failure.
    pub fn with_engine(
        config: WasmTransformerConfig,
        reroute_error: Option<DlqOutput>,
        reroute_reject: Option<DlqOutput>,
        engine: Arc<EngineCache>,
    ) -> Result<Self, PipelineError> {
        // 1. Topological DLQ validation
        if config.on_error == OnErrorPolicy::Reroute && reroute_error.is_none() {
            return Err(PipelineError::TopologicalSinkMissing(format!(
                "{}.__reroute_errored",
                config.id
            )));
        }

        if config.on_reject == OnRejectPolicy::Reroute && reroute_reject.is_none() {
            return Err(PipelineError::TopologicalSinkMissing(format!(
                "{}.__reroute_rejected",
                config.id
            )));
        }

        // 2. Concurrency validation
        if config.concurrency == 0 || config.concurrency > MAX_CONCURRENCY {
            return Err(PipelineError::Internal(format!(
                "wasm_transformer.concurrency must be between 1 and {MAX_CONCURRENCY}, got {}",
                config.concurrency
            )));
        }

        // 3. Security audit logging and passthrough policy validation
        if config.on_error == OnErrorPolicy::Passthrough {
            if !config.allow_unmasked_passthrough {
                return Err(PipelineError::Internal(
                    "Invalid configuration: on_error is set to 'passthrough' but allow_unmasked_passthrough is false; this would silently drop errored batches".to_string(),
                ));
            }
            warn!(
                transformer_id = %config.id,
                "SECURITY AUDIT: on_error=passthrough enabled. Input batches will bypass transformation on guest failure. Verify threat model."
            );
        }

        // 4. Read guest module from disk
        let wasm_bytes = std::fs::read(&config.module_path).map_err(|e| {
            PipelineError::Internal(format!(
                "Failed to read WASM module '{}': {e}",
                config.module_path
            ))
        })?;

        // 5. Optional SHA-256 integrity verification
        if let Some(ref expected_hash) = config.sha256 {
            let mut hasher = Sha256::new();
            hasher.update(&wasm_bytes);
            let calculated = hex::encode(hasher.finalize());
            if !calculated.eq_ignore_ascii_case(expected_hash) {
                return Err(PipelineError::Internal(format!(
                    "SHA256 mismatch for WASM module '{}': expected {expected_hash}, got {calculated}",
                    config.module_path
                )));
            }
        }

        // 6. Pre-compile WebAssembly module
        let module = engine
            .compile_module(&wasm_bytes)
            .map_err(|e| PipelineError::Internal(e.to_string()))?;

        let registry = Arc::new(crate::host_calls::MetricRegistry::new(&config.id));

        // 7. Surface worker initialization failures early by probing guest instantiation
        // across active runtime signal contexts.
        if config.env.contains_key("signal") {
            crate::worker::WasmWorker::probe_candidate(&engine, &module, &config, &registry)
                .map_err(|e| {
                    PipelineError::Internal(format!("WASM worker initialization failed: {e}"))
                })?;
        } else {
            for signal in ["logs", "traces", "metrics"] {
                let mut signal_config = config.clone();
                signal_config
                    .env
                    .insert("signal".to_string(), (*signal).to_string());
                crate::worker::WasmWorker::probe_candidate(
                    &engine,
                    &module,
                    &signal_config,
                    &registry,
                )
                .map_err(|e| {
                    PipelineError::Internal(format!(
                        "WASM worker initialization failed for signal '{signal}': {e}"
                    ))
                })?;
            }
        }

        Ok(Self {
            config,
            engine,
            module,
            reroute_error,
            reroute_reject,
            registry,
        })
    }

    /// Returns a reference to the transformer configuration.
    #[must_use]
    pub fn config(&self) -> &WasmTransformerConfig {
        &self.config
    }

    /// Returns a reference to the shared [`EngineCache`].
    #[must_use]
    pub fn engine(&self) -> &Arc<EngineCache> {
        &self.engine
    }

    /// Returns a reference to the compiled guest [`Module`].
    #[must_use]
    pub fn module(&self) -> &Arc<Module> {
        &self.module
    }

    /// Returns a reference to the shared [`crate::host_calls::MetricRegistry`].
    #[must_use]
    pub fn metric_registry(&self) -> &Arc<crate::host_calls::MetricRegistry> {
        &self.registry
    }

    /// Validates the WASM transformer configuration, module existence, and SHA-256 integrity.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if:
    /// - `concurrency` is 0
    /// - `on_error` is `passthrough` but `allow_unmasked_passthrough` is false
    /// - The WASM module file cannot be read
    /// - The SHA-256 hash does not match `config.sha256`
    /// - The WASM module fails Wasmtime compilation or guest probe validation
    pub fn validate_config(config: &WasmTransformerConfig) -> Result<(), PipelineError> {
        if config.module_path.is_empty() {
            return Err(PipelineError::Internal(
                "wasm_transformer.module_path cannot be empty".to_string(),
            ));
        }

        if config.concurrency == 0 || config.concurrency > MAX_CONCURRENCY {
            return Err(PipelineError::Internal(format!(
                "wasm_transformer.concurrency must be between 1 and {MAX_CONCURRENCY}, got {}",
                config.concurrency
            )));
        }

        if config.on_error == OnErrorPolicy::Passthrough && !config.allow_unmasked_passthrough {
            return Err(PipelineError::Internal(
                "Invalid configuration: on_error is set to 'passthrough' but allow_unmasked_passthrough is false; this would silently drop errored batches".to_string(),
            ));
        }

        let max_memory_bytes =
            crate::worker::parse_byte_size(&config.max_memory).ok_or_else(|| {
                PipelineError::Internal(format!(
                    "Invalid wasm_transformer.max_memory '{}'",
                    config.max_memory
                ))
            })?;

        if crate::worker::parse_byte_size(&config.rejuvenate_threshold).is_none() {
            return Err(PipelineError::Internal(format!(
                "Invalid wasm_transformer.rejuvenate_threshold '{}'",
                config.rejuvenate_threshold
            )));
        }

        if crate::worker::parse_duration(&config.max_execution_duration).is_none() {
            return Err(PipelineError::Internal(format!(
                "Invalid wasm_transformer.max_execution_duration '{}'",
                config.max_execution_duration
            )));
        }

        if crate::worker::parse_duration(&config.drain_timeout).is_none() {
            return Err(PipelineError::Internal(format!(
                "Invalid wasm_transformer.drain_timeout '{}'",
                config.drain_timeout
            )));
        }

        if crate::worker::parse_duration(&config.init_timeout).is_none() {
            return Err(PipelineError::Internal(format!(
                "Invalid wasm_transformer.init_timeout '{}'",
                config.init_timeout
            )));
        }

        let wasm_bytes = std::fs::read(&config.module_path).map_err(|e| {
            PipelineError::Internal(format!(
                "Failed to read WASM module '{}': {e}",
                config.module_path
            ))
        })?;

        if let Some(ref expected_hash) = config.sha256 {
            let mut hasher = Sha256::new();
            hasher.update(&wasm_bytes);
            let calculated = hex::encode(hasher.finalize());
            if !calculated.eq_ignore_ascii_case(expected_hash) {
                return Err(PipelineError::Internal(format!(
                    "SHA256 mismatch for WASM module '{}': expected {expected_hash}, got {calculated}",
                    config.module_path
                )));
            }
        }

        let pool_capacity = config
            .concurrency
            .max(1)
            .checked_mul(2)
            .and_then(|val| val.checked_add(1))
            .ok_or_else(|| {
                PipelineError::Internal(format!(
                    "wasm_transformer.concurrency {} overflows pool capacity calculation",
                    config.concurrency
                ))
            })?;
        let engine = Arc::new(
            EngineCache::new_pooling(pool_capacity, max_memory_bytes)
                .map_err(|e| PipelineError::Internal(e.to_string()))?,
        );

        let module = engine
            .compile_module(&wasm_bytes)
            .map_err(|e| PipelineError::Internal(e.to_string()))?;

        let registry = Arc::new(crate::host_calls::MetricRegistry::new(&config.id));
        for signal in ["logs", "traces", "metrics"] {
            let mut signal_config = config.clone();
            signal_config
                .env
                .insert("signal".to_string(), (*signal).to_string());
            crate::worker::WasmWorker::probe_candidate(&engine, &module, &signal_config, &registry)
                .map_err(|e| {
                    PipelineError::Internal(format!(
                        "WASM guest validation failed for signal '{signal}': {e}"
                    ))
                })?;
        }

        Ok(())
    }

    /// Reloads the configured WASM module from disk, verifies its SHA-256 integrity (if configured),
    /// recompiles the module into the shared [`EngineCache`], instantiates a probe worker to validate
    /// exports and guest initialization, and advances the module generation counter.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if the module cannot be read, the SHA-256 hash
    /// mismatches, compilation fails, or the probe worker fails initialization.
    pub fn reload_module(
        config: &WasmTransformerConfig,
        engine: &Arc<EngineCache>,
    ) -> Result<u64, PipelineError> {
        let wasm_bytes = std::fs::read(&config.module_path).map_err(|e| {
            PipelineError::Internal(format!(
                "Failed to read WASM module '{}' for reload: {e}",
                config.module_path
            ))
        })?;

        if let Some(ref expected_hash) = config.sha256 {
            let mut hasher = Sha256::new();
            hasher.update(&wasm_bytes);
            let calculated = hex::encode(hasher.finalize());
            if !calculated.eq_ignore_ascii_case(expected_hash) {
                return Err(PipelineError::Internal(format!(
                    "SHA256 mismatch for WASM module '{}' during reload: expected {expected_hash}, got {calculated}",
                    config.module_path
                )));
            }
        }

        let new_mod = Arc::new(wasmtime::Module::new(engine.engine(), &wasm_bytes).map_err(
            |e| PipelineError::Internal(format!("Failed to compile reloaded module: {e}")),
        )?);

        let registry = Arc::new(crate::host_calls::MetricRegistry::new(&config.id));
        for signal in ["logs", "traces", "metrics"] {
            let mut signal_config = config.clone();
            signal_config
                .env
                .insert("signal".to_string(), (*signal).to_string());
            crate::worker::WasmWorker::probe_candidate(engine, &new_mod, &signal_config, &registry)
                .map_err(|e| {
                    PipelineError::Internal(format!(
                        "WASM guest validation failed on reload for signal '{signal}': {e}"
                    ))
                })?;
        }

        let new_gen = engine.publish_module(new_mod);
        Ok(new_gen)
    }
}

#[async_trait]
impl Transform for WasmTransformer {
    async fn transform(
        &mut self,
        input: PipelineReceiver,
        output: PipelineSender,
    ) -> Result<(), PipelineError> {
        let dispatcher = WasmDispatcher::new(
            DispatcherConfig {
                concurrency: self.config.concurrency,
                worker_channel_capacity: self.config.worker_channel_capacity,
            },
            Arc::clone(&self.engine),
            Arc::clone(&self.module),
            self.config.clone(),
            output,
            self.reroute_error.clone(),
            self.reroute_reject.clone(),
            Arc::clone(&self.registry),
        );

        dispatcher
            .run(input)
            .await
            .map_err(|e| PipelineError::Internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, SchemaGuardMode};

    fn base_config(path: String) -> WasmTransformerConfig {
        WasmTransformerConfig {
            id: "test".to_string(),
            r#type: "wasm".to_string(),
            module_path: path,
            sha256: None,
            max_execution_duration: "1s".to_string(),
            drain_timeout: "1s".to_string(),
            max_batch_rows: 1000,
            concurrency: 1,
            worker_channel_capacity: 1,
            max_memory: "64MiB".to_string(),
            rejuvenate_threshold: "16MiB".to_string(),
            rejuvenate_batches: 1000,
            init_timeout: "1s".to_string(),
            on_error: OnErrorPolicy::Drop,
            allow_unmasked_passthrough: false,
            on_reject: OnRejectPolicy::Drop,
            schema_guard: SchemaGuardMode::Defensive,
            env_whitelist: vec![],
            env: std::collections::HashMap::new(),
            config: None,
            enable_sighup: false,
        }
    }

    #[test]
    fn test_new_topological_sink_missing_error() {
        let mut config = base_config("dummy".to_string());
        config.on_error = OnErrorPolicy::Reroute;
        let res = WasmTransformer::new(config, None, None);
        assert!(matches!(res, Err(PipelineError::TopologicalSinkMissing(_))));
    }

    #[test]
    fn test_new_topological_sink_missing_reject() {
        let mut config = base_config("dummy".to_string());
        config.on_reject = OnRejectPolicy::Reroute;
        let res = WasmTransformer::new(config, None, None);
        assert!(matches!(res, Err(PipelineError::TopologicalSinkMissing(_))));
    }

    #[test]
    fn test_new_missing_module_path() {
        let config = base_config("/invalid/path/that/does/not/exist.wasm".to_string());
        let res = WasmTransformer::new(config, None, None);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("Failed to read WASM module"))
        );
    }

    #[test]
    fn test_new_sha256_mismatch() {
        let path = std::env::temp_dir().join("test_new_sha256_mismatch.wasm");
        std::fs::write(&path, b"invalid wasm bytes").unwrap();

        let mut config = base_config(path.to_str().unwrap().to_string());
        config.sha256 =
            Some("0000000000000000000000000000000000000000000000000000000000000000".to_string());

        let res = WasmTransformer::new(config, None, None);
        let _ = std::fs::remove_file(&path);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("SHA256 mismatch"))
        );
    }

    #[test]
    fn test_new_invalid_wasm_compilation() {
        let path = std::env::temp_dir().join("test_new_invalid_wasm_compilation.wasm");
        std::fs::write(&path, b"invalid wasm bytes").unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::new(config, None, None);
        let _ = std::fs::remove_file(&path);
        assert!(matches!(res, Err(PipelineError::Internal(_))));
    }

    #[test]
    fn test_new_rejects_zero_concurrency() {
        let mut config = base_config("dummy".to_string());
        config.concurrency = 0;
        let res = WasmTransformer::new(config, None, None);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be between 1 and"))
        );
    }

    #[test]
    fn test_validate_config_rejects_zero_concurrency() {
        let mut config = base_config("dummy".to_string());
        config.concurrency = 0;
        let res = WasmTransformer::validate_config(&config);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be between 1 and"))
        );
    }

    #[test]
    fn test_new_rejects_overflow_concurrency() {
        let mut config = base_config("dummy".to_string());
        config.concurrency = usize::MAX;
        let res = WasmTransformer::new(config, None, None);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be between 1 and"))
        );
    }

    #[test]
    fn test_validate_config_rejects_overflow_concurrency() {
        let mut config = base_config("dummy".to_string());
        config.concurrency = usize::MAX;
        let res = WasmTransformer::validate_config(&config);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be between 1 and"))
        );
    }

    #[test]
    fn test_new_rejects_passthrough_without_allow_unmasked() {
        let mut config = base_config("dummy".to_string());
        config.on_error = OnErrorPolicy::Passthrough;
        config.allow_unmasked_passthrough = false;
        let res = WasmTransformer::new(config, None, None);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("on_error is set to 'passthrough' but allow_unmasked_passthrough is false"))
        );
    }

    #[test]
    fn test_validate_config_rejects_passthrough_without_allow_unmasked() {
        let mut config = base_config("dummy".to_string());
        config.on_error = OnErrorPolicy::Passthrough;
        config.allow_unmasked_passthrough = false;
        let res = WasmTransformer::validate_config(&config);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("on_error is set to 'passthrough' but allow_unmasked_passthrough is false"))
        );
    }

    #[test]
    fn test_validate_config_rejects_missing_abi_version() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path =
            std::env::temp_dir().join("test_validate_config_rejects_missing_abi_version.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("datalake_abi_version"))
        );
    }

    #[test]
    fn test_validate_config_rejects_unsupported_abi_version() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 2))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path =
            std::env::temp_dir().join("test_validate_config_rejects_unsupported_abi_version.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("ABI version mismatch: expected 1, got 2"))
        );
    }

    #[test]
    fn test_validate_config_rejects_missing_required_export() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path =
            std::env::temp_dir().join("test_validate_config_rejects_missing_required_export.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("datalake_alloc")));
    }

    #[test]
    fn test_validate_config_succeeds_for_compliant_module() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path =
            std::env::temp_dir().join("test_validate_config_succeeds_for_compliant_module.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(res.is_ok());
    }

    #[test]
    fn test_new_rejects_module_with_missing_abi_version_early() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path = std::env::temp_dir()
            .join("test_new_rejects_module_with_missing_abi_version_early.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::new(config, None, None);
        let _ = std::fs::remove_file(&path);

        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("datalake_abi_version"))
        );
    }

    #[test]
    fn test_reload_module_advances_generation() {
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path = std::env::temp_dir().join("test_reload_module_advances_generation.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let engine = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
        assert_eq!(engine.module_generation(), 0);

        let generation = WasmTransformer::reload_module(&config, &engine).unwrap();
        assert_eq!(generation, 1);
        assert_eq!(engine.module_generation(), 1);

        let generation2 = WasmTransformer::reload_module(&config, &engine).unwrap();
        assert_eq!(generation2, 2);
        assert_eq!(engine.module_generation(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_validate_config_succeeds_with_signal_aware_guest() {
        // Guest module that fails if "signal":"unknown" is received (checks for '"' followed by 'u' = 34, 117).
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
                (local $i i32)
                (local.set $i (local.get $ptr))
                (block $break
                    (loop $loop
                        (br_if $break (i32.ge_u (local.get $i) (i32.sub (i32.add (local.get $ptr) (local.get $len)) (i32.const 1))))
                        ;; Rejects '"' (34) followed by 'u' (117)
                        (if (i32.and
                                (i32.eq (i32.load8_u (local.get $i)) (i32.const 34))
                                (i32.eq (i32.load8_u (i32.add (local.get $i) (i32.const 1))) (i32.const 117)))
                            (then (return (i32.const 1)))
                        )
                        (local.set $i (i32.add (local.get $i) (i32.const 1)))
                        (br $loop)
                    )
                )
                (i32.const 0)
            )
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path = std::env::temp_dir().join("test_validate_signal_aware.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        // validate_config probes logs, traces, and metrics (none starts with "u), so it must succeed.
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(
            res.is_ok(),
            "validate_config must succeed for signal-aware guest"
        );
    }

    #[test]
    fn test_validate_config_rejects_guest_failing_specific_signal() {
        // Guest module that fails specifically when signal is 'logs' (checks for '"' followed by 'l' = 34, 108).
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
                (local $i i32)
                (local.set $i (local.get $ptr))
                (block $break
                    (loop $loop
                        (br_if $break (i32.ge_u (local.get $i) (i32.sub (i32.add (local.get $ptr) (local.get $len)) (i32.const 1))))
                        ;; Rejects '"' (34) followed by 'l' (108)
                        (if (i32.and
                                (i32.eq (i32.load8_u (local.get $i)) (i32.const 34))
                                (i32.eq (i32.load8_u (i32.add (local.get $i) (i32.const 1))) (i32.const 108)))
                            (then (return (i32.const 1)))
                        )
                        (local.set $i (i32.add (local.get $i) (i32.const 1)))
                        (br $loop)
                    )
                )
                (i32.const 0)
            )
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path = std::env::temp_dir().join("test_validate_reject_logs.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let res = WasmTransformer::validate_config(&config);
        let _ = std::fs::remove_file(&path);

        assert!(
            res.is_err(),
            "validate_config must fail when a specific signal probe fails"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("signal 'logs'"));
    }

    #[test]
    fn test_reload_module_probes_signal_contexts() {
        // Guest module that fails if "signal":"unknown" is received (checks for '"' followed by 'u' = 34, 117).
        let wat_src = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
                (local $i i32)
                (local.set $i (local.get $ptr))
                (block $break
                    (loop $loop
                        (br_if $break (i32.ge_u (local.get $i) (i32.sub (i32.add (local.get $ptr) (local.get $len)) (i32.const 1))))
                        ;; Rejects '"' (34) followed by 'u' (117)
                        (if (i32.and
                                (i32.eq (i32.load8_u (local.get $i)) (i32.const 34))
                                (i32.eq (i32.load8_u (i32.add (local.get $i) (i32.const 1))) (i32.const 117)))
                            (then (return (i32.const 1)))
                        )
                        (local.set $i (i32.add (local.get $i) (i32.const 1)))
                        (br $loop)
                    )
                )
                (i32.const 0)
            )
        )"#;
        let wasm_bytes = wat::parse_str(wat_src).unwrap();
        let path = std::env::temp_dir().join("test_reload_signal_aware.wasm");
        std::fs::write(&path, wasm_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());
        let engine = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());

        // reload_module probes logs, traces, and metrics, none of which starts with "u, so it must succeed.
        let generation = WasmTransformer::reload_module(&config, &engine).unwrap();
        assert_eq!(generation, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rapid_reloads_publish_module_atomically() {
        let engine = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).unwrap());
        let wat1 = r#"(module
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
        )"#;
        let wat2 = r#"(module
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
        )"#;
        let bytes1 = wat::parse_str(wat1).unwrap();
        let bytes2 = wat::parse_str(wat2).unwrap();
        let mod1 = Arc::new(wasmtime::Module::new(engine.engine(), &bytes1).unwrap());
        let mod2 = Arc::new(wasmtime::Module::new(engine.engine(), &bytes2).unwrap());

        assert_eq!(engine.module_generation(), 0);

        let gen1 = engine.publish_module(Arc::clone(&mod1));
        assert_eq!(gen1, 1);
        let snap1 = engine.current_snapshot().unwrap();
        assert_eq!(snap1.generation, 1);

        let gen2 = engine.publish_module(Arc::clone(&mod2));
        assert_eq!(gen2, 2);
        let snap2 = engine.current_snapshot().unwrap();
        assert_eq!(snap2.generation, 2);
    }

    #[test]
    fn test_reload_module_rejects_candidate_with_missing_exports_even_when_cached() {
        let engine = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());

        // Publish a valid module as generation 1
        let valid_wat = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let valid_bytes = wat::parse_str(valid_wat).unwrap();
        let valid_mod = Arc::new(wasmtime::Module::new(engine.engine(), &valid_bytes).unwrap());
        let gen1 = engine.publish_module(valid_mod);
        assert_eq!(gen1, 1);

        // Write an INVALID candidate module (missing datalake_alloc) to disk
        let invalid_wat = r#"(module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        )"#;
        let invalid_bytes = wat::parse_str(invalid_wat).unwrap();
        let path = std::env::temp_dir().join("test_reload_invalid_candidate.wasm");
        std::fs::write(&path, invalid_bytes).unwrap();

        let config = base_config(path.to_str().unwrap().to_string());

        // reload_module must probe the candidate module directly, detect the missing export,
        // fail, and NOT publish the invalid module
        let res = WasmTransformer::reload_module(&config, &engine);
        assert!(res.is_err());
        assert_eq!(engine.module_generation(), 1);

        let _ = std::fs::remove_file(&path);
    }
}
