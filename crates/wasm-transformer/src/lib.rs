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
pub mod reload;
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

/// WebAssembly whole-batch transformer executing in isolated sandbox workers.
///
/// Implements the [`Transform`] pipeline trait, dispatching incoming telemetry batches
/// across a pool of sandboxed Wasmtime workers, verifying DLQ routing topological integrity,
/// and executing transformations with zero-copy Arrow IPC memory interchange.
pub struct WasmTransformer {
    config: WasmTransformerConfig,
    engine: Arc<EngineCache>,
    module: Arc<Module>,
    reroute_error: Option<PipelineSender>,
    reroute_reject: Option<PipelineSender>,
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
        let max_memory_bytes =
            crate::worker::parse_byte_size(&config.max_memory).unwrap_or(64 * 1024 * 1024);
        let pool_capacity = config.concurrency.max(1).saturating_add(1);
        let engine = Arc::new(
            EngineCache::new_pooling(pool_capacity, max_memory_bytes)
                .map_err(|e| PipelineError::Internal(e.to_string()))?,
        );
        Self::with_engine(config, reroute_error, reroute_reject, engine)
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
    /// `Reroute` but the corresponding DLQ sender channel is not provided (`None`).
    /// Returns [`PipelineError::Internal`] if module loading, hash verification, engine
    /// initialization, or guest worker instantiation fails.
    pub fn with_engine(
        config: WasmTransformerConfig,
        reroute_error: Option<PipelineSender>,
        reroute_reject: Option<PipelineSender>,
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
        if config.concurrency == 0 {
            return Err(PipelineError::Internal(
                "wasm_transformer.concurrency must be greater than 0".to_string(),
            ));
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
        let probe = crate::worker::WasmWorker::new(
            0,
            Arc::clone(&engine),
            Arc::clone(&module),
            config.clone(),
            Arc::clone(&registry),
        )
        .map_err(|e| PipelineError::Internal(format!("WASM worker initialization failed: {e}")))?;
        drop(probe);

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

    /// Validates the WASM transformer configuration, verifying module path readability,
    /// SHA-256 integrity, memory configuration, duration strings, and WASM module compilation.
    pub fn validate_config(config: &WasmTransformerConfig) -> Result<(), PipelineError> {
        if config.module_path.is_empty() {
            return Err(PipelineError::Internal(
                "wasm_transformer.module_path cannot be empty".to_string(),
            ));
        }

        if config.concurrency == 0 {
            return Err(PipelineError::Internal(
                "wasm_transformer.concurrency must be greater than 0".to_string(),
            ));
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

        let pool_capacity = config.concurrency.max(1).saturating_add(1);
        let engine = Arc::new(
            EngineCache::new_pooling(pool_capacity, max_memory_bytes)
                .map_err(|e| PipelineError::Internal(e.to_string()))?,
        );

        let module = engine
            .compile_module(&wasm_bytes)
            .map_err(|e| PipelineError::Internal(e.to_string()))?;

        let registry = Arc::new(crate::host_calls::MetricRegistry::new(&config.id));
        let probe = crate::worker::WasmWorker::new(
            0,
            Arc::clone(&engine),
            Arc::clone(&module),
            config.clone(),
            registry,
        )
        .map_err(|e| PipelineError::Internal(format!("WASM guest validation failed: {e}")))?;
        drop(probe);

        Ok(())
    }

    /// Reloads the configured WASM module from disk, verifies its SHA-256 integrity (if configured),
    /// recompiles the module into the shared [`EngineCache`], instantiates a probe worker to validate
    /// exports and guest initialization, and advances the module generation counter.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Internal`] if file reading, hash verification, module compilation,
    /// or guest instantiation fails.
    pub fn reload_module(
        config: &WasmTransformerConfig,
        engine: &Arc<EngineCache>,
    ) -> Result<u64, PipelineError> {
        let wasm_bytes = std::fs::read(&config.module_path).map_err(|e| {
            PipelineError::Internal(format!(
                "Failed to read WASM module '{}' on reload: {e}",
                config.module_path
            ))
        })?;

        if let Some(ref expected_hash) = config.sha256 {
            let mut hasher = Sha256::new();
            hasher.update(&wasm_bytes);
            let calculated = hex::encode(hasher.finalize());
            if !calculated.eq_ignore_ascii_case(expected_hash) {
                return Err(PipelineError::Internal(format!(
                    "SHA256 mismatch for WASM module '{}' on reload: expected {expected_hash}, got {calculated}",
                    config.module_path
                )));
            }
        }

        let new_mod = engine.compile_module(&wasm_bytes).map_err(|e| {
            PipelineError::Internal(format!("Failed to compile reloaded module: {e}"))
        })?;

        let registry = Arc::new(crate::host_calls::MetricRegistry::new(&config.id));
        let probe = crate::worker::WasmWorker::new(
            0,
            Arc::clone(engine),
            new_mod,
            config.clone(),
            registry,
        )
        .map_err(|e| {
            PipelineError::Internal(format!("WASM guest validation failed on reload: {e}"))
        })?;
        drop(probe);

        let new_gen = engine.advance_generation();
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
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be greater than 0"))
        );
    }

    #[test]
    fn test_validate_config_rejects_zero_concurrency() {
        let mut config = base_config("dummy".to_string());
        config.concurrency = 0;
        let res = WasmTransformer::validate_config(&config);
        assert!(
            matches!(res, Err(PipelineError::Internal(msg)) if msg.contains("concurrency must be greater than 0"))
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
}
