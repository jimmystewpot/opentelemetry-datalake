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

        // 2. Security audit logging for passthrough policy
        if config.on_error == OnErrorPolicy::Passthrough {
            warn!(
                transformer_id = %config.id,
                "SECURITY AUDIT: on_error=passthrough enabled. Input batches will bypass transformation on guest failure. Verify threat model."
            );
        }

        // 3. EngineCache initialization with pooling allocator
        let max_memory_bytes =
            crate::worker::parse_byte_size(&config.max_memory).unwrap_or(64 * 1024 * 1024);

        let engine = Arc::new(
            EngineCache::new_pooling(config.concurrency, max_memory_bytes)
                .map_err(|e| PipelineError::Internal(e.to_string()))?,
        );

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

        Ok(Self {
            config,
            engine,
            module,
            reroute_error,
            reroute_reject,
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
}
