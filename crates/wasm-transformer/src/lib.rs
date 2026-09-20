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
