//! Wasmtime engine cache and pooling allocator configuration.

use crate::error::WasmTransformError;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, Module, PoolingAllocationConfig};

fn compute_sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// A snapshot of a compiled WebAssembly module and its corresponding generation counter.
#[derive(Clone, Debug)]
pub struct ModuleSnapshot {
    /// The compiled Wasmtime module.
    pub module: Arc<Module>,
    /// The generation counter associated with this module.
    pub generation: u64,
}

/// Engine cache maintaining a compiled WebAssembly module and generation counter.
#[derive(Debug)]
pub struct EngineCache {
    engine: Engine,
    snapshot: RwLock<Option<ModuleSnapshot>>,
    generation: AtomicU64,
    stop_epoch_ticker: Arc<AtomicBool>,
}

impl Drop for EngineCache {
    fn drop(&mut self) {
        self.stop_epoch_ticker.store(true, Ordering::Relaxed);
    }
}

impl EngineCache {
    /// Creates a new `EngineCache` configured with Wasmtime's pooling allocator.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmTransformError`] if the Wasmtime engine cannot be initialized
    /// with the provided pooling allocation configuration.
    pub fn new_pooling(
        concurrency: usize,
        max_memory_bytes: usize,
    ) -> Result<Self, WasmTransformError> {
        let active_slots = u32::try_from(concurrency.max(1)).unwrap_or(4);
        // Provide 2x instance/memory headroom so that active workers can instantiate candidate
        // instances during hot-reloads and transitions without exhausting the pool.
        let pool_slots = active_slots.saturating_mul(2);
        let mut pool_cfg = PoolingAllocationConfig::default();
        pool_cfg.total_core_instances(pool_slots);
        pool_cfg.total_memories(pool_slots);
        pool_cfg.total_tables(pool_slots);
        pool_cfg.max_memory_size(max_memory_bytes);

        let mut config = Config::new();
        config.epoch_interruption(true);
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool_cfg));

        let engine = Engine::new(&config)?;

        let stop_epoch_ticker = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop_epoch_ticker);
        let engine_clone = engine.clone();

        std::thread::Builder::new()
            .name("wasm-epoch-ticker".into())
            .spawn(move || {
                while !stop_clone.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    engine_clone.increment_epoch();
                }
            })
            .map_err(WasmTransformError::Io)?;

        Ok(Self {
            engine,
            snapshot: RwLock::new(None),
            generation: AtomicU64::new(0),
            stop_epoch_ticker,
        })
    }

    /// Compiles a WebAssembly binary into an `Arc<Module>` and stores it in the cache.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmTransformError`] if module compilation fails.
    pub fn compile_module(&self, bytes: &[u8]) -> Result<Arc<Module>, WasmTransformError> {
        let module = Arc::new(Module::new(&self.engine, bytes)?);
        self.publish_module(Arc::clone(&module));
        Ok(module)
    }

    /// Compiles a WebAssembly binary into an `Arc<Module>` and stores it in the cache,
    /// optionally verifying its SHA-256 hash if `expected_sha256` is provided.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError::Sha256Mismatch`] if hash verification fails,
    /// or [`WasmTransformError::Wasmtime`] if module compilation fails.
    pub fn compile_module_with_sha256(
        &self,
        bytes: &[u8],
        expected_sha256: Option<&str>,
    ) -> Result<Arc<Module>, WasmTransformError> {
        if let Some(expected) = expected_sha256 {
            let actual = compute_sha256_hex(bytes);
            if !actual.eq_ignore_ascii_case(expected.trim()) {
                return Err(WasmTransformError::Sha256Mismatch {
                    expected: expected.trim().to_string(),
                    actual,
                });
            }
        }
        self.compile_module(bytes)
    }

    /// Atomically publishes a new compiled module and increments the generation counter.
    pub fn publish_module(&self, module: Arc<Module>) -> u64 {
        let mut guard = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_gen = self.generation.load(Ordering::Relaxed) + 1;
        *guard = Some(ModuleSnapshot {
            module,
            generation: new_gen,
        });
        self.generation.store(new_gen, Ordering::Release);
        new_gen
    }

    /// Recompiles a WebAssembly module from bytes, verifies its SHA-256 hash if specified,
    /// atomically swaps the active module in the cache, and advances the generation counter.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError::Sha256Mismatch`] if `expected_sha` is provided and does
    /// not match the SHA-256 checksum of `new_bytes` (comparison is case-insensitive).
    /// Returns [`WasmTransformError::Wasmtime`] if the WebAssembly module fails compilation.
    /// Compiles a candidate WebAssembly module from bytes, verifies its SHA-256 hash if specified,
    /// and probes candidate guest ABI exports and initialization without modifying the active cache snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError::Sha256Mismatch`] if hash verification fails,
    /// [`WasmTransformError::Wasmtime`] if compilation fails,
    /// or a [`WasmTransformError`] if guest ABI verification or initialization fails.
    pub fn compile_and_probe_candidate(
        &self,
        new_bytes: &[u8],
        expected_sha: Option<&str>,
    ) -> Result<Arc<Module>, WasmTransformError> {
        if let Some(expected) = expected_sha {
            let actual = compute_sha256_hex(new_bytes);
            if !actual.eq_ignore_ascii_case(expected.trim()) {
                return Err(WasmTransformError::Sha256Mismatch {
                    expected: expected.trim().to_string(),
                    actual,
                });
            }
        }

        let new_module = Arc::new(Module::new(&self.engine, new_bytes)?);
        let probe_cfg = pipeline_core::config::WasmTransformerConfig {
            id: "probe".to_string(),
            r#type: "wasm".to_string(),
            module_path: String::new(),
            sha256: None,
            max_execution_duration: "5s".to_string(),
            drain_timeout: "5s".to_string(),
            max_batch_rows: 1000,
            concurrency: 1,
            worker_channel_capacity: 10,
            max_memory: "64MiB".to_string(),
            rejuvenate_threshold: "50MiB".to_string(),
            rejuvenate_batches: 1000,
            init_timeout: "5s".to_string(),
            on_error: pipeline_core::config::OnErrorPolicy::default(),
            allow_unmasked_passthrough: false,
            on_reject: pipeline_core::config::OnRejectPolicy::default(),
            schema_guard: pipeline_core::config::SchemaGuardMode::default(),
            env_whitelist: Vec::new(),
            env: std::collections::HashMap::new(),
            config: None,
            enable_sighup: false,
        };
        let registry = Arc::new(crate::host_calls::MetricRegistry::new("probe"));
        crate::worker::WasmWorker::probe_candidate(self, &new_module, &probe_cfg, &registry)?;
        Ok(new_module)
    }

    /// Recompiles a WebAssembly module from bytes, verifies its SHA-256 hash if specified,
    /// validates candidate guest ABI exports and initialization, atomically swaps the active
    /// module in the cache, and advances the generation counter.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError::Sha256Mismatch`] if `expected_sha` is provided and does
    /// not match the SHA-256 checksum of `new_bytes` (comparison is case-insensitive).
    /// Returns [`WasmTransformError::Wasmtime`] if the WebAssembly module fails compilation.
    /// Returns [`WasmTransformError`] if guest ABI verification or initialization fails.
    pub fn reload_from_bytes(
        &self,
        new_bytes: &[u8],
        expected_sha: Option<&str>,
    ) -> Result<u64, WasmTransformError> {
        let new_module = self.compile_and_probe_candidate(new_bytes, expected_sha)?;
        let next_gen = self.publish_module(new_module);
        Ok(next_gen)
    }

    /// Returns the currently active module snapshot, if available.
    #[must_use]
    pub fn current_snapshot(&self) -> Option<ModuleSnapshot> {
        let guard = self
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.clone()
    }

    /// Retrieves the currently compiled module from the cache, if available.
    #[must_use]
    pub fn module(&self) -> Option<Arc<Module>> {
        self.current_snapshot().map(|s| s.module)
    }

    /// Retrieves an atomic snapshot of the currently compiled module and its generation counter.
    ///
    /// The module reference and generation counter are sampled under the module read lock,
    /// ensuring that callers never observe a newer generation paired with an older module pointer.
    #[must_use]
    pub fn current_module(&self) -> (Option<Arc<Module>>, u64) {
        let guard = self
            .snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match *guard {
            Some(ref snap) => (Some(Arc::clone(&snap.module)), snap.generation),
            None => (None, self.generation.load(Ordering::Acquire)),
        }
    }

    /// Returns the current module generation counter.
    #[must_use]
    pub fn module_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Increments the module generation counter and returns the new generation.
    #[must_use]
    pub fn advance_generation(&self) -> u64 {
        let mut guard = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(ref mut snap) = *guard {
            snap.generation = new_gen;
        }
        new_gen
    }

    /// Returns a reference to the underlying Wasmtime [`Engine`].
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
