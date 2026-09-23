//! Wasmtime engine cache and pooling allocator configuration.

use crate::error::WasmTransformError;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, Module, PoolingAllocationConfig};

/// A snapshot of a compiled WebAssembly module and its corresponding generation counter.
#[derive(Clone)]
pub struct ModuleSnapshot {
    /// The compiled Wasmtime module.
    pub module: Arc<Module>,
    /// The generation counter associated with this module.
    pub generation: u64,
}

/// Engine cache maintaining a compiled WebAssembly module and generation counter.
pub struct EngineCache {
    engine: Engine,
    snapshot: RwLock<Option<ModuleSnapshot>>,
    generation: AtomicU64,
    stop_epoch_ticker: Arc<AtomicBool>,
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
        let mut pool_cfg = PoolingAllocationConfig::default();
        pool_cfg.total_core_instances(u32::try_from(concurrency).unwrap_or(4));
        pool_cfg.total_memories(u32::try_from(concurrency).unwrap_or(4));
        pool_cfg.total_tables(u32::try_from(concurrency).unwrap_or(4));
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
        let mut guard = self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_gen = self.generation.load(Ordering::Acquire);
        *guard = Some(ModuleSnapshot {
            module: Arc::clone(&module),
            generation: current_gen,
        });
        Ok(module)
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
    pub fn reload_from_bytes(
        &self,
        new_bytes: &[u8],
        expected_sha: Option<&str>,
    ) -> Result<u64, WasmTransformError> {
        if let Some(expected) = expected_sha {
            let actual = crate::reload::compute_sha256(new_bytes);
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(WasmTransformError::Sha256Mismatch {
                    expected: expected.to_string(),
                    actual,
                });
            }
        }

        let new_module = Arc::new(Module::new(&self.engine, new_bytes)?);
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
            None => (None, 0),
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

impl Drop for EngineCache {
    fn drop(&mut self) {
        self.stop_epoch_ticker.store(true, Ordering::Relaxed);
    }
}
