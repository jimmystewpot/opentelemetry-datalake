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
        let new_gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        *guard = Some(ModuleSnapshot {
            module,
            generation: new_gen,
        });
        new_gen
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

    /// Returns the currently compiled module and its generation counter as an atomic snapshot.
    #[must_use]
    pub fn current_module(&self) -> (Option<Arc<Module>>, u64) {
        if let Some(snap) = self.current_snapshot() {
            (Some(snap.module), snap.generation)
        } else {
            (None, self.generation.load(Ordering::Acquire))
        }
    }

    /// Retrieves the currently compiled module from the cache, if available.
    #[must_use]
    pub fn module(&self) -> Option<Arc<Module>> {
        self.current_snapshot().map(|s| s.module)
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
