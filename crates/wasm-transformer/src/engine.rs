//! Wasmtime engine cache and pooling allocator configuration.

use crate::error::WasmTransformError;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, Module, PoolingAllocationConfig};

fn compute_sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Engine cache maintaining a compiled WebAssembly module and generation counter.
#[derive(Debug)]
pub struct EngineCache {
    engine: Engine,
    module: RwLock<Option<Arc<Module>>>,
    generation: AtomicU64,
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
        let pool_slots = u32::try_from(concurrency.max(1)).unwrap_or(4);
        let mut pool_cfg = PoolingAllocationConfig::default();
        pool_cfg.total_memories(pool_slots);
        pool_cfg.total_tables(pool_slots);
        pool_cfg.max_memory_size(max_memory_bytes);

        let mut config = Config::new();
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool_cfg));
        config.epoch_interruption(true);

        let engine = Engine::new(&config)?;

        let engine_clone = engine.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(10));
                engine_clone.increment_epoch();
            }
        });

        Ok(Self {
            engine,
            module: RwLock::new(None),
            generation: AtomicU64::new(0),
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
            .module
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(Arc::clone(&module));
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

    /// Retrieves the currently compiled module from the cache, if available.
    #[must_use]
    pub fn module(&self) -> Option<Arc<Module>> {
        let guard = self
            .module
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.clone()
    }

    /// Returns the current module generation counter.
    #[must_use]
    pub fn module_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Increments the module generation counter and returns the new generation.
    #[must_use]
    pub fn advance_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Returns a reference to the underlying Wasmtime [`Engine`].
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
