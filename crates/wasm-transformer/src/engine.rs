//! Wasmtime engine cache and pooling allocator configuration.

use crate::error::WasmTransformError;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, Module, PoolingAllocationConfig};

/// Engine cache maintaining a compiled WebAssembly module and generation counter.
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
        let mut pool_cfg = PoolingAllocationConfig::default();
        pool_cfg.total_memories(u32::try_from(concurrency).unwrap_or(4));
        pool_cfg.total_tables(u32::try_from(concurrency).unwrap_or(4));
        pool_cfg.max_memory_size(max_memory_bytes);

        let mut config = Config::new();
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool_cfg));

        let engine = Engine::new(&config)?;
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
        let mut guard = self
            .module
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(Arc::clone(&new_module));
        Ok(self.advance_generation())
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
