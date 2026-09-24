//! WebAssembly instance pool.

use crate::engine::EngineCache;
use std::sync::Arc;
use wasmtime::Module;

/// Pool of reusable WebAssembly instances backed by an [`EngineCache`].
#[derive(Debug)]
pub struct InstancePool {
    engine: Arc<EngineCache>,
    module: Arc<Module>,
    available: usize,
}

impl InstancePool {
    /// Creates a new instance pool with the specified size.
    #[must_use]
    pub fn new(engine: Arc<EngineCache>, module: Arc<Module>, size: usize) -> Self {
        Self {
            engine,
            module,
            available: size,
        }
    }

    /// Returns the number of available instance slots in the pool.
    #[must_use]
    pub fn available_slots(&self) -> usize {
        self.available
    }

    /// Returns a reference to the underlying [`EngineCache`].
    #[must_use]
    pub fn engine(&self) -> &Arc<EngineCache> {
        &self.engine
    }

    /// Returns a reference to the compiled [`Module`].
    #[must_use]
    pub fn module(&self) -> &Arc<Module> {
        &self.module
    }
}
