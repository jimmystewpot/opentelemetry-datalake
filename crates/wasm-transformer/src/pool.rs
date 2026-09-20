//! WebAssembly instance pool.

use crate::engine::EngineCache;
use std::sync::Arc;
use wasmtime::Module;

/// Pool of reusable WebAssembly instances backed by an [`EngineCache`].
pub struct InstancePool {
    _engine: Arc<EngineCache>,
    _module: Arc<Module>,
    available: usize,
}

impl InstancePool {
    /// Creates a new instance pool with the specified size.
    #[must_use]
    pub fn new(engine: Arc<EngineCache>, module: Arc<Module>, size: usize) -> Self {
        Self {
            _engine: engine,
            _module: module,
            available: size,
        }
    }

    /// Returns the number of available instance slots in the pool.
    #[must_use]
    pub fn available_slots(&self) -> usize {
        self.available
    }
}
