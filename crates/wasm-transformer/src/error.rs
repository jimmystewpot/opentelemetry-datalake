//! Error types for the WebAssembly transformation engine.

use thiserror::Error;

/// Errors that can occur during WebAssembly module compilation, instantiation, or execution.
#[derive(Debug, Error)]
pub enum WasmTransformError {
    /// An error originating from the Wasmtime runtime engine.
    #[error("Wasmtime error: {0}")]
    Wasmtime(#[from] wasmtime::Error),

    /// A memory exhaustion event within the guest instance or pool.
    #[error("WASM OOM: module={module}, instance={instance}")]
    Oom {
        /// Name or identifier of the module that exhausted memory.
        module: String,
        /// Index of the instance within the pool.
        instance: usize,
    },

    /// The guest module reported an unsupported ABI version.
    #[error("ABI version mismatch: expected 1, got {0}")]
    AbiVersionMismatch(u32),

    /// A required function export was not found in the guest module.
    #[error("Missing required WASM export: {0}")]
    MissingExport(String),

    /// SHA-256 hash validation failed for the guest module.
    #[error("SHA-256 mismatch: expected {expected}, got {actual}")]
    Sha256Mismatch {
        /// Expected SHA-256 hash.
        expected: String,
        /// Actual computed SHA-256 hash.
        actual: String,
    },

    /// The guest initialization function failed.
    #[error("Guest init failed: {0}")]
    InitFailed(String),

    /// Execution of a guest transform timed out.
    #[error("Guest execution timeout after {0}ms")]
    ExecutionTimeout(u64),

    /// An underlying I/O error occurred.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// A general pipeline error occurred.
    #[error("Pipeline error: {0}")]
    Pipeline(String),

    /// An Arrow IPC serialization or deserialization error occurred.
    #[error("Arrow IPC error: {0}")]
    ArrowIpc(String),
}
