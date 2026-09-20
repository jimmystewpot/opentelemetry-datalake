//! WebAssembly transformation engine for OpenTelemetry data lake pipelines.
//!
//! Provides an ultra-high-performance, sandboxed WASM transformation engine executing
//! whole-batch Apache Arrow transformations using Wasmtime's pooling allocator.

pub mod engine;
pub mod error;
pub mod pool;
pub mod wasi_env;
pub mod worker;
