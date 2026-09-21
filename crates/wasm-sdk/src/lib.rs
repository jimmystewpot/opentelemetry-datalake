//! WebAssembly (WASM) Guest SDK for `opentelemetry-datalake`.
//!
//! Provides the low-overhead C-ABI v1 data structures, safe allocators,
//! canonical immutability guards, and transformation abstractions for WASM guest transform modules.

pub mod abi;
pub mod error;
pub mod helpers;
pub mod metrics;
pub mod panic;
pub mod traits;

