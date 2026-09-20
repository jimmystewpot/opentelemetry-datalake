//! WASM transform developer toolchain library.
//!
//! Provides validation, testing, and benchmarking utilities for
//! `opentelemetry-datalake` WebAssembly guest transform plugins.

#![allow(clippy::print_stdout)]

pub mod bench;
pub mod tester;
pub mod validator;
