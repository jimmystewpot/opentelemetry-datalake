//! WASM transform developer toolchain library.
//!
//! Provides validation, testing, and benchmarking utilities for
//! `opentelemetry-datalake` WebAssembly guest transform plugins.

#![allow(clippy::print_stdout)]

pub mod bench;
pub mod helpers;
pub mod tester;
pub mod validator;

use wasmtime::{Caller, Engine, Linker, Module};

/// Creates a [`Linker`] pre-populated with standard host import stubs
/// (`datalake_host_log`, `datalake_host_metric_emit`) and with unknown imports
/// defined as traps so that guest modules containing WASI or runtime externs
/// can be instantiated and validated.
///
/// # Concurrency Characteristics
///
/// The returned [`Linker`] is bound to the provided [`Engine`] and parameterized with `()`.
/// While the underlying [`Engine`] is thread-safe (`Send + Sync`) and can be freely shared across
/// threads, each [`Linker`] instance must be used within its own thread or store execution context
/// and is not intended for concurrent instantiation across threads without external synchronization.
///
/// # Errors
///
/// Returns an error if registering host functions or unknown traps fails.
pub fn create_default_linker(
    engine: &Engine,
    module: &Module,
) -> Result<Linker<()>, wasmtime::Error> {
    let mut linker = Linker::new(engine);
    linker.func_wrap(
        "env",
        "datalake_host_log",
        |_caller: Caller<'_, ()>, _level: u32, _msg_ptr: u32, _msg_len: u32| {},
    )?;
    linker.func_wrap(
        "env",
        "datalake_host_metric_emit",
        |_caller: Caller<'_, ()>, _type: u32, _name_ptr: u32, _name_len: u32, _val: u64| {},
    )?;
    linker.define_unknown_imports_as_traps(module)?;
    Ok(linker)
}
