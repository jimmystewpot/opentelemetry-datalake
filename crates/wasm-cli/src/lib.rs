//! WASM transform developer toolchain library.
//!
//! Provides validation, testing, and benchmarking utilities for
//! `opentelemetry-datalake` WebAssembly guest transform plugins.

#![allow(clippy::print_stdout)]

pub mod bench;
pub mod tester;
pub mod validator;

/// Constructs a default synchronous WASI Preview 1 context for CLI validation,
/// testing, and benchmarking runs.
#[must_use]
pub fn default_wasi_ctx() -> wasmtime_wasi::p1::WasiP1Ctx {
    wasmtime_wasi::WasiCtxBuilder::new().build_p1()
}

/// Maps a canonical OTLP signal name (`"logs"`, `"metrics"`, `"traces"`, case-insensitive)
/// to its C-ABI v1 `signal_type` discriminant (`0`, `1`, `2`), defaulting to `0` (`Logs`).
#[must_use]
pub fn parse_signal_type(signal: &str) -> u32 {
    if signal.eq_ignore_ascii_case("metrics") {
        1
    } else if signal.eq_ignore_ascii_case("traces") {
        2
    } else {
        0
    }
}

/// Builds a [`wasmtime::Linker`] pre-populated with WASI Preview 1 and the
/// `env` host import stubs required by `opentelemetry-datalake` WASM guests.
///
/// # Errors
///
/// Returns [`wasmtime::Error`] if registering WASI Preview 1 or any `env` host
/// function with the linker fails.
pub fn build_cli_linker(
    engine: &wasmtime::Engine,
) -> std::result::Result<wasmtime::Linker<wasmtime_wasi::p1::WasiP1Ctx>, wasmtime::Error> {
    let mut linker = wasmtime::Linker::new(engine);
    wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |ctx| ctx)?;

    linker.func_wrap(
        "env",
        "datalake_host_metric_emit",
        |_metric_type: u32, _name_ptr: u32, _name_len: u32, _value: u64| {},
    )?;
    linker.func_wrap(
        "env",
        "datalake_host_log",
        |_level: u32, _msg_ptr: u32, _msg_len: u32| {},
    )?;
    linker.func_wrap(
        "env",
        "datalake_host_has_capability",
        |_cap_ptr: u32, _cap_len: u32| -> u32 { 0 },
    )?;
    linker.func_wrap("env", "datalake_host_now_nanos", || -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
    })?;

    Ok(linker)
}
