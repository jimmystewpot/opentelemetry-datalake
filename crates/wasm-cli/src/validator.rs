//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use thiserror::Error;
use wasmtime::{Config, Engine, Instance, Module, Store};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid WASM: {0}")]
    InvalidWasm(#[source] anyhow::Error),
    #[error("Missing export '{0}'")]
    MissingExport(String),
    #[error("Instantiation failed: {0}")]
    InstantiationFailed(#[source] anyhow::Error),
    #[error("Cannot call datalake_abi_version: {0}")]
    AbiFunctionMissing(#[source] anyhow::Error),
    #[error("datalake_abi_version trap: {0}")]
    AbiTrap(#[source] anyhow::Error),
    #[error("ABI version mismatch: expected {expected}, got {got}")]
    AbiMismatch { expected: u32, got: u32 },
    #[error("Engine initialization failed: {0}")]
    EngineInit(#[source] anyhow::Error),
}

/// The set of required symbol exports defined by the datalake WASM ABI v1.
const REQUIRED_EXPORTS: &[&str] = &[
    "datalake_abi_version",
    "datalake_alloc",
    "datalake_dealloc",
    "datalake_init",
    "datalake_transform",
    "memory",
];

/// Validates raw WASM bytes against the C-ABI v1 specification.
///
/// # Errors
///
/// Returns an error string if:
/// - The bytecode is invalid WebAssembly.
/// - Any required export is missing from the module interface.
/// - The module cannot be instantiated.
/// - Calling `datalake_abi_version` fails or produces an unexpected version.
pub fn validate_wasm_bytes(bytes: &[u8]) -> std::result::Result<(), ValidationError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine =
        Engine::new(&config).map_err(|e| ValidationError::EngineInit(anyhow::anyhow!(e)))?;

    let module = Module::new(&engine, bytes)
        .map_err(|e| ValidationError::InvalidWasm(anyhow::anyhow!(e)))?;
    for &required in REQUIRED_EXPORTS {
        if !module.exports().any(|e| e.name() == required) {
            return Err(ValidationError::MissingExport(required.to_string()));
        }
    }
    let mut store: Store<()> = Store::new(&engine, ());
    store
        .set_fuel(100_000)
        .map_err(|e| ValidationError::EngineInit(anyhow::anyhow!(e)))?;

    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| ValidationError::InstantiationFailed(anyhow::anyhow!(e)))?;
    let abi_fn = instance
        .get_typed_func::<(), u32>(&mut store, "datalake_abi_version")
        .map_err(|e| ValidationError::AbiFunctionMissing(anyhow::anyhow!(e)))?;
    let version = abi_fn
        .call(&mut store, ())
        .map_err(|e| ValidationError::AbiTrap(anyhow::anyhow!(e)))?;
    if version != opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION {
        return Err(ValidationError::AbiMismatch {
            expected: opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION,
            got: version,
        });
    }
    Ok(())
}
