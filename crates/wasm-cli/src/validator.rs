//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use thiserror::Error;
use wasmtime::{Config, Engine, Instance, Module, Store};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid WASM: {0}")]
    InvalidWasm(#[source] wasmtime::Error),
    #[error("Missing export '{0}'")]
    MissingExport(String),
    #[error("Instantiation failed: {0}")]
    InstantiationFailed(#[source] wasmtime::Error),
    #[error("Cannot call datalake_abi_version: {0}")]
    AbiFunctionMissing(#[source] wasmtime::Error),
    #[error("datalake_abi_version trap: {0}")]
    AbiTrap(#[source] wasmtime::Error),
    #[error("ABI version mismatch: expected {expected}, got {got}")]
    AbiMismatch { expected: u32, got: u32 },
    #[error("Engine initialization failed: {0}")]
    EngineInit(#[source] wasmtime::Error),
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
/// Returns [`ValidationError`] if:
/// - The bytecode is invalid WebAssembly ([`ValidationError::InvalidWasm`]).
/// - Any required export is missing from the module interface ([`ValidationError::MissingExport`]).
/// - The wasmtime engine fails to initialize or configure fuel ([`ValidationError::EngineInit`]).
/// - The module cannot be instantiated ([`ValidationError::InstantiationFailed`]).
/// - Calling `datalake_abi_version` fails ([`ValidationError::AbiFunctionMissing`] or [`ValidationError::AbiTrap`]).
/// - The module's ABI version does not match the expected version ([`ValidationError::AbiMismatch`]).
pub fn validate_wasm_bytes(bytes: &[u8]) -> std::result::Result<(), ValidationError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config).map_err(ValidationError::EngineInit)?;

    let module = Module::new(&engine, bytes).map_err(ValidationError::InvalidWasm)?;
    for &required in REQUIRED_EXPORTS {
        if !module.exports().any(|e| e.name() == required) {
            return Err(ValidationError::MissingExport(required.to_string()));
        }
    }
    let mut store: Store<()> = Store::new(&engine, ());
    store
        .set_fuel(100_000)
        .map_err(ValidationError::EngineInit)?;

    let instance =
        Instance::new(&mut store, &module, &[]).map_err(ValidationError::InstantiationFailed)?;
    let abi_fn = instance
        .get_typed_func::<(), u32>(&mut store, "datalake_abi_version")
        .map_err(ValidationError::AbiFunctionMissing)?;
    let version = abi_fn
        .call(&mut store, ())
        .map_err(ValidationError::AbiTrap)?;
    if version != opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION {
        return Err(ValidationError::AbiMismatch {
            expected: opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION,
            got: version,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn assert_wasmtime_error(_: &wasmtime::Error) {}

    #[test]
    fn test_invalid_wasm_returns_typed_wasmtime_error() {
        let res = validate_wasm_bytes(b"not a valid wasm binary");
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::InvalidWasm(source) => {
                assert_wasmtime_error(source);
                assert!(err.source().is_some());
                assert!(err.to_string().contains("Invalid WASM:"));
            }
            other => panic!("expected InvalidWasm, got {other:?}"),
        }
    }

    #[test]
    fn test_missing_export_error_variant() {
        let wat_src = "(module)";
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::MissingExport(name) => {
                assert_eq!(name, "datalake_abi_version");
            }
            other => panic!("expected MissingExport, got {other:?}"),
        }
    }
}
