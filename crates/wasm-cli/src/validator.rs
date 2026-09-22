//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use thiserror::Error;
use wasmtime::{Config, Engine, ExternType, Module, Store, ValType};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid WASM: {0}")]
    InvalidWasm(#[source] wasmtime::Error),
    #[error("Missing export '{0}'")]
    MissingExport(String),
    #[error("Export '{name}' has invalid type: expected {expected}, got {got}")]
    InvalidExportType {
        name: String,
        expected: &'static str,
        got: String,
    },
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

struct FuncExportSpec {
    name: &'static str,
    expected: &'static str,
    params: &'static [ValType],
    results: &'static [ValType],
}

const REQUIRED_FUNCS: &[FuncExportSpec] = &[
    FuncExportSpec {
        name: "datalake_abi_version",
        expected: "() -> (i32)",
        params: &[],
        results: &[ValType::I32],
    },
    FuncExportSpec {
        name: "datalake_alloc",
        expected: "(i32) -> (i32)",
        params: &[ValType::I32],
        results: &[ValType::I32],
    },
    FuncExportSpec {
        name: "datalake_dealloc",
        expected: "(i32, i32) -> ()",
        params: &[ValType::I32, ValType::I32],
        results: &[],
    },
    FuncExportSpec {
        name: "datalake_init",
        expected: "(i32, i32) -> (i32)",
        params: &[ValType::I32, ValType::I32],
        results: &[ValType::I32],
    },
    FuncExportSpec {
        name: "datalake_transform",
        expected: "(i32, i32) -> (i32)",
        params: &[ValType::I32, ValType::I32],
        results: &[ValType::I32],
    },
];

/// Validates raw WASM bytes against the C-ABI v1 specification.
///
/// # Errors
///
/// Returns [`ValidationError`] if:
/// - The bytecode is invalid WebAssembly ([`ValidationError::InvalidWasm`]).
/// - Any required export is missing from the module interface ([`ValidationError::MissingExport`]).
/// - Any required export has an invalid type or signature ([`ValidationError::InvalidExportType`]).
/// - The wasmtime engine fails to initialize or configure fuel ([`ValidationError::EngineInit`]).
/// - The module cannot be instantiated ([`ValidationError::InstantiationFailed`]).
/// - Calling `datalake_abi_version` fails ([`ValidationError::AbiFunctionMissing`] or [`ValidationError::AbiTrap`]).
/// - The module's ABI version does not match the expected version ([`ValidationError::AbiMismatch`]).
pub fn validate_wasm_bytes(bytes: &[u8]) -> std::result::Result<(), ValidationError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config).map_err(ValidationError::EngineInit)?;

    let module = Module::new(&engine, bytes).map_err(ValidationError::InvalidWasm)?;

    for spec in REQUIRED_FUNCS {
        match module.get_export(spec.name) {
            None => return Err(ValidationError::MissingExport((*spec.name).to_string())),
            Some(ExternType::Func(func_type)) => {
                let params_match = func_type.params().len() == spec.params.len()
                    && func_type
                        .params()
                        .zip(spec.params.iter())
                        .all(|(a, b)| ValType::eq(&a, b));
                let results_match = func_type.results().len() == spec.results.len()
                    && func_type
                        .results()
                        .zip(spec.results.iter())
                        .all(|(a, b)| ValType::eq(&a, b));
                if !params_match || !results_match {
                    return Err(ValidationError::InvalidExportType {
                        name: (*spec.name).to_string(),
                        expected: spec.expected,
                        got: format!("{func_type:?}"),
                    });
                }
            }
            Some(other) => {
                return Err(ValidationError::InvalidExportType {
                    name: (*spec.name).to_string(),
                    expected: spec.expected,
                    got: format!("{other:?}"),
                });
            }
        }
    }

    match module.get_export("memory") {
        None => return Err(ValidationError::MissingExport("memory".to_string())),
        Some(ExternType::Memory(_)) => {}
        Some(other) => {
            return Err(ValidationError::InvalidExportType {
                name: "memory".to_string(),
                expected: "memory",
                got: format!("{other:?}"),
            });
        }
    }

    let mut store: Store<()> = Store::new(&engine, ());
    store
        .set_fuel(100_000)
        .map_err(ValidationError::EngineInit)?;

    let linker =
        crate::create_default_linker(&engine, &module).map_err(ValidationError::EngineInit)?;

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(ValidationError::InstantiationFailed)?;
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
