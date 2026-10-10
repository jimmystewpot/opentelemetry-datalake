//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use thiserror::Error;
use wasmtime::{Config, Engine, Instance, Module, Store, ValType};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Invalid WASM: {0}")]
    InvalidWasm(#[source] wasmtime::Error),
    #[error("Missing export '{0}'")]
    MissingExport(String),
    #[error("Invalid export '{name}': expected {expected}, found {found}")]
    InvalidExportKind {
        name: String,
        expected: String,
        found: String,
    },
    #[error("Invalid function signature for '{name}': expected {expected}")]
    InvalidSignature { name: String, expected: String },
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
/// - An export has the wrong export kind or invalid function signature ([`ValidationError::InvalidExportKind`], [`ValidationError::InvalidSignature`]).
/// - The wasmtime engine fails to initialize or configure fuel ([`ValidationError::EngineInit`]).
/// - The module cannot be instantiated ([`ValidationError::InstantiationFailed`]).
/// - Calling `datalake_abi_version` fails ([`ValidationError::AbiFunctionMissing`] or [`ValidationError::AbiTrap`]).
/// - The module's ABI version does not match the expected version ([`ValidationError::AbiMismatch`]).
#[allow(clippy::collapsible_match)]
fn validate_function_signature(
    name: &str,
    func: &wasmtime::FuncType,
) -> std::result::Result<(), ValidationError> {
    let params: Vec<ValType> = func.params().collect();
    let results: Vec<ValType> = func.results().collect();
    match name {
        "datalake_abi_version" => {
            if !params.is_empty() || results.len() != 1 || !matches!(results[0], ValType::I32) {
                return Err(ValidationError::InvalidSignature {
                    name: name.to_string(),
                    expected: "() -> i32".to_string(),
                });
            }
        }
        "datalake_alloc" => {
            if params.len() != 1
                || !matches!(params[0], ValType::I32)
                || results.len() != 1
                || !matches!(results[0], ValType::I32)
            {
                return Err(ValidationError::InvalidSignature {
                    name: name.to_string(),
                    expected: "(i32) -> i32".to_string(),
                });
            }
        }
        "datalake_dealloc" => {
            if params.len() != 2
                || !matches!(params[0], ValType::I32)
                || !matches!(params[1], ValType::I32)
                || !results.is_empty()
            {
                return Err(ValidationError::InvalidSignature {
                    name: name.to_string(),
                    expected: "(i32, i32) -> ()".to_string(),
                });
            }
        }
        "datalake_transform" => {
            let is_v1 = params.len() == 3
                && matches!(params[0], ValType::I32)
                && matches!(params[1], ValType::I32)
                && matches!(params[2], ValType::I32)
                && results.len() == 1
                && matches!(results[0], ValType::I64);
            let is_v0 = params.len() == 2
                && matches!(params[0], ValType::I32)
                && matches!(params[1], ValType::I32)
                && results.len() == 1
                && (matches!(results[0], ValType::I32) || matches!(results[0], ValType::I64));
            if !is_v1 && !is_v0 {
                return Err(ValidationError::InvalidSignature {
                    name: name.to_string(),
                    expected: "(i32, i32, i32) -> i64 or (i32, i32) -> i32/i64".to_string(),
                });
            }
        }
        "datalake_init" => {
            if params.len() != 2
                || !matches!(params[0], ValType::I32)
                || !matches!(params[1], ValType::I32)
                || results.len() != 1
                || !matches!(results[0], ValType::I32)
            {
                return Err(ValidationError::InvalidSignature {
                    name: name.to_string(),
                    expected: "(i32, i32) -> i32".to_string(),
                });
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_export(export: &wasmtime::ExportType) -> std::result::Result<(), ValidationError> {
    let name = export.name();
    let export_ty = export.ty();
    if name == "memory" {
        if export_ty.memory().is_none() {
            return Err(ValidationError::InvalidExportKind {
                name: "memory".to_string(),
                expected: "memory".to_string(),
                found: "non-memory".to_string(),
            });
        }
        return Ok(());
    }

    if matches!(
        name,
        "datalake_abi_version"
            | "datalake_alloc"
            | "datalake_dealloc"
            | "datalake_transform"
            | "datalake_init"
    ) {
        let Some(func) = export_ty.func() else {
            return Err(ValidationError::InvalidExportKind {
                name: name.to_string(),
                expected: "function".to_string(),
                found: "non-function".to_string(),
            });
        };
        validate_function_signature(name, func)?;
    }
    Ok(())
}

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

    for export in module.exports() {
        validate_export(&export)?;
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
                assert_eq!(err.to_string(), "Missing export 'datalake_abi_version'");
            }
            other => panic!("expected MissingExport, got {other:?}"),
        }
    }

    #[test]
    fn test_valid_module_v1_and_init() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
                (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        assert!(validate_wasm_bytes(&wasm).is_ok());
    }

    #[test]
    fn test_valid_module_v0_signatures() {
        let wat_v0_i32 = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_v0_i32).expect("valid wat");
        assert!(validate_wasm_bytes(&wasm).is_ok());

        let wat_v0_i64 = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_v0_i64).expect("valid wat");
        assert!(validate_wasm_bytes(&wasm).is_ok());
    }

    #[test]
    fn test_invalid_export_kind_memory() {
        let wat_src = r#"
            (module
                (func (export "memory"))
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::InvalidExportKind {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "memory");
                assert_eq!(expected, "memory");
                assert_eq!(found, "non-memory");
                assert!(err.to_string().contains("Invalid export 'memory'"));
            }
            other => panic!("expected InvalidExportKind, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_export_kind_function() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (global (export "datalake_alloc") i32 (i32.const 0))
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::InvalidExportKind {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "datalake_alloc");
                assert_eq!(expected, "function");
                assert_eq!(found, "non-function");
            }
            other => panic!("expected InvalidExportKind, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_abi_version() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i64) (i64.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_abi_version");
                assert_eq!(expected, "() -> i32");
                assert!(
                    err.to_string()
                        .contains("Invalid function signature for 'datalake_abi_version'")
                );
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_alloc() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        match res.unwrap_err() {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_alloc");
                assert_eq!(expected, "(i32) -> i32");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_dealloc() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32) (result i32) (i32.const 0))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        match res.unwrap_err() {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_dealloc");
                assert_eq!(expected, "(i32, i32) -> ()");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_transform() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        match res.unwrap_err() {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_transform");
                assert_eq!(expected, "(i32, i32, i32) -> i64 or (i32, i32) -> i32/i64");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_init() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
                (func (export "datalake_init") (param i32))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        match res.unwrap_err() {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_init");
                assert_eq!(expected, "(i32, i32) -> i32");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_abi_mismatch() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 99))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::AbiMismatch { expected, got } => {
                assert_eq!(*expected, 1);
                assert_eq!(*got, 99);
                assert_eq!(err.to_string(), "ABI version mismatch: expected 1, got 99");
            }
            other => panic!("expected AbiMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_abi_trap() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (unreachable))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::AbiTrap(_) => {
                assert!(err.to_string().contains("datalake_abi_version trap:"));
            }
            other => panic!("expected AbiTrap, got {other:?}"),
        }
    }

    #[test]
    fn test_instantiation_failed() {
        let wat_src = r#"
            (module
                (import "env" "missing_host_fn" (func))
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) (i32.const 1))
                (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let res = validate_wasm_bytes(&wasm);
        assert!(res.is_err());
        let err = res.unwrap_err();
        match &err {
            ValidationError::InstantiationFailed(_) => {
                assert!(err.to_string().contains("Instantiation failed:"));
            }
            other => panic!("expected InstantiationFailed, got {other:?}"),
        }
    }
}
