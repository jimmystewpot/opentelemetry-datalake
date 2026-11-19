//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use thiserror::Error;
use wasmtime::{Config, Engine, Module, Store, ValType};

/// Errors encountered when validating a compiled WASM guest module against C-ABI v1.
#[derive(Error, Debug)]
pub enum ValidationError {
    /// The input bytes could not be compiled as a valid WebAssembly module.
    #[error("Invalid WASM: {0}")]
    InvalidWasm(#[source] wasmtime::Error),
    /// A required C-ABI v1 symbol export was not found in the module.
    #[error("Missing export '{0}'")]
    MissingExport(String),
    /// An exported symbol has the wrong kind (e.g., global instead of function or memory).
    #[error("Invalid export '{name}': expected {expected}, found {found}")]
    InvalidExportKind {
        /// The name of the exported symbol.
        name: String,
        /// The expected export kind description.
        expected: String,
        /// The actual export kind description found in the module.
        found: String,
    },
    /// An exported function does not match its required C-ABI v1 parameter or result signature.
    #[error("Invalid function signature for '{name}': expected {expected}")]
    InvalidSignature {
        /// The name of the exported function.
        name: String,
        /// The expected function signature description.
        expected: String,
    },
    /// The module could not be instantiated in an isolated Wasmtime store.
    #[error("Instantiation failed: {0}")]
    InstantiationFailed(#[source] wasmtime::Error),
    /// The `datalake_abi_version` typed function export could not be resolved.
    #[error("Cannot call datalake_abi_version: {0}")]
    AbiFunctionMissing(#[source] wasmtime::Error),
    /// Calling `datalake_abi_version` resulted in a WebAssembly trap.
    #[error("datalake_abi_version trap: {0}")]
    AbiTrap(#[source] wasmtime::Error),
    /// The version returned by `datalake_abi_version` does not match the supported host ABI.
    #[error("ABI version mismatch: expected {expected}, got {got}")]
    AbiMismatch {
        /// The ABI version expected by the host validator.
        expected: u32,
        /// The ABI version reported by the guest module.
        got: u32,
    },
    /// The Wasmtime engine failed to initialize with the validator configuration.
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

/// Validates the parameter and result signature of a recognized C-ABI v1 function export.
fn validate_function_signature(
    name: &str,
    func: &wasmtime::FuncType,
) -> std::result::Result<(), ValidationError> {
    let params: Vec<ValType> = func.params().collect();
    let results: Vec<ValType> = func.results().collect();
    let (is_valid, expected) = match name {
        "datalake_abi_version" => (
            params.is_empty() && results.len() == 1 && matches!(results[0], ValType::I32),
            "() -> i32",
        ),
        "datalake_alloc" => (
            params.len() == 1
                && matches!(params[0], ValType::I32)
                && results.len() == 1
                && matches!(results[0], ValType::I32),
            "(i32) -> i32",
        ),
        "datalake_dealloc" => (
            params.len() == 2
                && matches!(params[0], ValType::I32)
                && matches!(params[1], ValType::I32)
                && results.is_empty(),
            "(i32, i32) -> ()",
        ),
        "datalake_transform" => (
            params.len() == 3
                && matches!(params[0], ValType::I32)
                && matches!(params[1], ValType::I32)
                && matches!(params[2], ValType::I32)
                && results.len() == 1
                && matches!(results[0], ValType::I64),
            "(i32, i32, i32) -> i64",
        ),
        "datalake_init" => (
            params.len() == 2
                && matches!(params[0], ValType::I32)
                && matches!(params[1], ValType::I32)
                && results.len() == 1
                && matches!(results[0], ValType::I32),
            "(i32, i32) -> i32",
        ),
        _ => return Ok(()),
    };
    if !is_valid {
        return Err(ValidationError::InvalidSignature {
            name: name.to_string(),
            expected: expected.to_string(),
        });
    }
    Ok(())
}

/// Validates that a single module export satisfies the required kind and signature for C-ABI v1.
fn validate_export(export: &wasmtime::ExportType) -> std::result::Result<(), ValidationError> {
    let name = export.name();
    let export_ty = export.ty();
    match name {
        "memory" => {
            if export_ty.memory().is_none() {
                return Err(ValidationError::InvalidExportKind {
                    name: "memory".to_string(),
                    expected: "memory".to_string(),
                    found: "non-memory".to_string(),
                });
            }
            Ok(())
        }
        "datalake_abi_version"
        | "datalake_alloc"
        | "datalake_dealloc"
        | "datalake_transform"
        | "datalake_init" => {
            let Some(func) = export_ty.func() else {
                return Err(ValidationError::InvalidExportKind {
                    name: name.to_string(),
                    expected: "function".to_string(),
                    found: "non-function".to_string(),
                });
            };
            validate_function_signature(name, func)
        }
        _ => Ok(()),
    }
}

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
    let mut store = Store::new(&engine, crate::default_wasi_ctx());
    store
        .set_fuel(100_000)
        .map_err(ValidationError::EngineInit)?;

    let linker = crate::build_cli_linker(&engine).map_err(ValidationError::EngineInit)?;
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

    #[test]
    fn test_valid_minimal_module() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        assert!(validate_wasm_bytes(&wasm).is_ok());
    }

    #[test]
    fn test_valid_module_with_init_and_extra_export() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
                (func (export "datalake_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "custom_extra_fn") (result i32) i32.const 42)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        assert!(validate_wasm_bytes(&wasm).is_ok());
    }

    #[test]
    fn test_invalid_export_kind_memory() {
        let wat_src = r#"
            (module
                (func (export "memory"))
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
            ValidationError::InvalidExportKind {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "memory");
                assert_eq!(expected, "memory");
                assert_eq!(found, "non-memory");
            }
            other => panic!("expected InvalidExportKind, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_export_kind_functions() {
        let targets = [
            "datalake_abi_version",
            "datalake_alloc",
            "datalake_dealloc",
            "datalake_transform",
            "datalake_init",
        ];
        for target in targets {
            let abi_export = if target == "datalake_abi_version" {
                r#"(global (export "datalake_abi_version") i32 (i32.const 1))"#
            } else {
                r#"(func (export "datalake_abi_version") (result i32) i32.const 1)"#
            };
            let alloc_export = if target == "datalake_alloc" {
                r#"(global (export "datalake_alloc") i32 (i32.const 0))"#
            } else {
                r#"(func (export "datalake_alloc") (param i32) (result i32) i32.const 0)"#
            };
            let dealloc_export = if target == "datalake_dealloc" {
                r#"(global (export "datalake_dealloc") i32 (i32.const 0))"#
            } else {
                r#"(func (export "datalake_dealloc") (param i32 i32))"#
            };
            let transform_export = if target == "datalake_transform" {
                r#"(global (export "datalake_transform") i32 (i32.const 0))"#
            } else {
                r#"(func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)"#
            };
            let init_export = if target == "datalake_init" {
                r#"(global (export "datalake_init") i32 (i32.const 0))"#
            } else {
                ""
            };
            let wat_src = format!(
                "(module (memory (export \"memory\") 1) {abi_export} {alloc_export} {dealloc_export} {transform_export} {init_export})"
            );
            let wasm = wat::parse_str(&wat_src).expect("valid wat");
            let err = validate_wasm_bytes(&wasm).unwrap_err();
            match err {
                ValidationError::InvalidExportKind {
                    name,
                    expected,
                    found,
                } => {
                    assert_eq!(name, target);
                    assert_eq!(expected, "function");
                    assert_eq!(found, "non-function");
                }
                other => panic!("expected InvalidExportKind for {target}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_invalid_signature_abi_version() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (param i32) (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_abi_version");
                assert_eq!(expected, "() -> i32");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_alloc() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
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
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
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
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
            ValidationError::InvalidSignature { name, expected } => {
                assert_eq!(name, "datalake_transform");
                assert_eq!(expected, "(i32, i32, i32) -> i64");
            }
            other => panic!("expected InvalidSignature, got {other:?}"),
        }
    }

    #[test]
    fn test_invalid_signature_init() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
                (func (export "datalake_init") (param i32) (result i32) i32.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
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
                (func (export "datalake_abi_version") (result i32) i32.const 99)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match err {
            ValidationError::AbiMismatch { expected, got } => {
                assert_eq!(expected, 1);
                assert_eq!(got, 99);
            }
            other => panic!("expected AbiMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_abi_trap() {
        let wat_src = r#"
            (module
                (memory (export "memory") 1)
                (func (export "datalake_abi_version") (result i32) unreachable)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match &err {
            ValidationError::AbiTrap(source) => {
                assert_wasmtime_error(source);
                assert!(err.source().is_some());
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
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 0)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let err = validate_wasm_bytes(&wasm).unwrap_err();
        match &err {
            ValidationError::InstantiationFailed(source) => {
                assert_wasmtime_error(source);
                assert!(err.source().is_some());
            }
            other => panic!("expected InstantiationFailed, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_function_signature_helper() {
        let engine = Engine::default();
        let wat_src = r#"
            (module
                (func (export "abi_ok") (result i32) i32.const 1)
                (func (export "alloc_ok") (param i32) (result i32) i32.const 0)
                (func (export "dealloc_ok") (param i32 i32))
                (func (export "transform_ok") (param i32 i32 i32) (result i64) i64.const 0)
                (func (export "init_ok") (param i32 i32) (result i32) i32.const 0)
                (func (export "bad_fn"))
            )
        "#;
        let wasm = wat::parse_str(wat_src).expect("valid wat");
        let module = Module::new(&engine, &wasm).expect("valid module");
        let find_func = |export_name: &str| {
            module
                .exports()
                .find(|e| e.name() == export_name)
                .and_then(|e| e.ty().func().cloned())
                .expect("func export exists")
        };

        assert!(validate_function_signature("datalake_abi_version", &find_func("abi_ok")).is_ok());
        assert!(validate_function_signature("datalake_alloc", &find_func("alloc_ok")).is_ok());
        assert!(validate_function_signature("datalake_dealloc", &find_func("dealloc_ok")).is_ok());
        assert!(
            validate_function_signature("datalake_transform", &find_func("transform_ok")).is_ok()
        );
        assert!(validate_function_signature("datalake_init", &find_func("init_ok")).is_ok());
        assert!(validate_function_signature("unknown_export", &find_func("bad_fn")).is_ok());

        let bad = find_func("bad_fn");
        for (name, expected_sig) in [
            ("datalake_abi_version", "() -> i32"),
            ("datalake_alloc", "(i32) -> i32"),
            ("datalake_dealloc", "(i32, i32) -> ()"),
            ("datalake_transform", "(i32, i32, i32) -> i64"),
            ("datalake_init", "(i32, i32) -> i32"),
        ] {
            let err = validate_function_signature(name, &bad).unwrap_err();
            match err {
                ValidationError::InvalidSignature {
                    name: err_name,
                    expected,
                } => {
                    assert_eq!(err_name, name);
                    assert_eq!(expected, expected_sig);
                }
                other => panic!("expected InvalidSignature for {name}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_validate_export_helper() {
        let engine = Engine::default();
        let valid_wasm = wat::parse_str(
            r#"
                (module
                    (memory (export "memory") 1)
                    (func (export "datalake_abi_version") (result i32) i32.const 1)
                    (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
                    (global (export "ignored_global") i32 (i32.const 7))
                )
            "#,
        )
        .expect("valid wat");
        let valid_module = Module::new(&engine, &valid_wasm).expect("valid module");
        for export in valid_module.exports() {
            assert!(validate_export(&export).is_ok());
        }

        let bad_mem_wasm =
            wat::parse_str(r#"(module (func (export "memory")))"#).expect("valid wat");
        let bad_mem_module = Module::new(&engine, &bad_mem_wasm).expect("valid module");
        let mem_export = bad_mem_module.exports().next().expect("export exists");
        match validate_export(&mem_export).unwrap_err() {
            ValidationError::InvalidExportKind {
                name,
                expected,
                found,
            } => {
                assert_eq!(name, "memory");
                assert_eq!(expected, "memory");
                assert_eq!(found, "non-memory");
            }
            other => panic!("expected InvalidExportKind, got {other:?}"),
        }

        let bad_fn_wasm =
            wat::parse_str(r#"(module (global (export "datalake_alloc") i32 (i32.const 0)))"#)
                .expect("valid wat");
        let bad_fn_module = Module::new(&engine, &bad_fn_wasm).expect("valid module");
        let fn_export = bad_fn_module.exports().next().expect("export exists");
        match validate_export(&fn_export).unwrap_err() {
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
}
