#![allow(clippy::unwrap_used, clippy::pedantic)]

use datalake_wasm_tool::validator::validate_wasm_bytes;

#[test]
fn test_validate_rejects_empty_module_missing_all_exports() {
    let invalid_wasm = wat::parse_str("(module)").unwrap();
    let res = validate_wasm_bytes(&invalid_wasm);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Missing export 'datalake_abi_version'")
    );
}

#[test]
fn test_validate_rejects_invalid_wasm_bytes() {
    let res = validate_wasm_bytes(b"not a wasm binary");
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Invalid WASM:"));
}

#[test]
fn test_validate_rejects_missing_alloc_export() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Missing export 'datalake_alloc'")
    );
}

#[test]
fn test_validate_rejects_abi_version_mismatch() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 2))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("ABI version mismatch: expected 1, got 2")
    );
}

#[test]
fn test_validate_accepts_conformant_module() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(validate_wasm_bytes(&wasm).is_ok());
}

#[test]
fn test_cli_subcommand_test_returns_not_yet_implemented() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let dummy_file = format!("{manifest_dir}/Cargo.toml");

    let output = std::process::Command::new(bin)
        .args(["test", &dummy_file])
        .output()
        .expect("Failed to execute datalake-wasm process");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Not yet implemented"),
        "Expected stderr to contain 'Not yet implemented', got: {stderr}"
    );
}

#[test]
fn test_cli_subcommand_bench_returns_not_yet_implemented() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let dummy_file = format!("{manifest_dir}/Cargo.toml");

    let output = std::process::Command::new(bin)
        .args(["bench", &dummy_file])
        .output()
        .expect("Failed to execute datalake-wasm process");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Not yet implemented"),
        "Expected stderr to contain 'Not yet implemented', got: {stderr}"
    );
}
