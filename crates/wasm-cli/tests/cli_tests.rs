#![allow(clippy::unwrap_used, clippy::pedantic)]

use datalake_wasm_tool::validator::{ValidationError, validate_wasm_bytes};

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
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
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
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
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
fn test_validate_rejects_memory_exported_as_function() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (func (export "memory"))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    match res {
        Err(ValidationError::InvalidExportType { name, .. }) => {
            assert_eq!(name, "memory");
        }
        other => panic!("expected InvalidExportType for memory, got {other:?}"),
    }
}

#[test]
fn test_validate_rejects_invalid_alloc_signature() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    match res {
        Err(ValidationError::InvalidExportType { name, .. }) => {
            assert_eq!(name, "datalake_alloc");
        }
        other => panic!("expected InvalidExportType for datalake_alloc, got {other:?}"),
    }
}

#[test]
fn test_validate_rejects_export_defined_as_global() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (global (export "datalake_transform") i32 (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    match res {
        Err(ValidationError::InvalidExportType { name, .. }) => {
            assert_eq!(name, "datalake_transform");
        }
        other => panic!("expected InvalidExportType for datalake_transform, got {other:?}"),
    }
}

#[test]
fn test_validate_accepts_module_with_host_sdk_imports() {
    let wat_src = r#"(module
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(validate_wasm_bytes(&wasm).is_ok());
}

#[test]
fn test_validate_accepts_conformant_module() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(validate_wasm_bytes(&wasm).is_ok());
}

#[test]
fn test_validate_accepts_module_with_wasi_or_unknown_imports() {
    let wat_src = r#"(module
        (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
        (import "env" "custom_unused_external" (func $custom_ext (param i32) (result i32)))
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(validate_wasm_bytes(&wasm).is_ok());
}

#[test]
fn test_cli_subcommand_test_conformant_module() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let temp_path = std::env::temp_dir().join(format!(
        "datalake_conformant_test_{}.wasm",
        std::process::id()
    ));
    std::fs::write(&temp_path, &wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args(["test", temp_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(&temp_path);
    assert!(
        output.status.success(),
        "Expected datalake-wasm test to succeed, got stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_cli_subcommand_bench_conformant_module() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let temp_path = std::env::temp_dir().join(format!(
        "datalake_conformant_bench_{}.wasm",
        std::process::id()
    ));
    std::fs::write(&temp_path, &wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args(["bench", temp_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(&temp_path);
    assert!(
        output.status.success(),
        "Expected datalake-wasm bench to succeed, got stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_cli_subcommand_test_with_signal_and_config() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
            ;; Verify config is non-empty
            (if (i32.eqz (local.get $len))
                (then (return (i32.const 1)))
            )
            (i32.const 0)
        )
        (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
            ;; Verify signal is 1 (metrics)
            (if (i32.ne (local.get $sig) (i32.const 1))
                (then (unreachable))
            )
            (i64.const 70368744177684)
        )
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let temp_path =
        std::env::temp_dir().join(format!("datalake_signal_test_{}.wasm", std::process::id()));
    std::fs::write(&temp_path, &wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args([
            "test",
            temp_path.to_str().unwrap(),
            "--signal",
            "metrics",
            "--config",
            r#"{"signal":"metrics","custom":123}"#,
        ])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(&temp_path);
    assert!(
        output.status.success(),
        "Expected datalake-wasm test with signal/config to succeed, got stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
