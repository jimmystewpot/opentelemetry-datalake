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
    validate_wasm_bytes(&wasm).unwrap();
}

#[test]
fn test_validate_accepts_module_without_optional_datalake_init() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    validate_wasm_bytes(&wasm).unwrap();
}

#[test]
fn test_validate_rejects_non_memory_export_for_memory() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (func (export "memory"))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Invalid export 'memory'")
    );
}

#[test]
fn test_validate_rejects_invalid_function_signature() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i64) (i64.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = validate_wasm_bytes(&wasm);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Invalid function signature for 'datalake_abi_version'")
    );
}

fn valid_echo_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 4)
        (global $heap (mut i32) (i32.const 1024))
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param $size i32) (result i32)
            (local $old i32)
            (local.set $old (global.get $heap))
            (global.set $heap (i32.add (global.get $heap) (local.get $size)))
            (local.get $old)
        )
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            ;; Header at 64: status=0, batch_count=1, batches_ptr=96
            (i32.store (i32.const 64) (i32.const 0))
            (i32.store (i32.const 68) (i32.const 1))
            (i32.store (i32.const 72) (i32.const 96))
            (i32.store (i32.const 76) (i32.const 0))
            (i32.store (i32.const 80) (i32.const 0))
            ;; BatchDescriptor at 96: ptr=$ptr, len=$len
            (i32.store (i32.const 96) (local.get $ptr))
            (i32.store (i32.const 100) (local.get $len))
            ;; Return (0 << 32) | 20
            (i64.const 274877906964) ;; (64 << 32) | 20
        )
    )"#
}

fn tampered_trace_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 4)
        (global $heap (mut i32) (i32.const 1024))
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param $size i32) (result i32)
            (local $old i32)
            (local.set $old (global.get $heap))
            (global.set $heap (i32.add (global.get $heap) (local.get $size)))
            (local.get $old)
        )
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            (local $i i32)
            (local.set $i (local.get $ptr))
            ;; Find first byte equal to '0' (48) in input buffer and replace with '9' (57)
            (block $found
                (loop $search
                    (br_if $found (i32.eq (i32.load8_u (local.get $i)) (i32.const 48)))
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br_if $search (i32.lt_u (local.get $i) (i32.add (local.get $ptr) (local.get $len))))
                )
            )
            (i32.store8 (local.get $i) (i32.const 57))

            ;; Header at 64: status=0, batch_count=1, batches_ptr=96
            (i32.store (i32.const 64) (i32.const 0))
            (i32.store (i32.const 68) (i32.const 1))
            (i32.store (i32.const 72) (i32.const 96))
            (i32.store (i32.const 76) (i32.const 0))
            (i32.store (i32.const 80) (i32.const 0))
            ;; BatchDescriptor at 96: ptr=$ptr, len=$len
            (i32.store (i32.const 96) (local.get $ptr))
            (i32.store (i32.const 100) (local.get $len))
            (i64.const 274877906964) ;; (64 << 32) | 20
        )
    )"#
}

#[test]
fn test_cli_subcommand_test_succeeds_on_valid_module() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wasm = wat::parse_str(valid_echo_wat()).unwrap();
    let temp_path = std::env::temp_dir().join("valid_echo.wasm");
    std::fs::write(&temp_path, wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args(["test", temp_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(temp_path);
    assert!(output.status.success(), "Expected exit 0, got: {output:?}");
}

#[test]
fn test_cli_subcommand_test_fails_on_tampered_module() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wasm = wat::parse_str(tampered_trace_wat()).unwrap();
    let temp_path = std::env::temp_dir().join("tampered_trace.wasm");
    std::fs::write(&temp_path, wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args(["test", temp_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(temp_path);
    assert!(
        !output.status.success(),
        "Expected failure for tampered module"
    );
}

#[test]
fn test_cli_subcommand_bench_succeeds_on_valid_module() {
    let bin = env!("CARGO_BIN_EXE_datalake-wasm");
    let wasm = wat::parse_str(valid_echo_wat()).unwrap();
    let temp_path = std::env::temp_dir().join("bench_echo.wasm");
    std::fs::write(&temp_path, wasm).unwrap();

    let output = std::process::Command::new(bin)
        .args(["bench", temp_path.to_str().unwrap()])
        .output()
        .expect("Failed to execute datalake-wasm process");

    let _ = std::fs::remove_file(temp_path);
    assert!(output.status.success(), "Expected exit 0, got: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("WARNING: This bench measures raw IPC round-trip"));
    assert!(stdout.contains("Throughput"));
}

#[test]
fn test_validate_rejects_v0_signature_if_abi_is_v1() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = datalake_wasm_tool::validator::validate_wasm_bytes(&wasm);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("expected (i32, i32, i32) -> i64"));
}
