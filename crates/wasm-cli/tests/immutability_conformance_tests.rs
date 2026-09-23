#![allow(clippy::unwrap_used, clippy::pedantic)]

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datalake_wasm_tool::bench::run_benchmark_with_disclaimer;
use datalake_wasm_tool::tester::{run_immutability_suite, verify_batch_immutability};
use std::sync::Arc;

#[test]
fn test_verify_batch_immutability_detects_tampered_trace_id() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_123"]))],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["trace_TAMPERED"]))],
    )
    .unwrap();
    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Value mismatch in immutable column trace_id")
    );
}

#[test]
fn test_verify_batch_immutability_detects_missing_column() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![Field::new(
        "span_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["span_456"])),
        ],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        out_schema,
        vec![Arc::new(StringArray::from(vec!["span_456"]))],
    )
    .unwrap();
    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Immutable column 'trace_id' present in input but missing from output")
    );
}

#[test]
fn test_verify_batch_immutability_passes_for_identical_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["span_456"])),
            Arc::new(Int64Array::from(vec![1_700_000_000_000_i64])),
        ],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["span_456"])),
            Arc::new(Int64Array::from(vec![1_700_000_000_000_i64])),
        ],
    )
    .unwrap();
    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_ok());
}

#[test]
fn test_verify_batch_immutability_allows_mutable_column_modification() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("custom_attr", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("custom_attr", DataType::Utf8, true),
        Field::new("new_field", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["original_value"])),
        ],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["modified_value"])),
            Arc::new(Int64Array::from(vec![42_i64])),
        ],
    )
    .unwrap();
    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_ok());
}

#[test]
fn test_verify_batch_immutability_when_immutable_column_not_in_input() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "other_field",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["value1"]))],
    )
    .unwrap();
    let output =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["value2"]))]).unwrap();
    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_ok());
}

#[test]
fn test_run_immutability_suite_conformant_module() {
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
    assert!(run_immutability_suite(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_with_disclaimer_conformant_module() {
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
    assert!(run_benchmark_with_disclaimer(&wasm).is_ok());
}

#[test]
fn test_verify_batch_immutability_typed_variants() {
    use datalake_wasm_tool::tester::TesterError;

    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
    ]));
    let out_schema_missing = Arc::new(Schema::new(vec![Field::new(
        "span_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        in_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["span_456"])),
        ],
    )
    .unwrap();
    let output_missing = RecordBatch::try_new(
        out_schema_missing,
        vec![Arc::new(StringArray::from(vec!["span_456"]))],
    )
    .unwrap();

    let err = verify_batch_immutability(&input, &output_missing).unwrap_err();
    assert_eq!(err, TesterError::MissingColumn("trace_id"));

    let output_tampered = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_TAMPERED"])),
            Arc::new(StringArray::from(vec!["span_456"])),
        ],
    )
    .unwrap();
    let err = verify_batch_immutability(&input, &output_tampered).unwrap_err();
    assert_eq!(err, TesterError::ValueMismatch("trace_id"));
}

#[test]
fn test_run_immutability_suite_rejects_module_failing_validation() {
    let res = run_immutability_suite(b"not a wasm module");
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Invalid WASM"));
}

#[test]
fn test_run_immutability_suite_detects_guest_error_status() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\03\00\00\00\00\00\00\00\00\00\00\00\14\40\00\00\0c\00\00\00custom error")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Guest transform failed: custom error"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_succeeds_with_returned_batch() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\00\00\00\00\01\00\00\00\14\40\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
            (i32.store (i32.const 16404) (local.get $ptr))
            (i32.store (i32.const 16408) (local.get $len))
            (i64.const 70368744177684)
        )
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_immutability_suite(&wasm).is_ok());
}

#[test]
fn test_run_immutability_suite_with_unknown_imports() {
    let wat_src = r#"(module
        (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
        (import "custom_ext" "external_lookup" (func $ext (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 16384) "\00\00\00\00\01\00\00\00\14\40\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
            (i32.store (i32.const 16404) (local.get $ptr))
            (i32.store (i32.const 16408) (local.get $len))
            (i64.const 70368744177684)
        )
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_immutability_suite(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_with_unknown_imports() {
    let wat_src = r#"(module
        (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_benchmark_with_disclaimer(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_rejects_guest_error_status() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\03\00\00\00\00\00\00\00\00\00\00\00\14\40\00\00\0c\00\00\00bench error")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_benchmark_with_disclaimer(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("bench error"), "actual err: {err}");
}

#[test]
fn test_run_immutability_suite_accepts_discard_status() {
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
    assert!(run_immutability_suite(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_accepts_discard_status() {
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
    assert!(run_benchmark_with_disclaimer(&wasm).is_ok());
}

#[test]
fn test_run_immutability_suite_rejects_null_response_pointer() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("null response header pointer"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_benchmark_rejects_null_response_pointer() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 0))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_benchmark_with_disclaimer(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("null response header pointer"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_short_response_header() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177674))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("invalid header length"), "actual err: {err}");
}

#[test]
fn test_run_immutability_suite_with_metrics_signal() {
    use datalake_wasm_tool::tester::run_immutability_suite_with_options;

    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
            ;; Verify signal is 1 (metrics)
            (if (i32.ne (local.get $sig) (i32.const 1))
                (then (unreachable))
            )
            (i64.const 70368744177684)
        )
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_immutability_suite_with_options(&wasm, 1, None).is_ok());
}

#[test]
fn test_run_immutability_suite_with_config_payload() {
    use datalake_wasm_tool::tester::run_immutability_suite_with_options;

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
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    // With config -> succeeds
    assert!(run_immutability_suite_with_options(&wasm, 0, Some(r#"{"key":"value"}"#)).is_ok());
    // Without config -> init returns 1, suite fails
    assert!(run_immutability_suite_with_options(&wasm, 0, None).is_err());
}

#[test]
fn test_run_benchmark_with_options() {
    use datalake_wasm_tool::bench::run_benchmark_with_options;

    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
            ;; Verify signal is 2 (traces)
            (if (i32.ne (local.get $sig) (i32.const 2))
                (then (unreachable))
            )
            (i64.const 70368744177684)
        )
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_benchmark_with_options(&wasm, 2, Some(r#"{"signal":"traces"}"#)).is_ok());
}

#[test]
fn test_run_immutability_suite_rejects_oversized_response_header() {
    // 16384 << 32 | 24 = 70368744177688
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177688))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("expected exactly 20 bytes, got 24"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_null_alloc_for_config() {
    use datalake_wasm_tool::tester::run_immutability_suite_with_options;

    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite_with_options(&wasm, 0, Some(r#"{"test":1}"#));
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("datalake_alloc returned null pointer when allocating config buffer"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_null_alloc_for_input() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("datalake_alloc returned null pointer when allocating input buffer"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_benchmark_rejects_null_alloc() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_benchmark_with_disclaimer(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("datalake_alloc returned null pointer when allocating input buffer"),
        "actual err: {err}"
    );
}

#[test]
fn test_canonical_batches_match_production_signal_schemas() {
    use arrow::datatypes::TimeUnit;
    use datalake_wasm_tool::tester::build_canonical_test_batch_for_signal;

    // Signal 0: Logs
    let logs_batch = build_canonical_test_batch_for_signal(0).unwrap();
    let logs_schema = logs_batch.schema();
    assert_eq!(logs_batch.num_rows(), 1);
    assert!(logs_schema.field_with_name("body").is_ok());
    assert!(logs_schema.field_with_name("trace_id").is_ok());
    assert!(logs_schema.field_with_name("span_id").is_ok());
    assert_eq!(
        logs_schema
            .field_with_name("timestamp")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );

    // Signal 1: Metrics
    let metrics_batch = build_canonical_test_batch_for_signal(1).unwrap();
    let metrics_schema = metrics_batch.schema();
    assert_eq!(metrics_batch.num_rows(), 1);
    assert!(metrics_schema.field_with_name("name").is_ok());
    assert!(metrics_schema.field_with_name("description").is_ok());
    assert!(metrics_schema.field_with_name("unit").is_ok());
    assert_eq!(
        metrics_schema.field_with_name("value").unwrap().data_type(),
        &DataType::Float64
    );
    assert_eq!(
        metrics_schema
            .field_with_name("timestamp")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );

    // Signal 2: Traces
    let traces_batch = build_canonical_test_batch_for_signal(2).unwrap();
    let traces_schema = traces_batch.schema();
    assert_eq!(traces_batch.num_rows(), 1);
    assert!(traces_schema.field_with_name("trace_id").is_ok());
    assert!(traces_schema.field_with_name("span_id").is_ok());
    assert!(traces_schema.field_with_name("status_code").is_ok());
    assert_eq!(
        traces_schema.field_with_name("kind").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        traces_schema
            .field_with_name("timestamp")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(
        traces_schema
            .field_with_name("end_time")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
}

#[test]
fn test_run_immutability_suite_rejects_discard_with_payload() {
    // 20-byte header with status = 1 (STATUS_DISCARD), but batch_count = 1 (invalid!)
    // Status 1: \01\00\00\00, batch_count 1: \01\00\00\00
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Discard response header must have zero batches and no message payload"),
        "actual err: {err}"
    );
}

#[test]
fn test_verify_batch_immutability_allows_filtered_empty_batch() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1"]))],
    )
    .unwrap();
    let empty_output = RecordBatch::new_empty(schema);

    assert_eq!(empty_output.num_rows(), 0);
    assert!(verify_batch_immutability(&input, &empty_output).is_ok());
}

#[test]
fn test_verify_batch_immutability_allows_filtered_subset_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1", "trace_2"]))],
    )
    .unwrap();
    let filtered_output =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["trace_2"]))]).unwrap();

    assert_eq!(filtered_output.num_rows(), 1);
    assert!(verify_batch_immutability(&input, &filtered_output).is_ok());
}

#[test]
fn test_verify_batch_immutability_allows_split_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1", "trace_2"]))],
    )
    .unwrap();

    let batch_1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1"]))],
    )
    .unwrap();
    let batch_2 =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["trace_2"]))]).unwrap();

    assert!(verify_batch_immutability(&input, &batch_1).is_ok());
    assert!(verify_batch_immutability(&input, &batch_2).is_ok());
}

#[test]
fn test_verify_batch_immutability_rejects_tampered_value_in_subset() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1", "trace_2"]))],
    )
    .unwrap();
    let tampered_output = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["trace_mutated"]))],
    )
    .unwrap();

    let res = verify_batch_immutability(&input, &tampered_output);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Value mismatch in immutable column trace_id")
    );
}

#[test]
fn test_run_immutability_suite_rejects_success_with_zero_count_nonzero_batches_ptr() {
    // 20-byte header: status = 0 (STATUS_SUCCESS), batch_count = 0, batches_ptr = 1000 (\e8\03\00\00)
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\00\00\00\00\00\00\00\00\e8\03\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Success response header has inconsistent batch fields"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_success_with_nonzero_count_zero_batches_ptr() {
    // 20-byte header: status = 0 (STATUS_SUCCESS), batch_count = 1, batches_ptr = 0
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\00\00\00\00\01\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Success response header has inconsistent batch fields"),
        "actual err: {err}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_success_with_message_payload() {
    // 20-byte header: status = 0 (STATUS_SUCCESS), batch_count = 0, batches_ptr = 0, message_ptr = 500, message_len = 5
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\00\00\00\00\00\00\00\00\00\00\00\00\f4\01\00\00\05\00\00\00")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64) (i64.const 70368744177684))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Success response header must have no message payload"),
        "actual err: {err}"
    );
}

#[test]
fn test_verify_batch_immutability_allows_reordered_rows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["trace_1", "trace_2"])),
            Arc::new(StringArray::from(vec!["body_1", "body_2"])),
        ],
    )
    .unwrap();

    // Reordered rows: [trace_2, trace_1] with modified body values
    let reordered_output = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_2", "trace_1"])),
            Arc::new(StringArray::from(vec![
                "body_2_modified",
                "body_1_modified",
            ])),
        ],
    )
    .unwrap();

    assert_eq!(reordered_output.num_rows(), 2);
    assert!(verify_batch_immutability(&input, &reordered_output).is_ok());
}

#[test]
fn test_verify_batch_immutability_rejects_multiplicity_violation() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["trace_1", "trace_2"]))],
    )
    .unwrap();

    // Output duplicates trace_1 and drops trace_2 (same length = 2 rows, but violated multiplicity)
    let duplicate_output = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["trace_1", "trace_1"]))],
    )
    .unwrap();

    let res = verify_batch_immutability(&input, &duplicate_output);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Value mismatch in immutable column trace_id")
    );
}
