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
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_immutability_suite(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_with_disclaimer_conformant_module() {
    let wat_src = r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        (memory (export "memory") 1)
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
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\14\40\00\00\0c\00\00\00custom error")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 16384))
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
        (func (export "datalake_transform") (param $ptr i32) (param $len i32) (result i32)
            (i32.store (i32.const 16404) (local.get $ptr))
            (i32.store (i32.const 16408) (local.get $len))
            (i32.const 16384)
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
        (func (export "datalake_transform") (param $ptr i32) (param $len i32) (result i32)
            (i32.store (i32.const 16404) (local.get $ptr))
            (i32.store (i32.const 16408) (local.get $len))
            (i32.const 16384)
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
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    assert!(run_benchmark_with_disclaimer(&wasm).is_ok());
}

#[test]
fn test_run_benchmark_rejects_guest_error_status() {
    let wat_src = r#"(module
        (memory (export "memory") 1)
        (data (i32.const 16384) "\01\00\00\00\00\00\00\00\00\00\00\00\14\40\00\00\0c\00\00\00bench error")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 16384))
    )"#;
    let wasm = wat::parse_str(wat_src).unwrap();
    let res = run_benchmark_with_disclaimer(&wasm);
    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(err.contains("bench error"), "actual err: {err}");
}
