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
            ;; Header at 0: status=0, batch_count=1, batches_ptr=32
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 32))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            ;; BatchDescriptor at 32: ptr=$ptr, len=$len
            (i32.store (i32.const 32) (local.get $ptr))
            (i32.store (i32.const 36) (local.get $len))
            ;; Return (0 << 32) | 20
            (i64.const 20)
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

            ;; Header at 0: status=0, batch_count=1, batches_ptr=32
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 32))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            ;; BatchDescriptor at 32: ptr=$ptr, len=$len
            (i32.store (i32.const 32) (local.get $ptr))
            (i32.store (i32.const 36) (local.get $len))
            (i64.const 20)
        )
    )"#
}

fn infinite_loop_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64)
            (loop (br 0))
            (i64.const 0)
        )
    )"#
}

fn error_status_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64)
            (i32.store (i32.const 0) (i32.const 3)) ;; status = 3 (Error)
            (i32.store (i32.const 4) (i32.const 0))
            (i32.store (i32.const 8) (i32.const 0))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            (i64.const 20)
        )
    )"#
}

fn trapping_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64)
            (unreachable)
        )
    )"#
}

fn missing_column_wasm() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "span_id",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["span_123"]))],
    )
    .unwrap();
    let mut ipc_bytes = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut ipc_bytes, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    let hex_escaped: String = ipc_bytes.iter().map(|b| format!("\\{:02x}", b)).collect();

    let wat = format!(
        r#"(module
            (memory (export "memory") 4)
            (data (i32.const 65536) "{hex_escaped}")
            (global $heap (mut i32) (i32.const 131072))
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param $size i32) (result i32)
                (local $old i32)
                (local.set $old (global.get $heap))
                (global.set $heap (i32.add (global.get $heap) (local.get $size)))
                (local.get $old)
            )
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
                ;; TransformResponseHeader at 0: status=0, batch_count=1, batches_ptr=32
                (i32.store (i32.const 0) (i32.const 0))
                (i32.store (i32.const 4) (i32.const 1))
                (i32.store (i32.const 8) (i32.const 32))
                (i32.store (i32.const 12) (i32.const 0))
                (i32.store (i32.const 16) (i32.const 0))
                ;; BatchDescriptor at 32: ptr=65536, len={len}
                (i32.store (i32.const 32) (i32.const 65536))
                (i32.store (i32.const 36) (i32.const {len}))
                (i64.const 20)
            )
        )"#,
        len = ipc_bytes.len()
    );
    wat::parse_str(&wat).unwrap()
}

#[test]
fn test_run_immutability_suite_accepts_valid_transform() {
    let wasm = wat::parse_str(valid_echo_wat()).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_ok(), "Expected valid transform to pass: {res:?}");
}

#[test]
fn test_run_immutability_suite_rejects_tampered_immutable_column() {
    let wasm = wat::parse_str(tampered_trace_wat()).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err(), "Expected tampered trace_id to be rejected");
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("trace_id") || err_str.contains("Value mismatch"),
        "Unexpected error: {err_str}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_missing_immutable_column() {
    let wasm = missing_column_wasm();
    let res = run_immutability_suite(&wasm);
    assert!(
        res.is_err(),
        "Expected missing immutable column to be rejected"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("MissingColumn") || err_str.contains("missing from output"),
        "Unexpected error: {err_str}"
    );
}

#[test]
fn test_run_immutability_suite_traps_infinite_loop_via_fuel() {
    let wasm = wat::parse_str(infinite_loop_wat()).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(
        res.is_err(),
        "Expected infinite loop to trap via fuel exhaustion"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("fuel") || err_str.contains("trap") || err_str.contains("exhausted"),
        "Unexpected error: {err_str}"
    );
}

#[test]
fn test_run_immutability_suite_rejects_error_status() {
    let wasm = wat::parse_str(error_status_wat()).unwrap();
    let res = run_immutability_suite(&wasm);
    assert!(res.is_err(), "Expected non-zero status to be rejected");
}

#[test]
fn test_run_benchmark_calculates_latency_and_throughput() {
    use datalake_wasm_tool::bench::run_benchmark;

    let wasm = wat::parse_str(valid_echo_wat()).unwrap();
    let res = run_benchmark(&wasm, 100);
    assert!(res.is_ok(), "Expected benchmark to succeed: {res:?}");
    let bench_res = res.unwrap();
    assert_eq!(bench_res.iterations, 100);
    assert!(bench_res.payload_bytes > 0);
    assert!(bench_res.total_bytes >= bench_res.payload_bytes * 100);
    assert!(bench_res.avg_latency > std::time::Duration::ZERO);
    assert!(bench_res.throughput_mb_per_sec > 0.0);
}

#[test]
fn test_run_benchmark_handles_trapping_module() {
    use datalake_wasm_tool::bench::run_benchmark;

    let wasm = wat::parse_str(trapping_wat()).unwrap();
    let res = run_benchmark(&wasm, 10);
    assert!(res.is_err(), "Expected trapping module to return Err");
}

#[test]
fn test_run_benchmark_with_disclaimer_succeeds() {
    let wasm = wat::parse_str(valid_echo_wat()).unwrap();
    let res = run_benchmark_with_disclaimer(&wasm);
    assert!(
        res.is_ok(),
        "Expected benchmark with disclaimer to succeed: {res:?}"
    );
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
