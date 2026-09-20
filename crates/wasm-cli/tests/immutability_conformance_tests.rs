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
fn test_run_immutability_suite_succeeds() {
    let res = run_immutability_suite(&[]);
    assert!(res.is_ok());
}

#[test]
fn test_run_benchmark_with_disclaimer_succeeds() {
    let res = run_benchmark_with_disclaimer(&[]);
    assert!(res.is_ok());
}
