use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use std::collections::HashMap;
use std::sync::Arc;
use wasm_transformer::guard::{backfill_missing_columns, verify_structural_immutability};

#[test]
fn test_o1_immutability_check_detects_all_null_immutable_column() {
    let in_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let out_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        true,
    )]));
    let input =
        RecordBatch::try_new(in_schema, vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
    let output =
        RecordBatch::try_new(out_schema, vec![new_null_array(&DataType::Utf8, 1)]).unwrap();
    assert!(verify_structural_immutability(&input, &output).is_err());
}

#[test]
fn test_o1_immutability_check_passes_non_null_immutable_column() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["id_1"]))],
    )
    .unwrap();
    let output =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_o1_immutability_check_zero_row_batch_succeeds() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let input = RecordBatch::new_empty(schema.clone());
    let output = RecordBatch::new_empty(schema);
    assert_eq!(input.num_rows(), 0);
    assert_eq!(output.num_rows(), 0);
    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_o1_immutability_check_unmutated_batch_with_all_immutable_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("custom_attr", DataType::Utf8, true),
    ]));

    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"])),
            Arc::new(StringArray::from(vec!["0102030405060708"])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(StringArray::from(vec!["test_span"])),
            Arc::new(StringArray::from(vec!["span"])),
            Arc::new(StringArray::from(vec!["custom_val"])),
        ],
    )
    .unwrap();

    let output = input.clone();
    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_o1_immutability_check_each_canonical_column_violation() {
    for &col_name in IMMUTABLE_COLUMNS {
        let in_schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Utf8,
            false,
        )]));
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Utf8,
            true,
        )]));
        let input = RecordBatch::try_new(
            in_schema,
            vec![Arc::new(StringArray::from(vec!["valid_val"]))],
        )
        .unwrap();
        let output =
            RecordBatch::try_new(out_schema, vec![new_null_array(&DataType::Utf8, 1)]).unwrap();

        let err = verify_structural_immutability(&input, &output).unwrap_err();
        assert!(
            err.to_string().contains(col_name),
            "Error message should mention violated column '{col_name}': {err}"
        );
    }
}

#[test]
fn test_o1_immutability_check_allows_all_null_mutable_column() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("mutable_notes", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("mutable_notes", DataType::Utf8, true),
    ]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["id_1"])),
            Arc::new(StringArray::from(vec!["initial note"])),
        ],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["id_1"])),
            new_null_array(&DataType::Utf8, 1),
        ],
    )
    .unwrap();

    // mutable_notes is entirely null, but it is NOT an immutable column, so this must pass
    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_backfill_adds_missing_columns_as_typed_nulls() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("status_code", DataType::Int64, true),
    ]));
    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["id_1"]))],
    )
    .unwrap();
    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();

    assert_eq!(backfilled.num_columns(), 3);
    assert_eq!(backfilled.num_rows(), 1);
    assert_eq!(backfilled.column(0).null_count(), 0);
    assert_eq!(backfilled.column(1).null_count(), 1);
    assert_eq!(backfilled.column(2).null_count(), 1);
    assert_eq!(backfilled.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(backfilled.schema().field(2).data_type(), &DataType::Int64);
}

#[test]
fn test_backfill_no_missing_columns_returns_unmodified() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("status_code", DataType::Int64, false),
    ]));
    let output = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["id_1"])),
            Arc::new(Int64Array::from(vec![200])),
        ],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&schema, output.clone()).unwrap();
    assert_eq!(backfilled.num_columns(), 2);
    assert_eq!(backfilled.num_rows(), 1);
    assert_eq!(backfilled, output);
}

#[test]
fn test_backfill_zero_rows_batch() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::new_empty(partial_schema);
    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();

    assert_eq!(backfilled.num_columns(), 2);
    assert_eq!(backfilled.num_rows(), 0);
    assert_eq!(backfilled.column(1).len(), 0);
}

#[test]
fn test_backfill_preserves_schema_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("otel.data_source".to_string(), "logs".to_string());
    metadata.insert("custom.key".to_string(), "custom_value".to_string());

    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let partial_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        metadata.clone(),
    ));

    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["id_1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.schema().metadata(), &metadata);
}
