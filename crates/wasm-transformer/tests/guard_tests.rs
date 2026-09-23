use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use std::collections::HashMap;
use std::sync::Arc;
use wasm_transformer::guard::{
    backfill_missing_columns, verify_strict_schema_equality, verify_structural_immutability,
};

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
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
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

#[test]
fn test_o1_immutability_check_detects_dropped_immutable_column() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let output =
        RecordBatch::try_new(out_schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

    let err = verify_structural_immutability(&input, &output).unwrap_err();
    assert!(
        err.to_string()
            .contains("Immutability violation: core field 'trace_id' was dropped by guest"),
        "Unexpected error: {err}"
    );
}

#[test]
fn test_o1_immutability_check_allows_dropped_immutable_column_if_input_was_null() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            new_null_array(&DataType::Utf8, 1),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let output =
        RecordBatch::try_new(out_schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_o1_immutability_check_allows_dropped_immutable_column_if_output_empty() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_123"])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let output = RecordBatch::new_empty(out_schema);

    assert!(verify_structural_immutability(&input, &output).is_ok());
}

#[test]
fn test_backfill_preserves_input_column_ordering_when_middle_column_dropped() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Int64, true),
        Field::new("col_c", DataType::Utf8, true),
    ]));
    let partial_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_c", DataType::Utf8, true),
    ]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![
            Arc::new(StringArray::from(vec!["a_val"])),
            Arc::new(StringArray::from(vec!["c_val"])),
        ],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();

    assert_eq!(backfilled.num_columns(), 3);
    assert_eq!(backfilled.schema().field(0).name(), "col_a");
    assert_eq!(backfilled.schema().field(1).name(), "col_b");
    assert_eq!(backfilled.schema().field(2).name(), "col_c");

    assert_eq!(backfilled.column(0).null_count(), 0);
    assert_eq!(backfilled.column(1).null_count(), 1);
    assert_eq!(backfilled.column(2).null_count(), 0);
}

#[test]
fn test_backfill_preserves_input_ordering_and_appends_guest_new_columns() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Int64, true),
    ]));
    let partial_schema = Arc::new(Schema::new(vec![
        Field::new("col_b", DataType::Int64, true),
        Field::new("new_guest_col", DataType::Utf8, true),
    ]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(StringArray::from(vec!["guest_val"])),
        ],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();

    assert_eq!(backfilled.num_columns(), 3);
    assert_eq!(backfilled.schema().field(0).name(), "col_a");
    assert_eq!(backfilled.schema().field(1).name(), "col_b");
    assert_eq!(backfilled.schema().field(2).name(), "new_guest_col");

    assert_eq!(backfilled.column(0).null_count(), 1);
    assert_eq!(backfilled.column(1).null_count(), 0);
    assert_eq!(backfilled.column(2).null_count(), 0);
}

#[test]
fn test_backfill_restores_column_ordering_when_guest_permutes_columns() {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Int64, false),
    ]));
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("col_b", DataType::Int64, false),
        Field::new("col_a", DataType::Utf8, false),
    ]));
    let output = RecordBatch::try_new(
        output_schema,
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();

    let restored = backfill_missing_columns(&input_schema, output).unwrap();

    assert_eq!(restored.num_columns(), 2);
    assert_eq!(restored.schema().field(0).name(), "col_a");
    assert_eq!(restored.schema().field(1).name(), "col_b");

    let col_a = restored
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col_a.value(0), "hello");

    let col_b = restored
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col_b.value(0), 42);
}

#[test]
fn test_backfill_non_nullable_missing_column_sets_nullable_true() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("non_nullable_counter", DataType::Int64, false),
    ]));
    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["id_1", "id_2"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 2);
    assert_eq!(backfilled.num_rows(), 2);
    assert_eq!(backfilled.column(1).null_count(), 2);

    // The backfilled column MUST have is_nullable == true, even though input was false
    assert!(
        backfilled.schema().field(1).is_nullable(),
        "Backfilled null column must have is_nullable == true"
    );
}

#[test]
fn test_immutability_check_detects_mutated_data_type() {
    let in_schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )]));
    let out_schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Int64,
        false,
    )]));
    let input = RecordBatch::try_new(
        in_schema,
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            1_700_000_000_000_000_000,
        ]))],
    )
    .unwrap();
    let output = RecordBatch::try_new(
        out_schema,
        vec![Arc::new(Int64Array::from(vec![1_700_000_000]))],
    )
    .unwrap();

    let err = verify_structural_immutability(&input, &output).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("data type mutated"),
        "Error message must indicate data type mutation: {msg}"
    );
}

#[test]
fn test_backfill_merges_input_and_output_schema_metadata() {
    let mut in_meta = HashMap::new();
    in_meta.insert(
        "otel::compliance::status".to_string(),
        "verified".to_string(),
    );
    in_meta.insert("source_system".to_string(), "collector".to_string());
    let full_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("dropped_col", DataType::Int32, true),
        ],
        in_meta,
    ));

    let mut out_meta = HashMap::new();
    out_meta.insert("guest_version".to_string(), "v1.2".to_string());
    let partial_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        out_meta,
    ));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    let backfilled_schema = backfilled.schema();
    let meta = backfilled_schema.metadata();
    assert_eq!(
        meta.get("otel::compliance::status").map(String::as_str),
        Some("verified")
    );
    assert_eq!(
        meta.get("source_system").map(String::as_str),
        Some("collector")
    );
    assert_eq!(meta.get("guest_version").map(String::as_str), Some("v1.2"));
}

#[test]
fn test_backfill_nested_struct_marks_child_fields_nullable() {
    let child_field = Arc::new(Field::new("service_name", DataType::Utf8, false));
    let struct_field = Field::new(
        "resource",
        DataType::Struct(vec![child_field].into()),
        false,
    );
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        struct_field,
    ]));

    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 2);
    let backfilled_schema = backfilled.schema();
    let res_field = backfilled_schema.field(1);
    assert!(res_field.is_nullable(), "Outer struct must be nullable");

    if let DataType::Struct(children) = res_field.data_type() {
        assert!(
            children[0].is_nullable(),
            "Nested child field inside backfilled null struct must be marked nullable"
        );
    } else {
        panic!("Expected struct data type");
    }
}

#[test]
fn test_backfill_nested_list_marks_child_field_nullable() {
    let child_field = Arc::new(Field::new("item", DataType::Int32, false));
    let list_field = Field::new("metrics_list", DataType::List(child_field), false);
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        list_field,
    ]));

    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 2);
    let backfilled_schema = backfilled.schema();
    let res_field = backfilled_schema.field(1);
    assert!(res_field.is_nullable(), "Outer list must be nullable");

    if let DataType::List(child) = res_field.data_type() {
        assert!(
            child.is_nullable(),
            "Child field inside backfilled null list must be marked nullable"
        );
    } else {
        panic!("Expected list data type");
    }
}

#[test]
fn test_backfill_nested_large_list_and_fixed_size_list_nullability() {
    let child1 = Arc::new(Field::new("item1", DataType::Int32, false));
    let child2 = Arc::new(Field::new("item2", DataType::Int64, false));
    let large_list = Field::new("large_list", DataType::LargeList(child1), false);
    let fixed_list = Field::new("fixed_list", DataType::FixedSizeList(child2, 4), false);
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        large_list,
        fixed_list,
    ]));

    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 3);
    let schema = backfilled.schema();

    let f1 = schema.field(1);
    assert!(f1.is_nullable());
    if let DataType::LargeList(child) = f1.data_type() {
        assert!(child.is_nullable());
    } else {
        panic!("Expected LargeList");
    }

    let f2 = schema.field(2);
    assert!(f2.is_nullable());
    if let DataType::FixedSizeList(child, 4) = f2.data_type() {
        assert!(child.is_nullable());
    } else {
        panic!("Expected FixedSizeList");
    }
}

#[test]
fn test_backfill_nested_map_and_list_views_nullability() {
    let map_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Int32, false)),
            ]
            .into(),
        ),
        false,
    ));
    let map_field = Field::new("map_col", DataType::Map(map_entries, false), false);
    let list_view_field = Field::new(
        "list_view_col",
        DataType::ListView(Arc::new(Field::new("item", DataType::Int32, false))),
        false,
    );
    let large_list_view_field = Field::new(
        "large_list_view_col",
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Int64, false))),
        false,
    );

    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        map_field,
        list_view_field,
        large_list_view_field,
    ]));

    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 4);
    let schema = backfilled.schema();

    // Map column
    let f1 = schema.field(1);
    assert!(f1.is_nullable());
    if let DataType::Map(entries, _) = f1.data_type() {
        assert!(entries.is_nullable());
        if let DataType::Struct(fields) = entries.data_type() {
            for sub in fields {
                assert!(sub.is_nullable());
            }
        } else {
            panic!("Expected struct entries in Map");
        }
    } else {
        panic!("Expected Map");
    }

    // ListView column
    let f2 = schema.field(2);
    assert!(f2.is_nullable());
    if let DataType::ListView(child) = f2.data_type() {
        assert!(child.is_nullable());
    } else {
        panic!("Expected ListView");
    }

    // LargeListView column
    let f3 = schema.field(3);
    assert!(f3.is_nullable());
    if let DataType::LargeListView(child) = f3.data_type() {
        assert!(child.is_nullable());
    } else {
        panic!("Expected LargeListView");
    }
}

#[test]
fn test_strict_schema_equality_identical_schemas_pass() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["t1"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["t2"])),
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(StringArray::from(vec!["world"])),
        ],
    )
    .unwrap();

    assert!(verify_strict_schema_equality(&batch1, &batch2).is_ok());
}

#[test]
fn test_strict_schema_equality_dropped_non_immutable_column_fails() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, true),
    ]));
    let in_batch = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["t1"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap();
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["t1"])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();

    let res = verify_strict_schema_equality(&in_batch, &out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Strict schema guard violation")
    );
}

#[test]
fn test_strict_schema_equality_added_column_fails() {
    let in_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("new_col", DataType::Utf8, true),
    ]));
    let in_batch =
        RecordBatch::try_new(in_schema, vec![Arc::new(StringArray::from(vec!["t1"]))]).unwrap();
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["t1"])),
            Arc::new(StringArray::from(vec!["extra"])),
        ],
    )
    .unwrap();

    let res = verify_strict_schema_equality(&in_batch, &out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Strict schema guard violation")
    );
}

#[test]
fn test_strict_schema_equality_dropped_metadata_fails() {
    let mut in_schema = Schema::new(vec![Field::new("body", DataType::Utf8, true)]);
    let mut meta = std::collections::HashMap::new();
    meta.insert(
        "otel::compliance::status".to_string(),
        "verified".to_string(),
    );
    in_schema.metadata = meta;
    let in_schema = Arc::new(in_schema);

    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));

    let in_batch =
        RecordBatch::try_new(in_schema, vec![Arc::new(StringArray::from(vec!["val"]))]).unwrap();
    let out_batch =
        RecordBatch::try_new(out_schema, vec![Arc::new(StringArray::from(vec!["val"]))]).unwrap();

    let res = verify_strict_schema_equality(&in_batch, &out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Strict schema guard violation")
    );
}

#[test]
fn test_strict_schema_equality_mutated_field_type_fails() {
    let in_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Int64, true)]));
    let in_batch =
        RecordBatch::try_new(in_schema, vec![Arc::new(StringArray::from(vec!["123"]))]).unwrap();
    let out_batch =
        RecordBatch::try_new(out_schema, vec![Arc::new(Int64Array::from(vec![123]))]).unwrap();

    let res = verify_strict_schema_equality(&in_batch, &out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Strict schema guard violation")
    );
}

#[test]
fn test_strict_schema_equality_reordered_fields_fail() {
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Utf8, true),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("col_b", DataType::Utf8, true),
        Field::new("col_a", DataType::Utf8, true),
    ]));
    let in_batch = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
        ],
    )
    .unwrap();
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();

    let res = verify_strict_schema_equality(&in_batch, &out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Strict schema guard violation")
    );
}

#[test]
fn test_backfill_rejects_mutated_data_type() {
    let in_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Int64, true)]));
    let out_batch =
        RecordBatch::try_new(out_schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();

    let res = backfill_missing_columns(&in_schema, out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("data type mutated from Utf8 to Int64")
    );
}

#[test]
fn test_backfill_rejects_nullability_mutation() {
    let in_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    let out_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let out_batch =
        RecordBatch::try_new(out_schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

    let res = backfill_missing_columns(&in_schema, out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("nullability mutated from non-nullable to nullable")
    );
}

#[test]
fn test_backfill_rejects_non_nullable_new_column() {
    let in_schema = Arc::new(Schema::new(vec![Field::new("col_a", DataType::Utf8, true)]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("new_required_col", DataType::Int64, false),
    ]));
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(StringArray::from(vec!["hello"])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .unwrap();

    let res = backfill_missing_columns(&in_schema, out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("newly added column 'new_required_col' must be nullable")
    );
}

#[test]
fn test_backfill_rejects_tampered_compliance_metadata() {
    let mut in_meta = HashMap::new();
    in_meta.insert(
        "otel::compliance::status".to_string(),
        "verified".to_string(),
    );
    let in_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        in_meta,
    ));

    let mut out_meta = HashMap::new();
    out_meta.insert("otel::compliance::status".to_string(), "forged".to_string());
    let out_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        out_meta,
    ));
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let res = backfill_missing_columns(&in_schema, out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("guest attempted to mutate compliance metadata 'otel::compliance::status'")
    );
}

#[test]
fn test_backfill_preserves_upstream_metadata_on_conflict() {
    let mut in_meta = HashMap::new();
    in_meta.insert(
        "otel::compliance::status".to_string(),
        "verified".to_string(),
    );
    in_meta.insert("conflict_key".to_string(), "upstream_value".to_string());
    let in_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        in_meta,
    ));

    let mut out_meta = HashMap::new();
    out_meta.insert("conflict_key".to_string(), "guest_override".to_string());
    out_meta.insert("guest_key".to_string(), "guest_value".to_string());
    let out_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false)],
        out_meta,
    ));
    let out_batch = RecordBatch::try_new(
        out_schema,
        vec![Arc::new(StringArray::from(vec!["trace-1"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&in_schema, out_batch).unwrap();
    let backfilled_schema = backfilled.schema();
    let meta = backfilled_schema.metadata();
    assert_eq!(
        meta.get("conflict_key").map(String::as_str),
        Some("upstream_value")
    );
    assert_eq!(
        meta.get("guest_key").map(String::as_str),
        Some("guest_value")
    );
    assert_eq!(
        meta.get("otel::compliance::status").map(String::as_str),
        Some("verified")
    );
}

#[test]
fn test_backfill_rejects_nullable_to_non_nullable_mutation() {
    let in_schema = Arc::new(Schema::new(vec![Field::new("col_a", DataType::Utf8, true)]));
    let out_schema = Arc::new(Schema::new(vec![Field::new(
        "col_a",
        DataType::Utf8,
        false,
    )]));
    let out_batch =
        RecordBatch::try_new(out_schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

    let res = backfill_missing_columns(&in_schema, out_batch);
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("column 'col_a' nullability mutated from nullable to non-nullable")
    );
}

/// Regression: `backfill_missing_columns` previously used O(n²) linear scan for field lookup.
/// This test verifies correctness of the O(n) HashMap-based implementation with a large schema.
#[test]
fn test_backfill_on_schema_missing_multiple_columns_is_correct() {
    // Build a 50-column input schema
    let mut input_fields: Vec<Field> = (0..50)
        .map(|i| Field::new(format!("col_{i}").as_str(), DataType::Int64, true))
        .collect();
    input_fields.push(Field::new("trace_id", DataType::Utf8, false));
    let input_schema = Arc::new(Schema::new(input_fields));

    // Output has only 10 of the 50 columns (guest dropped 40) plus 2 new guest columns
    let mut output_fields: Vec<Field> = (0..10)
        .map(|i| Field::new(format!("col_{i}").as_str(), DataType::Int64, true))
        .collect();
    output_fields.push(Field::new("trace_id", DataType::Utf8, false));
    output_fields.push(Field::new("guest_col_a", DataType::Utf8, true));
    output_fields.push(Field::new("guest_col_b", DataType::Int64, true));
    let output_schema = Arc::new(Schema::new(output_fields));

    let array_len = 3usize;
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = (0..10)
        .map(|_| Arc::new(Int64Array::from(vec![1i64, 2, 3])) as Arc<dyn arrow::array::Array>)
        .collect();
    arrays.push(Arc::new(StringArray::from(vec!["a", "b", "c"])));
    arrays.push(Arc::new(StringArray::from(vec!["x", "y", "z"])));
    arrays.push(Arc::new(Int64Array::from(vec![10i64, 20, 30])));
    let output_batch = RecordBatch::try_new(output_schema, arrays).unwrap();

    let result = backfill_missing_columns(&input_schema, output_batch).unwrap();
    let result_schema = result.schema();

    // Verify all input columns are present (40 missing → backfilled as nulls)
    for i in 0..50 {
        let col_name = format!("col_{i}");
        assert!(
            result_schema.index_of(&col_name).is_ok(),
            "col_{i} must be present after backfill"
        );
        if i >= 10 {
            // Dropped columns must be null arrays
            let idx = result_schema.index_of(&col_name).unwrap();
            assert_eq!(
                result.column(idx).null_count(),
                array_len,
                "col_{i} must be all-null after backfill"
            );
        }
    }
    // trace_id must be present
    assert!(result_schema.index_of("trace_id").is_ok());
    // New guest columns must be preserved
    assert!(result_schema.index_of("guest_col_a").is_ok());
    assert!(result_schema.index_of("guest_col_b").is_ok());
    // Total columns: 50 input + 1 trace_id + 2 guest = 53
    assert_eq!(result_schema.fields().len(), 53);
}
