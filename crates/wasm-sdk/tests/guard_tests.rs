use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::error::SdkError;
use opentelemetry_datalake_wasm_sdk::helpers::{
    IMMUTABLE_COLUMNS, is_immutable_column, nullify_column,
};
use opentelemetry_datalake_wasm_sdk::traits::{BatchTransformer, SignalType, TransformResult};
use std::collections::HashSet;
use std::sync::Arc;

#[test]
fn test_nullify_immutable_column_fails_fast() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let err = nullify_column(&batch, schema, "trace_id").unwrap_err();
    assert_eq!(
        err,
        SdkError::ImmutableFieldViolation("trace_id".to_string())
    );
    assert_eq!(
        err.to_string(),
        "Cannot modify or nullify immutable OpenTelemetry core field: trace_id"
    );
}

#[test]
fn test_nullify_mutable_column_succeeds() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["abc", "def"])),
            Arc::new(StringArray::from(vec!["attr1", "attr2"])),
        ],
    )
    .unwrap();

    let ok_batch = nullify_column(&batch, schema, "scope_attributes").unwrap();
    assert_eq!(ok_batch.column(1).null_count(), 2);
    assert_eq!(ok_batch.num_rows(), 2);
    // Trace ID remains unmodified
    let trace_col = ok_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(trace_col.value(0), "abc");
    assert_eq!(trace_col.value(1), "def");
}

#[test]
fn test_nullify_non_nullable_column_rewrites_schema_and_succeeds() {
    use std::collections::HashMap;

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "unit-test".to_string());

    let field = Field::new("custom_field", DataType::Utf8, false).with_metadata(metadata.clone());
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false), field],
        metadata.clone(),
    ));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc", "def"])),
            Arc::new(StringArray::from(vec!["val1", "val2"])),
        ],
    )
    .unwrap();

    let target_field =
        Field::new("custom_field", DataType::Utf8, true).with_metadata(metadata.clone());
    let target_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("trace_id", DataType::Utf8, false), target_field],
        metadata,
    ));

    let result = nullify_column(&batch, target_schema, "custom_field").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.column(1).null_count(), 2);
    // Verified that column is now nullable
    assert!(result.schema().field(1).is_nullable());
    // Metadata preserved
    assert_eq!(
        result
            .schema()
            .field(1)
            .metadata()
            .get("source")
            .map(String::as_str),
        Some("unit-test")
    );
    assert_eq!(
        result.schema().metadata().get("source").map(String::as_str),
        Some("unit-test")
    );
    // Non-nullified column preserved
    assert!(!result.schema().field(0).is_nullable());
}

#[test]
fn test_nullify_dictionary_column_preserves_dictionary_type_and_metadata() {
    use arrow::array::{Array, DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    use std::collections::HashMap;

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let mut field_metadata = HashMap::new();
    field_metadata.insert("semantic_role".to_string(), "dictionary_lookup".to_string());

    let dict_field =
        Field::new("dict_column", dict_type.clone(), false).with_metadata(field_metadata.clone());
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        dict_field,
    ]));

    let keys = Int32Array::from(vec![0, 1, 0]);
    let values = Arc::new(StringArray::from(vec!["frontend", "backend"]));
    let dict_array = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace-1", "trace-2", "trace-3"])),
            dict_array,
        ],
    )
    .unwrap();

    let target_dict_field =
        Field::new("dict_column", dict_type.clone(), true).with_metadata(field_metadata);
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        target_dict_field,
    ]));

    let result = nullify_column(&batch, target_schema, "dict_column").unwrap();

    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.column(1).null_count(), 3);

    // Verify dictionary type is preserved
    assert_eq!(result.schema().field(1).data_type(), &dict_type);
    assert_eq!(result.column(1).data_type(), &dict_type);

    // Verify dictionary structure is preserved via downcast
    let nullified_dict = result
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("column should downcast to DictionaryArray<Int32Type>");
    assert_eq!(nullified_dict.len(), 3);
    assert!(nullified_dict.is_null(0));
    assert!(nullified_dict.is_null(1));
    assert!(nullified_dict.is_null(2));

    // Verify metadata is retained
    assert_eq!(
        result
            .schema()
            .field(1)
            .metadata()
            .get("semantic_role")
            .map(String::as_str),
        Some("dictionary_lookup")
    );

    // Verify nullability was updated to true if originally non-nullable
    assert!(result.schema().field(1).is_nullable());

    // Verify non-nullified column preserved
    let trace_col = result
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(trace_col.value(0), "trace-1");
    assert_eq!(trace_col.value(1), "trace-2");
    assert_eq!(trace_col.value(2), "trace-3");
    assert!(!result.schema().field(0).is_nullable());
}

#[test]
fn test_nullify_empty_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let empty_batch = RecordBatch::new_empty(schema.clone());

    let ok_batch = nullify_column(&empty_batch, schema, "body").unwrap();
    assert_eq!(ok_batch.num_rows(), 0);
    assert_eq!(ok_batch.column(1).null_count(), 0);
}

#[test]
fn test_nullify_nonexistent_column_returns_column_not_found() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let err = nullify_column(&batch, schema, "nonexistent").unwrap_err();
    assert_eq!(err, SdkError::ColumnNotFound("nonexistent".to_string()));
    assert_eq!(err.to_string(), "Column not found in schema: nonexistent");
}

#[test]
fn test_nullify_target_schema_mismatch_returns_schema_mismatch_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let target_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("extra_column", DataType::Utf8, true),
    ]));

    let err = nullify_column(&batch, target_schema, "scope_attributes").unwrap_err();
    match err {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Column count mismatch"));
        }
        other => panic!("expected SdkError::SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_nullify_target_schema_field_name_mismatch_returns_schema_mismatch_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let target_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("wrong_column", DataType::Utf8, true),
    ]));

    let err = nullify_column(&batch, target_schema, "scope_attributes").unwrap_err();
    match err {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Field mismatch at index 1"));
        }
        other => panic!("expected SdkError::SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_sdk_error_arrow_variant_display() {
    let err = SdkError::Arrow("schema mismatch".to_string());
    assert_eq!(err.to_string(), "Arrow error: schema mismatch");
}

#[test]
fn test_immutable_columns_constant_is_public_and_covers_all_spec_fields() {
    assert!(IMMUTABLE_COLUMNS.contains(&"trace_id"));
    assert!(IMMUTABLE_COLUMNS.contains(&"span_id"));
    assert!(IMMUTABLE_COLUMNS.contains(&"timestamp"));
    assert!(IMMUTABLE_COLUMNS.contains(&"observed_timestamp"));
    assert!(IMMUTABLE_COLUMNS.contains(&"name"));
    assert!(IMMUTABLE_COLUMNS.contains(&"type"));
    assert_eq!(IMMUTABLE_COLUMNS.len(), 6);
}

#[test]
fn test_is_immutable_column_helper() {
    assert!(is_immutable_column("trace_id"));
    assert!(is_immutable_column("span_id"));
    assert!(is_immutable_column("timestamp"));
    assert!(is_immutable_column("observed_timestamp"));
    assert!(is_immutable_column("name"));
    assert!(is_immutable_column("type"));
    assert!(!is_immutable_column("body"));
    assert!(!is_immutable_column("attributes"));
}

#[test]
fn test_signal_type_traits() {
    let mut set = HashSet::new();
    set.insert(SignalType::Logs);
    set.insert(SignalType::Metrics);
    set.insert(SignalType::Traces);
    assert_eq!(set.len(), 3);
    assert_eq!(SignalType::Logs, SignalType::Logs);
    assert_ne!(SignalType::Logs, SignalType::Metrics);
    let copied = SignalType::Traces;
    assert_eq!(copied, SignalType::Traces);
}

#[test]
fn test_transform_result_variants_debug() {
    let discard = TransformResult::Discard;
    assert_eq!(format!("{discard:?}"), "Discard");

    let reject = TransformResult::Reject {
        reason: "corrupted data".to_string(),
    };
    assert_eq!(
        format!("{reject:?}"),
        "Reject { reason: \"corrupted data\" }"
    );

    let error = TransformResult::Error {
        reason: "fatal crash".to_string(),
    };
    assert_eq!(format!("{error:?}"), "Error { reason: \"fatal crash\" }");
}

#[test]
fn test_signal_type_from_u32() {
    assert_eq!(SignalType::from_u32(0), Some(SignalType::Logs));
    assert_eq!(SignalType::from_u32(1), Some(SignalType::Metrics));
    assert_eq!(SignalType::from_u32(2), Some(SignalType::Traces));
    assert_eq!(SignalType::from_u32(3), None);
    assert_eq!(SignalType::from_u32(u32::MAX), None);

    // Verify explicit repr(u32) values match C-ABI v1
    assert_eq!(SignalType::Logs as u32, 0);
    assert_eq!(SignalType::Metrics as u32, 1);
    assert_eq!(SignalType::Traces as u32, 2);
}

#[test]
fn test_transform_result_convenience_constructors() {
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]));
    let batch = RecordBatch::new_empty(schema);

    // ok
    let res = TransformResult::ok(batch.clone());
    match res {
        TransformResult::Continue(batches) => {
            assert_eq!(batches.len(), 1);
        }
        _ => panic!("expected Continue"),
    }

    // ok_multiple
    let res = TransformResult::ok_multiple(vec![batch.clone(), batch]);
    match res {
        TransformResult::Continue(batches) => {
            assert_eq!(batches.len(), 2);
        }
        _ => panic!("expected Continue"),
    }

    // ok_empty
    let res = TransformResult::ok_empty();
    match res {
        TransformResult::Continue(batches) => {
            assert!(batches.is_empty());
        }
        _ => panic!("expected Continue"),
    }

    // discard
    let res = TransformResult::discard();
    assert!(matches!(res, TransformResult::Discard));

    // reject
    let res = TransformResult::reject("bad batch");
    match res {
        TransformResult::Reject { reason } => {
            assert_eq!(reason, "bad batch");
        }
        _ => panic!("expected Reject"),
    }

    // error
    let res = TransformResult::error("internal error");
    match res {
        TransformResult::Error { reason } => {
            assert_eq!(reason, "internal error");
        }
        _ => panic!("expected Error"),
    }
}

#[test]
fn test_sdk_error_ipc_and_invalid_signal_type_display() {
    let err_ipc = SdkError::Ipc("deserialization failed".to_string());
    assert_eq!(err_ipc.to_string(), "IPC error: deserialization failed");
    assert_eq!(err_ipc, SdkError::Ipc("deserialization failed".to_string()));

    let err_sig = SdkError::InvalidSignalType(42);
    assert_eq!(err_sig.to_string(), "Invalid signal type: 42");
    assert_eq!(err_sig, SdkError::InvalidSignalType(42));
}

struct DummyTransformer;

impl BatchTransformer for DummyTransformer {
    fn init(signal: SignalType, config_json: Option<&str>) -> Result<Self, String> {
        assert_eq!(signal, SignalType::Logs);
        assert_eq!(config_json, Some("{}"));
        Ok(Self)
    }

    fn transform(&mut self, batch: RecordBatch) -> TransformResult {
        TransformResult::Continue(vec![batch])
    }
}

#[test]
fn test_batch_transformer_trait_mock() {
    let mut transformer = DummyTransformer::init(SignalType::Logs, Some("{}")).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();

    match transformer.transform(batch) {
        TransformResult::Continue(batches) => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
        }
        _ => panic!("expected TransformResult::Continue"),
    }
}

#[test]
fn test_panic_payload_extraction() {
    use opentelemetry_datalake_wasm_sdk::panic::extract_panic_payload;

    let str_payload: Box<dyn std::any::Any> = Box::new("static literal error");
    assert_eq!(extract_panic_payload(&*str_payload), "static literal error");

    let string_payload: Box<dyn std::any::Any> = Box::new("dynamic String error".to_string());
    assert_eq!(
        extract_panic_payload(&*string_payload),
        "dynamic String error"
    );

    let non_str_payload: Box<dyn std::any::Any> = Box::new(42_i32);
    assert_eq!(
        extract_panic_payload(&*non_str_payload),
        "Wasm Guest Panic (OOM or unformattable)"
    );
}

#[test]
fn test_wasm_panic_hook_does_not_crash_on_non_string_payload() {
    let prev_hook = std::panic::take_hook();
    opentelemetry_datalake_wasm_sdk::panic::init_panic_hook();

    let caught = std::panic::catch_unwind(|| {
        std::panic::panic_any(12345);
    });
    std::panic::set_hook(prev_hook);
    assert!(caught.is_err());
}

#[test]
fn test_nullify_column_rejects_reordered_target_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["val_a"])),
            Arc::new(StringArray::from(vec!["val_b"])),
        ],
    )
    .unwrap();

    // Reordered target schema: [col_b, col_a]
    let reordered_target = Arc::new(Schema::new(vec![
        Field::new("col_b", DataType::Utf8, true),
        Field::new("col_a", DataType::Utf8, false),
    ]));

    let res = nullify_column(&batch, reordered_target, "col_b");
    assert!(res.is_err());
    match res.unwrap_err() {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Field mismatch at index 0"));
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_nullify_column_rejects_non_nullable_target_field() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["val_a"])),
            Arc::new(StringArray::from(vec!["val_b"])),
        ],
    )
    .unwrap();

    // Matching order, but col_b is marked not nullable in target schema
    let non_nullable_target = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Utf8, false),
    ]));

    let res = nullify_column(&batch, non_nullable_target, "col_b");
    assert!(res.is_err());
    match res.unwrap_err() {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Target schema field 'col_b' must be nullable"));
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}
