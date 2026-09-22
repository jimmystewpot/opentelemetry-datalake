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

#[test]
fn test_nullify_column_rejects_column_count_mismatch() {
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

    let count_mismatch_target = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Utf8, true),
        Field::new("col_c", DataType::Utf8, true),
    ]));

    let res = nullify_column(&batch, count_mismatch_target, "col_b");
    assert!(res.is_err());
    match res.unwrap_err() {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Column count mismatch: batch has 2, target schema has 3"));
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_nullify_column_rejects_data_type_mismatch() {
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

    let type_mismatch_target = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Int32, false),
        Field::new("col_b", DataType::Utf8, true),
    ]));

    let res = nullify_column(&batch, type_mismatch_target, "col_b");
    assert!(res.is_err());
    match res.unwrap_err() {
        SdkError::SchemaMismatch(msg) => {
            assert!(msg.contains("Field mismatch at index 0"));
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_sdk_error_schema_mismatch_display() {
    let err = SdkError::SchemaMismatch("field mismatch at index 0".to_string());
    assert_eq!(
        err.to_string(),
        "Schema mismatch: field mismatch at index 0"
    );
    assert_eq!(
        err,
        SdkError::SchemaMismatch("field mismatch at index 0".to_string())
    );
}

#[test]
fn test_nullify_column_preserves_and_merges_schema_metadata() {
    let mut input_metadata = std::collections::HashMap::new();
    input_metadata.insert(
        "otel::compliance::status".to_string(),
        "verified".to_string(),
    );
    input_metadata.insert("source".to_string(), "otel".to_string());

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, true),
        ],
        input_metadata,
    ));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    // Target schema without metadata (e.g. constructed via Schema::new)
    let target_schema_no_meta = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));

    let res_no_meta = nullify_column(&batch, target_schema_no_meta, "scope_attributes").unwrap();
    let res_no_meta_schema = res_no_meta.schema();
    let res_no_meta_map = res_no_meta_schema.metadata();
    assert_eq!(
        res_no_meta_map
            .get("otel::compliance::status")
            .map(String::as_str),
        Some("verified")
    );
    assert_eq!(
        res_no_meta_map.get("source").map(String::as_str),
        Some("otel")
    );

    // Target schema with custom metadata
    let mut target_metadata = std::collections::HashMap::new();
    target_metadata.insert("custom_tag".to_string(), "wasm_processed".to_string());
    let target_schema_with_meta = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, true),
        ],
        target_metadata,
    ));

    let res = nullify_column(&batch, target_schema_with_meta, "scope_attributes").unwrap();
    let res_schema = res.schema();
    let res_metadata = res_schema.metadata();
    assert_eq!(
        res_metadata
            .get("otel::compliance::status")
            .map(String::as_str),
        Some("verified")
    );
    assert_eq!(res_metadata.get("source").map(String::as_str), Some("otel"));
    assert_eq!(
        res_metadata.get("custom_tag").map(String::as_str),
        Some("wasm_processed")
    );
}

#[test]
fn test_nullify_column_rejects_untouched_column_nullability_mutation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("val_a"), None])),
            Arc::new(StringArray::from(vec![Some("val_b1"), Some("val_b2")])),
        ],
    )
    .unwrap();

    // Target schema attempts to change untouched field col_a from nullable to non-nullable
    let invalid_target = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, false),
        Field::new("col_b", DataType::Utf8, true),
    ]));

    let res = nullify_column(&batch, invalid_target, "col_b");
    assert!(res.is_err());
    match res.unwrap_err() {
        SdkError::SchemaMismatch(msg) => {
            assert!(
                msg.contains("nullability cannot be changed"),
                "expected nullability mismatch error, got: {msg}"
            );
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

#[test]
fn test_nullify_column_succeeds_with_untouched_nullable_column_containing_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("val_a"), None])),
            Arc::new(StringArray::from(vec![Some("val_b1"), Some("val_b2")])),
        ],
    )
    .unwrap();

    let valid_target = Arc::new(Schema::new(vec![
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Utf8, true),
    ]));

    let res = nullify_column(&batch, valid_target, "col_b").unwrap();
    assert_eq!(res.num_rows(), 2);
    // col_a remains untouched with its null
    assert_eq!(res.column(0).null_count(), 1);
    assert!(res.schema().field(0).is_nullable());
    // col_b is nullified
    assert_eq!(res.column(1).null_count(), 2);
    assert!(res.schema().field(1).is_nullable());
}
