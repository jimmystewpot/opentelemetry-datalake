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
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let err = nullify_column(&batch, "trace_id").unwrap_err();
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
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc", "def"])),
            Arc::new(StringArray::from(vec!["attr1", "attr2"])),
        ],
    )
    .unwrap();

    let ok_batch = nullify_column(&batch, "scope_attributes").unwrap();
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
    let empty_batch = RecordBatch::new_empty(schema);

    let ok_batch = nullify_column(&empty_batch, "body").unwrap();
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
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    )
    .unwrap();

    let err = nullify_column(&batch, "nonexistent").unwrap_err();
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
