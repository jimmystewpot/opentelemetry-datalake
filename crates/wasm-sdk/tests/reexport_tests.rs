use std::sync::Arc;

use opentelemetry_datalake_wasm_sdk::arrow::array::Int32Array;
use opentelemetry_datalake_wasm_sdk::arrow::datatypes::{DataType, Field, Schema};
use opentelemetry_datalake_wasm_sdk::arrow::record_batch::RecordBatch;

#[test]
fn test_arrow_reexport_accessible_and_constructible() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_new(schema, vec![array]).expect("failed to create batch");
    assert_eq!(batch.num_rows(), 3);
}
