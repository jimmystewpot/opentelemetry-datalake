//! Helper functions and canonical immutability definitions for WASM transforms.

use crate::error::SdkError;
use arrow::array::new_null_array;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Canonical list of core OpenTelemetry fields that guests must not drop or nullify.
///
/// Single source of truth across SDK, CLI conformance harness, and host structural check.
pub const IMMUTABLE_COLUMNS: &[&str] = &[
    "trace_id",
    "span_id",
    "timestamp",
    "observed_timestamp",
    "name",
    "type",
];

/// Returns whether the given column name is one of the canonical immutable columns.
#[must_use]
pub fn is_immutable_column(column: &str) -> bool {
    IMMUTABLE_COLUMNS.contains(&column)
}

/// Nullifies a column in the given [`RecordBatch`] using the provided `target_schema`.
///
/// Returns an error if the column is immutable, not found in the schema, the target schema
/// does not match the batch schema's field names, order, or data types, or the target column
/// is not nullable.
pub fn nullify_column(
    batch: &RecordBatch,
    target_schema: Arc<Schema>,
    column_name: &str,
) -> Result<RecordBatch, SdkError> {
    if is_immutable_column(column_name) {
        return Err(SdkError::ImmutableFieldViolation(column_name.to_string()));
    }

    if batch.num_columns() != target_schema.fields().len() {
        return Err(SdkError::SchemaMismatch(format!(
            "Column count mismatch: batch has {}, target schema has {}",
            batch.num_columns(),
            target_schema.fields().len()
        )));
    }

    let batch_schema = batch.schema();
    for i in 0..batch.num_columns() {
        let batch_field = batch_schema.field(i);
        let target_field = target_schema.field(i);
        if batch_field.name() != target_field.name() {
            return Err(SdkError::SchemaMismatch(format!(
                "Field mismatch at index {i}: expected '{}', found '{}'",
                batch_field.name(),
                target_field.name()
            )));
        }
        if batch_field.data_type() != target_field.data_type() {
            return Err(SdkError::SchemaMismatch(format!(
                "Field mismatch at index {i}: expected '{}', found '{}'",
                batch_field.data_type(),
                target_field.data_type()
            )));
        }
    }

    let src_idx = batch_schema
        .index_of(column_name)
        .map_err(|_| SdkError::ColumnNotFound(column_name.to_string()))?;

    if !target_schema.field(src_idx).is_nullable() {
        return Err(SdkError::SchemaMismatch(format!(
            "Target schema field '{column_name}' must be nullable"
        )));
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
    columns[src_idx] = new_null_array(target_schema.field(src_idx).data_type(), batch.num_rows());

    RecordBatch::try_new(target_schema, columns).map_err(|e| SdkError::Arrow(e.to_string()))
}
