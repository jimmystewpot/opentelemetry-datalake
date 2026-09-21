//! Helper functions and canonical immutability definitions for WASM transforms.

use crate::error::SdkError;
use arrow::array::new_null_array;
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

/// Nullifies a column in the given [`RecordBatch`] while preserving schema and metadata.
///
/// Returns an error if the column is immutable or not found in the schema.
pub fn nullify_column(batch: &RecordBatch, column_name: &str) -> Result<RecordBatch, SdkError> {
    if is_immutable_column(column_name) {
        return Err(SdkError::ImmutableFieldViolation(column_name.to_string()));
    }
    let schema = batch.schema();
    let idx = schema
        .index_of(column_name)
        .map_err(|_| SdkError::ColumnNotFound(column_name.to_string()))?;
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
    let field = schema.field(idx);
    columns[idx] = new_null_array(field.data_type(), batch.num_rows());

    let final_schema = if field.is_nullable() {
        schema.clone()
    } else {
        let mut fields = schema.fields().to_vec();
        fields[idx] = Arc::new(field.as_ref().clone().with_nullable(true));
        Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ))
    };

    RecordBatch::try_new(final_schema, columns).map_err(|e| SdkError::Arrow(e.to_string()))
}
