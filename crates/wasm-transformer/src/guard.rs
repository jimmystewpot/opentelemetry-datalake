//! Defensive schema guard and structural immutability verification for WASM transformations.
//!
//! Provides fast $O(1)$ immutability validation preventing guest transforms from wiping
//! core OpenTelemetry identity fields, and defensive schema backfilling with typed null
//! arrays for downstream query safety.

use crate::error::WasmTransformError;
use arrow::array::{Array, new_null_array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use std::sync::Arc;
use tracing::warn;

/// Verifies that canonical immutable columns have not been dropped or completely wiped to nulls.
///
/// Executes an $O(1)$ metadata check on the output [`RecordBatch`] for each canonical
/// OpenTelemetry field defined in [`IMMUTABLE_COLUMNS`]. If a column was not entirely null
/// in the input batch, and is either completely nullified or dropped entirely in the output
/// batch (when non-empty), an error is returned.
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if any immutable column present in `input` is
/// dropped or entirely null in `output` with `!output.is_empty()`.
pub fn verify_structural_immutability(
    input: &RecordBatch,
    output: &RecordBatch,
) -> Result<(), WasmTransformError> {
    let schema = output.schema();
    let input_schema = input.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        let input_was_null = if let Ok(idx) = input_schema.index_of(col_name) {
            input.column(idx).null_count() == input.column(idx).len()
        } else {
            true
        };

        if let Ok(idx) = schema.index_of(col_name) {
            let col = output.column(idx);
            if col.null_count() == col.len() && !col.is_empty() && !input_was_null {
                return Err(WasmTransformError::Pipeline(format!(
                    "Immutability violation: core field '{col_name}' is entirely null in output"
                )));
            }
        } else if !input_was_null && output.num_rows() > 0 {
            return Err(WasmTransformError::Pipeline(format!(
                "Immutability violation: core field '{col_name}' was dropped by guest"
            )));
        }
    }
    Ok(())
}

/// Backfills any missing columns from `input_schema` into `output` using typed null arrays,
/// preserving input column ordering.
///
/// In defensive schema guard mode, if a guest transformation drops columns that existed
/// in the upstream schema, this function re-inserts them as typed null arrays matching
/// the input field data types and row count, preserving the input column ordering and
/// ensuring downstream consumers and columnar writers (e.g. Parquet/Iceberg) do not
/// encounter schema truncation or drift. Any new columns appended by the guest are
/// preserved at the end.
///
/// If no columns are missing and schema ordering is unchanged, returns `Ok(output)` without
/// allocating a new [`RecordBatch`].
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if reconstructing the [`RecordBatch`] fails.
pub fn backfill_missing_columns(
    input_schema: &Schema,
    output: RecordBatch,
) -> Result<RecordBatch, WasmTransformError> {
    let output_schema = output.schema();
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(input_schema.fields().len());
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    let num_rows = output.num_rows();
    let mut added = false;

    for field in input_schema.fields() {
        if let Ok(idx) = output_schema.index_of(field.name()) {
            fields.push(Arc::clone(&output_schema.fields()[idx]));
            columns.push(Arc::clone(output.column(idx)));
        } else {
            warn!(
                column = %field.name(),
                "Schema guard: backfilling missing column with typed nulls"
            );
            fields.push(Arc::clone(field));
            columns.push(new_null_array(field.data_type(), num_rows));
            added = true;
        }
    }

    // Preserve any new columns added by the guest
    for (idx, field) in output_schema.fields().iter().enumerate() {
        if input_schema.index_of(field.name()).is_err() {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(output.column(idx)));
        }
    }

    if !added
        && output_schema.fields().len() == fields.len()
        && output_schema
            .fields()
            .iter()
            .zip(&fields)
            .all(|(a, b)| a == b)
    {
        return Ok(output);
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| WasmTransformError::Pipeline(e.to_string()))
}
