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

/// Verifies that canonical immutable columns have not been completely wiped to nulls.
///
/// Executes an $O(1)$ metadata check on the output [`RecordBatch`] for each canonical
/// OpenTelemetry field defined in [`IMMUTABLE_COLUMNS`]. If a column is present in the output
/// batch, has a non-zero row count, and its null count equals its length (i.e. it was
/// completely nullified by the guest), an error is returned.
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if any immutable column in `output` with `!col.is_empty()`
/// is entirely null.
pub fn verify_structural_immutability(
    _input: &RecordBatch,
    output: &RecordBatch,
) -> Result<(), WasmTransformError> {
    let schema = output.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        if let Ok(idx) = schema.index_of(col_name) {
            let col = output.column(idx);
            if col.null_count() == col.len() && !col.is_empty() {
                return Err(WasmTransformError::Pipeline(format!(
                    "Immutability violation: core field '{col_name}' is entirely null in output"
                )));
            }
        }
    }
    Ok(())
}

/// Backfills any missing columns from `input_schema` into `output` using typed null arrays.
///
/// In defensive schema guard mode, if a guest transformation drops columns that existed
/// in the upstream schema, this function re-inserts them as typed null arrays matching
/// the input field data types and row count, ensuring downstream consumers and columnar
/// writers (e.g. Parquet/Iceberg) do not encounter schema truncation or drift.
///
/// If no columns are missing, returns `Ok(output)` without allocating a new [`RecordBatch`].
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if reconstructing the [`RecordBatch`] fails.
pub fn backfill_missing_columns(
    input_schema: &Schema,
    output: RecordBatch,
) -> Result<RecordBatch, WasmTransformError> {
    let output_schema = output.schema();
    let mut columns: Vec<Arc<dyn Array>> = output.columns().to_vec();
    let mut fields = output_schema.fields().to_vec();
    let num_rows = output.num_rows();
    let mut added = false;

    for field in input_schema.fields() {
        if output_schema.index_of(field.name()).is_err() {
            warn!(
                column = %field.name(),
                "Schema guard: backfilling missing column with typed nulls"
            );
            fields.push(Arc::clone(field));
            columns.push(new_null_array(field.data_type(), num_rows));
            added = true;
        }
    }

    if !added {
        return Ok(output);
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        fields,
        output_schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| WasmTransformError::Pipeline(e.to_string()))
}
