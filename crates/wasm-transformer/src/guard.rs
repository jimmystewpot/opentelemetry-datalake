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

/// Recursively marks a field and any nested children as nullable.
fn make_field_nullable(field: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    match field.data_type() {
        arrow::datatypes::DataType::Struct(subfields) => {
            let nullable_subfields: Vec<Arc<arrow::datatypes::Field>> = subfields
                .iter()
                .map(|f| Arc::new(make_field_nullable(f)))
                .collect();
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::Struct(
                nullable_subfields.into(),
            ));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::List(child) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::List(Arc::new(
                make_field_nullable(child),
            )));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::LargeList(child) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::LargeList(Arc::new(
                make_field_nullable(child),
            )));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::FixedSizeList(child, size) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::FixedSizeList(
                Arc::new(make_field_nullable(child)),
                *size,
            ));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::ListView(child) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::ListView(Arc::new(
                make_field_nullable(child),
            )));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::LargeListView(child) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::LargeListView(Arc::new(
                make_field_nullable(child),
            )));
            new_f.with_nullable(true)
        }
        arrow::datatypes::DataType::Map(child, sorted) => {
            let mut new_f = field.clone();
            new_f = new_f.with_data_type(arrow::datatypes::DataType::Map(
                Arc::new(make_field_nullable(child)),
                *sorted,
            ));
            new_f.with_nullable(true)
        }
        _ => field.clone().with_nullable(true),
    }
}

/// Verifies that canonical immutable columns have not been dropped, completely wiped to nulls,
/// or had their data types mutated.
///
/// Executes an $O(1)$ metadata check on the output [`RecordBatch`] for each canonical
/// OpenTelemetry field defined in [`IMMUTABLE_COLUMNS`]. If a column was not entirely null
/// in the input batch, and is either completely nullified or dropped entirely in the output
/// batch (when non-empty), an error is returned. If an immutable column is present in both
/// input and output, its [`arrow::datatypes::DataType`] must remain identical.
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if any immutable column present in `input` is
/// dropped, entirely null in `output` with `!output.is_empty()`, or has its data type mutated.
pub fn verify_structural_immutability(
    input: &RecordBatch,
    output: &RecordBatch,
) -> Result<(), WasmTransformError> {
    let schema = output.schema();
    let input_schema = input.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        let input_col_info = if let Ok(idx) = input_schema.index_of(col_name) {
            let col = input.column(idx);
            Some((col.data_type().clone(), col.null_count() == col.len()))
        } else {
            None
        };

        let input_was_null = input_col_info
            .as_ref()
            .is_none_or(|(_, was_null)| *was_null);

        if let Ok(idx) = schema.index_of(col_name) {
            let col = output.column(idx);
            if let Some((in_dtype, _)) = input_col_info
                && col.data_type() != &in_dtype
            {
                return Err(WasmTransformError::Pipeline(format!(
                    "Immutability violation: core field '{col_name}' data type mutated from {in_dtype:?} to {:?}",
                    col.data_type()
                )));
            }
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
/// preserving input column ordering and merging schema metadata.
///
/// In defensive schema guard mode, if a guest transformation drops columns that existed
/// in the upstream schema, this function re-inserts them as typed null arrays matching
/// the input field data types and row count, preserving the input column ordering and
/// ensuring downstream consumers and columnar writers (e.g. Parquet/Iceberg) do not
/// encounter schema truncation or drift. Any new columns appended by the guest are
/// preserved at the end. Any nested structs in backfilled columns have their child fields
/// recursively marked nullable to satisfy downstream format requirements.
///
/// Schema metadata from `input_schema` is preserved and merged with `output_schema` metadata,
/// ensuring upstream compliance flags (e.g. `otel::compliance::status = "verified"`) remain intact.
///
/// If no columns are missing, schema ordering is unchanged, and metadata matches, returns
/// `Ok(output)` without allocating a new [`RecordBatch`].
///
/// # Errors
///
/// Returns [`WasmTransformError::Pipeline`] if reconstructing the [`RecordBatch`] fails.
pub fn backfill_missing_columns(
    input_schema: &Schema,
    output: RecordBatch,
) -> Result<RecordBatch, WasmTransformError> {
    let output_schema = output.schema();
    let capacity = input_schema
        .fields()
        .len()
        .max(output_schema.fields().len());
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(capacity);
    let mut fields = Vec::with_capacity(capacity);
    let num_rows = output.num_rows();
    let mut added = false;

    // Build an O(1) lookup map from field name → output column index to avoid O(n²) scanning.
    let output_field_index: std::collections::HashMap<&str, usize> = output_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    for field in input_schema.fields() {
        if let Some(&idx) = output_field_index.get(field.name().as_str()) {
            fields.push(Arc::clone(&output_schema.fields()[idx]));
            columns.push(Arc::clone(output.column(idx)));
        } else {
            warn!(
                column = %field.name(),
                "Schema guard: backfilling missing column with typed nulls"
            );
            let backfilled_field = if field.is_nullable() {
                Arc::clone(field)
            } else {
                Arc::new(make_field_nullable(field))
            };
            columns.push(new_null_array(backfilled_field.data_type(), num_rows));
            fields.push(backfilled_field);
            added = true;
        }
    }

    // Preserve any new columns added by the guest (not present in the input schema).
    for (idx, field) in output_schema.fields().iter().enumerate() {
        if input_schema.index_of(field.name()).is_err() {
            fields.push(Arc::clone(field));
            columns.push(Arc::clone(output.column(idx)));
        }
    }

    let mut merged_metadata = input_schema.metadata().clone();
    for (k, v) in output_schema.metadata() {
        if k.starts_with("otel::compliance::") {
            continue;
        }
        merged_metadata
            .entry(k.clone())
            .or_insert_with(|| v.clone());
    }

    if !added
        && output_schema.fields().len() == fields.len()
        && output_schema
            .fields()
            .iter()
            .zip(&fields)
            .all(|(a, b)| a == b)
        && output_schema.metadata() == &merged_metadata
    {
        return Ok(output);
    }

    let new_schema = Arc::new(Schema::new_with_metadata(fields, merged_metadata));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| WasmTransformError::Pipeline(e.to_string()))
}
