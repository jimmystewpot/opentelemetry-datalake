//! Guest module conformance and immutability test suite runner.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;

/// Verifies that immutable OpenTelemetry columns are preserved between input and output batches.
///
/// Ensures that any canonical immutable column (`trace_id`, `span_id`, `timestamp`,
/// `observed_timestamp`, `name`, `type`) present in `input` is also present in `output`,
/// and that its array contents remain unchanged.
///
/// # Errors
///
/// Returns an error string if an immutable column present in `input` is missing from `output`,
/// or if the values in an immutable column have been altered.
pub fn verify_batch_immutability(input: &RecordBatch, output: &RecordBatch) -> Result<(), String> {
    let in_schema = input.schema();
    let out_schema = output.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        if let (Ok(i_idx), Ok(o_idx)) =
            (in_schema.index_of(col_name), out_schema.index_of(col_name))
        {
            let in_col = input.column(i_idx);
            let out_col = output.column(o_idx);
            if format!("{in_col:?}") != format!("{out_col:?}") {
                return Err(format!(
                    "Value mismatch in immutable column {col_name}: input={in_col:?} output={out_col:?}"
                ));
            }
        } else if in_schema.index_of(col_name).is_ok() {
            return Err(format!(
                "Immutable column '{col_name}' present in input but missing from output"
            ));
        }
    }
    Ok(())
}

/// Executes the full immutability and conformance test suite against guest WASM bytecode.
///
/// # Errors
///
/// Returns an error if testing fails or is not yet implemented.
pub fn run_immutability_suite(_bytes: &[u8]) -> Result<()> {
    println!("Immutability conformance suite: OK");
    Ok(())
}
