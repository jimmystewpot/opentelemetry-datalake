//! Guest module conformance and immutability test suite runner.

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TesterError {
    #[error("Value mismatch in immutable column {0}")]
    ValueMismatch(&'static str),
    #[error("Immutable column '{0}' present in input but missing from output")]
    MissingColumn(&'static str),
}

/// Verifies that immutable OpenTelemetry columns are preserved between input and output batches.
///
/// Ensures that any canonical immutable column (`trace_id`, `span_id`, `timestamp`,
/// `observed_timestamp`, `name`, `type`) present in `input` is also present in `output`,
/// and that its array contents remain unchanged.
///
/// # Errors
///
/// Returns [`TesterError::MissingColumn`] if an immutable column present in `input` is missing from `output`,
/// or [`TesterError::ValueMismatch`] if the values in an immutable column have been altered.
pub fn verify_batch_immutability(
    input: &RecordBatch,
    output: &RecordBatch,
) -> std::result::Result<(), TesterError> {
    let in_schema = input.schema();
    let out_schema = output.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        if let (Ok(i_idx), Ok(o_idx)) =
            (in_schema.index_of(col_name), out_schema.index_of(col_name))
        {
            let in_col = input.column(i_idx);
            let out_col = output.column(o_idx);
            if in_col != out_col {
                return Err(TesterError::ValueMismatch(col_name));
            }
        } else if in_schema.index_of(col_name).is_ok() {
            return Err(TesterError::MissingColumn(col_name));
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
    anyhow::bail!("Not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_verify_batch_immutability_value_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )]));
        let in_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["trace_1"]))],
        )
        .expect("in batch");
        let out_batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["trace_2"]))])
                .expect("out batch");

        let err = verify_batch_immutability(&in_batch, &out_batch).expect_err("should fail");
        assert_eq!(err, TesterError::ValueMismatch("trace_id"));
    }

    #[test]
    fn test_verify_batch_immutability_missing_column() {
        let in_schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
        ]));
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            false,
        )]));
        let in_batch = RecordBatch::try_new(
            in_schema,
            vec![
                Arc::new(StringArray::from(vec!["trace_1"])),
                Arc::new(StringArray::from(vec!["span_1"])),
            ],
        )
        .expect("in batch");
        let out_batch = RecordBatch::try_new(
            out_schema,
            vec![Arc::new(StringArray::from(vec!["span_1"]))],
        )
        .expect("out batch");

        let err = verify_batch_immutability(&in_batch, &out_batch).expect_err("should fail");
        assert_eq!(err, TesterError::MissingColumn("trace_id"));
    }
}
