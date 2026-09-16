use crate::error::PipelineError;
use arrow::compute::{SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Sort direction for order-by specifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    /// Ascending sort order (smallest to largest).
    #[default]
    Asc,
    /// Descending sort order (largest to smallest).
    Desc,
}

/// Placement of NULL values in sorted outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NullsPosition {
    /// Place NULL values before non-null values.
    First,
    /// Place NULL values after non-null values.
    #[default]
    Last,
}

/// Action to take when a specified sort column does not exist in the batch schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MissingColumnAction {
    /// Silently omit the missing column and continue sorting on remaining columns.
    #[default]
    Skip,
    /// Return an error and abort batch processing.
    Error,
}

/// A sort column definition supporting both shorthand string and structured table syntax.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortColumnDef {
    /// Shorthand SQL-like string, e.g. `"service_name ASC NULLS LAST"`.
    Shorthand(String),
    /// Structured representation specifying column name, direction, and null placement.
    Structured {
        /// Name of the column to sort on.
        column: String,
        /// Direction of sort order (defaults to `Asc`).
        #[serde(default)]
        direction: SortDirection,
        /// Placement of null values (defaults to `Last`).
        #[serde(default)]
        nulls: NullsPosition,
    },
}

/// Configuration specifying pre-sort columns for telemetry signal types.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortConfig {
    /// Behavior when a designated sort column is missing from the record batch.
    #[serde(default)]
    pub on_missing_column: MissingColumnAction,
    /// Sort column definitions for log record batches.
    #[serde(default)]
    pub logs: Vec<SortColumnDef>,
    /// Sort column definitions for metric record batches.
    #[serde(default)]
    pub metrics: Vec<SortColumnDef>,
    /// Sort column definitions for trace record batches.
    #[serde(default)]
    pub traces: Vec<SortColumnDef>,
}

/// Pre-parsed, normalized sort column with Arrow-native `SortOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSortKey {
    /// Target column name.
    pub column: String,
    /// Arrow compute sort options (descending flag and nulls placement).
    pub options: SortOptions,
}

/// Parses shorthand strings like `"service_name ASC NULLS LAST"` into a `NormalizedSortKey`.
///
/// # Errors
/// Returns `PipelineError::Internal` if the string is empty or contains unrecognised tokens.
pub fn parse_shorthand(input: &str) -> Result<NormalizedSortKey, PipelineError> {
    let tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        return Err(PipelineError::Internal(
            "Sort column shorthand cannot be empty".to_string(),
        ));
    }

    let column = tokens[0].to_string();
    let mut descending = false;
    let mut nulls_first = false;

    let mut idx = 1;
    while idx < tokens.len() {
        match tokens[idx].to_ascii_uppercase().as_str() {
            "ASC" => {
                descending = false;
                idx += 1;
            }
            "DESC" => {
                descending = true;
                idx += 1;
            }
            "NULLS" => {
                if idx + 1 >= tokens.len() {
                    return Err(PipelineError::Internal(format!(
                        "Expected 'FIRST' or 'LAST' after 'NULLS' in shorthand: '{input}'"
                    )));
                }
                match tokens[idx + 1].to_ascii_uppercase().as_str() {
                    "FIRST" => nulls_first = true,
                    "LAST" => nulls_first = false,
                    other => {
                        return Err(PipelineError::Internal(format!(
                            "Invalid NULLS option '{other}' in shorthand: '{input}'"
                        )));
                    }
                }
                idx += 2;
            }
            other => {
                return Err(PipelineError::Internal(format!(
                    "Unexpected token '{other}' in sort column shorthand: '{input}'"
                )));
            }
        }
    }

    Ok(NormalizedSortKey {
        column,
        options: SortOptions {
            descending,
            nulls_first,
        },
    })
}

/// Distinguishes the incoming telemetry signal stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    /// OpenTelemetry Log telemetry.
    Logs,
    /// OpenTelemetry Metric telemetry.
    Metrics,
    /// OpenTelemetry Trace telemetry.
    Traces,
}

/// Centralized pre-sorting engine for Arrow `RecordBatches`.
#[derive(Debug, Clone, Default)]
pub struct BatchSorter {
    on_missing_column: MissingColumnAction,
    logs_keys: Vec<NormalizedSortKey>,
    metrics_keys: Vec<NormalizedSortKey>,
    traces_keys: Vec<NormalizedSortKey>,
}

impl BatchSorter {
    /// Creates a new `BatchSorter` from a `SortConfig`.
    ///
    /// # Errors
    /// Returns `PipelineError::Internal` if shorthand parsing fails.
    pub fn from_config(config: &SortConfig) -> Result<Self, PipelineError> {
        Ok(Self {
            on_missing_column: config.on_missing_column,
            logs_keys: normalize_column_defs(&config.logs)?,
            metrics_keys: normalize_column_defs(&config.metrics)?,
            traces_keys: normalize_column_defs(&config.traces)?,
        })
    }

    /// Sorts an incoming Arrow `RecordBatch` according to the configured signal sort keys.
    ///
    /// # Errors
    /// Returns `PipelineError::Arrow` if lexsort or projection fails, or
    /// `PipelineError::Internal` if a required column is missing and `on_missing_column = Error`.
    pub fn sort(
        &self,
        batch: &RecordBatch,
        signal: SignalType,
    ) -> Result<RecordBatch, PipelineError> {
        self.sort_with_extra_lead_column(batch, signal, None)
    }

    /// Sorts an incoming Arrow `RecordBatch`, optionally prepending a lead column (such as
    /// a Kafka partition key) to the sort tuple.
    ///
    /// # Errors
    /// Returns `PipelineError::Arrow` if compute operations fail, or
    /// `PipelineError::Internal` if a required column is missing and `on_missing_column = Error`.
    pub fn sort_with_extra_lead_column(
        &self,
        batch: &RecordBatch,
        signal: SignalType,
        lead_column: Option<&str>,
    ) -> Result<RecordBatch, PipelineError> {
        if batch.num_rows() <= 1 {
            return Ok(batch.clone());
        }

        let keys = match signal {
            SignalType::Logs => &self.logs_keys,
            SignalType::Metrics => &self.metrics_keys,
            SignalType::Traces => &self.traces_keys,
        };

        let total_capacity = keys.len() + usize::from(lead_column.is_some());
        let mut sort_cols = Vec::with_capacity(total_capacity);

        if let Some(lead) = lead_column {
            if let Some(col) = batch.column_by_name(lead) {
                sort_cols.push(SortColumn {
                    values: Arc::clone(col),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                });
            } else {
                match self.on_missing_column {
                    MissingColumnAction::Error => {
                        return Err(PipelineError::Internal(format!(
                            "Missing configured lead sort column '{lead}'"
                        )));
                    }
                    MissingColumnAction::Skip => {
                        tracing::debug!(
                            column = %lead,
                            "Lead sort column not present in RecordBatch schema; skipping"
                        );
                    }
                }
            }
        }

        for key in keys {
            if let Some(col) = batch.column_by_name(&key.column) {
                sort_cols.push(SortColumn {
                    values: Arc::clone(col),
                    options: Some(key.options),
                });
            } else {
                match self.on_missing_column {
                    MissingColumnAction::Error => {
                        return Err(PipelineError::Internal(format!(
                            "Missing configured sort column '{}'",
                            key.column
                        )));
                    }
                    MissingColumnAction::Skip => {
                        tracing::debug!(
                            column = %key.column,
                            "Sort column not present in RecordBatch schema; skipping"
                        );
                    }
                }
            }
        }

        if sort_cols.is_empty() {
            return Ok(batch.clone());
        }

        let indices =
            arrow::compute::lexsort_to_indices(&sort_cols, None).map_err(PipelineError::Arrow)?;

        let columns = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(PipelineError::Arrow)?;

        RecordBatch::try_new(batch.schema(), columns).map_err(PipelineError::Arrow)
    }
}

fn normalize_column_defs(defs: &[SortColumnDef]) -> Result<Vec<NormalizedSortKey>, PipelineError> {
    let mut keys = Vec::with_capacity(defs.len());
    for def in defs {
        match def {
            SortColumnDef::Shorthand(s) => keys.push(parse_shorthand(s)?),
            SortColumnDef::Structured {
                column,
                direction,
                nulls,
            } => {
                keys.push(NormalizedSortKey {
                    column: column.clone(),
                    options: SortOptions {
                        descending: matches!(direction, SortDirection::Desc),
                        nulls_first: matches!(nulls, NullsPosition::First),
                    },
                });
            }
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_shorthand_valid() {
        let key = parse_shorthand("service_name ASC").expect("should parse");
        assert_eq!(key.column, "service_name");
        assert!(!key.options.descending);
        assert!(!key.options.nulls_first);

        let key2 = parse_shorthand("timestamp DESC NULLS FIRST").expect("should parse");
        assert_eq!(key2.column, "timestamp");
        assert!(key2.options.descending);
        assert!(key2.options.nulls_first);

        let key3 = parse_shorthand("  latency_ms   nulls   last  ").expect("should parse");
        assert_eq!(key3.column, "latency_ms");
        assert!(!key3.options.descending);
        assert!(!key3.options.nulls_first);

        let key4 = parse_shorthand("column_only").expect("should parse");
        assert_eq!(key4.column, "column_only");
        assert!(!key4.options.descending);
        assert!(!key4.options.nulls_first);
    }

    #[test]
    fn test_parse_shorthand_invalid() {
        assert!(parse_shorthand("").is_err());
        assert!(parse_shorthand("   ").is_err());
        assert!(parse_shorthand("service_name ASC EXTRA").is_err());
        assert!(parse_shorthand("service_name INVALID").is_err());
        assert!(parse_shorthand("service_name NULLS").is_err());
        assert!(parse_shorthand("service_name NULLS INVALID").is_err());
    }

    #[test]
    fn test_deserialize_dual_syntax_toml() {
        let toml_str = r#"
            on_missing_column = "error"
            logs = ["service_name ASC", "timestamp DESC"]
            [[metrics]]
            column = "metric_name"
            direction = "desc"
            nulls = "first"
        "#;
        let config: SortConfig = toml::from_str(toml_str).expect("should deserialize");
        assert_eq!(config.on_missing_column, MissingColumnAction::Error);
        assert_eq!(config.logs.len(), 2);
        assert_eq!(
            config.logs[0],
            SortColumnDef::Shorthand("service_name ASC".to_string())
        );
        assert_eq!(
            config.logs[1],
            SortColumnDef::Shorthand("timestamp DESC".to_string())
        );
        assert_eq!(config.metrics.len(), 1);
        assert_eq!(
            config.metrics[0],
            SortColumnDef::Structured {
                column: "metric_name".to_string(),
                direction: SortDirection::Desc,
                nulls: NullsPosition::First,
            }
        );
        assert!(config.traces.is_empty());
    }

    #[test]
    fn test_deserialize_default_config() {
        let config: SortConfig = toml::from_str("").expect("should deserialize empty toml");
        assert_eq!(config.on_missing_column, MissingColumnAction::Skip);
        assert!(config.logs.is_empty());
        assert!(config.metrics.is_empty());
        assert!(config.traces.is_empty());
    }

    #[test]
    fn test_deserialize_structured_defaults() {
        let toml_str = r#"
            [[traces]]
            column = "trace_id"
        "#;
        let config: SortConfig = toml::from_str(toml_str).expect("should deserialize");
        assert_eq!(config.traces.len(), 1);
        assert_eq!(
            config.traces[0],
            SortColumnDef::Structured {
                column: "trace_id".to_string(),
                direction: SortDirection::Asc,
                nulls: NullsPosition::Last,
            }
        );
    }

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int64, true),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let service = Arc::new(StringArray::from(vec![
            Some("frontend"),
            Some("backend"),
            Some("frontend"),
            Some("backend"),
        ]));
        let severity = Arc::new(Int64Array::from(vec![Some(9), Some(13), Some(5), Some(9)]));
        let timestamp = Arc::new(Int64Array::from(vec![100, 200, 150, 50]));

        RecordBatch::try_new(schema, vec![service, severity, timestamp]).unwrap()
    }

    #[test]
    fn test_batch_sorter_multi_column_sort() {
        let config = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![
                SortColumnDef::Shorthand("service_name ASC".to_string()),
                SortColumnDef::Shorthand("severity_number ASC".to_string()),
            ],
            metrics: vec![],
            traces: vec![],
        };

        let sorter = BatchSorter::from_config(&config).expect("sorter init");
        let batch = make_test_batch();
        let sorted = sorter.sort(&batch, SignalType::Logs).expect("sort ok");

        let service_col = sorted
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let severity_col = sorted
            .column_by_name("severity_number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(service_col.value(0), "backend");
        assert_eq!(severity_col.value(0), 9);
        assert_eq!(service_col.value(1), "backend");
        assert_eq!(severity_col.value(1), 13);
        assert_eq!(service_col.value(2), "frontend");
        assert_eq!(severity_col.value(2), 5);
        assert_eq!(service_col.value(3), "frontend");
        assert_eq!(severity_col.value(3), 9);
    }

    #[test]
    fn test_batch_sorter_empty_and_single_row() {
        let config = SortConfig::default();
        let sorter = BatchSorter::from_config(&config).expect("sorter init");
        let batch = make_test_batch();
        let empty = batch.slice(0, 0);
        let single = batch.slice(0, 1);

        assert_eq!(sorter.sort(&empty, SignalType::Logs).unwrap().num_rows(), 0);
        assert_eq!(
            sorter.sort(&single, SignalType::Logs).unwrap().num_rows(),
            1
        );

        // Verify bypass occurs before missing column evaluation when on_missing_column is Error
        let config_missing_err = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Shorthand("non_existent ASC".to_string())],
            ..Default::default()
        };
        let sorter_err = BatchSorter::from_config(&config_missing_err).unwrap();
        assert_eq!(
            sorter_err
                .sort(&empty, SignalType::Logs)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            sorter_err
                .sort(&single, SignalType::Logs)
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            sorter_err
                .sort_with_extra_lead_column(&empty, SignalType::Logs, Some("missing_lead"))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            sorter_err
                .sort_with_extra_lead_column(&single, SignalType::Logs, Some("missing_lead"))
                .unwrap()
                .num_rows(),
            1
        );
    }

    #[test]
    fn test_batch_sorter_missing_column_skip_and_error() {
        let config_skip = SortConfig {
            on_missing_column: MissingColumnAction::Skip,
            logs: vec![SortColumnDef::Shorthand("non_existent ASC".to_string())],
            ..Default::default()
        };
        let sorter_skip = BatchSorter::from_config(&config_skip).unwrap();
        let batch = make_test_batch();
        assert!(sorter_skip.sort(&batch, SignalType::Logs).is_ok());

        let config_err = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Shorthand("non_existent ASC".to_string())],
            ..Default::default()
        };
        let sorter_err = BatchSorter::from_config(&config_err).unwrap();
        assert!(sorter_err.sort(&batch, SignalType::Logs).is_err());
    }

    #[test]
    fn test_batch_sorter_with_lead_column() {
        let config = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Shorthand("timestamp DESC".to_string())],
            ..Default::default()
        };
        let sorter = BatchSorter::from_config(&config).unwrap();
        let batch = make_test_batch();

        // Lead column "service_name" ASC, then "timestamp" DESC
        let sorted = sorter
            .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("service_name"))
            .expect("sort with lead column ok");

        let service_col = sorted
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ts_col = sorted
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // "backend" with ts 200, then 50; "frontend" with ts 150, then 100
        assert_eq!(service_col.value(0), "backend");
        assert_eq!(ts_col.value(0), 200);
        assert_eq!(service_col.value(1), "backend");
        assert_eq!(ts_col.value(1), 50);
        assert_eq!(service_col.value(2), "frontend");
        assert_eq!(ts_col.value(2), 150);
        assert_eq!(service_col.value(3), "frontend");
        assert_eq!(ts_col.value(3), 100);

        // Missing lead column with Error
        let err = sorter
            .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("missing_col"))
            .unwrap_err();
        assert!(err.to_string().contains("missing_col"));

        // Missing lead column with Skip
        let config_skip = SortConfig {
            on_missing_column: MissingColumnAction::Skip,
            logs: vec![SortColumnDef::Shorthand("timestamp ASC".to_string())],
            ..Default::default()
        };
        let sorter_skip = BatchSorter::from_config(&config_skip).unwrap();
        let sorted_skip = sorter_skip
            .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("missing_col"))
            .expect("missing lead skipped");
        let ts_col_skip = sorted_skip
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col_skip.value(0), 50);
        assert_eq!(ts_col_skip.value(3), 200);
    }

    #[test]
    fn test_batch_sorter_signal_routing() {
        let config = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Shorthand("service_name ASC".to_string())],
            metrics: vec![SortColumnDef::Shorthand("severity_number ASC".to_string())],
            traces: vec![SortColumnDef::Shorthand("timestamp ASC".to_string())],
        };
        let sorter = BatchSorter::from_config(&config).unwrap();
        let batch = make_test_batch();

        let sorted_metrics = sorter.sort(&batch, SignalType::Metrics).unwrap();
        let severity = sorted_metrics
            .column_by_name("severity_number")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(severity.value(0), 5);
        assert_eq!(severity.value(1), 9);
        assert_eq!(severity.value(2), 9);
        assert_eq!(severity.value(3), 13);

        let sorted_traces = sorter.sort(&batch, SignalType::Traces).unwrap();
        let ts = sorted_traces
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 50);
        assert_eq!(ts.value(1), 100);
        assert_eq!(ts.value(2), 150);
        assert_eq!(ts.value(3), 200);
    }

    #[test]
    fn test_batch_sorter_structured_and_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let val_array = Arc::new(Int64Array::from(vec![Some(10), None, Some(5)]));
        let batch = RecordBatch::try_new(schema, vec![val_array]).unwrap();

        // Nulls last (Ascending)
        let config_nulls_last = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Structured {
                column: "value".to_string(),
                direction: SortDirection::Asc,
                nulls: NullsPosition::Last,
            }],
            ..Default::default()
        };
        let sorter_last = BatchSorter::from_config(&config_nulls_last).unwrap();
        let sorted_last = sorter_last.sort(&batch, SignalType::Logs).unwrap();
        let col_last = sorted_last
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col_last.value(0), 5);
        assert_eq!(col_last.value(1), 10);
        assert!(col_last.is_null(2));

        // Nulls first (Ascending)
        let config_nulls_first = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Structured {
                column: "value".to_string(),
                direction: SortDirection::Asc,
                nulls: NullsPosition::First,
            }],
            ..Default::default()
        };
        let sorter_first = BatchSorter::from_config(&config_nulls_first).unwrap();
        let sorted_first = sorter_first.sort(&batch, SignalType::Logs).unwrap();
        let col_first = sorted_first
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(col_first.is_null(0));
        assert_eq!(col_first.value(1), 5);
        assert_eq!(col_first.value(2), 10);
    }

    #[test]
    fn test_batch_sorter_no_keys_configured() {
        let sorter = BatchSorter::default();
        let batch = make_test_batch();
        let sorted = sorter.sort(&batch, SignalType::Logs).unwrap();
        assert_eq!(sorted.num_rows(), batch.num_rows());
    }

    #[test]
    fn test_batch_sorter_from_config_invalid() {
        let config = SortConfig {
            logs: vec![SortColumnDef::Shorthand(
                "bad syntax here invalid".to_string(),
            )],
            ..Default::default()
        };
        assert!(BatchSorter::from_config(&config).is_err());
    }

    #[test]
    fn test_batch_sorter_lead_column_nulls_placed_last() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("lead", DataType::Utf8, true),
            Field::new("val", DataType::Int64, true),
        ]));
        let lead_array = Arc::new(StringArray::from(vec![Some("b"), None, Some("a")]));
        let val_array = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)]));
        let batch = RecordBatch::try_new(schema, vec![lead_array, val_array]).unwrap();

        let config = SortConfig::default();
        let sorter = BatchSorter::from_config(&config).unwrap();
        let sorted = sorter
            .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("lead"))
            .expect("sort with lead column ok");

        let lead_out = sorted
            .column_by_name("lead")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val_out = sorted
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // "a" (val 3), then "b" (val 1), then NULL (val 2)
        assert_eq!(lead_out.value(0), "a");
        assert_eq!(val_out.value(0), 3);
        assert_eq!(lead_out.value(1), "b");
        assert_eq!(val_out.value(1), 1);
        assert!(lead_out.is_null(2));
        assert_eq!(val_out.value(2), 2);
    }
}
