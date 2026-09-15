use crate::error::PipelineError;
use arrow::compute::SortOptions;
use serde::{Deserialize, Serialize};

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
}
