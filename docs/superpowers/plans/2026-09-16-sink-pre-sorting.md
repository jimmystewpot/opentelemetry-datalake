# Pre-Sorted Sink Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a centralized, zero-copy, zero-panic pre-sorting engine (`BatchSorter`) in `pipeline-core` with dual-syntax configuration, integrating it into `StarRocksSink` (with optional batch accumulation), `IcebergSink` (refactored for backward compatibility), and `KafkaSink` (with zero-copy partition slicing), verified through end-to-end integration tests and Criterion micro-benchmarks.

**Architecture:** A unified `BatchSorter` in `pipeline-core::sort` parses sorting rules once during sink initialization from either SQL shorthand strings or structured TOML definitions. Sinks call `BatchSorter::sort` on incoming Arrow `RecordBatch` instances before write/stream operations. `KafkaSink` leverages leading partition keys and `RecordBatch::slice` for zero-copy partition routing, while `StarRocksSink` supports optional time- and byte-bounded batch consolidation.

**Tech Stack:** Rust 2021, Apache Arrow 58.3/58.4 (`RecordBatch`, `lexsort_to_indices`, `take`, `slice`), Tokio 1.x, Serde, Figment, RdKafka, StarRocks Stream Load SDK, Iceberg-Rust, Criterion.

**Spec:** `docs/superpowers/specs/2026-09-16-sink-pre-sorting-design.md`

## Global Constraints

- Strict zero-panic policy in production code (`unwrap()`, `expect()`, `panic!()` prohibited in `src/` files).
- Arrow errors mapped strictly to `PipelineError::Arrow` or `PipelineError::Internal`.
- Bypasses for empty (`batch.num_rows() == 0`) and single-row (`batch.num_rows() == 1`) batches to prevent unnecessary allocation.
- In `KafkaSink`, partition batch splitting must use zero-copy `RecordBatch::slice(offset, length)` instead of allocating new vectors.
- Iceberg sink must maintain 100% backward compatibility when `order_by` is omitted from configuration.
- Clippy must pass cleanly: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`.
- Cargo formatting must be verified: `cargo fmt --all -- --check`.

---

### Task 1: Core Sort Configuration Types & Dual-Syntax Deserializer

**Files:**
- Create: `crates/core/src/sort.rs`
- Modify: `crates/core/src/lib.rs:1-5`
- Test: `crates/core/src/sort.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `pipeline_core::error::PipelineError`, `serde::{Deserialize, Serialize}`
- Produces:
  - `pub enum SortDirection { Asc, Desc }`
  - `pub enum NullsPosition { First, Last }`
  - `pub enum MissingColumnAction { Skip, Error }`
  - `pub enum SortColumnDef { Shorthand(String), Structured { column: String, direction: SortDirection, nulls: NullsPosition } }`
  - `pub struct SortConfig { on_missing_column: MissingColumnAction, logs: Vec<SortColumnDef>, metrics: Vec<SortColumnDef>, traces: Vec<SortColumnDef> }`
  - `pub struct NormalizedSortKey { pub column: String, pub options: arrow::compute::SortOptions }`
  - `pub fn parse_shorthand(input: &str) -> Result<NormalizedSortKey, PipelineError>`

- [ ] **Step 1: Write the failing unit tests for sort config parsing and deserialization**

In `crates/core/src/sort.rs`:
```rust
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
    }

    #[test]
    fn test_parse_shorthand_invalid() {
        assert!(parse_shorthand("").is_err());
        assert!(parse_shorthand("service_name ASC EXTRA").is_err());
        assert!(parse_shorthand("service_name INVALID").is_err());
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
        assert_eq!(config.metrics.len(), 1);
        assert!(config.traces.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pipeline-core test_parse_shorthand`
Expected: FAIL (module not declared or function not found).

- [ ] **Step 3: Implement minimal configuration structures and shorthand parser**

Create `crates/core/src/sort.rs`:
```rust
use crate::error::PipelineError;
use arrow::compute::SortOptions;
use serde::{Deserialize, Serialize};

/// Sort direction for order-by specifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

/// Placement of NULL values in sorted outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NullsPosition {
    First,
    #[default]
    Last,
}

/// Action to take when a specified sort column does not exist in the batch schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MissingColumnAction {
    #[default]
    Skip,
    Error,
}

/// A sort column definition supporting both shorthand string and structured table syntax.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortColumnDef {
    Shorthand(String),
    Structured {
        column: String,
        #[serde(default)]
        direction: SortDirection,
        #[serde(default)]
        nulls: NullsPosition,
    },
}

/// Configuration specifying pre-sort columns for telemetry signal types.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortConfig {
    #[serde(default)]
    pub on_missing_column: MissingColumnAction,
    #[serde(default)]
    pub logs: Vec<SortColumnDef>,
    #[serde(default)]
    pub metrics: Vec<SortColumnDef>,
    #[serde(default)]
    pub traces: Vec<SortColumnDef>,
}

/// Pre-parsed, normalized sort column with Arrow-native SortOptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSortKey {
    pub column: String,
    pub options: SortOptions,
}

/// Parses shorthand strings like "service_name ASC NULLS LAST" into a `NormalizedSortKey`.
///
/// # Errors
/// Returns `PipelineError::Internal` if the string is empty or contains unrecognised tokens.
pub fn parse_shorthand(input: &str) -> Result<NormalizedSortKey, PipelineError> {
    let tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        return Err(PipelineError::Internal("Sort column shorthand cannot be empty".to_string()));
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
```

In `crates/core/src/lib.rs`, append:
```rust
pub mod sort;
```

In `crates/core/Cargo.toml`, add `toml` under `[dev-dependencies]`:
```toml
toml = { workspace = true }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pipeline-core test_parse_shorthand && cargo test -p pipeline-core test_deserialize_dual_syntax_toml`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/sort.rs crates/core/src/lib.rs crates/core/Cargo.toml
git commit -m "feat(core): add sort configuration models and shorthand parser"
```

---

### Task 2: Core `BatchSorter` Engine Implementation

**Files:**
- Modify: `crates/core/src/sort.rs`
- Test: `crates/core/src/sort.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `NormalizedSortKey`, `SortConfig`, `SortColumnDef`, `arrow::record_batch::RecordBatch`, `arrow::compute::lexsort_to_indices`, `arrow::compute::take`
- Produces:
  - `pub enum SignalType { Logs, Metrics, Traces }`
  - `pub struct BatchSorter`
  - `BatchSorter::from_config(config: &SortConfig) -> Result<Self, PipelineError>`
  - `BatchSorter::sort(&self, batch: &RecordBatch, signal: SignalType) -> Result<RecordBatch, PipelineError>`
  - `BatchSorter::sort_with_extra_lead_column(&self, batch: &RecordBatch, signal: SignalType, lead_column: Option<&str>) -> Result<RecordBatch, PipelineError>`

- [ ] **Step 1: Write the failing unit tests for `BatchSorter`**

In `crates/core/src/sort.rs` `mod tests`:
```rust
    use arrow::array::{Int64Array, StringArray};
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
        assert_eq!(sorter.sort(&single, SignalType::Logs).unwrap().num_rows(), 1);
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p pipeline-core test_batch_sorter`
Expected: FAIL (BatchSorter and SignalType not yet implemented).

- [ ] **Step 3: Implement `BatchSorter` in `crates/core/src/sort.rs`**

In `crates/core/src/sort.rs`:
```rust
use std::sync::Arc;
use arrow::record_batch::RecordBatch;

/// Distinguishes the incoming telemetry signal stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Logs,
    Metrics,
    Traces,
}

/// Centralized pre-sorting engine for Arrow RecordBatches.
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
        let normalize = |defs: &[SortColumnDef]| -> Result<Vec<NormalizedSortKey>, PipelineError> {
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
        };

        Ok(Self {
            on_missing_column: config.on_missing_column,
            logs_keys: normalize(&config.logs)?,
            metrics_keys: normalize(&config.metrics)?,
            traces_keys: normalize(&config.traces)?,
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
    /// Returns `PipelineError::Arrow` if compute operations fail.
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
                sort_cols.push(arrow::compute::SortColumn {
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
                sort_cols.push(arrow::compute::SortColumn {
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

        let indices = arrow::compute::lexsort_to_indices(&sort_cols, None)
            .map_err(PipelineError::Arrow)?;

        let columns = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(PipelineError::Arrow)?;

        RecordBatch::try_new(batch.schema(), columns).map_err(PipelineError::Arrow)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p pipeline-core test_batch_sorter`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/core/src/sort.rs
git commit -m "feat(core): implement BatchSorter with lexsort and missing column handling"
```

---

### Task 3: Iceberg Sink Refactoring to Unified `BatchSorter`

**Files:**
- Modify: `crates/storage/src/iceberg.rs`
- Test: `crates/storage/src/iceberg.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `pipeline_core::sort::{BatchSorter, SortConfig, SortColumnDef, SignalType}`
- Produces:
  - `IcebergSinkConfig::order_by: Option<SortConfig>`
  - `IcebergSink::sorter: BatchSorter`
  - Fully backward-compatible sort behavior when `order_by` is None.

- [ ] **Step 1: Write the failing unit tests for Iceberg custom order_by**

In `crates/storage/src/iceberg.rs` `mod tests`:
```rust
    #[test]
    fn test_iceberg_sink_custom_order_by() {
        let mut cfg = IcebergSinkConfig::default();
        cfg.order_by = Some(pipeline_core::sort::SortConfig {
            logs: vec![
                pipeline_core::sort::SortColumnDef::Shorthand("timestamp DESC".to_string()),
            ],
            ..Default::default()
        });
        let sink = IcebergSink::new(cfg);

        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["svc", "svc"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        ).unwrap();

        let sorted = sink.sort_logs(&batch).expect("should sort");
        let ts = sorted.column_by_name("timestamp").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ts.value(0), 20);
        assert_eq!(ts.value(1), 10);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p storage test_iceberg_sink_custom_order_by`
Expected: FAIL (field `order_by` not found in `IcebergSinkConfig`).

- [ ] **Step 3: Refactor `IcebergSink` to use `BatchSorter`**

In `crates/storage/src/iceberg.rs`:
1. Add `order_by: Option<pipeline_core::sort::SortConfig>` to `IcebergSinkConfig`:
```rust
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct IcebergSinkConfig {
    // ... existing fields ...
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,
}
```
2. Add `sorter: pipeline_core::sort::BatchSorter` to `IcebergSink`.
3. In `IcebergSink::new`, initialize `sorter`:
```rust
let sorter = if let Some(ref sort_cfg) = config.order_by {
    pipeline_core::sort::BatchSorter::from_config(sort_cfg)
        .unwrap_or_default()
} else {
    // Default legacy Iceberg sort configuration
    let default_cfg = pipeline_core::sort::SortConfig {
        on_missing_column: pipeline_core::sort::MissingColumnAction::Error,
        logs: vec![
            pipeline_core::sort::SortColumnDef::Shorthand("service_name ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("severity_text ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        metrics: vec![
            pipeline_core::sort::SortColumnDef::Shorthand("service_name ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("name ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("attributes ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        traces: vec![
            pipeline_core::sort::SortColumnDef::Shorthand("service_name ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("name ASC".to_string()),
            pipeline_core::sort::SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
    };
    pipeline_core::sort::BatchSorter::from_config(&default_cfg).unwrap_or_default()
};
```
4. Refactor `sort_logs`, `sort_metrics`, and `sort_traces` to delegate directly to `self.sorter.sort`:
```rust
    pub fn sort_logs(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, pipeline_core::sort::SignalType::Logs)
    }

    pub fn sort_metrics(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, pipeline_core::sort::SignalType::Metrics)
    }

    pub fn sort_traces(&self, batch: &RecordBatch) -> Result<RecordBatch, PipelineError> {
        self.sorter.sort(batch, pipeline_core::sort::SignalType::Traces)
    }
```
5. Remove private redundant `sort_batch` helper.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p storage`
Expected: ALL PASS (including existing legacy tests and new custom order_by test).

- [ ] **Step 5: Commit**

```bash
git add crates/storage/src/iceberg.rs
git commit -m "refactor(storage): integrate BatchSorter into IcebergSink with backward-compatible defaults"
```

---

### Task 4: StarRocks Sink Pre-Sorting Integration

**Files:**
- Modify: `crates/starrocks-sink/src/lib.rs`
- Test: `crates/starrocks-sink/src/lib.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `pipeline_core::sort::{BatchSorter, SortConfig, SignalType}`
- Produces:
  - `StarRocksSinkConfig::order_by: Option<SortConfig>`
  - `StarRocksSink::sorter: BatchSorter`
  - Pre-sorted payloads emitted via `serialize_batch` in `StarRocksSink::run`.

- [ ] **Step 1: Write the failing unit tests for StarRocksSink pre-sorting**

In `crates/starrocks-sink/src/lib.rs` `mod tests`:
```rust
    #[test]
    fn test_starrocks_sink_pre_sorted_serialization() {
        let mut config = make_config();
        config.order_by = Some(pipeline_core::sort::SortConfig {
            logs: vec![pipeline_core::sort::SortColumnDef::Shorthand("id DESC".to_string())],
            ..Default::default()
        });
        let sink = StarRocksSink::try_new(config).expect("try_new failed");

        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 3, 2])),
                Arc::new(StringArray::from(vec!["a", "c", "b"])),
            ],
        ).unwrap();

        let sorted = sink.sorter().sort(&batch, pipeline_core::sort::SignalType::Logs).unwrap();
        let id_col = sorted.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_col.value(0), 3);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p starrocks-sink test_starrocks_sink_pre_sorted_serialization`
Expected: FAIL (no `order_by` field or `sorter` method on `StarRocksSink`).

- [ ] **Step 3: Integrate `BatchSorter` into `StarRocksSink`**

In `crates/starrocks-sink/src/lib.rs`:
1. Add `order_by: Option<pipeline_core::sort::SortConfig>` to `StarRocksSinkConfig`:
```rust
    /// Optional row order configuration for incoming signal batches.
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,
```
2. In `StarRocksSink`, add `sorter: pipeline_core::sort::BatchSorter`.
3. In `StarRocksSink::try_new` and `with_manager`:
```rust
let sorter = if let Some(ref sort_cfg) = config.order_by {
    pipeline_core::sort::BatchSorter::from_config(sort_cfg)?
} else {
    pipeline_core::sort::BatchSorter::default()
};
```
4. Expose `pub fn sorter(&self) -> &pipeline_core::sort::BatchSorter { &self.sorter }`.
5. In `Sink::run`:
```rust
            let signal_type_enum = match &signal {
                SignalBatch::Logs(_) => pipeline_core::sort::SignalType::Logs,
                SignalBatch::Metrics(_) => pipeline_core::sort::SignalType::Metrics,
                SignalBatch::Traces(_) => pipeline_core::sort::SignalType::Traces,
            };

            let batch = self.sorter.sort(&batch, signal_type_enum)?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p starrocks-sink`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/starrocks-sink/src/lib.rs
git commit -m "feat(starrocks): integrate BatchSorter into StarRocksSink"
```

---

### Task 5: StarRocks Sink Batch Accumulation & Timed/Sized Flushing

**Files:**
- Modify: `crates/starrocks-sink/src/lib.rs`
- Test: `crates/starrocks-sink/src/lib.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `arrow::compute::concat_batches`, `tokio::time::interval`
- Produces:
  - `pub struct StarRocksBatchingConfig { max_batch_size_bytes: usize, max_batch_interval_sec: u64, max_batch_records: Option<usize> }`
  - `StarRocksSinkConfig::batching: Option<StarRocksBatchingConfig>`
  - Buffer accumulation and flushing loop in `StarRocksSink::run`.

- [ ] **Step 1: Write the failing unit tests for batch accumulation**

In `crates/starrocks-sink/src/lib.rs` `mod tests`:
```rust
    #[tokio::test]
    async fn test_starrocks_batch_accumulation_flush() {
        let mut config = make_config();
        config.batching = Some(StarRocksBatchingConfig {
            max_batch_size_bytes: 1024 * 1024,
            max_batch_interval_sec: 1,
            max_batch_records: Some(4),
        });
        // Verify batching config deserialization and fields
        assert_eq!(config.batching.as_ref().unwrap().max_batch_records, Some(4));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p starrocks-sink test_starrocks_batch_accumulation_flush`
Expected: FAIL (`StarRocksBatchingConfig` not defined).

- [ ] **Step 3: Implement `StarRocksBatchingConfig` and buffered run loop**

In `crates/starrocks-sink/src/lib.rs`:
1. Define batching config:
```rust
fn default_max_batch_size_bytes() -> usize {
    52_428_800 // 50 MiB
}

fn default_max_batch_interval_sec() -> u64 {
    30
}

/// Configuration for buffering and accumulating batches prior to stream loading.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StarRocksBatchingConfig {
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    #[serde(default)]
    pub max_batch_records: Option<usize>,
}
```
2. In `StarRocksSinkConfig`, add:
```rust
    /// Optional batch buffering configuration for accumulating records.
    #[serde(default)]
    pub batching: Option<StarRocksBatchingConfig>,
```
3. Update `Sink::run` to handle batching. Replace the existing run loop logic with a buffer aggregator:
```rust
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        let batching = self.config.batching.clone();
        let max_bytes = batching.as_ref().map(|b| b.max_batch_size_bytes).unwrap_or(0);
        let max_interval = batching.as_ref().map(|b| b.max_batch_interval_sec).unwrap_or(86400);

        let mut buffer = Vec::new();
        let mut buffer_bytes = 0;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(max_interval));
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick(), if batching.is_some() && !buffer.is_empty() => {
                    self.flush_buffer(&mut buffer, &mut buffer_bytes).await?;
                }
                msg = input.recv() => {
                    match msg {
                        Some(signal) => {
                            let (signal_type, batch) = match signal {
                                SignalBatch::Logs(b) => (pipeline_core::sort::SignalType::Logs, b),
                                SignalBatch::Metrics(b) => (pipeline_core::sort::SignalType::Metrics, b),
                                SignalBatch::Traces(b) => (pipeline_core::sort::SignalType::Traces, b),
                            };
                            
                            if batch.num_rows() == 0 { continue; }

                            if batching.is_none() {
                                let sorted = self.sorter.sort(&batch, signal_type)?;
                                self.send_batch(&sorted, signal_type).await?;
                            } else {
                                buffer_bytes += batch.get_array_memory_size();
                                buffer.push((signal_type, batch));

                                if buffer_bytes >= max_bytes {
                                    self.flush_buffer(&mut buffer, &mut buffer_bytes).await?;
                                    interval.reset();
                                }
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                self.flush_buffer(&mut buffer, &mut buffer_bytes).await?;
                            }
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    
    async fn flush_buffer(&mut self, buffer: &mut Vec<(pipeline_core::sort::SignalType, arrow::record_batch::RecordBatch)>, bytes: &mut usize) -> Result<(), PipelineError> {
        if buffer.is_empty() { return Ok(()); }
        
        let schema = buffer[0].1.schema();
        let batches: Vec<&arrow::record_batch::RecordBatch> = buffer.iter().map(|(_, b)| b).collect();
        let combined = arrow::compute::concat_batches(&schema, &batches).map_err(PipelineError::Arrow)?;
        
        let signal_type = buffer[0].0;
        let sorted = self.sorter.sort(&combined, signal_type)?;
        self.send_batch(&sorted, signal_type).await?;
        
        buffer.clear();
        *bytes = 0;
        Ok(())
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p starrocks-sink`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/starrocks-sink/src/lib.rs
git commit -m "feat(starrocks): implement batch accumulation with size and interval flushing"
```

---

### Task 6: Kafka Sink Partition Key & Zero-Copy Slicing Integration

**Files:**
- Modify: `crates/kafka-sink/src/lib.rs`
- Modify: `src/main.rs:30-40,280-320`
- Test: `crates/kafka-sink/src/lib.rs` (inline module `tests`)

**Interfaces:**
- Consumes: `RecordBatch::slice`, `BatchSorter::sort_with_extra_lead_column`
- Produces:
  - `KafkaSink::with_sorting(mut self, sorter: BatchSorter, partition_key: Option<String>) -> Self`
  - Contiguous slice scanning and keyed record emission in `KafkaSink::run`.

- [ ] **Step 1: Write the failing unit tests for Kafka zero-copy slice iteration**

In `crates/kafka-sink/src/lib.rs` `mod tests`:
```rust
    #[test]
    fn test_find_contiguous_partition_slices() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["auth", "auth", "billing", "gateway"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
            ],
        ).unwrap();

        let slices = extract_partition_slices(&batch, "service_name").expect("slices");
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].0, "auth");
        assert_eq!(slices[0].1.num_rows(), 2);
        assert_eq!(slices[1].0, "billing");
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0, "gateway");
        assert_eq!(slices[2].1.num_rows(), 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kafka-sink test_find_contiguous_partition_slices`
Expected: FAIL (`extract_partition_slices` not found).

- [ ] **Step 3: Implement `extract_partition_slices` and update `KafkaSink`**

In `crates/kafka-sink/src/lib.rs`:
1. Implement zero-copy partition slice helper:
```rust
/// Scans a contiguous sorted partition key column and slices the batch into sub-batches.
pub fn extract_partition_slices<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    key_column: &str,
) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, PipelineError> {
    let col = batch
        .column_by_name(key_column)
        .ok_or_else(|| PipelineError::Internal(format!("Missing partition key column '{key_column}'")))?;

    let str_arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| PipelineError::Internal(format!("Partition key column '{key_column}' must be Utf8")))?;

    let mut slices = Vec::new();
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(slices);
    }

    let mut start = 0;
    let mut current_val = str_arr.value(0).to_string();

    for i in 1..num_rows {
        let val = str_arr.value(i);
        if val != current_val {
            slices.push((current_val, batch.slice(start, i - start)));
            start = i;
            current_val = val.to_string();
        }
    }
    slices.push((current_val, batch.slice(start, num_rows - start)));

    Ok(slices)
}
```
2. Add `sorter: pipeline_core::sort::BatchSorter` and `partition_key: Option<String>` to `KafkaSink`.
3. Add builder methods:
```rust
    #[must_use]
    pub fn with_sorting(
        mut self,
        sorter: pipeline_core::sort::BatchSorter,
        partition_key: Option<String>,
    ) -> Self {
        self.sorter = sorter;
        self.partition_key = partition_key;
        self
    }
```
4. Update `Sink::run` to sort with extra lead column and emit keyed sub-batches:
```rust
    let signal_type = match &signal {
        SignalBatch::Logs(_) => pipeline_core::sort::SignalType::Logs,
        SignalBatch::Metrics(_) => pipeline_core::sort::SignalType::Metrics,
        SignalBatch::Traces(_) => pipeline_core::sort::SignalType::Traces,
    };

    let sorted_batch = self.sorter.sort_with_extra_lead_column(
        &batch,
        signal_type,
        self.partition_key.as_deref(),
    )?;

    if let Some(ref p_key) = self.partition_key {
        let slices = extract_partition_slices(&sorted_batch, p_key)?;
        for (key_str, sub_batch) in slices {
            self.serialize_batch(&sub_batch, &mut buffer)?;
            let record = FutureRecord::to(&self.topic).payload(&buffer).key(&key_str);
            self.producer.send(record, tokio::time::Duration::from_secs(5)).await
                .map_err(|(e, _)| PipelineError::Internal(format!("Kafka send error: {e}")))?;
        }
    } else {
        self.serialize_batch(&sorted_batch, &mut buffer)?;
        let record = FutureRecord::to(&self.topic).payload(&buffer).key("");
        self.producer.send(record, tokio::time::Duration::from_secs(5)).await
            .map_err(|(e, _)| PipelineError::Internal(format!("Kafka send error: {e}")))?;
    }
```
5. In `src/main.rs`, update `KafkaConfig` struct to include partition keys and `order_by`:
```rust
#[derive(Debug, Deserialize, Clone)]
struct KafkaConfig {
    brokers: String,
    logs_topic: String,
    traces_topic: String,
    metrics_topic: String,
    logs_format: String,
    traces_format: String,
    metrics_format: String,
    logs_partition_key: Option<String>,
    metrics_partition_key: Option<String>,
    traces_partition_key: Option<String>,
    #[serde(default)]
    order_by: Option<pipeline_core::sort::SortConfig>,
    #[serde(default)]
    options: std::collections::HashMap<String, String>,
}
```
And update the sink initialisation blocks in `src/main.rs` to pass the sorter:
```rust
    let sorter = if let Some(ref sort_cfg) = kafka_cfg.order_by {
        pipeline_core::sort::BatchSorter::from_config(sort_cfg)?
    } else {
        pipeline_core::sort::BatchSorter::default()
    };

    let mut logs_sink = kafka_sink::KafkaSink::try_new(
        &kafka_cfg.brokers,
        &kafka_cfg.logs_topic,
        kafka_cfg.logs_format.parse()?,
        &kafka_cfg.options,
    )?.with_sorting(sorter.clone(), kafka_cfg.logs_partition_key.clone());
    
    // (Apply identical .with_sorting() pattern to metrics_sink and traces_sink)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p kafka-sink`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kafka-sink/src/lib.rs src/main.rs
git commit -m "feat(kafka): implement zero-copy partition key slicing and keyed message publishing"
```

---

### Task 7: End-to-End Integration Testing

**Files:**
- Create: `tests/sink_pre_sorting_e2e.rs`

**Interfaces:**
- Consumes: `pipeline-core`, `starrocks-sink`, `kafka-sink`, `storage`
- Produces: Full pipeline integration tests verifying sorting, schema tolerance, and slicing.

- [ ] **Step 1: Write end-to-end integration tests**

Create `tests/sink_pre_sorting_e2e.rs`:
```rust
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::SignalBatch;
use pipeline_core::sort::{BatchSorter, MissingColumnAction, SignalType, SortColumnDef, SortConfig};
use std::sync::Arc;

#[test]
fn test_e2e_sorter_with_interleaved_telemetry() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["web", "auth", "web", "auth"])),
            Arc::new(Int64Array::from(vec![9, 13, 5, 9])),
            Arc::new(Int64Array::from(vec![1000, 1050, 1010, 1040])),
        ],
    )
    .expect("batch creation failed");

    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };

    let sorter = BatchSorter::from_config(&sort_config).expect("sorter creation");
    let sorted = sorter.sort(&batch, SignalType::Logs).expect("sort failed");

    let services = sorted
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let timestamps = sorted
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(services.value(0), "auth");
    assert_eq!(timestamps.value(0), 1040);
    assert_eq!(services.value(1), "auth");
    assert_eq!(timestamps.value(1), 1050);
    assert_eq!(services.value(2), "web");
    assert_eq!(timestamps.value(2), 1000);
    assert_eq!(services.value(3), "web");
    assert_eq!(timestamps.value(3), 1010);
}

#[test]
fn test_e2e_kafka_partition_key_slicing_flow() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["payment", "checkout", "payment"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![200, 100, 150])),
        ],
    )
    .unwrap();

    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Error,
        logs: vec![SortColumnDef::Shorthand("timestamp ASC".to_string())],
        metrics: vec![],
        traces: vec![],
    };

    let sorter = BatchSorter::from_config(&sort_config).unwrap();
    let sorted = sorter
        .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("service_name"))
        .unwrap();

    let slices = kafka_sink::extract_partition_slices(&sorted, "service_name").unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].0, "checkout");
    assert_eq!(slices[0].1.num_rows(), 1);
    assert_eq!(slices[1].0, "payment");
    assert_eq!(slices[1].1.num_rows(), 2);

    let payment_ts = slices[1].1.column_by_name("timestamp").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(payment_ts.value(0), 150);
    assert_eq!(payment_ts.value(1), 200);
}
```

- [ ] **Step 2: Run end-to-end integration tests**

Run: `cargo test --test sink_pre_sorting_e2e`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/sink_pre_sorting_e2e.rs
git commit -m "test(e2e): add end-to-end integration tests for pre-sorting and zero-copy partition slicing"
```

---

### Task 8: Criterion Micro-Benchmarks for Pre-Sorting

**Files:**
- Create: `crates/core/benches/sort_bench.rs`
- Modify: `crates/core/Cargo.toml:24-27`

**Interfaces:**
- Consumes: `criterion`, `pipeline-core::sort::{BatchSorter, SortConfig, SortColumnDef, SignalType}`
- Produces: Benchmark harness measuring sorting latency across 100, 1,000, and 10,000 rows.

- [ ] **Step 1: Create the Criterion benchmark file**

Create `crates/core/benches/sort_bench.rs`:
```rust
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use pipeline_core::sort::{BatchSorter, MissingColumnAction, SignalType, SortColumnDef, SortConfig};
use std::sync::Arc;

fn generate_bench_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let services = ["frontend", "backend", "checkout", "auth", "gateway"];
    let service_vec: Vec<&str> = (0..rows).map(|i| services[i % services.len()]).collect();
    let severity_vec: Vec<i64> = (0..rows).map(|i| (i % 24) as i64).collect();
    let timestamp_vec: Vec<i64> = (0..rows).map(|i| (rows - i) as i64).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(service_vec)),
            Arc::new(Int64Array::from(severity_vec)),
            Arc::new(Int64Array::from(timestamp_vec)),
        ],
    )
    .unwrap()
}

fn bench_batch_sorter(c: &mut Criterion) {
    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("severity_number ASC".to_string()),
            SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };
    let sorter = BatchSorter::from_config(&sort_config).unwrap();

    for size in [100, 1_000, 10_000] {
        let batch = generate_bench_batch(size);
        c.bench_function(&format!("batch_sort_3_columns_{size}_rows"), |b| {
            b.iter(|| {
                sorter.sort(&batch, SignalType::Logs).unwrap();
            });
        });
    }
}

criterion_group!(benches, bench_batch_sorter);
criterion_main!(benches);
```

- [ ] **Step 2: Register benchmark in `crates/core/Cargo.toml`**

In `crates/core/Cargo.toml`, append:
```toml
[[bench]]
name = "sort_bench"
harness = false
```

- [ ] **Step 3: Run benchmark harness check**

Run: `cargo bench -p pipeline-core --bench sort_bench -- --test`
Expected: PASS (compiles and executes 1 iteration test).

- [ ] **Step 4: Commit**

```bash
git add crates/core/benches/sort_bench.rs crates/core/Cargo.toml
git commit -m "bench(core): add Criterion micro-benchmarks for BatchSorter"
```

---

### Task 9: Quality Gates & Workspace Verification

**Files:**
- None (Verification only)

**Interfaces:**
- Validates: entire workspace against formatting, linting, tests, and benchmarks.

- [ ] **Step 1: Check code formatting**

Run: `cargo fmt --all -- --check`
Expected: Clean exit (code 0).

- [ ] **Step 2: Run strict clippy quality gate**

Run: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: Clean exit with zero warnings and zero errors.

- [ ] **Step 3: Run all workspace unit and integration tests**

Run: `cargo test --workspace`
Expected: ALL PASS.

- [ ] **Step 4: Run all benchmark harnesses in test mode**

Run: `cargo bench --workspace -- --test`
Expected: ALL PASS.

- [ ] **Step 5: Verify git cleanliness and final commit**

Run: `git status`
Expected: Working tree clean.
