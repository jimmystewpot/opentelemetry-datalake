# Pre-Sorted Sink Ingestion Specification

## 1. Overview

High-throughput OpenTelemetry ingestion pipelines process interleaved streams of logs, metrics, and traces from diverse producers. In analytical lakehouses and OLAP databases (StarRocks, Apache Iceberg, ClickHouse, and stream consumers like Apache Flink), records within a given ingestion batch typically belong to the same narrow time window, but their dimension keys (`service_name`, `host`, `severity_number`, `metric_name`) arrive randomly interleaved.

When written unsorted, the downstream database or storage writer must perform heavy in-memory partitioning, bucketing, and sorting across storage segments and tablets. This leads to:
1. High CPU consumption on storage nodes (e.g. StarRocks Backend Stream Load workers).
2. Out-of-order segment generation and severe LSM-tree compaction backlog.
3. Suboptimal columnar compression due to broken dictionary and Run-Length Encoding (RLE) runs.

This specification introduces a unified, zero-copy, zero-panic **Pre-Sort and Order-By Architecture** across sinks in `opentelemetry-datalake`.

---

## 2. Core Architecture

The design adopts a centralized sorting engine in `pipeline-core` configured independently on a **per-sink** basis.

```
                  ┌──────────────────────────────────────────────┐
                  │          OTLP Ingestion & Codec             │
                  └──────────────────────┬───────────────────────┘
                                         │ SignalBatch (Arrow)
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │                 Pipeline Fanout              │
                  └──────┬───────────────────────┬───────────────┘
                         │                       │
                         ▼                       ▼
            ┌─────────────────────────┐ ┌─────────────────────────┐
            │      StarRocksSink      │ │        KafkaSink        │
            │                         │ │                         │
            │  ┌───────────────────┐  │ │  ┌───────────────────┐  │
            │  │ Optional Buffer   │  │ │  │ Prepend Partition │  │
            │  │ (Accumulate Batch)│  │ │  │ Key to Sort Tuple │  │
            │  └─────────┬─────────┘  │ │  └─────────┬─────────┘  │
            │            ▼            │ │            ▼            │
            │  ┌───────────────────┐  │ │  ┌───────────────────┐  │
            │  │    BatchSorter    │  │ │  │    BatchSorter    │  │
            │  │ (pipeline-core)   │  │ │  │ (pipeline-core)   │  │
            │  └─────────┬─────────┘  │ │  └─────────┬─────────┘  │
            │            ▼            │ │            ▼            │
            │  ┌───────────────────┐  │ │  ┌───────────────────┐  │
            │  │ HTTP Stream Load  │  │ │  │ Zero-Copy Slice & │  │
            │  │ (IPC/JSON/CSV)    │  │ │  │ Keyed rdkafka Msg │  │
            │  └───────────────────┘  │ │  └───────────────────┘  │
            └─────────────────────────┘ └─────────────────────────┘
```

---

## 3. Configuration Specification

Sorting is configured on a per-sink basis using dual-syntax support (string shorthand or structured tables) for each telemetry signal type (`logs`, `metrics`, `traces`).

### 3.1 Syntax Support

#### Option A: String Shorthand
Format: `"<column_name> [ASC|DESC] [NULLS FIRST|NULLS LAST]"` (case-insensitive, default direction `ASC`, default nulls `NULLS LAST`).
```toml
[starrocks.order_by]
on_missing_column = "skip" # "skip" (default) | "error"
logs = ["service_name ASC", "severity_text ASC", "timestamp ASC"]
metrics = ["service_name ASC", "name ASC", "timestamp ASC"]
traces = ["service_name ASC", "span_name ASC", "timestamp ASC"]
```

#### Option B: Structured Table Definition
```toml
[starrocks.order_by]
on_missing_column = "skip"

[[starrocks.order_by.logs]]
column = "service_name"
direction = "asc"        # "asc" | "desc"
nulls = "last"           # "first" | "last"

[[starrocks.order_by.logs]]
column = "timestamp"
direction = "asc"
nulls = "last"
```

### 3.2 Configuration Models in `pipeline-core`

```rust
// crates/core/src/sort.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum NullsPosition {
    First,
    #[default]
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum MissingColumnAction {
    #[default]
    Skip,
    Error,
}

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
```

---

## 4. `BatchSorter` Engine Specification

### 4.1 Normalized Data Structures
To eliminate string parsing overhead during hot-path data processing, configuration is parsed once at sink initialization into normalized Arrow compute options:

```rust
use arrow::compute::SortOptions;

#[derive(Debug, Clone)]
pub struct NormalizedSortKey {
    pub column: String,
    pub options: SortOptions,
}

#[derive(Debug, Clone, Default)]
pub struct BatchSorter {
    on_missing_column: MissingColumnAction,
    logs_keys: Vec<NormalizedSortKey>,
    metrics_keys: Vec<NormalizedSortKey>,
    traces_keys: Vec<NormalizedSortKey>,
}
```

### 4.2 Signal Type Discrimination
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Logs,
    Metrics,
    Traces,
}
```

### 4.3 Sorting Execution Algorithm
The `BatchSorter::sort` method executes as follows:

```rust
impl BatchSorter {
    pub fn sort(
        &self,
        batch: &RecordBatch,
        signal: SignalType,
    ) -> Result<RecordBatch, PipelineError> {
        if batch.num_rows() <= 1 {
            return Ok(batch.clone());
        }

        let keys = match signal {
            SignalType::Logs => &self.logs_keys,
            SignalType::Metrics => &self.metrics_keys,
            SignalType::Traces => &self.traces_keys,
        };

        if keys.is_empty() {
            return Ok(batch.clone());
        }

        let mut sort_cols = Vec::with_capacity(keys.len());
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

---

## 5. Sink Integration Specifications

### 5.1 StarRocks Sink Integration

#### Configuration
`StarRocksSinkConfig` is extended with:
- `order_by: Option<SortConfig>`
- `batching: Option<StarRocksBatchingConfig>`

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StarRocksBatchingConfig {
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize, // Default: 50 MiB (52,428,800)
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64, // Default: 30s
    #[serde(default)]
    pub max_batch_records: Option<usize>,
}
```

#### Write Behavior
1. **Without Batching (`batching: None`)**:
   Upon receiving a `SignalBatch`:
   `sorter.sort(&batch, signal_type)` is executed immediately before serializing to IPC/JSON/CSV.
2. **With Batching (`batching: Some(cfg)`)**:
   `RecordBatch`es accumulate per table buffer. When `total_bytes >= max_batch_size_bytes`, `elapsed >= max_batch_interval_sec`, or during graceful shutdown:
   - `arrow::compute::concat_batches(&schema, &buffered_batches)` consolidates records.
   - `sorter.sort(&combined_batch, signal_type)` sorts the consolidated chunk.
   - The sorted batch is serialized and streamed via `StreamLoadManager`.

### 5.2 Iceberg Sink Refactoring
- Deprecate and remove redundant `sort_logs`, `sort_metrics`, `sort_traces`, and private `sort_batch` from `crates/storage/src/iceberg.rs`.
- Embed `BatchSorter` in `IcebergSink`.
- If `[iceberg.order_by]` is unspecified, initialize `BatchSorter` with default sort tuples:
  - Logs: `["service_name ASC", "severity_text ASC", "timestamp ASC"]`
  - Metrics: `["service_name ASC", "name ASC", "attributes ASC", "timestamp ASC"]`
  - Traces: `["service_name ASC", "span_name ASC", "timestamp ASC"]`
- If user supplies `[iceberg.order_by]`, user definitions take precedence.

### 5.3 Kafka Sink Integration

#### Configuration
```toml
[kafka]
brokers = "localhost:9092"
logs_topic = "telemetry-logs"
traces_topic = "telemetry-traces"
metrics_topic = "telemetry-metrics"

logs_partition_key = "service_name"
metrics_partition_key = "service_name"
traces_partition_key = "service_name"

[kafka.order_by]
on_missing_column = "skip"
logs = ["severity_text ASC", "timestamp ASC"]
metrics = ["name ASC", "timestamp ASC"]
traces = ["span_name ASC", "timestamp ASC"]
```

#### Zero-Copy Partition Slicing Flow
1. If `partition_key` is set for the signal:
   - A synthetic sort column list `[partition_key ASC, order_by_keys...]` is evaluated by `BatchSorter`.
   - Sorting by `partition_key` as the lead column ensures that all rows with identical partition keys are contiguous in memory.
2. The sink scans the sorted `partition_key` column to find contiguous row index ranges `[start..end)`.
3. For each contiguous slice:
   - `let sub_batch = batch.slice(start, length);` creates an Arrow slice with zero array memory allocations.
   - The sub-batch is serialized and published via `rdkafka` with `record.key(&partition_key_value)`.
4. If no `partition_key` is set, the entire batch is sorted with `order_by` and published with empty key `""`.

---

## 6. Error Handling & Safety

1. **Zero-Panic Policy**: No `unwrap()`, `expect()`, or slice out-of-bounds indexing in production code. All errors propagate via `Result<_, PipelineError>`.
2. **Backpressure**: When `on_missing_column = "error"`, schema validation failures trigger `PipelineError` and block channel consumption, returning HTTP 503 / gRPC UNAVAILABLE to OTLP producers.
3. **Memory Safety**: Fast-path bypass on `num_rows <= 1` or empty keys prevents extraneous allocations. Kafka sub-batching uses Arrow's zero-copy reference counted buffers (`RecordBatch::slice`).
4. **Code Reuse**: Sorters and parsers are strictly centralized in `pipeline-core`.

---

## 7. Verification Plan

1. **Unit Tests (`pipeline-core`)**:
   - Parse dual syntax (strings with directions/nulls, structured tables).
   - Case-insensitivity verification.
   - Multi-column sorting across `Utf8`, `TimestampNanosecond`, `Int64`, `Float64`.
   - Null ordering (`NULLS FIRST` vs `NULLS LAST`).
   - Missing column handling (`skip` vs `error`).
   - 0-row and 1-row short-circuit tests.
2. **Sink Integration Tests**:
   - `starrocks-sink`: Sort verification across IPC, JSON, and CSV payloads, plus batch accumulation trigger tests.
   - `kafka-sink`: Zero-copy partition slice verification ensuring distinct Kafka message keys and internal row order.
   - `storage`: Backward-compatible Iceberg test pass and custom `order_by` override verification.
3. **Benchmarks**:
   - Micro-benchmarks comparing unsorted vs sorted batch throughput.
4. **Quality Gates**:
   - `cargo fmt --all -- --check`
   - `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
   - `cargo test --workspace`
   - `cargo bench --workspace -- --test`
