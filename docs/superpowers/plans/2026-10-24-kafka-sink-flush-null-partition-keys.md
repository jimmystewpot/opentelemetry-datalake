# Kafka Sink Producer Flush & Null Partition Key Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure `KafkaSink::run` flushes `librdkafka`'s internal producer queue inside `tokio::task::spawn_blocking` on graceful shutdown and guard `extract_partition_slices` against unchecked Arrow null accesses using `Option<String>` partition keys (#130).

**Architecture:** `extract_partition_slices` returns `Result<Vec<(Option<String>, RecordBatch)>, PipelineError>`, checking `str_arr.is_null(i)` before `str_arr.value(i)` so null keys map to `None` (unkeyed Kafka records) without conflating with `Some("")`. `KafkaSink` gains a configurable `flush_timeout: Duration` (defaulting to 10s, resolved from `delivery.timeout.ms` / `message.timeout.ms` > 0 or `.with_flush_timeout(...)`) and executes `Producer::flush` inside `tokio::task::spawn_blocking` when the input channel closes.

**Tech Stack:** Rust, Apache Arrow (`arrow` v59), `rdkafka` (v0.39), `tokio`, `thiserror`.

**Spec:** `docs/superpowers/specs/2026-10-24-kafka-sink-flush-null-partition-keys-design.md`

## Global Constraints

- Zero-panic policy in production code (`src/`): no `.unwrap()`, `.expect()`, `panic!()`, or panicking Arrow casts (`.as_string()`).
- Mandatory Arrow null checking: never call `str_arr.value(i)` without first verifying `!str_arr.is_null(i)`.
- Strict async/blocking isolation: `rdkafka::producer::Producer::flush` is synchronous C polling and MUST execute inside `tokio::task::spawn_blocking`.
- Zero heap string allocation per row during partition slice scanning: compare `Option<&str>` across rows and allocate `String` only once per emitted contiguous slice.
- Prefer `crate::` over `super::` in production `src/` code (`super::` is permitted inside `#[cfg(test)]`).

## Review Focus

1. **Conflation of `None` (null) and `Some("")` (empty string) partition keys:** A batch containing adjacent `None` and `Some("")` rows must produce two separate slices (`(None, ...)` and `(Some(""), ...)`), never merging them into one slice.Pinned in Task 1 (`test_extract_partition_slices_distinct_empty_and_null`).
2. **Zero-timeout (`"0"`) in `librdkafka` options (`message.timeout.ms = "0"`):** In `librdkafka`, `0` means infinite timeout; converting `"0"` into `Duration::from_millis(0)` causes `producer.flush` to time out in 0ms without waiting for in-flight messages. Values `<= 0` or unparseable strings must fall back to `DEFAULT_FLUSH_TIMEOUT_MS` (10,000ms). Pinned in Task 1 (`test_kafka_sink_flush_timeout_resolution`) and Task 3 (`test_validate_config_rejects_zero_kafka_flush_timeout`).
3. **Unreachable broker hang in unit tests:** Calling `FutureProducer::send().await` in a unit test without a live broker blocks for `message.timeout.ms` (300s default). Shutdown flush failure propagation must be tested by enqueueing via non-blocking `producer.send_result(...)` to an unreachable port (`127.0.0.1:1`) with a short `with_flush_timeout(Duration::from_millis(50))`, then closing the input channel. Pinned in Task 1 (`test_kafka_sink_run_flushes_on_channel_close`).
4. **All-null and single-row null batches:** A batch of `[None]` (1 row) or `[None, None, None]` (3 rows) must not call `str_arr.value(0)` unchecked and must return `(None, slice)`. Pinned in Task 1 (`test_extract_partition_slices_single_null`, `test_extract_partition_slices_all_nulls`).
5. **Pre-sorting integration with null partition keys:** `BatchSorter::sort_with_extra_lead_column` places nulls last (`nulls_last: true`) on the lead column; `extract_partition_slices` on the sorted output must group all trailing null rows into a single trailing `(None, RecordBatch)` slice. Pinned in Task 2 (`test_e2e_kafka_sink_presorting_with_null_partition_keys`).

---

### Task 1: Core `kafka-sink` Null-Safe Partition Slicing and Shutdown Flush

**Files:**
- Modify: `crates/kafka-sink/src/lib.rs:31-208`
- Test: `crates/kafka-sink/src/lib.rs:210-382`

**Interfaces:**
- Consumes: `pipeline_core::error::PipelineError`, `rdkafka::producer::{FutureProducer, FutureRecord, Producer}`
- Produces:
  - `pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 10_000;`
  - `pub fn extract_partition_slices(batch: &arrow::record_batch::RecordBatch, key_column: &str) -> Result<Vec<(Option<String>, arrow::record_batch::RecordBatch)>, PipelineError>`
  - `pub fn KafkaSink::with_flush_timeout(mut self, timeout: std::time::Duration) -> Self`
  - `pub fn KafkaSink::flush_timeout(&self) -> std::time::Duration`

- [ ] **Step 1: Write the failing unit tests in `crates/kafka-sink/src/lib.rs`**

Update `test_find_contiguous_partition_slices` in `crates/kafka-sink/src/lib.rs` to assert `Option<String>` keys (`Some("auth".to_string())`, etc.) and add the following unit tests inside `mod tests`:

```rust
    #[test]
    fn test_extract_partition_slices_with_null_keys() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("svc-a"),
                None,
                Some("svc-b"),
            ]))],
        )
        .unwrap();

        let slices = extract_partition_slices(&batch, "service_name").unwrap();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].0.as_deref(), Some("svc-a"));
        assert_eq!(slices[0].1.num_rows(), 1);
        assert_eq!(slices[1].0, None);
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0.as_deref(), Some("svc-b"));
        assert_eq!(slices[2].1.num_rows(), 1);
    }

    #[test]
    fn test_extract_partition_slices_all_nulls_and_single_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));

        let single_null = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None::<&str>,
            ]))],
        )
        .unwrap();
        let slices = extract_partition_slices(&single_null, "service_name").unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 1);

        let all_nulls = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None::<&str>,
                None::<&str>,
                None::<&str>,
            ]))],
        )
        .unwrap();
        let slices = extract_partition_slices(&all_nulls, "service_name").unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 3);
    }

    #[test]
    fn test_extract_partition_slices_distinct_empty_and_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None,
                Some(""),
                None,
                Some("auth"),
                Some("auth"),
                None,
                None,
            ]))],
        )
        .unwrap();

        let slices = extract_partition_slices(&batch, "service_name").unwrap();
        assert_eq!(slices.len(), 5);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 1);
        assert_eq!(slices[1].0.as_deref(), Some(""));
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0, None);
        assert_eq!(slices[2].1.num_rows(), 1);
        assert_eq!(slices[3].0.as_deref(), Some("auth"));
        assert_eq!(slices[3].1.num_rows(), 2);
        assert_eq!(slices[4].0, None);
        assert_eq!(slices[4].1.num_rows(), 2);
    }

    #[test]
    fn test_kafka_sink_flush_timeout_resolution() {
        use std::time::Duration;

        // 1. Default 10s
        let options = HashMap::new();
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &options).unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS));

        // 2. From message.timeout.ms
        let mut opts_msg = HashMap::new();
        opts_msg.insert("message.timeout.ms".to_string(), "3000".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_msg).unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(3000));

        // 3. delivery.timeout.ms takes precedence over message.timeout.ms
        let mut opts_both = HashMap::new();
        opts_both.insert("message.timeout.ms".to_string(), "3000".to_string());
        opts_both.insert("delivery.timeout.ms".to_string(), "4500".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_both).unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(4500));

        // 4. Zero ("0") falls back to default 10s
        let mut opts_zero = HashMap::new();
        opts_zero.insert("message.timeout.ms".to_string(), "0".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_zero).unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS));

        // 5. Builder override wins
        let sink = sink.with_flush_timeout(Duration::from_millis(250));
        assert_eq!(sink.flush_timeout(), Duration::from_millis(250));
    }

    #[tokio::test]
    async fn test_kafka_sink_run_flushes_on_channel_close() {
        use std::time::Duration;
        let options = HashMap::new();

        // Case A: Clean empty-queue shutdown flush succeeds
        let mut sink_ok = KafkaSink::try_new(
            "127.0.0.1:1",
            "test-topic",
            SerializationFormat::Json,
            &options,
        )
        .unwrap()
        .with_flush_timeout(Duration::from_millis(50));

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        drop(tx);
        let res = sink_ok.run(rx).await;
        assert!(res.is_ok(), "Empty producer queue flush on EOF should succeed: {res:?}");

        // Case B: Enqueued message to unreachable broker fails flush on EOF with PipelineError::Storage
        let mut sink_fail = KafkaSink::try_new(
            "127.0.0.1:1",
            "test-topic",
            SerializationFormat::Json,
            &options,
        )
        .unwrap()
        .with_flush_timeout(Duration::from_millis(50));

        // Enqueue directly into librdkafka C queue without awaiting delivery
        let _ = sink_fail
            .producer
            .send_result(FutureRecord::to("test-topic").payload(b"payload").key("k"));

        let (tx2, rx2) = tokio::sync::mpsc::channel(4);
        drop(tx2);
        let err = sink_fail
            .run(rx2)
            .await
            .expect_err("Flush with undelivered message to 127.0.0.1:1 must time out");
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Expected PipelineError::Storage from failed flush, got: {err:?}"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kafka-sink`
Expected: FAIL to compile (`DEFAULT_FLUSH_TIMEOUT_MS`, `flush_timeout`, `with_flush_timeout`, and `Option<String>` return type on `extract_partition_slices` not yet implemented).

- [ ] **Step 3: Implement null-safe `extract_partition_slices`, `flush_timeout`, and `spawn_blocking` flush in `crates/kafka-sink/src/lib.rs`**

1. Define `pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 10_000;`.
2. Add `flush_timeout: std::time::Duration` to `KafkaSink`.
3. In `KafkaSink::try_new`, resolve `flush_timeout` from `options.get("delivery.timeout.ms").or_else(|| options.get("message.timeout.ms"))`, parsing as `u64` and filtering `.filter(|&ms| ms > 0)`, defaulting to `Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS)`.
4. Add `pub fn with_flush_timeout(mut self, timeout: std::time::Duration) -> Self` and `pub fn flush_timeout(&self) -> std::time::Duration`.
5. Update `extract_partition_slices` to return `Result<Vec<(Option<String>, arrow::record_batch::RecordBatch)>, PipelineError>`, using a closure `let get_key = |i: usize| -> Option<&str> { if str_arr.is_null(i) { None } else { Some(str_arr.value(i)) } };` and scanning `1..num_rows` without the redundant `if num_rows == 1` branch.
6. In `KafkaSink::run`:
   - When `self.partition_key` is `None`, set `batches_to_send` to `vec![(None, sorted_batch)]`.
   - Build `let mut record = FutureRecord::to(&self.topic).payload(&buffer); if let Some(ref k) = key_opt { record = record.key(k.as_str()); }`.
   - After the `while let Some(signal) = input.recv().await` loop exits, clone `self.producer` and `self.flush_timeout` into `tokio::task::spawn_blocking`, invoke `rdkafka::producer::Producer::flush(&producer, timeout)`, mapping flush errors to `PipelineError::Storage(Box::new(e))` and join errors to `PipelineError::Internal`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p kafka-sink`
Expected: PASS (11 passed; 0 failed).

- [ ] **Step 5: Commit**

```bash
git add crates/kafka-sink/src/lib.rs
git commit -m "fix(kafka-sink): guard null partition keys and flush producer on shutdown (#130)"
```

---

### Task 2: End-to-End Pre-Sorting and Partition Slicing Integration Tests

**Files:**
- Modify: `tests/sink_pre_sorting_e2e.rs:98-114`
- Modify: `tests/sink_pre_sorting_e2e.rs:420-476`

**Interfaces:**
- Consumes: `kafka_sink::extract_partition_slices` returning `Result<Vec<(Option<String>, RecordBatch)>, PipelineError>`
- Produces: Updated e2e test suite verifying `Option<String>` slice keys and pre-sorting with null partition keys.

- [ ] **Step 1: Update existing assertions and add null partition key e2e test in `tests/sink_pre_sorting_e2e.rs`**

1. In `test_e2e_kafka_sink_presorting` (`tests/sink_pre_sorting_e2e.rs:100-102`), update assertions to:
   ```rust
   assert_eq!(slices[0].0.as_deref(), Some("checkout"));
   assert_eq!(slices[0].1.num_rows(), 1);
   assert_eq!(slices[1].0.as_deref(), Some("payment"));
   assert_eq!(slices[1].1.num_rows(), 2);
   ```
2. In `test_kafka_partition_extraction_edge_cases` (`tests/sink_pre_sorting_e2e.rs:437, 452`), update assertions to:
   ```rust
   assert_eq!(single_slices[0].0.as_deref(), Some("solo_service"));
   // ...
   assert_eq!(uniform_slices[0].0.as_deref(), Some("auth"));
   ```
3. Add `test_e2e_kafka_sink_presorting_with_null_partition_keys` at the end of `tests/sink_pre_sorting_e2e.rs`:
   ```rust
   #[test]
   fn test_e2e_kafka_sink_presorting_with_null_partition_keys() {
       let schema = Arc::new(Schema::new(vec![
           Field::new("service_name", DataType::Utf8, true),
           Field::new("timestamp", DataType::Int64, false),
       ]));

       let batch = RecordBatch::try_new(
           schema,
           vec![
               Arc::new(StringArray::from(vec![
                   None,
                   Some("payment"),
                   Some(""),
                   None,
                   Some("payment"),
               ])),
               Arc::new(Int64Array::from(vec![300, 200, 100, 150, 50])),
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
       let output_batch = sorter
           .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("service_name"))
           .unwrap();

       let slices = kafka_sink::extract_partition_slices(&output_batch, "service_name").unwrap();
       // Lead column sorts ASC with nulls_last = true: Some(""), Some("payment") (2 rows), None (2 rows)
       assert_eq!(slices.len(), 3);
       assert_eq!(slices[0].0.as_deref(), Some(""));
       assert_eq!(slices[0].1.num_rows(), 1);
       assert_eq!(slices[1].0.as_deref(), Some("payment"));
       assert_eq!(slices[1].1.num_rows(), 2);
       assert_eq!(slices[2].0, None);
       assert_eq!(slices[2].1.num_rows(), 2);

       let null_ts = slices[2]
           .1
           .column_by_name("timestamp")
           .unwrap()
           .as_any()
           .downcast_ref::<Int64Array>()
           .unwrap();
       assert_eq!(null_ts.value(0), 150);
       assert_eq!(null_ts.value(1), 300);
   }
   ```

- [ ] **Step 2: Run integration tests to verify they pass**

Run: `cargo test --test sink_pre_sorting_e2e`
Expected: PASS (all tests in `sink_pre_sorting_e2e` pass).

- [ ] **Step 3: Commit**

```bash
git add tests/sink_pre_sorting_e2e.rs
git commit -m "test(kafka-sink): update e2e partition slicing tests for Option keys and nulls (#130)"
```

---

### Task 3: `KafkaConfig` `flush_timeout_ms` Wiring, Validation, and Documentation

**Files:**
- Modify: `src/main.rs:36-53`, `src/main.rs:140-145`, `src/main.rs:919-942`
- Modify: `docs/buffer.md:68-74`
- Modify: `docs/configuration.md:70-89`
- Test: `src/main.rs` (unit tests in `mod tests`)

**Interfaces:**
- Consumes: `KafkaSink::with_flush_timeout(std::time::Duration)` from Task 1
- Produces: `KafkaConfig::flush_timeout_ms: Option<u64>`, validation in `validate_config`, sink wiring in `main`, updated `docs/buffer.md` and `docs/configuration.md`.

- [ ] **Step 1: Write the failing configuration validation test in `src/main.rs`**

Add `test_validate_config_kafka_flush_timeout` to `mod tests` in `src/main.rs`:

```rust
    #[test]
    fn test_validate_config_kafka_flush_timeout() {
        let valid_toml = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        flush_timeout_ms = 5000
        "#;
        let cfg: AppConfig = Figment::new().merge(Toml::string(valid_toml)).extract().unwrap();
        assert_eq!(cfg.kafka.as_ref().unwrap().flush_timeout_ms, Some(5000));
        assert!(validate_config(&cfg).is_ok());

        let zero_toml = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [kafka]
        brokers = "localhost:9092"
        logs_topic = "logs"
        traces_topic = "traces"
        metrics_topic = "metrics"
        logs_format = "json"
        traces_format = "json"
        metrics_format = "json"
        flush_timeout_ms = 0
        "#;
        let zero_cfg: AppConfig = Figment::new().merge(Toml::string(zero_toml)).extract().unwrap();
        let err = validate_config(&zero_cfg).unwrap_err();
        assert!(
            err.to_string().contains("flush_timeout_ms must be greater than 0"),
            "Unexpected error message: {err}"
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin opentelemetry-datalake test_validate_config_kafka_flush_timeout`
Expected: FAIL (`no field flush_timeout_ms on type &KafkaConfig`).

- [ ] **Step 3: Implement `flush_timeout_ms` in `src/main.rs` and update `docs/buffer.md` & `docs/configuration.md`**

1. In `src/main.rs`:
   - Add `#[serde(default)] flush_timeout_ms: Option<u64>` to `KafkaConfig`.
   - In `validate_config` under `else if let Some(ref kafka_cfg) = config.kafka`:
     ```rust
     if kafka_cfg.flush_timeout_ms == Some(0) {
         anyhow::bail!(
             "Configuration validation failed: kafka.flush_timeout_ms must be greater than 0"
         );
     }
     ```
   - In `main()` where `logs_sink`, `traces_sink`, and `metrics_sink` are constructed (`src/main.rs:919-942`), if `let Some(ms) = kafka_cfg.flush_timeout_ms`, apply `.with_flush_timeout(std::time::Duration::from_millis(ms))` to each sink.
2. In `docs/buffer.md`:
   - Remove `*   \`KafkaSink\` does not flush its producer queue on shutdown (#130).` from `### Known gaps (not yet at-least-once)`.
3. In `docs/configuration.md`:
   - Add `| \`flush_timeout_ms\` | Integer | \`10000\` | Maximum time in milliseconds to wait for in-flight messages in \`librdkafka\` to flush during graceful shutdown (must be > 0). |` to the `## Kafka Section ([kafka])` table.

- [ ] **Step 4: Run tests and clippy across the workspace to verify**

Run: `cargo test --bin opentelemetry-datalake test_validate_config_kafka_flush_timeout && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS with 0 warnings.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs docs/buffer.md docs/configuration.md
git commit -m "feat(kafka): wire configurable flush_timeout_ms and update docs (#130)"
```
