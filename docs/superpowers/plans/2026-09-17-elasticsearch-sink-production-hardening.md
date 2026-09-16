# Elasticsearch Sink Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all resilience, efficiency, and architectural vulnerabilities from the pre-production review and the latest GitHub PR #52 review comments (`4031150704`, `4031150712`, `4031150722`), upgrading `crates/elasticsearch-sink` to production-grade "A" status.

**Architecture:**
1. Map domain errors to `PipelineError::Storage` to preserve full root-cause error diagnostics.
2. Sanitize and validate unpacked JSON attributes in `serializer.rs` to protect the NDJSON wire protocol against formatting newlines and malformed JSON.
3. Parse and honor HTTP `Retry-After` headers and iterate across alternate endpoints during startup validation (`health_check` and `validate_index_template`), adding atomic cooldown circuit breaking in `client.rs`.
4. Offload sorting and NDJSON serialization in the non-batching branch of `Sink::run` to `tokio::task::spawn_blocking`.
5. Guarantee complete `JoinSet` task drainage upon task errors in both `dispatch` and `Sink::run` to prevent abrupt socket abortions and telemetry drops.

**Tech Stack:**
- Rust 2024 / Tokio async runtime
- Apache Arrow (`RecordBatch`)
- `reqwest`, `serde_json`, `httpdate`
- `wiremock`, `tokio::task::JoinSet`, `tokio::sync::Semaphore`

**Spec:** [docs/superpowers/specs/2026-09-16-elasticsearch-sink-design.md](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-16-elasticsearch-sink-design.md)

## Global Constraints

- Zero panic policy: no `.unwrap()`, `.expect()`, or `panic!()` in production source code (`src/`). Allowed in `#[cfg(test)]` modules only.
- Strict clippy check: must pass `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc` (both default and `--features aws`).
- Format check: must pass `cargo fmt --all -- --check`.
- Absolute test integrity: no test mutations may use unsafe environment modifications; all tests must pass concurrently.

---

### Task 1: Preserve Root Cause in PipelineError Conversion

**Files:**
- Modify: `crates/elasticsearch-sink/src/error.rs:43-52, 107-120`

**Interfaces:**
- Consumes: `ElasticsearchError` enum implementing `std::error::Error + Send + Sync + 'static`
- Produces: `From<ElasticsearchError> for pipeline_core::error::PipelineError` mapping all variants to `PipelineError::Storage(Box<dyn std::error::Error + Send + Sync>)`

- [ ] **Step 1: Write the failing unit test**

In `crates/elasticsearch-sink/src/error.rs`, replace `test_pipeline_error_conversion_downstream_closed` with a test asserting that `AuthenticationFailed` and `BulkFailed` convert to `PipelineError::Storage` preserving their error messages:

```rust
    #[test]
    fn test_pipeline_error_conversion_preserves_root_cause_in_storage() {
        let auth_err = ElasticsearchError::AuthenticationFailed("invalid api key 123".to_string());
        let pipeline_err: PipelineError = auth_err.into();
        assert!(matches!(pipeline_err, PipelineError::Storage(_)));
        assert!(pipeline_err.to_string().contains("invalid api key 123"));

        let bulk_err = ElasticsearchError::BulkFailed {
            retries: 3,
            message: "cluster saturated with 429".to_string(),
        };
        let pipeline_err2: PipelineError = bulk_err.into();
        assert!(matches!(pipeline_err2, PipelineError::Storage(_)));
        assert!(pipeline_err2.to_string().contains("cluster saturated with 429"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p elasticsearch-sink test_pipeline_error_conversion_preserves_root_cause_in_storage`
Expected: FAIL (currently matches `DownstreamClosed`).

- [ ] **Step 3: Implement minimal error conversion**

Update `From<ElasticsearchError>` in `crates/elasticsearch-sink/src/error.rs`:

```rust
impl From<ElasticsearchError> for pipeline_core::error::PipelineError {
    fn from(err: ElasticsearchError) -> Self {
        Self::Storage(Box::new(err))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p elasticsearch-sink error::tests`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/error.rs
git commit -m "fix(elasticsearch-sink): preserve root cause diagnostics in PipelineError::Storage"
```

---

### Task 2: Sanitize NDJSON Attribute Serialization Against Newlines & Malformed JSON

**Files:**
- Modify: `crates/elasticsearch-sink/src/serializer.rs:202-221, 330-370`

**Interfaces:**
- Consumes: `val: &str` from Arrow string array for attribute columns
- Produces: Sanitized, single-line JSON bytes written directly into `buf: &mut Vec<u8>` without violating the NDJSON newline boundary

- [ ] **Step 1: Write failing unit tests for multiline and malformed JSON attributes**

In `crates/elasticsearch-sink/src/serializer.rs`, add tests verifying:
1. Multiline/pretty-printed JSON attribute strings are compacted to a single line without raw `\n` in the document body.
2. Malformed JSON starting with `{` is safely escaped as a string rather than emitted verbatim.

```rust
    #[test]
    fn test_serialize_multiline_json_attribute_compacted_to_single_line() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        let multiline_json = "{\n  \"error.stack\": \"line1\\nline2\",\n  \"retries\": 3\n}";
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_726_500_000_000_000_000i64])),
                Arc::new(StringArray::from(vec![multiline_json])),
            ],
        ).unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        // Must still be exactly 2 NDJSON lines (1 action line + 1 document line)
        assert_eq!(lines.len(), 2, "Multiline JSON must not split the NDJSON document line");
        assert_eq!(lines[0], r#"{"create":{}}"#);
        assert!(lines[1].contains(r#""attributes":{"#));
        assert!(lines[1].contains(r#""retries":3"#));
    }

    #[test]
    fn test_serialize_malformed_json_starting_with_brace_is_escaped_as_string() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        let malformed_json = "{broken json without closing brace";
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_726_500_000_000_000_000i64])),
                Arc::new(StringArray::from(vec![malformed_json])),
            ],
        ).unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), 2, "Malformed JSON must not split lines");
        // Must be safely escaped as a string
        assert!(lines[1].contains(r#""attributes":"{broken json without closing brace""#));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p elasticsearch-sink test_serialize_multiline_json_attribute_compacted_to_single_line`
Expected: FAIL (produces 4 lines instead of 2).

- [ ] **Step 3: Implement minimal sanitization in `write_string_col`**

Update `write_string_col` in `crates/elasticsearch-sink/src/serializer.rs`:

```rust
fn write_string_col(
    buf: &mut Vec<u8>,
    col: &dyn Array,
    row: usize,
    data_type: &DataType,
    unpack_json: bool,
) {
    let val = match data_type {
        DataType::Utf8 => col.as_string::<i32>().value(row),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row),
        _ => return,
    };
    if unpack_json && (val.starts_with('{') || val.starts_with('[')) {
        if val.contains('\n') || val.contains('\r') {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(val) {
                let _ = serde_json::to_writer(&mut *buf, &parsed);
            } else {
                write_escaped_string(buf, val);
            }
        } else if serde_json::from_str::<serde_json::Value>(val).is_ok() {
            buf.extend_from_slice(val.as_bytes());
        } else {
            write_escaped_string(buf, val);
        }
    } else {
        write_escaped_string(buf, val);
    }
}
```

- [ ] **Step 4: Run serializer tests to verify they pass**

Run: `cargo test -p elasticsearch-sink serializer::tests`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/serializer.rs
git commit -m "fix(elasticsearch-sink): sanitize unpacked JSON attributes against newlines and invalid syntax"
```

---

### Task 3: Support `Retry-After` Header & Multi-Node Failover with Endpoint Cooldown

**Files:**
- Modify: `crates/elasticsearch-sink/src/client.rs:250-340, 525-600, 675-930`

**Interfaces:**
- Consumes: `reqwest::Response` status codes, headers (`Retry-After`), and multi-endpoint list
- Produces: 
  - Alternate endpoint retry loops in `health_check` and `validate_index_template` (PR Comment `4031150712`)
  - `parse_retry_after(response: &reqwest::Response) -> Option<Duration>`
  - `endpoint_cooldowns: Arc<Vec<AtomicU64>>` with automatic dead-node avoidance in `next_endpoint()`

- [ ] **Step 1: Write failing unit tests for `Retry-After` and multi-endpoint failover**

In `crates/elasticsearch-sink/src/client.rs`, add tests:
1. `test_health_check_succeeds_when_first_endpoint_is_unavailable`: Server 1 is offline, Server 2 is online with valid cluster info JSON; `health_check()` succeeds by falling back to Server 2.
2. `test_validate_index_template_succeeds_when_first_endpoint_is_unavailable`: Server 1 returns connection error, Server 2 returns template simulation JSON; `validate_index_template()` succeeds.
3. `test_parse_retry_after_numeric_seconds`: verifies parsing `Retry-After: 15`.
4. `test_endpoint_cooldown_avoids_failed_endpoint`: verifies that marking an endpoint failed routes subsequent requests to other healthy endpoints.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p elasticsearch-sink test_health_check_succeeds_when_first_endpoint_is_unavailable`
Expected: FAIL (currently fails immediately on first endpoint error).

- [ ] **Step 3: Implement multi-node fallback, cooldowns, and `Retry-After`**

In `crates/elasticsearch-sink/src/client.rs`:
1. Add `endpoint_cooldowns: Arc<Vec<std::sync::atomic::AtomicU64>>` to `HttpClient`.
2. Update `HttpClient::try_new` to initialize `endpoint_cooldowns` with `AtomicU64::new(0)` for each endpoint.
3. Update `next_endpoint(&self) -> &str` to skip cooled-down endpoints unless all are in cooldown.
4. Add `mark_endpoint_failed(&self, endpoint: &str)` (10s cooldown).
5. Refactor `health_check(&self)` to iterate over configured endpoints:
   - For each endpoint in `self.endpoints`, attempt `GET /`.
   - If successful and returns a valid version number, record info and return `Ok(())`.
   - If an endpoint fails with a transport error or 5xx, mark it failed and try the next endpoint.
   - If all endpoints fail, return `ElasticsearchError::StartupValidation` containing all error messages.
6. Refactor `validate_index_template(&self, data_stream: &str)` to iterate across endpoints:
   - For each endpoint, attempt `POST /_index_template/_simulate_index/<data_stream>` and fallback `GET /_index_template/<data_stream>`.
   - If template found, return `Ok(())`.
   - If transport/connection error occurs, try the next endpoint.
   - If 404 returned on all reachable nodes, return `ElasticsearchError::StartupValidation`.
7. Add `parse_retry_after(response: &reqwest::Response) -> Option<Duration>`.
8. In `send_bulk`:
   - On transient status (429/503) or network error: mark endpoint failed.
   - If `parse_retry_after` yields a duration, sleep for `retry_after.min(Duration::from_secs(60))` instead of fixed backoff.

- [ ] **Step 4: Run client tests to verify they pass**

Run: `cargo test -p elasticsearch-sink client::tests`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/client.rs
git commit -m "feat(elasticsearch-sink): support Retry-After, endpoint cooldowns, and multi-node startup validation failover"
```

---

### Task 4: Offload Non-Batching Sorting & Serialization to `spawn_blocking`

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs:418-435`

**Interfaces:**
- Consumes: `batch: RecordBatch`, `signal_type: SignalType`
- Produces: Non-blocking offloading of sorting and serialization via `tokio::task::spawn_blocking` (PR Comment `4031150722`)

- [ ] **Step 1: Write failing unit test for non-batching CPU offloading**

In `crates/elasticsearch-sink/src/lib.rs`, add a test verifying that `Sink::run` without batching processes large batches without stalling:

```rust
    #[tokio::test]
    async fn test_sink_run_non_batching_offloads_serialization() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "took": 1,
                "errors": false,
                "items": []
            })))
            .mount(&server)
            .await;

        let mut config = make_test_config(server.uri(), false);
        config.batching = None;
        let mut sink = ElasticsearchSink::try_new(config).unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let batch = make_log_batch();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx);

        let res = sink.run(rx).await;
        assert!(res.is_ok());
    }
```

- [ ] **Step 2: Run test to verify it executes**

Run: `cargo test -p elasticsearch-sink test_sink_run_non_batching_offloads_serialization`
Expected: PASS/FAIL (baseline).

- [ ] **Step 3: Wrap non-batching sort and serialization in `spawn_blocking`**

In `crates/elasticsearch-sink/src/lib.rs`, update the `if batching.is_none()` block:

```rust
if batching.is_none() {
    let sorter = self.sorter.clone();
    let unpack_attributes = self.config.unpack_attributes;
    let max_payload_bytes = self.config.max_payload_bytes;

    let payload = tokio::task::spawn_blocking(move || -> Result<Bytes, PipelineError> {
        let sorted = sorter.sort(&batch, signal_type)?;
        crate::serializer::serialize_batch(
            &sorted,
            unpack_attributes,
            max_payload_bytes,
        )
        .map_err(PipelineError::from)
    })
    .await
    .map_err(|e| PipelineError::Internal(format!("Serialization task panicked: {e}")))??;

    let target = self.data_stream_for(signal_type).to_string();
    Self::dispatch(
        Arc::clone(&self.client),
        Arc::clone(&self.semaphore),
        &mut join_set,
        target,
        payload,
    )
    .await?;
}
```

- [ ] **Step 4: Run sink tests to verify they pass**

Run: `cargo test -p elasticsearch-sink test_sink_run_non_batching_offloads_serialization`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -m "perf(elasticsearch-sink): offload non-batching sorting and serialization to spawn_blocking"
```

---

### Task 5: Complete In-Flight Task Drainage on Error in `dispatch` and `run`

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs:181-230, 390-410`

**Interfaces:**
- Consumes: `JoinSet<Result<BulkResponse, ElasticsearchError>>`
- Produces: Clean drainage (`drain_join_set`) on any task failure before error return (PR Comment `4031150704`)

- [ ] **Step 1: Write failing unit test for task drainage on mid-run failure**

In `crates/elasticsearch-sink/src/lib.rs`:
Add unit test `test_sink_run_drains_all_inflight_tasks_on_task_failure` verifying that when a task encounters an error in `join_set` during active `run()`, all remaining in-flight tasks in `join_set` are drained before returning error.

- [ ] **Step 2: Run test to verify failure**

Run: `cargo test -p elasticsearch-sink test_sink_run_drains_all_inflight_tasks_on_task_failure`
Expected: FAIL

- [ ] **Step 3: Implement clean drainage in `dispatch` and `run`**

In `crates/elasticsearch-sink/src/lib.rs`:
1. In `dispatch`:
   ```rust
   while let Some(res) = join_set.try_join_next() {
       match res {
           Ok(Ok(_resp)) => {}
           Ok(Err(es_err)) => {
               let _ = Self::drain_join_set(join_set).await;
               return Err(es_err.into());
           }
           Err(join_err) => {
               let _ = Self::drain_join_set(join_set).await;
               return Err(PipelineError::Internal(format!(
                   "Bulk dispatch task failed: {join_err}"
               )));
           }
       }
   }
   ```
2. In `Sink::run`:
   ```rust
   Some(res) = join_set.join_next(), if !join_set.is_empty() => {
       match res {
           Ok(Ok(_resp)) => {}
           Ok(Err(es_err)) => {
               let _ = Self::drain_join_set(&mut join_set).await;
               return Err(es_err.into());
           }
           Err(join_err) => {
               let _ = Self::drain_join_set(&mut join_set).await;
               return Err(PipelineError::Internal(format!(
                   "Bulk dispatch task failed: {join_err}"
               )));
           }
       }
   }
   ```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p elasticsearch-sink tests`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -m "fix(elasticsearch-sink): ensure all in-flight tasks drain cleanly on early task failure"
```

---

### Task 6: End-to-End Quality Gates, Verification, Git Push & PR Replies

**Files:**
- Modify: None (verification and PR update task)

- [ ] **Step 1: Run format check**

Run: `cargo fmt --all -- --check`
Expected: PASS

- [ ] **Step 2: Run strict pedantic clippy**

Run: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Run: `cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: 0 warnings, 0 errors

- [ ] **Step 3: Run full workspace test suite**

Run: `cargo test --workspace`
Run: `cargo test -p elasticsearch-sink --features aws`
Expected: ALL PASS

- [ ] **Step 4: Run benchmarks**

Run: `make bench`
Expected: ALL PASS

- [ ] **Step 5: Push signed commits and reply to PR review comments**

Push branch:
```bash
git push origin feat/elasticsearch-sink
```

Reply to PR review comments via `gh api`:
1. Comment `4031150704`: Reply indicating that `join_next()` in `Sink::run()` now awaits `drain_join_set()` prior to returning, preserving in-flight tasks.
2. Comment `4031150712`: Reply indicating that `health_check()` and `validate_index_template()` now iterate across all configured endpoints for failover.
3. Comment `4031150722`: Reply indicating that non-batching sorting and serialization is now offloaded to `spawn_blocking`.
