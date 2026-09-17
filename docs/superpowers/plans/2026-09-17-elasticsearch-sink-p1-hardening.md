# Elasticsearch Sink P1 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three confirmed P1 correctness and observability bugs in the Elasticsearch sink: task-drain on flush error, missing `data_stream` field in simulate-index validation, and absent buffer-state telemetry.

**Architecture:** All three fixes are surgical changes within `crates/elasticsearch-sink/src/`. No new files are created. Each fix is independently testable and committed separately. Tasks must be executed sequentially because Tasks 1 and 3 touch adjacent code in `lib.rs`.

**Tech Stack:** Rust stable, `tokio`, `tracing`, `reqwest`, `serde_json`, `wiremock` (test harness).

**Spec:** `AGENTS.md` (zero-panic policy, buffer telemetry requirement), `docs/buffer.md`, `docs/instrumentation.md`, Elasticsearch simulate-index API documentation.

## Global Constraints

- Zero `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` paths — test modules excepted.
- All changes must pass: `cargo fmt --all -- --check`
- All changes must pass: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo test -p elasticsearch-sink`
- All changes must pass: `cargo test -p elasticsearch-sink --features aws`
- Commits must be GPG-signed and include sign-off: `git commit -s -S`
- Prefer `crate::` over `super::` in production paths; `super::` is fine in `#[cfg(test)]` modules.
- Use `tracing::debug!` for all observability events.
- Structured log fields: `signal` (snake_case signal type string), `bytes`, `records`, `data_stream`.

---

## File Map

| File | Tasks | Change type |
|---|---|---|
| `crates/elasticsearch-sink/src/lib.rs` | 1, 3 | Modify |
| `crates/elasticsearch-sink/src/client.rs` | 2 | Modify |

---

### Task 1: Drain JoinSet on Flush Error in `Sink::run`

**Problem:** Three call-sites in `Sink::run` use the `?` operator directly on `flush_buffer` / `flush_all_buffers`. When a flush fails, the function returns immediately without draining the `JoinSet`, silently aborting in-flight HTTP bulk tasks and potentially losing data acknowledgement.

**Call-sites to fix (all in `crates/elasticsearch-sink/src/lib.rs`):**

| Approx. line | Arm | Call |
|---|---|---|
| 418 | `interval.tick()` | `self.flush_all_buffers(...).await?` |
| 476–481 | `input.recv()` threshold branch | `self.flush_buffer(...).await?` |
| 486 | `input.recv()` shutdown branch | `self.flush_all_buffers(...).await?` |

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs` (lines ~418, ~476–481, ~486)

**Interfaces:**
- Consumes: `Self::drain_join_set(&mut join_set) -> Result<(), PipelineError>` (already exists at line ~244)
- Consumes: `self.flush_all_buffers(...) -> Result<(), PipelineError>` (already exists at line ~329)
- Consumes: `self.flush_buffer(...) -> Result<(), PipelineError>` (already exists at line ~272)
- Produces: No new public interfaces; behavioural fix only.

- [ ] **Step 1: Write a failing test that verifies flush errors trigger drain**

Add inside the existing `#[cfg(test)]` block in `lib.rs`:

```rust
#[tokio::test]
async fn test_flush_error_drains_join_set() {
    let mock_server = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .expect(1..)
        .mount(&mock_server)
        .await;

    let config = make_test_config_with_batching(mock_server.uri());
    let mut sink = ElasticsearchSink::try_new(config).expect("sink construction failed");

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let batch = make_test_log_batch(100);
    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx);

    let result = sink.run(pipeline_core::pipeline::PipelineReceiver::from(rx)).await;
    assert!(result.is_err(), "expected flush error to propagate");
}
```

> Check the existing test module for `make_test_config` and extend it with `batching: Some(ElasticsearchBatchingConfig { max_bytes: 1, max_records: 1, max_interval_secs: 60 })` so the threshold fires immediately. Reuse any existing batch builder before creating new ones.

- [ ] **Step 2: Run the test to verify it fails or hangs (confirming the bug)**

```bash
cargo test -p elasticsearch-sink test_flush_error_drains_join_set -- --nocapture 2>&1 | head -50
```

Expected: fails, hangs, or panics — confirming the drain gap exists.

- [ ] **Step 3: Fix all three call-sites in `Sink::run`**

*Interval tick arm (~line 418):*
```rust
// BEFORE:
self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await?;

// AFTER:
if let Err(e) = self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await {
    let _ = Self::drain_join_set(&mut join_set).await;
    return Err(e);
}
```

*Threshold flush arm (~line 476–481):*
```rust
// BEFORE:
self.flush_buffer(buf, signal_type, self.data_stream_for(signal_type), &mut join_set).await?;

// AFTER:
if let Err(e) = self.flush_buffer(buf, signal_type, self.data_stream_for(signal_type), &mut join_set).await {
    let _ = Self::drain_join_set(&mut join_set).await;
    return Err(e);
}
```

*Shutdown flush arm (~line 486):*
```rust
// BEFORE:
self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await?;

// AFTER:
if let Err(e) = self.flush_all_buffers(&mut logs_buf, &mut metrics_buf, &mut traces_buf, &mut join_set).await {
    let _ = Self::drain_join_set(&mut join_set).await;
    return Err(e);
}
```

> The shutdown success path at ~line 489 (`Self::drain_join_set(&mut join_set).await?`) must remain unchanged — it handles the normal drain-on-shutdown.

- [ ] **Step 4: Run the test to verify it now passes**

```bash
cargo test -p elasticsearch-sink test_flush_error_drains_join_set -- --nocapture
```

- [ ] **Step 5: Run the full test suite**

```bash
cargo test -p elasticsearch-sink
cargo test -p elasticsearch-sink --features aws
```

- [ ] **Step 6: Run quality gates**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

- [ ] **Step 7: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -s -S -m "fix(elasticsearch-sink): drain JoinSet on flush error to prevent silent task abort"
```

---

### Task 2: Add `data_stream` Field to `SimulateIndexResponse` and Validate It

**Problem:** `SimulateIndexResponse` (~line 242 in `client.rs`) only deserializes `template`. The ES `POST /_index_template/_simulate_index/{name}` API returns a top-level `data_stream: {}` **only** when the matched template has `data_stream: {}` configured. A conventional composable template also returns a non-empty `template`, so the current check (~line 1030) incorrectly returns `true` for non-data-stream templates — causing startup validation to pass and bulk writes to fail with `400: index is not a data stream` at runtime.

**Also fix:** `check_get_index_template` (~line 1092) only checks `index_templates` is non-empty, without verifying any entry has `data_stream` configured.

**Files:**
- Modify: `crates/elasticsearch-sink/src/client.rs` (lines ~242–245, ~1030–1037, ~1086–1103)

**Interfaces:**
- Consumes: `SimulateIndexResponse` (private, modified here)
- Consumes: `IndexTemplatesResponse` (private, `serde_json::Value` entries)
- Produces: No new public interfaces.

- [ ] **Step 1: Write failing tests for `check_simulate_index_template`**

Add inside the `#[cfg(test)]` block in `client.rs`:

```rust
#[tokio::test]
async fn test_simulate_index_rejects_non_data_stream_template() {
    let mock_server = wiremock::MockServer::start().await;
    let body = serde_json::json!({
        "template": { "settings": {}, "mappings": {} }
        // No "data_stream" key — conventional template
    });
    wiremock::Mock::given(method("POST"))
        .and(path("/_index_template/_simulate_index/logs-test-default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&body))
        .mount(&mock_server)
        .await;

    let config = make_test_config(mock_server.uri());
    let client = HttpClient::try_new(&config).expect("client creation failed");
    let result = client
        .check_simulate_index_template(&mock_server.uri(), "logs-test-default")
        .await;
    assert!(matches!(result, Ok(false)),
        "expected Ok(false) for non-data-stream template, got: {result:?}");
}

#[tokio::test]
async fn test_simulate_index_accepts_data_stream_template() {
    let mock_server = wiremock::MockServer::start().await;
    let body = serde_json::json!({
        "template": { "settings": {}, "mappings": {} },
        "data_stream": {}
    });
    wiremock::Mock::given(method("POST"))
        .and(path("/_index_template/_simulate_index/logs-test-default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&body))
        .mount(&mock_server)
        .await;

    let config = make_test_config(mock_server.uri());
    let client = HttpClient::try_new(&config).expect("client creation failed");
    let result = client
        .check_simulate_index_template(&mock_server.uri(), "logs-test-default")
        .await;
    assert!(matches!(result, Ok(true)),
        "expected Ok(true) for data-stream template, got: {result:?}");
}
```

> If `check_simulate_index_template` is not accessible from tests, make it `pub(crate)`.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p elasticsearch-sink test_simulate_index_rejects_non_data_stream_template -- --nocapture
cargo test -p elasticsearch-sink test_simulate_index_accepts_data_stream_template -- --nocapture
```

Expected: first test FAILS (currently returns `Ok(true)`).

- [ ] **Step 3: Add `data_stream` field to `SimulateIndexResponse`**

```rust
// BEFORE (client.rs ~line 240):
#[derive(Debug, Deserialize)]
struct SimulateIndexResponse {
    #[serde(default)]
    template: Option<serde_json::Map<String, serde_json::Value>>,
}

// AFTER:
/// Simulate index template response returned by `POST /_index_template/_simulate_index/<index_name>`.
///
/// The `data_stream` field is present **only** when the matched template has `data_stream: {}`
/// configured, identifying it as a data-stream template rather than a conventional index template.
#[derive(Debug, Deserialize)]
struct SimulateIndexResponse {
    #[serde(default)]
    template: Option<serde_json::Map<String, serde_json::Value>>,
    /// Present only if the matched template is a data-stream template.
    #[serde(default)]
    data_stream: Option<serde_json::Value>,
}
```

- [ ] **Step 4: Update the match arm in `check_simulate_index_template` to require `data_stream`**

```rust
// BEFORE (client.rs ~line 1029):
match parsed {
    Ok(p) if p.template.as_ref().is_some_and(|t| !t.is_empty()) => {
        tracing::info!(endpoint = %endpoint, data_stream = %data_stream,
            "Index template validation succeeded via simulate_index");
        Ok(true)
    }
    Ok(_) => Ok(false),
    Err(e) => Err(TemplateCheckError::Validation(format!(
        "failed to parse simulate index response for '{data_stream}' on '{endpoint}': {e}"
    ))),
}

// AFTER:
match parsed {
    Ok(p)
        if p.data_stream.is_some()
            && p.template.as_ref().is_some_and(|t| !t.is_empty()) =>
    {
        tracing::info!(endpoint = %endpoint, data_stream = %data_stream,
            "Index template validation succeeded via simulate_index");
        Ok(true)
    }
    Ok(_) => Ok(false),
    Err(e) => Err(TemplateCheckError::Validation(format!(
        "failed to parse simulate index response for '{data_stream}' on '{endpoint}': {e}"
    ))),
}
```

- [ ] **Step 5: Write a failing test for `check_get_index_template` with a non-data-stream entry**

```rust
#[tokio::test]
async fn test_get_index_template_rejects_non_data_stream_template() {
    let mock_server = wiremock::MockServer::start().await;
    let body = serde_json::json!({
        "index_templates": [{
            "name": "logs-template",
            "index_template": {
                "index_patterns": ["logs-test-*"],
                "template": { "settings": {} }
                // no "data_stream" key
            }
        }]
    });
    wiremock::Mock::given(method("GET"))
        .and(path("/_index_template/logs-test-default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&body))
        .mount(&mock_server)
        .await;

    let config = make_test_config(mock_server.uri());
    let client = HttpClient::try_new(&config).expect("client creation failed");
    let result = client
        .check_get_index_template(&mock_server.uri(), "logs-test-default")
        .await;
    assert!(matches!(result, Err(TemplateCheckError::Validation(_))),
        "expected Validation error for non-data-stream template, got: {result:?}");
}
```

- [ ] **Step 6: Run the failing test to verify the bug exists**

```bash
cargo test -p elasticsearch-sink test_get_index_template_rejects_non_data_stream_template -- --nocapture
```

- [ ] **Step 7: Add `data_stream` validation to `check_get_index_template`**

In `client.rs` at ~line 1092, after the `is_empty()` check, before `tracing::info!`:

```rust
// After the is_empty() guard, add:
let has_data_stream_template = parsed.index_templates.iter().any(|entry| {
    entry
        .get("index_template")
        .and_then(|it| it.get("data_stream"))
        .is_some()
});

if !has_data_stream_template {
    return Err(TemplateCheckError::Validation(format!(
        "index template for data stream '{data_stream}' exists but is not \
         configured as a data-stream template (missing 'data_stream' field)"
    )));
}
```

- [ ] **Step 8: Run all three new tests**

```bash
cargo test -p elasticsearch-sink test_simulate_index_rejects_non_data_stream_template -- --nocapture
cargo test -p elasticsearch-sink test_simulate_index_accepts_data_stream_template -- --nocapture
cargo test -p elasticsearch-sink test_get_index_template_rejects_non_data_stream_template -- --nocapture
```

Expected: all three PASS.

- [ ] **Step 9: Run the full test suite**

```bash
cargo test -p elasticsearch-sink
cargo test -p elasticsearch-sink --features aws
```

- [ ] **Step 10: Run quality gates**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

- [ ] **Step 11: Commit**

```bash
git add crates/elasticsearch-sink/src/client.rs
git commit -s -S -m "fix(elasticsearch-sink): require data_stream field in index template validation to reject non-data-stream templates"
```

---

### Task 3: Emit Structured Buffer-State Telemetry on Accumulation and Flush

**Problem:** AGENTS.md mandates event-driven telemetry per `docs/buffer.md`. The accumulation block (~471–473) and `flush_buffer` (~283–286) update counters silently. Operators cannot observe buffer growth, threshold crossings, or flush events.

**Required events:**

| Location | Level | Fields | Message |
|---|---|---|---|
| After `buf.batches.push(batch)` | `debug` | `signal`, `bytes`, `records`, `data_stream` | `"Buffer accumulated batch"` |
| When threshold fires (before `flush_buffer`) | `debug` | `signal`, `bytes`, `records`, `data_stream`, `reason="threshold"` | `"Buffer threshold reached, flushing"` |
| Start of `flush_buffer` (before `std::mem::take`) | `debug` | `signal`, `bytes`, `records`, `data_stream` | `"Flushing buffer"` |
| After `flush_buffer` completes | `debug` | `signal`, `data_stream` | `"Buffer flushed and reset"` |

**Files:**
- Modify: `crates/elasticsearch-sink/src/lib.rs` (accumulation block ~464–482, `flush_buffer` ~272–325)

**Interfaces:**
- Consumes: `SignalType` — check `crates/core/src/sort.rs` for `impl Display for SignalType`. If absent, use a local `match` to `&str`.
- Produces: No new public interfaces.

- [ ] **Step 1: Check if `SignalType` implements `Display`**

```bash
grep -n "impl.*Display.*SignalType\|fmt::Display.*for SignalType" \
  crates/core/src/sort.rs crates/core/src/lib.rs 2>/dev/null
```

If found, use `signal = %signal_type` in tracing macros.  
If not, bind a label before first use:
```rust
let signal_label = match signal_type {
    SignalType::Logs => "logs",
    SignalType::Metrics => "metrics",
    SignalType::Traces => "traces",
};
```

- [ ] **Step 2: Check `tracing-subscriber` is in `[dev-dependencies]`**

```bash
grep -A2 "tracing-subscriber" crates/elasticsearch-sink/Cargo.toml
```

If missing, add:
```toml
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 3: Write a failing test that asserts accumulation emits a debug event**

Add inside the `#[cfg(test)]` block in `lib.rs`:

```rust
#[tokio::test]
async fn test_buffer_accumulation_emits_debug_event() {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Default, Clone)]
    struct EventCapture(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventCapture {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" { self.0 = value.to_string(); }
                }
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                    if field.name() == "message" { self.0 = format!("{value:?}"); }
                }
            }
            let mut v = Visitor(String::new());
            event.record(&mut v);
            if !v.0.is_empty() { self.0.lock().unwrap().push(v.0); }
        }
    }

    let captured = EventCapture::default();
    let events = Arc::clone(&captured.0);
    let subscriber = tracing_subscriber::registry().with(captured);
    let _guard = tracing::subscriber::set_default(subscriber);

    let mock_server = wiremock::MockServer::start().await;
    let config = make_test_config_with_batching(mock_server.uri());
    let mut sink = ElasticsearchSink::try_new(config).expect("sink construction");

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let batch = make_test_log_batch(1); // below threshold
    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx);

    let _ = sink.run(pipeline_core::pipeline::PipelineReceiver::from(rx)).await;

    let msgs = events.lock().unwrap();
    assert!(
        msgs.iter().any(|m| m.contains("Buffer accumulated batch")),
        "expected 'Buffer accumulated batch' debug event; got: {msgs:?}"
    );
}
```

- [ ] **Step 4: Run the test to verify it fails**

```bash
cargo test -p elasticsearch-sink test_buffer_accumulation_emits_debug_event -- --nocapture
```

Expected: FAIL.

- [ ] **Step 5: Add telemetry to the accumulation block in `Sink::run`**

Inside the batching `else` branch (~line 464), replace the current silent accumulation:

```rust
let signal_label = match signal_type {
    SignalType::Logs => "logs",
    SignalType::Metrics => "metrics",
    SignalType::Traces => "traces",
};

// Compute sizes before batch is moved into the buffer
let batch_bytes = batch.get_array_memory_size();
let batch_rows = batch.num_rows();

buf.bytes = buf.bytes.saturating_add(batch_bytes);
buf.records = buf.records.saturating_add(batch_rows);
buf.batches.push(batch);

tracing::debug!(
    signal = signal_label,
    bytes = buf.bytes,
    records = buf.records,
    data_stream = self.data_stream_for(signal_type),
    "Buffer accumulated batch"
);

if buf.bytes >= max_bytes || buf.records >= max_records {
    tracing::debug!(
        signal = signal_label,
        bytes = buf.bytes,
        records = buf.records,
        data_stream = self.data_stream_for(signal_type),
        reason = "threshold",
        "Buffer threshold reached, flushing"
    );
    if let Err(e) = self.flush_buffer(
        buf,
        signal_type,
        self.data_stream_for(signal_type),
        &mut join_set,
    )
    .await
    {
        let _ = Self::drain_join_set(&mut join_set).await;
        return Err(e);
    }
}
```

> `batch.get_array_memory_size()` and `batch.num_rows()` must be called **before** `buf.batches.push(batch)` consumes `batch`. Bind `batch_bytes` and `batch_rows` first.

- [ ] **Step 6: Add telemetry to `flush_buffer`**

In `lib.rs` inside `flush_buffer` (~line 272):

```rust
async fn flush_buffer(
    &self,
    buf: &mut BufferState,
    signal_type: SignalType,
    target_data_stream: &str,
    join_set: &mut tokio::task::JoinSet<Result<BulkResponse, ElasticsearchError>>,
) -> Result<(), PipelineError> {
    if buf.batches.is_empty() {
        return Ok(());
    }

    let signal_label = match signal_type {
        SignalType::Logs => "logs",
        SignalType::Metrics => "metrics",
        SignalType::Traces => "traces",
    };

    tracing::debug!(
        signal = signal_label,
        bytes = buf.bytes,
        records = buf.records,
        data_stream = target_data_stream,
        "Flushing buffer"
    );

    let batches = std::mem::take(&mut buf.batches);
    buf.bytes = 0;
    buf.records = 0;

    // ... existing spawn_blocking + concat + sort + serialize logic unchanged ...

    if let Some(payload) = payload_opt {
        Self::dispatch(
            Arc::clone(&self.client),
            Arc::clone(&self.semaphore),
            join_set,
            target_data_stream.to_string(),
            payload,
        )
        .await?;
    }

    tracing::debug!(
        signal = signal_label,
        data_stream = target_data_stream,
        "Buffer flushed and reset"
    );

    Ok(())
}
```

> Place `"Buffer flushed and reset"` after the `if let Some(payload)` block, just before `Ok(())`.

- [ ] **Step 7: Run the test to verify it passes**

```bash
cargo test -p elasticsearch-sink test_buffer_accumulation_emits_debug_event -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Run the full test suite**

```bash
cargo test -p elasticsearch-sink
cargo test -p elasticsearch-sink --features aws
```

- [ ] **Step 9: Run quality gates**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

- [ ] **Step 10: Commit**

```bash
git add crates/elasticsearch-sink/src/lib.rs
git commit -s -S -m "feat(elasticsearch-sink): emit structured buffer state telemetry on accumulation and flush"
```

---

### Task 4: Final Verification and Push

**Files:** none (verification only)

- [ ] **Step 1: Run the complete test suite**

```bash
cargo test -p elasticsearch-sink
cargo test -p elasticsearch-sink --features aws
```

- [ ] **Step 2: Run all quality gates**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

- [ ] **Step 3: Review the commit log**

```bash
git log --oneline -5
```

Expected: three new commits (Task 1, Task 2, Task 3) on top of `2dae73a`.

- [ ] **Step 4: Push to origin**

```bash
git push origin feat/elasticsearch-sink
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Buffer telemetry → Task 3. Simulate-index correctness → Task 2. Flush drain gap → Task 1. All three mapped.
- [x] **Placeholder scan:** All steps contain real code. No TBD/TODO.
- [x] **Type consistency:** `SignalType`, `BufferState` fields, `drain_join_set` all referenced by exact names matching the live source.
- [x] **Ordering:** Task 1 (~418, ~476, ~486) and Task 3 (~272–325, ~464–481) touch different regions of `lib.rs`. Sequential execution avoids conflicts. Task 2 (`client.rs` only) is fully independent.
