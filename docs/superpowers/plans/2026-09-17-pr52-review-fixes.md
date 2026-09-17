# PR #52 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all 3 issues raised in automated review [#pullrequestreview-5233117290](https://github.com/jimmystewpot/opentelemetry-datalake/pull/52#pullrequestreview-5233117290) on PR #52: template simulation matching, Retry-After behavior with zero base interval, and Elasticsearch configuration validation under `--check`.

**Architecture:** 
- Task 1 adjusts `handle_bulk_transient_delay` in `crates/elasticsearch-sink/src/client.rs` so that server-requested `Retry-After` delays are honored regardless of whether `retry_interval_secs` is zero.
- Task 2 adds `ElasticsearchSinkConfig::validate(&self)` in `crates/elasticsearch-sink/src/config.rs`, calls it from `ElasticsearchSink::try_new`, and hooks it into `validate_config` in `src/main.rs` so `--check` catches invalid configurations statically without network calls.
- Task 3 fixes index template validation in `crates/elasticsearch-sink/src/client.rs`: removes the non-existent `data_stream` field from `SimulateIndexResponse` to accept valid simulation responses, and extends `check_get_index_template` to perform composable template pattern matching across `GET /_index_template` when the template name differs from the data stream name.

**Tech Stack:** Rust 2021, `tokio`, `reqwest`, `serde`, `serde_json`, `wiremock`, `anyhow`.

**Spec:** OpenTelemetry Datalake `AGENTS.md`, Elasticsearch Composable Index Template and Simulate Index API specifications, HTTP RFC 7231 (Retry-After).

## Global Constraints

- Zero `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` paths — test modules excepted.
- All changes must pass: `cargo fmt --all -- --check`
- All changes must pass: `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
- All changes must pass: `cargo test -p elasticsearch-sink`
- All changes must pass: `cargo test -p elasticsearch-sink --features aws`
- Commits must be GPG-signed and include sign-off: `git commit -s -S`
- Prefer `crate::` over `super::` in production paths; `super::` is fine in `#[cfg(test)]` modules.

---

## File Map

| File | Tasks | Change type |
|---|---|---|
| `crates/elasticsearch-sink/src/client.rs` | 1, 3 | Modify |
| `crates/elasticsearch-sink/src/config.rs` | 2 | Modify |
| `crates/elasticsearch-sink/src/lib.rs` | 2 | Modify |
| `src/main.rs` | 2 | Modify |

---

### Task 1: Honor `Retry-After` When Base Retry Interval Is Zero

**Problem:** 
In `crates/elasticsearch-sink/src/client.rs` line 741:
```rust
if self.retry_interval_secs == 0 {
    tokio::task::yield_now().await;
} else {
    tokio::time::sleep(capped).await;
}
```
When an operator sets `retry_interval_secs = 0` (or in tests where backoff is disabled), responses containing `Retry-After` are retried immediately via `yield_now()`. This bypasses the server's backoff instruction, potentially hammering an overloaded cluster and exhausting retries prematurely.

**Files:**
- Modify: `crates/elasticsearch-sink/src/client.rs:737-750`

**Interfaces:**
- Consumes: `parse_retry_after(response: &reqwest::Response) -> Option<Duration>`
- Consumes: `self.retry_interval_secs: u64`
- Produces: Correct sleep delay on `Retry-After` without relying on `self.retry_interval_secs > 0`.

- [ ] **Step 1: Write a failing unit test verifying Retry-After delay is respected when `retry_interval_secs == 0`**

Add inside `crates/elasticsearch-sink/src/client.rs` in `mod tests`:

```rust
    #[tokio::test]
    async fn test_retry_after_honored_when_retry_interval_zero() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "1")
                    .set_body_string("rate limited"),
            )
            .expect(2)
            .mount(&mock_server)
            .await;

        let mut config = make_test_config(vec![mock_server.uri()]);
        config.retry_interval_secs = 0;
        config.max_retries = 1;

        let client = HttpClient::try_new(&config).expect("client creation failed");
        let start = std::time::Instant::now();
        let result = client
            .send_bulk("logs-test-default", Bytes::from_static(b"{}\n"))
            .await;

        assert!(result.is_err(), "expected error after retries exhausted");
        assert!(
            start.elapsed() >= Duration::from_millis(900),
            "expected at least 900ms sleep honoring Retry-After: 1, got {:?}",
            start.elapsed()
        );
    }
```

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test -p elasticsearch-sink test_retry_after_honored_when_retry_interval_zero -- --nocapture
```
Expected: FAIL because `elapsed` is near 0ms (`yield_now` was executed instead of sleeping).

- [ ] **Step 3: Implement fix in `handle_bulk_transient_delay`**

In `crates/elasticsearch-sink/src/client.rs` lines 737-750, replace:
```rust
    /// Delays between bulk retry attempts, respecting `Retry-After` if present or using backoff.
    async fn handle_bulk_transient_delay(&self, response: &reqwest::Response, attempt: usize) {
        if let Some(delay) = parse_retry_after(response) {
            let capped = delay.min(Duration::from_secs(60));
            if self.retry_interval_secs == 0 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(capped).await;
            }
        } else {
            self.sleep_backoff(attempt).await;
        }
    }
```
with:
```rust
    /// Delays between bulk retry attempts, respecting `Retry-After` if present or using backoff.
    async fn handle_bulk_transient_delay(&self, response: &reqwest::Response, attempt: usize) {
        if let Some(delay) = parse_retry_after(response) {
            let capped = delay.min(Duration::from_secs(60));
            if capped.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(capped).await;
            }
        } else {
            self.sleep_backoff(attempt).await;
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p elasticsearch-sink test_retry_after_honored_when_retry_interval_zero -- --nocapture
```
Expected: PASS.

- [ ] **Step 5: Run quality gates and commit**

```bash
cargo fmt --all -- --check
cargo clippy -p elasticsearch-sink --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
git add crates/elasticsearch-sink/src/client.rs
git commit -s -S -m "fix(elasticsearch-sink): honor Retry-After header when base retry interval is zero"
```

---

### Task 2: Validate Elasticsearch Settings in Static Configuration & `--check` Mode

**Problem:** 
`validate_config` in `src/main.rs` only verifies `config.elasticsearch.is_some()`. If `--check` is specified, `main()` exits with success even when `endpoints = []` or `max_concurrent_requests = 0`, causing `--check` to succeed for invalid configurations that will fail at runtime.

**Files:**
- Modify: `crates/elasticsearch-sink/src/config.rs`
- Modify: `crates/elasticsearch-sink/src/lib.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Produces: `pub fn validate(&self) -> Result<(), ElasticsearchError>` on `ElasticsearchSinkConfig`
- Consumes: Called in `ElasticsearchSink::try_new` and `src/main.rs::validate_config`

- [ ] **Step 1: Write failing unit tests for `ElasticsearchSinkConfig::validate` and `validate_config` in `main.rs`**

In `crates/elasticsearch-sink/src/config.rs` in `mod tests`:
```rust
    #[test]
    fn test_validate_rejects_empty_endpoints() {
        let mut cfg = make_minimal_config();
        cfg.endpoints.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_blank_endpoint() {
        let mut cfg = make_minimal_config();
        cfg.endpoints = vec!["   ".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_url() {
        let mut cfg = make_minimal_config();
        cfg.endpoints = vec!["not a url".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_zero_concurrency() {
        let mut cfg = make_minimal_config();
        cfg.max_concurrent_requests = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_empty_data_stream_names() {
        let mut cfg = make_minimal_config();
        cfg.data_streams.logs = "   ".to_string();
        assert!(cfg.validate().is_err());
    }
```

In `src/main.rs` in `mod tests`:
```rust
    #[test]
    fn test_config_validation_fails_with_invalid_elasticsearch_config() {
        let toml_str = r#"
        [server]
        grpc_addr = "127.0.0.1:4317"
        http_addr = "127.0.0.1:4318"

        [elasticsearch]
        endpoints = []
        [elasticsearch.data_streams]
        logs = "logs-otel-default"
        metrics = "metrics-otel-default"
        traces = "traces-otel-default"
        "#;

        let config: AppConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("Config should deserialize");

        let err = validate_config(&config).expect_err("Validation should fail for empty endpoints");
        assert!(err.to_string().contains("at least one endpoint must be configured"));
    }
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test -p elasticsearch-sink test_validate_rejects_empty_endpoints
```
Expected: FAIL (method `validate` does not exist yet).

- [ ] **Step 3: Implement `ElasticsearchSinkConfig::validate` in `crates/elasticsearch-sink/src/config.rs`**

Add method to `impl ElasticsearchSinkConfig`:
```rust
impl ElasticsearchSinkConfig {
    /// Validates static configuration constraints without initiating network transport.
    pub fn validate(&self) -> Result<(), crate::error::ElasticsearchError> {
        if self.endpoints.is_empty() {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "at least one endpoint must be configured".to_string(),
            ));
        }

        for ep in &self.endpoints {
            let trimmed = ep.trim().trim_end_matches('/');
            if trimmed.is_empty() {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "endpoint URL cannot be empty".to_string(),
                ));
            }
            if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
                return Err(crate::error::ElasticsearchError::StartupValidation(format!(
                    "endpoint '{ep}' must start with http:// or https://"
                )));
            }
            if reqwest::Url::parse(trimmed).is_err() {
                return Err(crate::error::ElasticsearchError::StartupValidation(format!(
                    "endpoint '{ep}' is not a valid URL"
                )));
            }
        }

        if self.max_concurrent_requests == 0 {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "max_concurrent_requests must be greater than 0".to_string(),
            ));
        }

        if self.max_payload_bytes == 0 {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "max_payload_bytes must be greater than 0".to_string(),
            ));
        }

        if self.data_streams.logs.trim().is_empty()
            || self.data_streams.metrics.trim().is_empty()
            || self.data_streams.traces.trim().is_empty()
        {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "data stream names for logs, metrics, and traces must not be empty".to_string(),
            ));
        }

        if let Some(ref batching) = self.batching {
            if batching.max_batch_size_bytes == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_size_bytes must be greater than 0".to_string(),
                ));
            }
            if batching.max_batch_interval_sec == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_interval_sec must be greater than 0".to_string(),
                ));
            }
            if batching.max_batch_records == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_records must be greater than 0".to_string(),
                ));
            }
        }

        if let Some(ref ca_path) = self.tls.ca_cert_path {
            if !std::path::Path::new(ca_path).is_file() {
                return Err(crate::error::ElasticsearchError::StartupValidation(format!(
                    "CA certificate file does not exist: {ca_path}"
                )));
            }
        }

        Ok(())
    }
}
```

- [ ] **Step 4: Invoke `config.validate()` in `crates/elasticsearch-sink/src/lib.rs` and `src/main.rs`**

In `crates/elasticsearch-sink/src/lib.rs` in `try_new`:
```rust
    pub fn try_new(config: ElasticsearchSinkConfig) -> Result<Self, PipelineError> {
        config
            .validate()
            .map_err(|e| PipelineError::Internal(e.to_string()))?;

        let client = HttpClient::try_new(&config)?;
```

In `src/main.rs` in `validate_config`:
```rust
    if let Some(ref iceberg_cfg) = config.iceberg {
        ...
    } else if let Some(ref es_cfg) = config.elasticsearch {
        es_cfg
            .validate()
            .map_err(|e| anyhow::anyhow!("Configuration validation failed: {e}"))?;
    } else if config.kafka.is_none() && config.starrocks.is_none() {
        anyhow::bail!(
            "Configuration validation failed: one of [kafka], [iceberg], [starrocks], or [elasticsearch] configuration must be provided"
        );
    }
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p elasticsearch-sink test_validate
cargo test --bin opentelemetry-datalake test_config_validation
```
Expected: PASS.

- [ ] **Step 6: Run quality gates and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
git add crates/elasticsearch-sink/src/config.rs crates/elasticsearch-sink/src/lib.rs src/main.rs
git commit -s -S -m "feat(elasticsearch-sink): validate Elasticsearch settings statically and in --check mode"
```

---

### Task 3: Fix Index Template Validation for Data Streams in `client.rs`

**Problem:** 
1. `SimulateIndexResponse` has `data_stream: Option<serde_json::Value>` and requires `p.data_stream.is_some()`. But Elasticsearch's `POST /_index_template/_simulate_index/<index_name>` only returns `{ "template": { ... }, "overlapping": [ ... ] }` without a top-level `data_stream` field. This causes `check_simulate_index_template` to always return `Ok(false)` on real clusters.
2. `check_get_index_template` queries `GET /_index_template/{data_stream}`. When the index template has a different name than the data stream itself (e.g. template named `logs-template` matching `logs-*-*`), exact lookup returns 404.

**Files:**
- Modify: `crates/elasticsearch-sink/src/client.rs:240-252, 1030-1120`

**Interfaces:**
- Consumes: `POST /_index_template/_simulate_index/<data_stream>`
- Consumes: `GET /_index_template` composable templates list
- Produces: Correct detection of composable data stream templates regardless of template naming.

- [ ] **Step 1: Write failing unit tests for simulate-index without data_stream field and pattern-based template resolution**

In `crates/elasticsearch-sink/src/client.rs` in `mod tests`:
```rust
    #[tokio::test]
    async fn test_simulate_index_accepts_real_elasticsearch_response() {
        let mock_server = MockServer::start().await;
        // Real Elasticsearch simulate_index response has template and overlapping, NO top-level data_stream field
        let body = serde_json::json!({
            "template": {
                "settings": {
                    "index": { "number_of_shards": "1" }
                },
                "mappings": {
                    "properties": {
                        "@timestamp": { "type": "date" }
                    }
                }
            },
            "overlapping": []
        });
        Mock::given(method("POST"))
            .and(path("/_index_template/_simulate_index/logs-test-default"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let config = make_test_config(vec![mock_server.uri()]);
        let client = HttpClient::try_new(&config).expect("client creation failed");
        let result = client
            .check_simulate_index_template(&mock_server.uri(), "logs-test-default")
            .await;
        assert!(
            matches!(result, Ok(true)),
            "expected Ok(true) for standard simulate_index response, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_get_index_template_resolves_different_name_via_pattern() {
        let mock_server = MockServer::start().await;
        // Exact name lookup returns 404
        Mock::given(method("GET"))
            .and(path("/_index_template/logs-prod-cluster1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        // Listing templates via GET /_index_template returns the matching composable template
        let body = serde_json::json!({
            "index_templates": [{
                "name": "corporate-logs-template",
                "index_template": {
                    "index_patterns": ["logs-prod-*"],
                    "data_stream": {},
                    "priority": 200,
                    "template": {
                        "settings": {}
                    }
                }
            }]
        });
        Mock::given(method("GET"))
            .and(path("/_index_template"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let config = make_test_config(vec![mock_server.uri()]);
        let client = HttpClient::try_new(&config).expect("client creation failed");
        let result = client
            .check_get_index_template(&mock_server.uri(), "logs-prod-cluster1")
            .await;
        assert!(
            result.is_ok(),
            "expected pattern resolution to succeed, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn test_get_index_template_rejects_pattern_match_without_data_stream() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_index_template/logs-prod-cluster1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        // Template matches pattern, but lacks data_stream
        let body = serde_json::json!({
            "index_templates": [{
                "name": "regular-index-template",
                "index_template": {
                    "index_patterns": ["logs-prod-*"],
                    "priority": 100,
                    "template": {}
                }
            }]
        });
        Mock::given(method("GET"))
            .and(path("/_index_template"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&body))
            .mount(&mock_server)
            .await;

        let config = make_test_config(vec![mock_server.uri()]);
        let client = HttpClient::try_new(&config).expect("client creation failed");
        let result = client
            .check_get_index_template(&mock_server.uri(), "logs-prod-cluster1")
            .await;
        assert!(
            matches!(result, Err(TemplateCheckError::Validation(_))),
            "expected Validation error for matching non-data-stream template, got: {result:?}"
        );
    }
```

- [ ] **Step 2: Run tests to verify failures**

```bash
cargo test -p elasticsearch-sink test_simulate_index_accepts_real_elasticsearch_response
cargo test -p elasticsearch-sink test_get_index_template_resolves_different_name_via_pattern
```
Expected: FAIL.

- [ ] **Step 3: Update `SimulateIndexResponse` and `check_simulate_index_template`**

In `crates/elasticsearch-sink/src/client.rs`:
```rust
/// Simulate index template response returned by `POST /_index_template/_simulate_index/<index_name>`.
#[derive(Debug, Deserialize)]
struct SimulateIndexResponse {
    #[serde(default)]
    template: Option<serde_json::Map<String, serde_json::Value>>,
}
```

In `check_simulate_index_template`:
```rust
        let parsed: Result<SimulateIndexResponse, _> = response.json().await;
        match parsed {
            Ok(p) if p.template.as_ref().is_some_and(|t| !t.is_empty()) => {
                tracing::info!(
                    endpoint = %endpoint,
                    data_stream = %data_stream,
                    "Index template validation succeeded via simulate_index"
                );
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(e) => Err(TemplateCheckError::Validation(format!(
                "failed to parse simulate index response for '{data_stream}' on '{endpoint}': {e}"
            ))),
        }
```

- [ ] **Step 4: Implement pattern-based fallback in `check_get_index_template`**

Add helper function for simple wildcard/glob matching (supporting `*` and `?` without heavy external regex dependencies):
```rust
fn pattern_matches(pattern: &str, target: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let mut p_chars = pattern.chars().peekable();
    let mut t_chars = target.chars().peekable();

    fn match_recursive(
        mut p_it: std::iter::Peekable<std::str::Chars>,
        mut t_it: std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        while let Some(&p) = p_it.peek() {
            if p == '*' {
                p_it.next();
                if p_it.peek().is_none() {
                    return true;
                }
                while t_it.peek().is_some() {
                    if match_recursive(p_it.clone(), t_it.clone()) {
                        return true;
                    }
                    t_it.next();
                }
                return match_recursive(p_it, t_it);
            } else if p == '?' {
                if t_it.next().is_none() {
                    return false;
                }
                p_it.next();
            } else {
                if t_it.next() != Some(p) {
                    return false;
                }
                p_it.next();
            }
        }
        t_it.peek().is_none()
    }

    match_recursive(p_chars, t_chars)
}
```

Update `check_get_index_template`:
1. Query exact path `GET /_index_template/{data_stream}`.
2. If exact query returns 404, query `GET /_index_template` to list all composable templates.
3. For all returned `index_templates`, inspect `index_patterns`.
4. Collect all templates whose `index_patterns` match `data_stream`.
5. If no templates match, return `TemplateCheckError::Validation(format!("no index template matches data stream '{data_stream}'"))`.
6. Sort matching templates by `priority` (defaulting to 0) descending.
7. Inspect the highest-priority template: if `data_stream` object is present, validation succeeds with info log. If absent, return validation error indicating the matching template is not configured as a data-stream template.

- [ ] **Step 5: Run tests to verify all pass**

```bash
cargo test -p elasticsearch-sink
cargo test -p elasticsearch-sink --features aws
cargo test --bin opentelemetry-datalake
```
Expected: PASS.

- [ ] **Step 6: Run quality gates and commit**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo clippy --all-targets --features aws -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
git add crates/elasticsearch-sink/src/client.rs
git commit -s -S -m "fix(elasticsearch-sink): fix data stream index template simulation and pattern-matching fallback"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Comment 1 (simulate index + data stream matching) → Task 3. Comment 2 (Retry-After with retry_interval_secs == 0) → Task 1. Comment 3 (configuration validation in --check mode) → Task 2.
- [x] **Placeholder scan:** No TODO/TBD or vague descriptions. Exact signatures and test code supplied.
- [x] **Type consistency:** Methods, error types (`ElasticsearchError::StartupValidation`), and response types are matched to live source code.
- [x] **Ordering:** Tasks are logically sequenced: Task 1 (isolated Retry-After logic), Task 2 (static config validation across crates and main), Task 3 (template query and pattern matching).
