# Arrow Codec Zero-Allocation Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate per-record heap allocations in `crates/arrow-codec` via stack-buffered hex encoding, streaming typed JSON attribute serialization, pre-sized Arrow builders, and static `LazyLock<SchemaRef>` caching.

**Architecture:** Centralize static Arrow schemas (`LOGS_SCHEMA`, `TRACES_SCHEMA`, `METRICS_SCHEMA`), stack-buffered hex helpers, and streaming JSON/string attribute writers in `arrow-codec::common`. Update `logs.rs`, `traces.rs`, and `metrics.rs` to pre-allocate all `StringBuilder` capacities with `saturating_mul` and reuse per-batch scratch buffers across records.

**Tech Stack:** Rust 2024, `arrow` v59, `opentelemetry-proto` v0.33, `opentelemetry-semantic-conventions` v0.33, `hex` v0.4, `criterion` v0.8.

**Spec:** `docs/superpowers/specs/2026-10-24-arrow-codec-zero-alloc-design.md`

## Global Constraints

- **Zero-Panic Policy:** Never use `unwrap()`, `expect()`, `panic!()`, or `todo!()` in production paths (`src/` outside `#[cfg(test)]`).
- **Safe Arithmetic:** Always use `saturating_mul` and `saturating_add` when computing builder or buffer capacities (e.g. `total_records.saturating_mul(128)`).
- **Zero Raw Semconv Keys:** Use `opentelemetry_semantic_conventions::resource::SERVICE_NAME` rather than raw `"service.name"` in production code.
- **Path Resolution:** Use `crate::` rather than `super::` in production source code (`super::` is permitted inside `#[cfg(test)]`).
- **Lint Gate:** All code must pass `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc` and `cargo fmt --all -- --check`.

## Review Focus

- **Non-finite `DoubleValue` (`f64::NAN`, `f64::INFINITY`, `f64::NEG_INFINITY`) in `KeyValue` attributes:** Must emit `null` instead of unquoted `NaN`/`inf` so the output is strictly valid RFC 8259 JSON parseable by `serde_json::from_str`.
- **All-`None` `KeyValue` slice (`[KeyValue { key: "a", value: None }, KeyValue { key: "b", value: Some(AnyValue { value: None }) }]`):** Must emit `"{}"` with no stray commas or keys.
- **Duplicate attribute keys in a `KeyValue` slice:** Must deduplicate with last-write-wins semantics (`[{"k": 1}, {"k": 2}]` -> `{"k":2}`), and ignore trailing entries whose value is `None` when checking whether an earlier valid entry is overwritten.
- **Control characters (`\x00`..`\x1f`), quotes, and backslashes in attribute keys and string values:** Must produce valid RFC 8259 escaped strings (`\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\f`, `\u00XX`) that round-trip through `serde_json::from_str`.
- **Multi-resource batches where an earlier resource has populated attributes/service name and a subsequent resource has `resource: None`:** Reusable `resource_attrs_json` must be reset to `"{}"` and `service_name` reset to `"unknown"` without leaking the previous resource's state.

---

### Task 1: Zero-Allocation Hex Encoding, Static Schemas, and Service Name Extractor in `common.rs`

**Files:**
- Modify: `crates/arrow-codec/Cargo.toml`
- Modify: `crates/arrow-codec/src/common.rs`

**Interfaces:**
- Consumes: `hex = { workspace = true }`, `arrow::array::StringBuilder`, `arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit}`, `opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value}`
- Produces (in `arrow_codec::common`):
  - `pub static LOGS_SCHEMA: std::sync::LazyLock<arrow::datatypes::SchemaRef>` (13 fields: `timestamp`, `observed_timestamp`, `severity_number`, `severity_text`, `body`, `trace_id`, `span_id`, `flags`, `attributes`, `service_name`, `resource_attributes`, `scope_name`, `scope_version`)
  - `pub static TRACES_SCHEMA: std::sync::LazyLock<arrow::datatypes::SchemaRef>` (15 fields: `trace_id`, `span_id`, `trace_state`, `parent_span_id`, `name`, `kind`, `timestamp`, `end_time`, `attributes`, `service_name`, `resource_attributes`, `scope_name`, `scope_version`, `status_code`, `status_message`)
  - `pub static METRICS_SCHEMA: std::sync::LazyLock<arrow::datatypes::SchemaRef>` (10 fields: `name`, `description`, `unit`, `timestamp`, `value`, `attributes`, `service_name`, `resource_attributes`, `scope_name`, `scope_version`)
  - `pub fn append_hex_to_builder(builder: &mut arrow::array::StringBuilder, bytes: &[u8])`
  - `pub fn write_hex_to_string(buf: &mut String, bytes: &[u8])`
  - `pub fn to_hex_string(bytes: &[u8]) -> String`
  - `pub fn extract_service_name<'a>(attributes: &'a [KeyValue]) -> std::borrow::Cow<'a, str>`

- [ ] **Step 1: Add `hex` workspace dependency and write failing unit tests in `crates/arrow-codec/src/common.rs`**

Add `hex = { workspace = true }` under `[dependencies]` in `crates/arrow-codec/Cargo.toml`. Then add these tests to `crates/arrow-codec/src/common.rs` inside `mod tests`:

```rust
    #[test]
    fn test_append_hex_zero_copy_equivalence() {
        let mut builder = arrow::array::StringBuilder::with_capacity(5, 256);
        let empty: [u8; 0] = [];
        let span_id = [0x12u8, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0];
        let trace_id = [0xabu8; 16];
        let hash_32 = [0xefu8; 32];
        let oversize_48 = [0x5au8; 48];

        append_hex_to_builder(&mut builder, &empty);
        append_hex_to_builder(&mut builder, &span_id);
        append_hex_to_builder(&mut builder, &trace_id);
        append_hex_to_builder(&mut builder, &hash_32);
        append_hex_to_builder(&mut builder, &oversize_48);

        let arr = builder.finish();
        assert_eq!(arr.value(0), "");
        assert_eq!(arr.value(1), hex::encode(span_id));
        assert_eq!(arr.value(2), hex::encode(trace_id));
        assert_eq!(arr.value(3), hex::encode(hash_32));
        assert_eq!(arr.value(4), hex::encode(oversize_48));
    }

    #[test]
    fn test_write_hex_to_string_chunked() {
        let mut buf = String::new();
        let oversize_48 = [0xcd_u8; 48];
        write_hex_to_string(&mut buf, &oversize_48);
        assert_eq!(buf, hex::encode(oversize_48));
    }

    #[test]
    fn test_extract_service_name_variants() {
        use std::borrow::Cow;
        assert_eq!(extract_service_name(&[]), Cow::Borrowed("unknown"));

        let str_attrs = vec![KeyValue {
            key: opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("checkout".to_string())),
            }),
            ..Default::default()
        }];
        let res = extract_service_name(&str_attrs);
        assert!(matches!(res, Cow::Borrowed("checkout")));

        let int_attrs = vec![KeyValue {
            key: opentelemetry_semantic_conventions::resource::SERVICE_NAME.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(99)),
            }),
            ..Default::default()
        }];
        let res_int = extract_service_name(&int_attrs);
        assert_eq!(res_int.as_ref(), "99");
    }

    #[test]
    fn test_static_schemas_field_counts_and_names() {
        assert_eq!(LOGS_SCHEMA.fields().len(), 13);
        assert_eq!(LOGS_SCHEMA.field(7).name(), "flags");
        assert_eq!(TRACES_SCHEMA.fields().len(), 15);
        assert_eq!(TRACES_SCHEMA.field(3).name(), "parent_span_id");
        assert_eq!(METRICS_SCHEMA.fields().len(), 10);
        assert_eq!(METRICS_SCHEMA.field(0).name(), "name");
        assert_eq!(METRICS_SCHEMA.field(1).name(), "description");
        assert_eq!(METRICS_SCHEMA.field(2).name(), "unit");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p arrow-codec common::tests`
Expected: FAIL to compile (`append_hex_to_builder`, `write_hex_to_string`, `extract_service_name`, `LOGS_SCHEMA`, `TRACES_SCHEMA`, `METRICS_SCHEMA` not found).

- [ ] **Step 3: Implement `LOGS_SCHEMA`, `TRACES_SCHEMA`, `METRICS_SCHEMA`, `append_hex_to_builder`, `write_hex_to_string`, `to_hex_string`, and `extract_service_name` in `crates/arrow-codec/src/common.rs`**

Define the three `LazyLock<SchemaRef>` statics and the four functions in `crates/arrow-codec/src/common.rs` exactly as specified in Section 3.1 of `docs/superpowers/specs/2026-10-24-arrow-codec-zero-alloc-design.md`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p arrow-codec`
Expected: PASS (all existing + 4 new tests pass).

- [ ] **Step 5: Commit**

```bash
git add crates/arrow-codec/Cargo.toml crates/arrow-codec/src/common.rs
git commit -m "perf(arrow-codec): add static schemas, stack hex encoding, and zero-copy service_name (#131)"
```

---

### Task 2: Streaming Typed JSON Attribute Serializer and Unquoted AnyValue Formatter in `common.rs`

**Files:**
- Modify: `crates/arrow-codec/src/common.rs`

**Interfaces:**
- Consumes (from Task 1): `append_hex_to_builder`, `write_hex_to_string`
- Produces (in `arrow_codec::common`):
  - `pub fn escape_json_str(s: &str, buf: &mut String)`
  - `pub fn write_attributes_json(buf: &mut String, attrs: &[KeyValue])`
  - `pub fn write_any_value_string(buf: &mut String, val: &AnyValue)`
  - `pub fn append_any_value_to_builder(builder: &mut arrow::array::StringBuilder, scratch: &mut String, val: Option<&AnyValue>)`
  - `pub fn convert_attributes(attrs: &[KeyValue]) -> String`
  - `pub fn any_value_to_string(val: &AnyValue) -> String`

- [ ] **Step 1: Write failing unit tests for streaming typed JSON attributes and edge cases in `crates/arrow-codec/src/common.rs`**

Update `test_convert_attributes_mixed_types` in `crates/arrow-codec/src/common.rs` to assert typed JSON (`"count":42`, `"rate":1.23`, `"enabled":true`) and add tests covering escaping, non-finite floats, all-`None` attributes, duplicate key deduplication, nested structures, and `append_any_value_to_builder`:

```rust
    #[test]
    fn test_convert_attributes_mixed_types() {
        let attrs = vec![
            KeyValue {
                key: "count".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(42)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "rate".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(1.23)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "enabled".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "str".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("val".to_string())),
                }),
                ..Default::default()
            },
        ];
        let json = convert_attributes(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["count"], serde_json::json!(42));
        assert_eq!(parsed["rate"], serde_json::json!(1.23));
        assert_eq!(parsed["enabled"], serde_json::json!(true));
        assert_eq!(parsed["str"], serde_json::json!("val"));
    }

    #[test]
    fn test_write_attributes_json_non_finite_doubles_and_escaping() {
        let attrs = vec![
            KeyValue {
                key: "nan_val".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(f64::NAN)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "inf_val".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(f64::INFINITY)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "quote\"key\\test".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        "line1\nline2\r\t\x08\x0c\x01\"\\".to_string(),
                    )),
                }),
                ..Default::default()
            },
        ];
        let mut buf = String::new();
        write_attributes_json(&mut buf, &attrs);
        let parsed: serde_json::Value = serde_json::from_str(&buf).unwrap();
        assert!(parsed["nan_val"].is_null());
        assert!(parsed["inf_val"].is_null());
        assert_eq!(
            parsed["quote\"key\\test"],
            serde_json::json!("line1\nline2\r\t\x08\x0c\x01\"\\")
        );
    }

    #[test]
    fn test_write_attributes_json_all_none_and_duplicate_keys() {
        let all_none = vec![
            KeyValue {
                key: "a".to_string(),
                value: None,
                ..Default::default()
            },
            KeyValue {
                key: "b".to_string(),
                value: Some(AnyValue { value: None }),
                ..Default::default()
            },
        ];
        assert_eq!(convert_attributes(&all_none), "{}");

        let dupes = vec![
            KeyValue {
                key: "k".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(1)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "k".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(2)),
                }),
                ..Default::default()
            },
            KeyValue {
                key: "k".to_string(),
                value: None,
                ..Default::default()
            },
        ];
        let json = convert_attributes(&dupes);
        assert_eq!(json, r#"{"k":2}"#);
    }

    #[test]
    fn test_append_any_value_to_builder_all_variants() {
        let mut builder = arrow::array::StringBuilder::with_capacity(5, 128);
        let mut scratch = String::with_capacity(64);

        append_any_value_to_builder(&mut builder, &mut scratch, None);
        append_any_value_to_builder(&mut builder, &mut scratch, Some(&AnyValue { value: None }));
        append_any_value_to_builder(
            &mut builder,
            &mut scratch,
            Some(&AnyValue {
                value: Some(any_value::Value::StringValue("hello".to_string())),
            }),
        );
        append_any_value_to_builder(
            &mut builder,
            &mut scratch,
            Some(&AnyValue {
                value: Some(any_value::Value::IntValue(123)),
            }),
        );
        append_any_value_to_builder(
            &mut builder,
            &mut scratch,
            Some(&AnyValue {
                value: Some(any_value::Value::BytesValue(vec![0xca, 0xfe])),
            }),
        );

        let arr = builder.finish();
        assert_eq!(arr.value(0), "");
        assert_eq!(arr.value(1), "");
        assert_eq!(arr.value(2), "hello");
        assert_eq!(arr.value(3), "123");
        assert_eq!(arr.value(4), "cafe");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p arrow-codec common::tests`
Expected: FAIL (`escape_json_str`, `write_attributes_json`, `append_any_value_to_builder` not defined; `test_convert_attributes_mixed_types` fails because current `convert_attributes` quotes numbers/booleans).

- [ ] **Step 3: Implement `escape_json_str`, `write_attributes_json`, `write_kvlist_json`, `write_any_value_json`, `write_any_value_string`, `append_any_value_to_builder`, `convert_attributes`, and `any_value_to_string` in `crates/arrow-codec/src/common.rs`**

Implement the streaming functions in `crates/arrow-codec/src/common.rs` per Section 3.1 of the spec, removing `use std::collections::HashMap;`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p arrow-codec`
Expected: PASS (all `common::tests` and crate tests pass).

- [ ] **Step 5: Commit**

```bash
git add crates/arrow-codec/src/common.rs
git commit -m "perf(arrow-codec): stream typed JSON attributes without intermediate HashMap (#131)"
```

---

### Task 3: Zero-Allocation Logs & Traces Decoders (`logs.rs` and `traces.rs`)

**Files:**
- Modify: `crates/arrow-codec/src/logs.rs`
- Modify: `crates/arrow-codec/src/traces.rs`

**Interfaces:**
- Consumes (from Tasks 1 & 2 in `crate::common`):
  - `LOGS_SCHEMA`, `TRACES_SCHEMA`
  - `append_hex_to_builder`, `append_any_value_to_builder`, `extract_service_name`, `write_attributes_json`, `timestamp_to_i64`
- Produces:
  - `pub fn decode_logs(req: &ExportLogsServiceRequest) -> Result<RecordBatch, PipelineError>`
  - `pub fn decode_traces(req: &ExportTraceServiceRequest) -> Result<RecordBatch, PipelineError>`

- [ ] **Step 1: Write failing tests for static schema pointer identity, multi-resource state reset, and typed attribute output in `crates/arrow-codec/src/logs.rs` and `crates/arrow-codec/src/traces.rs`**

Add to `crates/arrow-codec/src/logs.rs` (`mod tests`):

```rust
    #[test]
    fn test_decode_logs_static_schema_identity_and_resource_reset() {
        use arrow::array::AsArray;
        let b1 = decode_logs(&ExportLogsServiceRequest::default()).unwrap();
        let b2 = decode_logs(&ExportLogsServiceRequest::default()).unwrap();
        assert!(Arc::ptr_eq(&b1.schema(), &b2.schema()));

        let req = ExportLogsServiceRequest {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: opentelemetry_semantic_conventions::resource::SERVICE_NAME
                                .to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("svc-1".to_string())),
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: 100,
                            attributes: vec![KeyValue {
                                key: "status".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(200)),
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: None,
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: 200,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };
        let batch = decode_logs(&req).unwrap();
        let attrs_col = batch.column_by_name("attributes").unwrap().as_string::<i32>();
        let res_attrs_col = batch
            .column_by_name("resource_attributes")
            .unwrap()
            .as_string::<i32>();
        let svc_col = batch
            .column_by_name("service_name")
            .unwrap()
            .as_string::<i32>();

        assert_eq!(attrs_col.value(0), r#"{"status":200}"#);
        assert_eq!(attrs_col.value(1), "{}");
        assert_eq!(res_attrs_col.value(0), r#"{"service.name":"svc-1"}"#);
        assert_eq!(res_attrs_col.value(1), "{}");
        assert_eq!(svc_col.value(0), "svc-1");
        assert_eq!(svc_col.value(1), "unknown");
    }
```

Add to `crates/arrow-codec/src/traces.rs` (`mod tests`):

```rust
    #[test]
    fn test_decode_traces_static_schema_identity() {
        let b1 = decode_traces(&ExportTraceServiceRequest::default()).unwrap();
        let b2 = decode_traces(&ExportTraceServiceRequest::default()).unwrap();
        assert!(Arc::ptr_eq(&b1.schema(), &b2.schema()));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p arrow-codec`
Expected: FAIL (`Arc::ptr_eq` assertion fails in both `logs.rs` and `traces.rs` because schemas are currently allocated on every call).

- [ ] **Step 3: Refactor `decode_logs` in `crates/arrow-codec/src/logs.rs` and `decode_traces` in `crates/arrow-codec/src/traces.rs`**

Update `logs.rs` and `traces.rs` per Section 3.2 of the spec:
1. Pre-size all `StringBuilder`s with `StringBuilder::with_capacity(total_records, total_records.saturating_mul(...))`.
2. Allocate `let mut resource_attrs_json = String::with_capacity(256);` and `let mut scratch = String::with_capacity(256);` before the resource loop.
3. For each resource: if `Some(ref res)`, extract `service_name` via `extract_service_name(&res.attributes)` and populate `write_attributes_json(&mut resource_attrs_json, &res.attributes)`; if `None`, clear `resource_attrs_json` and push `"{}"`, and set `service_name` to `Cow::Borrowed("unknown")`.
4. Use `append_hex_to_builder` for `trace_id`, `span_id`, and `parent_span_id`, `append_any_value_to_builder` for `log.body`, and `write_attributes_json(&mut scratch, ...)` for record/span attributes.
5. Pass `Arc::clone(&*LOGS_SCHEMA)` and `Arc::clone(&*TRACES_SCHEMA)` to `RecordBatch::try_new`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p arrow-codec`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/arrow-codec/src/logs.rs crates/arrow-codec/src/traces.rs
git commit -m "perf(arrow-codec): pre-size builders, reuse buffers, and cache schemas in logs and traces (#131)"
```

---

### Task 4: Zero-Allocation Metrics Decoder (`metrics.rs`) and Workspace Verification

**Files:**
- Modify: `crates/arrow-codec/src/metrics.rs`

**Interfaces:**
- Consumes (from Tasks 1 & 2 in `crate::common`):
  - `METRICS_SCHEMA`, `extract_service_name`, `write_attributes_json`, `timestamp_to_i64`
- Produces:
  - `pub fn decode_metrics(req: &ExportMetricsServiceRequest) -> Result<RecordBatch, PipelineError>`

- [ ] **Step 1: Write failing test for static schema pointer identity and typed datapoint attributes in `crates/arrow-codec/src/metrics.rs`**

Add to `crates/arrow-codec/src/metrics.rs` (`mod tests`):

```rust
    #[test]
    fn test_decode_metrics_static_schema_identity_and_typed_attributes() {
        let b1 = decode_metrics(&ExportMetricsServiceRequest::default()).unwrap();
        let b2 = decode_metrics(&ExportMetricsServiceRequest::default()).unwrap();
        assert!(Arc::ptr_eq(&b1.schema(), &b2.schema()));

        let r_metric = ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "req_count".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(5),
                            ),
                            attributes: vec![KeyValue {
                                key: "worker".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(3)),
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let batch = decode_metrics(&ExportMetricsServiceRequest {
            resource_metrics: vec![r_metric],
        })
        .unwrap();
        let attrs = batch
            .column_by_name("attributes")
            .unwrap()
            .as_string::<i32>()
            .value(0);
        assert_eq!(attrs, r#"{"worker":3}"#);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p arrow-codec metrics::tests`
Expected: FAIL (`Arc::ptr_eq` assertion fails because `decode_metrics` allocates a new `Schema` on each call).

- [ ] **Step 3: Refactor `MetricsRecordBuilder`, `process_gauge`, `process_sum`, `process_histogram`, and `decode_metrics` in `crates/arrow-codec/src/metrics.rs`**

Update `crates/arrow-codec/src/metrics.rs` per Section 3.2 of the spec:
1. In `MetricsRecordBuilder::with_capacity(capacity)`, replace all `StringBuilder::new()` calls with `StringBuilder::with_capacity(capacity, capacity.saturating_mul(...))`.
2. Add `scratch: &mut String` parameter to `process_gauge`, `process_sum`, and `process_histogram`, calling `write_attributes_json(scratch, &dp.attributes)` and passing `scratch.as_str()` to `builder.append_record`.
3. In `decode_metrics`, reuse `resource_attrs_json` (`String::with_capacity(256)`) and `scratch` (`String::with_capacity(256)`), use `extract_service_name`, and pass `Arc::clone(&*METRICS_SCHEMA)` to `RecordBatch::try_new`.

- [ ] **Step 4: Run crate tests, full workspace tests, clippy, formatting, and benchmarks**

Run:
```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo fmt --all -- --check
cargo bench -p arrow-codec -- --quick
```
Expected: All tests pass, zero clippy warnings, clean formatting, benchmarks complete cleanly.

- [ ] **Step 5: Commit**

```bash
git add crates/arrow-codec/src/metrics.rs
git commit -m "perf(arrow-codec): pre-size builders, reuse buffers, and cache schema in metrics (#131)"
```
