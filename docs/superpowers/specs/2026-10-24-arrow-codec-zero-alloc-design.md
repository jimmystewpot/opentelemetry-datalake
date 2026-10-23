# Design Specification: Zero-Allocation Optimizations for Arrow Codec

- **Status**: Approved (Updated after Principal Architecture Review)
- **Author**: Antigravity Assistant & Jimmy Stewpot
- **Date**: 2026-10-24
- **Target Issue**: [#131](https://github.com/jimmystewpot/opentelemetry-datalake/issues/131)
- **Component**: `crates/arrow-codec`

---

## 1. Overview & Context

`opentelemetry-datalake` is an ultra-high-throughput receiver translating OTLP metrics, traces, and logs directly into Apache Arrow `RecordBatch` streams for ingestion into lakehouses and storage backends.

As highlighted in GitHub Issue [#131](https://github.com/jimmystewpot/opentelemetry-datalake/issues/131) and mandated by `AGENTS.md` (Section 2: Memory & Layout Strategy), the current `arrow-codec` decoders exhibit hot-path heap churn:
1. `convert_attributes` allocates an intermediate `HashMap<&str, String>`, per-value `String`s, and a final serialized JSON `String` for every record.
2. `to_hex_string` allocates temporary heap `String`s for trace IDs and span IDs that are immediately copied into an Arrow `StringBuilder` and dropped.
3. Arrow `StringBuilder` instances are initialized with `StringBuilder::new()` without pre-allocated capacity, causing repeated buffer reallocations during batch population.
4. Full `Schema` objects and `Arc<Schema>` references are reconstructed on every decode invocation.
5. `service_name` and `log.body` clone strings unnecessarily on every record when reading `StringValue`.

This design eliminates per-record heap allocations in `crates/arrow-codec`, introduces typed JSON attribute serialization, pre-sizes Arrow builders using safe arithmetic, and caches schemas statically using `std::sync::LazyLock`.

---

## 2. Requirements & Goals

### Functional Requirements
* **Typed JSON Attribute Serialization**:
  * OTLP attributes (`KeyValue` pairs) must be streamed directly into a reusable scratch buffer without allocating an intermediate `HashMap` or heap `String`s (including for nested `KvlistValue` and `ArrayValue` variants).
  * Scalar and composite types must be serialized as valid RFC 8259 typed JSON:
    * `IntValue`: numeric literal (e.g. `42`)
    * `DoubleValue`: floating-point literal if finite (e.g. `3.14`), or `null` if non-finite (`NaN`, `Infinity`, `-Infinity`) to guarantee valid RFC 8259 JSON for downstream parsers (`ComplianceEngine`, `parquet-sink`, `elasticsearch-sink`)
    * `BoolValue`: boolean literal (`true` / `false`)
    * `StringValue`: JSON-escaped string (e.g. `"hello"`)
    * `BytesValue`: lowercase hex string (e.g. `"010203"`), encoded via stack buffer chunks with zero heap allocation
    * `ArrayValue`: JSON array (e.g. `[1,2,"three"]`)
    * `KvlistValue`: nested JSON object (e.g. `{"env":"prod"}`), written directly into the active buffer without allocating an intermediate `String`
  * Attributes where `value` is `None` (or `AnyValue { value: None }`) must be omitted from top-level/nested attribute objects (matching `HashMap` omission semantics); inside `ArrayValue`, `AnyValue { value: None }` serializes as `null`.
  * Duplicate keys within the same `KeyValue` slice must follow last-write-wins deduplication semantics (matching the previous `HashMap` behavior).
  * Empty attribute slices (or slices where all values are `None`) must serialize to `"{}"`.
* **Zero-Allocation Hex ID Encoding**:
  * Trace IDs (16 bytes = 32 hex chars), span IDs (8 bytes = 16 hex chars), and parent span IDs must be formatted using stack-allocated `[u8; 64]` buffers and `hex::encode_to_slice`, appending directly to `StringBuilder` via `builder.append_value(&str)`.
  * Oversized byte slices (>32 bytes) must safely fall back to `hex::encode(bytes)`.
  * Empty byte slices must append `""` to the builder without allocating.
* **Pre-sized Column Builders**:
  * All builders in `logs.rs`, `traces.rs`, and `metrics.rs` must be sized using `total_records` for item capacity and estimated byte counts (using `saturating_mul`) for string buffer capacities via `StringBuilder::with_capacity(num_rows, estimated_bytes)`.
* **Static Schema Caching**:
  * Schemas for logs (13 fields), traces (15 fields), and metrics (10 fields) must match the exact existing column names and types, instantiated once and cached statically in `std::sync::LazyLock<SchemaRef>`.
  * Decoders must return `RecordBatch::try_new(Arc::clone(&*SCHEMA), columns)`.
* **Hot-Path String Borrowing**:
  * `extract_service_name` returns `Cow<'a, str>`: borrowing `s.as_str()` directly when `service.name` is `StringValue(s)`, defaulting to `Cow::Borrowed("unknown")` when absent or `None`, and falling back to `Cow::Owned(any_value_to_string(val))` for non-string scalar variants.
  * In `logs.rs`, `log.body` appends `s.as_str()` directly when `Some(StringValue(s))`, appends `""` when `None` or `AnyValue { value: None }`, and formats into the reusable `scratch` buffer via `write_any_value_string` for non-string variants.

### Non-Functional Requirements & Constraints
* **Zero-Panic Policy**: No `unwrap()`, `expect()`, or panicking array downcasts in production paths. Use `saturating_mul` / `saturating_add` for capacity calculations.
* **Downstream Compatibility**: Maintain exact column names, types, and downstream sink compatibility with `crates/storage` (Iceberg), `crates/parquet-sink`, `crates/starrocks-sink`, `crates/elasticsearch-sink`, and `crates/kafka-sink`.
* **Code Quality**: Pass `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc` with zero warnings.

---

## 3. Detailed Component Architecture

### 3.1 `crates/arrow-codec/src/common.rs`

#### Dependencies
Add `hex = { workspace = true }` to `crates/arrow-codec/Cargo.toml`.

#### Static Schema Cache
The cached schemas preserve the exact field names, types, nullability, and ordering of the existing decoders:

```rust
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::{Arc, LazyLock};

/// Cached Arrow schema for OTLP Logs (13 fields).
pub static LOGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("observed_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]))
});

/// Cached Arrow schema for OTLP Traces (15 fields).
pub static TRACES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, false),
    ]))
});

/// Cached Arrow schema for OTLP Metrics (10 fields).
pub static METRICS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]))
});
```

#### Stack-Buffered Hex Formatting
```rust
#[inline]
pub fn append_hex_to_builder(builder: &mut StringBuilder, bytes: &[u8]) {
    if bytes.is_empty() {
        builder.append_value("");
        return;
    }
    if bytes.len() <= 32 {
        let mut buf = [0u8; 64];
        let len = bytes.len() * 2;
        if hex::encode_to_slice(bytes, &mut buf[..len]).is_ok()
            && let Ok(s) = std::str::from_utf8(&buf[..len])
        {
            builder.append_value(s);
            return;
        }
    }
    builder.append_value(hex::encode(bytes));
}

#[inline]
pub fn write_hex_to_string(buf: &mut String, bytes: &[u8]) {
    let mut hex_buf = [0u8; 64];
    for chunk in bytes.chunks(32) {
        let len = chunk.len() * 2;
        if hex::encode_to_slice(chunk, &mut hex_buf[..len]).is_ok()
            && let Ok(s) = std::str::from_utf8(&hex_buf[..len])
        {
            buf.push_str(s);
        }
    }
}

#[must_use]
pub fn to_hex_string(bytes: &[u8]) -> String {
    hex::encode(bytes)
}
```

#### Fast JSON String Escaping
```rust
pub fn escape_json_str(s: &str, buf: &mut String) {
    if !s.bytes().any(|b| b == b'"' || b == b'\\' || b < 0x20) {
        buf.push_str(s);
        return;
    }
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            '\x08' => buf.push_str("\\b"),
            '\x0C' => buf.push_str("\\f"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(buf, "\\u{:04x}", u32::from(c));
            }
            c => buf.push(c),
        }
    }
}
```

#### Streaming Typed JSON Serializer (Zero-Allocation Including Nested Structures)
```rust
pub fn write_attributes_json(buf: &mut String, attrs: &[KeyValue]) {
    buf.clear();
    write_kvlist_json(buf, attrs);
}

fn write_kvlist_json(buf: &mut String, attrs: &[KeyValue]) {
    if attrs.is_empty() {
        buf.push_str("{}");
        return;
    }

    buf.push('{');
    let mut first = true;
    for (idx, attr) in attrs.iter().enumerate() {
        let Some(ref val) = attr.value else {
            continue;
        };
        if val.value.is_none() {
            continue;
        }
        // Last-write-wins deduplication if duplicate keys exist in the slice
        let is_overwritten = attrs[idx + 1..]
            .iter()
            .any(|later| later.value.as_ref().is_some_and(|v| v.value.is_some()) && later.key == attr.key);
        if is_overwritten {
            continue;
        }

        if !first {
            buf.push(',');
        }
        first = false;

        buf.push('"');
        escape_json_str(attr.key.as_str(), buf);
        buf.push_str("\":");

        write_any_value_json(buf, val);
    }
    buf.push('}');
}

fn write_any_value_json(buf: &mut String, val: &AnyValue) {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => {
            buf.push('"');
            escape_json_str(s.as_str(), buf);
            buf.push('"');
        }
        Some(any_value::Value::IntValue(i)) => {
            let _ = write!(buf, "{i}");
        }
        Some(any_value::Value::DoubleValue(d)) => {
            if d.is_finite() {
                let start = buf.len();
                let _ = write!(buf, "{d}");
                if !buf.as_bytes()[start..]
                    .iter()
                    .any(|&b| b == b'.' || b == b'e' || b == b'E')
                {
                    buf.push_str(".0");
                }
            } else {
                buf.push_str("null");
            }
        }
        Some(any_value::Value::BoolValue(b)) => {
            buf.push_str(if *b { "true" } else { "false" });
        }
        Some(any_value::Value::BytesValue(bytes)) => {
            buf.push('"');
            write_hex_to_string(buf, bytes);
            buf.push('"');
        }
        Some(any_value::Value::ArrayValue(arr)) => {
            buf.push('[');
            for (idx, item) in arr.values.iter().enumerate() {
                if idx > 0 {
                    buf.push(',');
                }
                write_any_value_json(buf, item);
            }
            buf.push(']');
        }
        Some(any_value::Value::KvlistValue(kvlist)) => {
            write_kvlist_json(buf, &kvlist.values);
        }
        None => {
            buf.push_str("null");
        }
    }
}

#[must_use]
pub fn convert_attributes(attrs: &[KeyValue]) -> String {
    let mut buf = String::with_capacity(attrs.len().saturating_mul(32).saturating_add(2));
    write_attributes_json(&mut buf, attrs);
    buf
}
```

#### Non-JSON String Representation (`any_value_to_string` & `append_any_value_to_builder`)
Used for `log.body` and fallback string conversions where scalar strings are unquoted:

```rust
pub fn write_any_value_string(buf: &mut String, val: &AnyValue) {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => buf.push_str(s),
        Some(any_value::Value::IntValue(i)) => {
            let _ = write!(buf, "{i}");
        }
        Some(any_value::Value::DoubleValue(d)) => {
            let _ = write!(buf, "{d}");
        }
        Some(any_value::Value::BoolValue(b)) => {
            buf.push_str(if *b { "true" } else { "false" });
        }
        Some(any_value::Value::KvlistValue(kvlist)) => {
            write_kvlist_json(buf, &kvlist.values);
        }
        Some(any_value::Value::ArrayValue(arr)) => {
            buf.push('[');
            for (idx, elem) in arr.values.iter().enumerate() {
                if idx > 0 {
                    buf.push(',');
                }
                write_any_value_string(buf, elem);
            }
            buf.push(']');
        }
        Some(any_value::Value::BytesValue(bytes)) => {
            write_hex_to_string(buf, bytes);
        }
        None => {}
    }
}

#[inline]
pub fn append_any_value_to_builder(
    builder: &mut StringBuilder,
    scratch: &mut String,
    val: Option<&AnyValue>,
) {
    let Some(v) = val else {
        builder.append_value("");
        return;
    };
    match &v.value {
        None => builder.append_value(""),
        Some(any_value::Value::StringValue(s)) => builder.append_value(s.as_str()),
        Some(any_value::Value::BoolValue(b)) => {
            builder.append_value(if *b { "true" } else { "false" });
        }
        Some(any_value::Value::BytesValue(bytes)) => {
            append_hex_to_builder(builder, bytes);
        }
        _ => {
            scratch.clear();
            write_any_value_string(scratch, v);
            builder.append_value(scratch.as_str());
        }
    }
}

#[must_use]
pub fn any_value_to_string(val: &AnyValue) -> String {
    let mut buf = String::new();
    write_any_value_string(&mut buf, val);
    buf
}
```

#### Service Name Extraction
```rust
#[must_use]
pub fn extract_service_name(attributes: &[KeyValue]) -> std::borrow::Cow<'_, str> {
    let Some(val) = attributes
        .iter()
        .find(|kv| kv.key == opentelemetry_semantic_conventions::resource::SERVICE_NAME)
        .and_then(|kv| kv.value.as_ref())
    else {
        return std::borrow::Cow::Borrowed("unknown");
    };

    match &val.value {
        Some(any_value::Value::StringValue(s)) => std::borrow::Cow::Borrowed(s.as_str()),
        None => std::borrow::Cow::Borrowed("unknown"),
        Some(_) => std::borrow::Cow::Owned(any_value_to_string(val)),
    }
}
```

---

### 3.2 Signal Decoders (`logs.rs`, `traces.rs`, `metrics.rs`)

#### `logs.rs`
1. Compute `total_records`.
2. Pre-size builders using `saturating_mul`:
   * `timestamp_builder`: `total_records`
   * `observed_timestamp_builder`: `total_records`
   * `severity_num_builder`: `total_records`
   * `severity_text_builder`: `(total_records, total_records.saturating_mul(8))`
   * `body_builder`: `(total_records, total_records.saturating_mul(64))`
   * `trace_id_builder`: `(total_records, total_records.saturating_mul(32))`
   * `span_id_builder`: `(total_records, total_records.saturating_mul(16))`
   * `flags_builder`: `total_records`
   * `attributes_builder`: `(total_records, total_records.saturating_mul(128))`
   * `service_name_builder`: `(total_records, total_records.saturating_mul(24))`
   * `resource_attributes_builder`: `(total_records, total_records.saturating_mul(128))`
   * `scope_name_builder`: `(total_records, total_records.saturating_mul(24))`
   * `scope_version_builder`: `(total_records, total_records.saturating_mul(16))`
3. Allocate two reusable buffers once per `decode_logs` call:
   * `resource_attrs_json = String::with_capacity(256)` (re-populated once per `ResourceLogs`)
   * `scratch = String::with_capacity(256)` (reused per record for body fallback and attributes)
4. In record loop:
   * Use `append_any_value_to_builder(&mut body_builder, &mut scratch, log.body.as_ref())`.
   * Use `append_hex_to_builder` for `trace_id` and `span_id`.
   * `write_attributes_json(&mut scratch, &log.attributes)` and `attributes_builder.append_value(&scratch)`.
5. Return `RecordBatch::try_new(Arc::clone(&*LOGS_SCHEMA), columns)`.

#### `traces.rs`
1. Compute `total_records`.
2. Pre-size builders using `saturating_mul`:
   * `trace_id_builder`: `(total_records, total_records.saturating_mul(32))`
   * `span_id_builder`: `(total_records, total_records.saturating_mul(16))`
   * `trace_state_builder`: `(total_records, total_records.saturating_mul(16))`
   * `parent_span_id_builder`: `(total_records, total_records.saturating_mul(16))`
   * `name_builder`: `(total_records, total_records.saturating_mul(32))`
   * `kind_builder`: `total_records`
   * `timestamp_builder`: `total_records`
   * `end_time_builder`: `total_records`
   * `attributes_builder`: `(total_records, total_records.saturating_mul(128))`
   * `service_name_builder`: `(total_records, total_records.saturating_mul(24))`
   * `resource_attributes_builder`: `(total_records, total_records.saturating_mul(128))`
   * `scope_name_builder`: `(total_records, total_records.saturating_mul(24))`
   * `scope_version_builder`: `(total_records, total_records.saturating_mul(16))`
   * `status_code_builder`: `total_records`
   * `status_message_builder`: `(total_records, total_records.saturating_mul(16))`
3. Reuse `resource_attrs_json` and `scratch`.
4. In span loop:
   * Use `append_hex_to_builder` for `trace_id`, `span_id`, and `parent_span_id`.
   * `write_attributes_json(&mut scratch, &span.attributes)` and `attributes_builder.append_value(&scratch)`.
5. Return `RecordBatch::try_new(Arc::clone(&*TRACES_SCHEMA), columns)`.

#### `metrics.rs`
1. Update `MetricsRecordBuilder::with_capacity(capacity)` to pre-allocate all string builders using `saturating_mul`:
   * `name`: `(capacity, capacity.saturating_mul(32))`
   * `desc`: `(capacity, capacity.saturating_mul(64))`
   * `unit`: `(capacity, capacity.saturating_mul(8))`
   * `attributes`: `(capacity, capacity.saturating_mul(128))`
   * `service_name`: `(capacity, capacity.saturating_mul(24))`
   * `resource_attributes`: `(capacity, capacity.saturating_mul(128))`
   * `scope_name`: `(capacity, capacity.saturating_mul(24))`
   * `scope_version`: `(capacity, capacity.saturating_mul(16))`
2. In `decode_metrics`, maintain `resource_attrs_json = String::with_capacity(256)` and `scratch = String::with_capacity(256)`.
3. Pass `&mut scratch` into `process_gauge`, `process_sum`, and `process_histogram` to format `dp.attributes` via `write_attributes_json(&mut scratch, &dp.attributes)` without per-record allocations.
4. Return `RecordBatch::try_new(Arc::clone(&*METRICS_SCHEMA), columns)`.

---

## 4. Verification & Testing

1. **Equivalence & Behavior Tests**:
   * `test_append_hex_zero_copy_equivalence`: compares `append_hex_to_builder` with `hex::encode` for empty, 8B, 16B, 32B, and 48B slices.
   * `test_convert_attributes_typed_json`: verifies typed integers (`"count":42`), floats (`"rate":1.23`), non-finite floats (`"nan":null`), booleans (`"enabled":true`), strings (`"name":"app"`), duplicate key deduplication, and nested `KvlistValue` / `ArrayValue` structures.
   * `test_escape_json_str`: tests escaping of `\"`, `\\`, control chars (`\x00`–`\x1f`), newlines, and raw UTF-8.
   * `test_static_schema_pointer_identity`: verifies `Arc::ptr_eq(&batch1.schema(), &batch2.schema())` across consecutive calls to `decode_logs`, `decode_traces`, and `decode_metrics`.
2. **Regression Testing**:
   * All existing unit tests in `arrow-codec` pass (with `test_convert_attributes_mixed_types` updated to assert typed JSON values).
   * Entire workspace test suite (`cargo test --workspace`) passes with zero failures.
3. **Performance Benchmarks**:
   * Benchmark comparison using `cargo bench -p arrow-codec`.
4. **Linting & Formatting**:
   * `cargo fmt --all -- --check`
   * `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
