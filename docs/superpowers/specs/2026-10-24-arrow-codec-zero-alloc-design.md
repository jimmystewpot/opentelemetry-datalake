# Design Specification: Zero-Allocation Optimizations for Arrow Codec

- **Status**: Approved
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
3. Arrow builders (`StringBuilder`, `UInt32Builder`, etc.) are initialized with `StringBuilder::new()` without pre-allocated capacity, causing repeated buffer reallocations during batch population.
4. Full `Schema` objects and `Arc<Schema>` references are reconstructed on every decode invocation.
5. `service_name` and `log.body` clone strings unnecessarily on every record when reading `StringValue`.

This design completely eliminates per-record heap allocations in `crates/arrow-codec`, introduces typed JSON attribute serialization, pre-sizes Arrow builders, and caches schemas statically using `std::sync::LazyLock`.

---

## 2. Requirements & Goals

### Functional Requirements
* **Typed JSON Attribute Serialization**:
  * OTLP attributes (`KeyValue` pairs) must be streamed directly into a reusable scratch buffer without allocating an intermediate `HashMap` or heap `String`s.
  * Scalar types must be serialized as typed JSON:
    * `IntValue`: numeric literal (e.g. `42`)
    * `DoubleValue`: floating-point literal (e.g. `3.14`)
    * `BoolValue`: boolean literal (`true` / `false`)
    * `StringValue`: JSON-escaped string (e.g. `"hello"`)
    * `BytesValue`: hex string (e.g. `"010203"`)
    * `ArrayValue`: JSON array (e.g. `[1, 2, "three"]`)
    * `KvlistValue`: nested JSON object (e.g. `{"env":"prod"}`)
  * Attributes where `value` is `None` must be omitted.
  * Empty attribute slices must serialize to `"{}"`.
* **Zero-Allocation Hex ID Encoding**:
  * Trace IDs (16 bytes = 32 hex chars), span IDs (8 bytes = 16 hex chars), and parent span IDs must be formatted using stack-allocated `[u8; 64]` buffers and `hex::encode_to_slice`, appending directly to `StringBuilder` via `builder.append_value(&str)`.
  * Oversized byte slices (>32 bytes) must safely fall back to `hex::encode(bytes)`.
  * Empty byte slices must append `""` to the builder without allocating.
* **Pre-sized Column Builders**:
  * All builders in `logs.rs`, `traces.rs`, and `metrics.rs` must be sized using `total_records` for item capacity and estimated byte counts for string buffer capacities via `StringBuilder::with_capacity(num_rows, estimated_bytes)`.
* **Static Schema Caching**:
  * Schemas for logs, traces, and metrics must be instantiated once and cached statically in `std::sync::LazyLock<SchemaRef>`.
  * Decoders must return `RecordBatch::try_new(Arc::clone(&*SCHEMA), columns)`.
* **Hot-Path String Borrowing**:
  * `service_name` must borrow `&str` directly from `KeyValue::value` when it is `StringValue`, defaulting to `"unknown"` without allocating.
  * In `logs.rs`, `log.body` must borrow `s.as_str()` directly when it is `Some(StringValue(s))`, defaulting to `""` without allocating.

### Non-Functional Requirements & Constraints
* **Zero-Panic Policy**: No `unwrap()`, `expect()`, or panicking array downcasts in production paths. Safe arithmetic and bounds checks throughout.
* **Downstream Compatibility**: Maintain schema and downstream sink compatibility with `crates/storage` (Iceberg), `crates/parquet-sink`, `crates/starrocks-sink`, and `crates/elasticsearch-sink`.
* **Code Quality**: Pass `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc` with zero warnings.

---

## 3. Detailed Component Architecture

### 3.1 `crates/arrow-codec/src/common.rs`

#### Dependencies
Add `hex = { workspace = true }` to `crates/arrow-codec/Cargo.toml`.

#### Static Schema Cache
```rust
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::LazyLock;

pub static LOGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("observed_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_flags", DataType::UInt32, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]))
});

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

pub static METRICS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, false),
        Field::new("metric_unit", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
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
        if hex::encode_to_slice(bytes, &mut buf[..len]).is_ok() {
            if let Ok(s) = std::str::from_utf8(&buf[..len]) {
                builder.append_value(s);
                return;
            }
        }
    }
    builder.append_value(hex::encode(bytes));
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
            c if (c as u32) < 0x20 => {
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
}
```

#### Streaming Typed JSON Serializer
```rust
pub fn write_attributes_json(buf: &mut String, attrs: &[KeyValue]) {
    buf.clear();
    if attrs.is_empty() {
        buf.push_str("{}");
        return;
    }

    buf.push('{');
    let mut first = true;
    for attr in attrs {
        let Some(ref val) = attr.value else {
            continue;
        };
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
            let _ = write!(buf, "{d}");
        }
        Some(any_value::Value::BoolValue(b)) => {
            buf.push_str(if *b { "true" } else { "false" });
        }
        Some(any_value::Value::BytesValue(bytes)) => {
            buf.push('"');
            if bytes.len() <= 32 {
                let mut hex_buf = [0u8; 64];
                let len = bytes.len() * 2;
                if hex::encode_to_slice(bytes, &mut hex_buf[..len]).is_ok() {
                    if let Ok(s) = std::str::from_utf8(&hex_buf[..len]) {
                        buf.push_str(s);
                    }
                }
            } else {
                buf.push_str(&hex::encode(bytes));
            }
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
            let mut inner = String::with_capacity(64);
            write_attributes_json(&mut inner, &kvlist.values);
            buf.push_str(&inner);
        }
        None => {
            buf.push_str("null");
        }
    }
}

#[must_use]
pub fn convert_attributes(attrs: &[KeyValue]) -> String {
    let mut buf = String::with_capacity(attrs.len() * 32 + 2);
    write_attributes_json(&mut buf, attrs);
    buf
}
```

#### Service Name Extraction
```rust
#[must_use]
pub fn extract_service_name<'a>(attributes: &'a [KeyValue]) -> &'a str {
    attributes
        .iter()
        .find(|kv| kv.key == opentelemetry_semantic_conventions::resource::SERVICE_NAME)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|val| match &val.value {
            Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("unknown")
}
```

---

### 3.2 Signal Decoders (`logs.rs`, `traces.rs`, `metrics.rs`)

#### `logs.rs`
1. Compute `total_records`.
2. Pre-size builders:
   * `timestamp_builder`: `total_records`
   * `observed_timestamp_builder`: `total_records`
   * `severity_num_builder`: `total_records`
   * `severity_text_builder`: capacity `(total_records, total_records * 8)`
   * `body_builder`: capacity `(total_records, total_records * 64)`
   * `trace_id_builder`: capacity `(total_records, total_records * 32)`
   * `span_id_builder`: capacity `(total_records, total_records * 16)`
   * `flags_builder`: `total_records`
   * `attributes_builder`: capacity `(total_records, total_records * 128)`
   * `service_name_builder`: capacity `(total_records, total_records * 24)`
   * `resource_attributes_builder`: capacity `(total_records, total_records * 128)`
   * `scope_name_builder`: capacity `(total_records, total_records * 24)`
   * `scope_version_builder`: capacity `(total_records, total_records * 16)`
3. Allocate two reusable buffers:
   * `resource_attrs_json = String::with_capacity(512)` (populated once per `ResourceLogs`)
   * `scratch = String::with_capacity(1024)` (cleared per record)
4. In record loop:
   * Use `append_hex_to_builder` for `trace_id` and `span_id`.
   * For `body`: if `Some(StringValue(s))`, append `s.as_str()`; if `None`, append `""`; otherwise format into scratch and append.
   * `write_attributes_json(&mut scratch, &log.attributes)` and `attributes_builder.append_value(&scratch)`.
5. Return `RecordBatch::try_new(Arc::clone(&*LOGS_SCHEMA), columns)`.

#### `traces.rs`
1. Compute `total_records`.
2. Pre-size builders with estimated capacities.
3. Reuse `resource_attrs_json` and `scratch`.
4. In span loop:
   * Use `append_hex_to_builder` for `trace_id`, `span_id`, and `parent_span_id`.
   * `write_attributes_json(&mut scratch, &span.attributes)` and `attributes_builder.append_value(&scratch)`.
5. Return `RecordBatch::try_new(Arc::clone(&*TRACES_SCHEMA), columns)`.

#### `metrics.rs`
1. Update `MetricsRecordBuilder::with_capacity(capacity)` to pre-allocate all string builders with estimated capacities.
2. In `decode_metrics`, maintain `scratch = String::with_capacity(1024)`.
3. Pass `&mut scratch` into `process_gauge`, `process_sum`, and `process_histogram` to format `dp.attributes` without allocating.
4. Return `RecordBatch::try_new(Arc::clone(&*METRICS_SCHEMA), columns)`.

---

## 4. Verification & Testing

1. **Equivalence & Behavior Tests**:
   * `test_append_hex_zero_copy_equivalence`: compares `append_hex_to_builder` with `hex::encode` for empty, 8B, 16B, 32B, and 48B slices.
   * `test_convert_attributes_typed_json`: verifies typed integers (`"count":42`), floats (`"rate":1.23`), booleans (`"enabled":true`), strings (`"name":"app"`), and nested structures.
   * `test_escape_json_str`: tests escaping of `\"`, `\\`, control chars, newlines, and raw ASCII.
   * `test_static_schema_pointer_identity`: verifies `Arc::ptr_eq(batch1.schema(), batch2.schema())`.
2. **Regression Testing**:
   * All 49 existing unit tests in `arrow-codec` pass.
   * Entire workspace test suite (`cargo test --workspace`) passes with zero failures.
3. **Performance Benchmarks**:
   * Benchmark comparison using `cargo bench -p arrow-codec` before and after changes.
4. **Linting & Formatting**:
   * `cargo fmt --all -- --check`
   * `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
