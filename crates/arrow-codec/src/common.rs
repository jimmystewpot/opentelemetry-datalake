/// Shared OTLP-to-Arrow conversion helpers used across all signal codecs.
///
/// These functions handle the transformation of OpenTelemetry protobuf
/// attribute structures into JSON-serialized strings suitable for Arrow
/// `Utf8` columns. Centralised here to avoid duplication across
/// logs, traces, and metrics decoders.
use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use pipeline_core::error::PipelineError;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Write;
use std::sync::{Arc, LazyLock};

/// Cached Arrow schema for OTLP Logs (13 fields).
pub static LOGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
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
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
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
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ]))
});

/// Appends a hex-encoded byte slice directly to an Arrow [`StringBuilder`].
///
/// Uses a stack-allocated buffer for byte slices up to 32 bytes (such as 16-byte
/// trace IDs and 8-byte span IDs) to completely eliminate heap allocation on the hot path.
/// Larger slices safely fall back to [`hex::encode`].
#[inline]
pub fn append_hex_to_builder(builder: &mut StringBuilder, bytes: &[u8]) {
    if bytes.is_empty() {
        builder.append_value("");
        return;
    }
    if bytes.len() <= 32 {
        let mut buf = [0u8; 64];
        let len = bytes.len().saturating_mul(2);
        if hex::encode_to_slice(bytes, &mut buf[..len]).is_ok()
            && let Ok(s) = std::str::from_utf8(&buf[..len])
        {
            builder.append_value(s);
            return;
        }
    }
    builder.append_value(hex::encode(bytes));
}

/// Appends a hex-encoded byte slice into an existing [`String`] buffer without allocating.
///
/// Encodes in stack-allocated chunks of up to 32 bytes to avoid heap allocations.
#[inline]
pub fn write_hex_to_string(buf: &mut String, bytes: &[u8]) {
    buf.reserve(bytes.len().saturating_mul(2));
    let mut hex_buf = [0u8; 64];
    for chunk in bytes.chunks(32) {
        let len = chunk.len().saturating_mul(2);
        if hex::encode_to_slice(chunk, &mut hex_buf[..len]).is_ok()
            && let Ok(s) = std::str::from_utf8(&hex_buf[..len])
        {
            buf.push_str(s);
        }
    }
}

/// Encodes a byte slice as a lowercase hexadecimal string.
///
/// Used primarily for trace IDs (16 bytes) and span IDs (8 bytes).
#[must_use]
pub fn to_hex_string(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Extracts the service name from a slice of OTLP resource attributes.
///
/// Returns a borrowed [`Cow`] when `service.name` is present as a string value,
/// defaults to `"unknown"` when missing, and formats non-string scalars via [`any_value_to_string`].
#[must_use]
pub fn extract_service_name(attributes: &[KeyValue]) -> Cow<'_, str> {
    let Some(val) = attributes
        .iter()
        .rfind(|kv| {
            kv.key == opentelemetry_semantic_conventions::resource::SERVICE_NAME
                && kv.value.as_ref().is_some_and(|v| v.value.is_some())
        })
        .and_then(|kv| kv.value.as_ref())
    else {
        return Cow::Borrowed("unknown");
    };

    match &val.value {
        Some(any_value::Value::StringValue(s)) => Cow::Borrowed(s.as_str()),
        None => Cow::Borrowed("unknown"),
        Some(_) => Cow::Owned(any_value_to_string(val)),
    }
}

/// Escapes special JSON characters in `s` and appends the result to `buf`.
///
/// If `s` contains no characters requiring escaping, appends `s` directly without allocation.
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

/// Serializes a slice of OTLP [`KeyValue`] attributes into `buf` as a JSON object.
///
/// Clears `buf` before serializing. Attributes with `None` values are omitted,
/// and duplicate keys adhere to last-write-wins deduplication.
pub fn write_attributes_json(buf: &mut String, attrs: &[KeyValue]) {
    buf.clear();
    write_kvlist_json(buf, attrs);
}

/// Threshold up to which duplicate-key detection uses a zero-allocation linear lookahead scan.
/// Slices larger than this threshold switch to a linear-time `HashSet` pass to prevent
/// quadratic algorithmic complexity on pathological payloads.
const LINEAR_SCAN_THRESHOLD: usize = 32;

/// Serializes a slice of OTLP [`KeyValue`] attributes into `buf` as a JSON object without clearing `buf`.
///
/// Attributes with `None` values are omitted, and duplicate keys adhere to
/// last-write-wins deduplication.
pub fn write_kvlist_json(buf: &mut String, attrs: &[KeyValue]) {
    if attrs.is_empty() {
        buf.push_str("{}");
        return;
    }

    // For slices larger than `LINEAR_SCAN_THRESHOLD`, precompute the winning (last non-None)
    // index for each key in O(n) time so pathological payloads cannot trigger O(n^2) scans.
    let keep_mask: Option<Vec<bool>> = if attrs.len() > LINEAR_SCAN_THRESHOLD {
        let mut seen = HashSet::with_capacity(attrs.len());
        let mut keep = vec![false; attrs.len()];
        for (idx, attr) in attrs.iter().enumerate().rev() {
            if attr.value.as_ref().is_some_and(|v| v.value.is_some())
                && seen.insert(attr.key.as_str())
                && let Some(slot) = keep.get_mut(idx)
            {
                *slot = true;
            }
        }
        Some(keep)
    } else {
        None
    };

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
        let is_overwritten = if let Some(ref keep) = keep_mask {
            !keep.get(idx).copied().unwrap_or(false)
        } else {
            attrs.get(idx.saturating_add(1)..).is_some_and(|remaining| {
                remaining.iter().any(|later| {
                    later.key == attr.key && later.value.as_ref().is_some_and(|v| v.value.is_some())
                })
            })
        };
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

/// Serializes a single OTLP [`AnyValue`] as typed JSON into `buf`.
///
/// Floats are formatted as numbers if finite, or `"null"` if non-finite.
/// Strings and nested key-value lists are properly escaped.
pub fn write_any_value_json(buf: &mut String, val: &AnyValue) {
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
        _ => {
            buf.push_str("null");
        }
    }
}

/// Serializes a single OTLP [`AnyValue`] into `buf` as an unquoted string.
///
/// Strings are appended directly without quotes or escaping.
/// Nested structures (arrays, key-value lists) and numbers are formatted cleanly.
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
        _ => {}
    }
}

/// Appends a single OTLP [`AnyValue`] to an Arrow [`StringBuilder`].
///
/// Reuses `scratch` for formatting non-string variants to avoid heap allocation.
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

/// Converts a single OTLP [`AnyValue`] into its string representation.
///
/// Handles all value variants defined in the OpenTelemetry protobuf spec:
/// - Scalar types: string, int, double, bool
/// - Nested types: key-value lists (recursively serialized as JSON)
/// - Array types: serialized as `[elem1,elem2,...]`
/// - Bytes: hex-encoded via [`to_hex_string`]
/// - Unknown/None: empty string
#[must_use]
pub fn any_value_to_string(val: &AnyValue) -> String {
    let mut buf = String::new();
    write_any_value_string(&mut buf, val);
    buf
}

/// Serializes a slice of OTLP [`KeyValue`] attributes into a JSON object string.
///
/// Pre-allocates buffer capacity based on the attribute count using safe arithmetic.
#[must_use]
pub fn convert_attributes(attrs: &[KeyValue]) -> String {
    let mut buf = String::with_capacity(attrs.len().saturating_mul(32).saturating_add(2));
    write_attributes_json(&mut buf, attrs);
    buf
}

/// Safely converts an OTLP `uint64` nanosecond timestamp into an `i64`.
///
/// OTLP timestamps are protobuf `uint64`, but Arrow's
/// `TimestampNanosecondType` stores them as `i64`. Values above `i64::MAX`
/// (~year 2262) are rejected rather than silently wrapping.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if the value exceeds `i64::MAX`.
pub fn timestamp_to_i64(nanos: u64) -> Result<i64, PipelineError> {
    i64::try_from(nanos).map_err(|_| {
        PipelineError::Internal(format!(
            "OTLP timestamp {nanos} exceeds i64::MAX, cannot represent in Arrow TimestampNanosecond"
        ))
    })
}

/// Safely downcasts an Arrow column to a `StringArray`.
///
/// Replaces the panicking `col.as_string::<i32>()` pattern with a checked
/// downcast that returns a descriptive error instead.
///
/// # Errors
///
/// Returns `PipelineError::Internal` if the column is not a UTF-8 string array.
pub fn downcast_string_array<'a>(
    col: &'a dyn arrow::array::Array,
    col_name: &str,
) -> Result<&'a arrow::array::StringArray, PipelineError> {
    col.as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| {
            PipelineError::Internal(format!("column '{col_name}' is not a Utf8 `StringArray`"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

    #[test]
    fn test_to_hex_string_empty() {
        assert_eq!(to_hex_string(&[]), "");
    }

    #[test]
    fn test_to_hex_string_trace_id() {
        let bytes = [0x01; 16];
        assert_eq!(to_hex_string(&bytes), "01010101010101010101010101010101");
    }

    #[test]
    fn test_to_hex_string_span_id() {
        let bytes = [0xff; 8];
        assert_eq!(to_hex_string(&bytes), "ffffffffffffffff");
    }

    #[test]
    fn test_convert_attributes_empty() {
        assert_eq!(convert_attributes(&[]), "{}");
    }

    #[test]
    fn test_convert_attributes_string_value() {
        let attrs = vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("my-svc".to_string())),
            }),
            ..Default::default()
        }];
        let json = convert_attributes(&attrs);
        assert!(json.contains("\"service.name\""));
        assert!(json.contains("\"my-svc\""));
    }

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
            KeyValue {
                key: "whole_float".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(42.0)),
                }),
                ..Default::default()
            },
        ];
        let json = convert_attributes(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["count"], serde_json::json!(42));
        assert!(parsed["count"].is_i64());
        assert_eq!(parsed["rate"], serde_json::json!(1.23));
        assert!(parsed["rate"].is_f64());
        assert!(parsed["whole_float"].is_f64());
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

    #[test]
    fn test_any_value_to_string_bytes() {
        let val = AnyValue {
            value: Some(any_value::Value::BytesValue(vec![0xde, 0xad])),
        };
        assert_eq!(any_value_to_string(&val), "dead");
    }

    #[test]
    fn test_any_value_to_string_none() {
        let val = AnyValue { value: None };
        assert_eq!(any_value_to_string(&val), "");
    }

    #[test]
    fn test_timestamp_to_i64_valid() {
        assert_eq!(timestamp_to_i64(1_000_000_000).unwrap(), 1_000_000_000);
        assert_eq!(timestamp_to_i64(0).unwrap(), 0);
    }

    #[test]
    fn test_timestamp_to_i64_max() {
        let max = i64::MAX as u64;
        assert!(timestamp_to_i64(max).is_ok());
        assert!(timestamp_to_i64(max + 1).is_err());
    }

    #[test]
    fn test_any_value_to_string_int() {
        let val = AnyValue {
            value: Some(any_value::Value::IntValue(42)),
        };
        assert_eq!(any_value_to_string(&val), "42");
    }

    #[test]
    fn test_any_value_to_string_double() {
        let val = AnyValue {
            value: Some(any_value::Value::DoubleValue(123.45)),
        };
        let s = any_value_to_string(&val);
        assert!(s.starts_with("123.45"), "expected '123.45…', got: {s}");
    }

    #[test]
    fn test_any_value_to_string_bool() {
        let val_true = AnyValue {
            value: Some(any_value::Value::BoolValue(true)),
        };
        let val_false = AnyValue {
            value: Some(any_value::Value::BoolValue(false)),
        };
        assert_eq!(any_value_to_string(&val_true), "true");
        assert_eq!(any_value_to_string(&val_false), "false");
    }

    #[test]
    fn test_any_value_to_string_array() {
        use opentelemetry_proto::tonic::common::v1::ArrayValue;
        let val = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(any_value::Value::StringValue("x".to_string())),
                    },
                    AnyValue {
                        value: Some(any_value::Value::IntValue(7)),
                    },
                ],
            })),
        };
        let s = any_value_to_string(&val);
        assert_eq!(s, "[x,7]", "Array must be serialised as comma-list: {s}");
    }

    #[test]
    fn test_any_value_to_string_kvlist() {
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        let val = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "env".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("prod".to_string())),
                    }),
                    ..Default::default()
                }],
            })),
        };
        let s = any_value_to_string(&val);
        assert!(
            s.contains("\"env\""),
            "kvlist must produce JSON with key: {s}"
        );
        assert!(
            s.contains("\"prod\""),
            "kvlist must produce JSON with value: {s}"
        );
    }

    /// `downcast_string_array` must return `PipelineError::Internal` when the
    /// column is not a Utf8 `StringArray`.
    #[test]
    fn test_downcast_string_array_error_on_wrong_type() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as _])
                .unwrap();
        let col = batch.column(0);
        let result = downcast_string_array(col.as_ref(), "n");
        assert!(
            result.is_err(),
            "downcast_string_array must fail for a non-string column"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains('n'),
            "Error message must reference the column name: {err}"
        );
    }

    /// Attributes where `value` is None must be silently dropped from the
    /// resulting JSON object.
    #[test]
    fn test_convert_attributes_value_none_is_omitted() {
        let attrs = vec![KeyValue {
            key: "dropped".to_string(),
            value: None,
            ..Default::default()
        }];
        let json = convert_attributes(&attrs);
        // The key must not appear in the output
        assert!(
            !json.contains("dropped"),
            "Attribute with None value must be omitted: {json}"
        );
    }

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

    #[test]
    fn test_write_kvlist_json_large_slice_linear_deduplication() {
        // Construct > 32 attributes (LINEAR_SCAN_THRESHOLD) with duplicates and trailing None values
        let mut attrs = Vec::with_capacity(100);
        for i in 0_i64..64 {
            attrs.push(KeyValue {
                key: format!("key_{i}"),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(i)),
                }),
                ..Default::default()
            });
        }
        // Overwrite key_0 and key_33 with new values (last-write-wins)
        attrs.push(KeyValue {
            key: "key_0".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(1000)),
            }),
            ..Default::default()
        });
        attrs.push(KeyValue {
            key: "key_33".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(2033)),
            }),
            ..Default::default()
        });
        // Append trailing None value for key_0 that must be ignored
        attrs.push(KeyValue {
            key: "key_0".to_string(),
            value: None,
            ..Default::default()
        });

        let json_str = convert_attributes(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.len(), 64);
        assert_eq!(obj.get("key_0"), Some(&serde_json::json!(1000)));
        assert_eq!(obj.get("key_33"), Some(&serde_json::json!(2033)));
        assert_eq!(obj.get("key_63"), Some(&serde_json::json!(63)));
    }
}
