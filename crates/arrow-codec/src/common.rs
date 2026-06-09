/// Shared OTLP-to-Arrow conversion helpers used across all signal codecs.
///
/// These functions handle the transformation of OpenTelemetry protobuf
/// attribute structures into JSON-serialized strings suitable for Arrow
/// `Utf8` columns. Centralised here to avoid duplication across
/// logs, traces, and metrics decoders.
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use pipeline_core::error::PipelineError;
use std::collections::HashMap;
use std::fmt::Write;

/// Encodes a byte slice as a lowercase hexadecimal string.
///
/// Used primarily for trace IDs (16 bytes) and span IDs (8 bytes).
/// Pre-allocates the exact capacity required to avoid reallocation.
#[must_use]
pub fn to_hex_string(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        // SAFETY rationale: `write!` into a `String` is infallible.
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

/// Converts a single OTLP `AnyValue` into its string representation.
///
/// Handles all value variants defined in the OpenTelemetry protobuf spec:
/// - Scalar types: string, int, double, bool
/// - Nested types: key-value lists (recursively serialized as JSON)
/// - Array types: serialized as `[elem1,elem2,...]`
/// - Bytes: hex-encoded via [`to_hex_string`]
/// - Unknown/None: empty string
#[must_use]
pub fn any_value_to_string(val: &AnyValue) -> String {
    match &val.value {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(d)) => d.to_string(),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        Some(any_value::Value::KvlistValue(kvlist)) => convert_attributes(&kvlist.values),
        Some(any_value::Value::ArrayValue(arr)) => {
            let elements: Vec<String> = arr.values.iter().map(any_value_to_string).collect();
            format!("[{}]", elements.join(","))
        }
        Some(any_value::Value::BytesValue(bytes)) => to_hex_string(bytes),
        _ => String::new(),
    }
}

/// Serializes a slice of OTLP `KeyValue` attributes into a JSON object string.
///
/// Each attribute key becomes a JSON object key; the value is converted via
/// [`any_value_to_string`] which handles all OTLP value types including
/// nested key-value lists and arrays.
///
/// Returns `"{}"` if serialization fails (should not happen for valid `HashMap`
/// contents).
#[must_use]
pub fn convert_attributes(attrs: &[KeyValue]) -> String {
    let mut map = HashMap::with_capacity(attrs.len());
    for attr in attrs {
        if let Some(ref val) = attr.value {
            let val_str = any_value_to_string(val);
            map.insert(attr.key.as_str(), val_str);
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
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
            PipelineError::Internal(format!("column '{col_name}' is not a Utf8 StringArray"))
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
        ];
        let json = convert_attributes(&attrs);
        assert!(json.contains("\"42\""));
        assert!(json.contains("\"1.23\""));
        assert!(json.contains("\"true\""));
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
}
