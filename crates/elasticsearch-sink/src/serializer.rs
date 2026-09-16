//! Single-pass Arrow `RecordBatch` to NDJSON bulk serialization engine.

use crate::error::ElasticsearchError;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

/// Static bulk action header for data stream `create` operations.
const BULK_ACTION: &[u8] = b"{\"create\":{}}\n";

/// Hexadecimal digit lookup table for escaping control characters.
const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Serializes an Arrow `RecordBatch` into NDJSON bulk format for the Elasticsearch Bulk API.
///
/// Each row produces two lines: `{"create":{}}\n` followed by the JSON document `{...}\n`.
/// The `timestamp` column (nanosecond i64) is mapped to `@timestamp` in ISO 8601 format.
/// Attribute columns are either unpacked as native JSON objects or preserved as strings.
pub fn serialize_batch(
    batch: &RecordBatch,
    unpack_attributes: bool,
    max_payload_bytes: usize,
) -> Result<Bytes, ElasticsearchError> {
    if batch.num_rows() == 0 {
        return Ok(Bytes::new());
    }

    let mem_size = batch.get_array_memory_size();
    let mut buf = Vec::with_capacity(mem_size.saturating_add(mem_size / 5));
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    for row in 0..num_rows {
        buf.extend_from_slice(BULK_ACTION);
        buf.push(b'{');

        let mut first_field = true;
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let name = field.name();

            // Map "timestamp" to "@timestamp" with ISO 8601 formatting
            let output_name = if name == "timestamp" {
                "@timestamp"
            } else {
                name.as_str()
            };

            if col.is_null(row) {
                continue;
            }

            if !first_field {
                buf.push(b',');
            }
            first_field = false;

            // Write field name
            buf.push(b'"');
            buf.extend_from_slice(output_name.as_bytes());
            buf.extend_from_slice(b"\":");

            // Check if this is an attribute field that should be unpacked
            let is_attr_field = name == "attributes" || name == "resource_attributes";

            write_value(
                &mut buf,
                col.as_ref(),
                row,
                field.data_type(),
                unpack_attributes && is_attr_field,
            )?;
        }

        buf.extend_from_slice(b"}\n");
    }

    let actual = buf.len();
    if actual > max_payload_bytes {
        return Err(ElasticsearchError::PayloadTooLarge {
            actual,
            limit: max_payload_bytes,
        });
    }

    Ok(Bytes::from(buf))
}

/// Formats and writes an ISO 8601 UTC timestamp to the buffer.
fn write_timestamp(buf: &mut Vec<u8>, secs: i64, subsec_nanos: u32) {
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, subsec_nanos) {
        use std::io::Write;
        buf.push(b'"');
        let _ = write!(buf, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.9fZ"));
        buf.push(b'"');
    } else {
        buf.extend_from_slice(b"null");
    }
}

/// Writes a timestamp Arrow column value to the buffer formatted as ISO 8601 UTC.
fn write_timestamp_col(buf: &mut Vec<u8>, col: &dyn Array, row: usize, unit: TimeUnit) {
    match unit {
        TimeUnit::Nanosecond => {
            let arr = col.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
            let nanos = arr.value(row);
            let secs = nanos.div_euclid(1_000_000_000);
            let subsec_nanos = u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0);
            write_timestamp(buf, secs, subsec_nanos);
        }
        TimeUnit::Microsecond => {
            let arr = col.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
            let micros = arr.value(row);
            let secs = micros.div_euclid(1_000_000);
            let subsec_nanos = u32::try_from(micros.rem_euclid(1_000_000) * 1_000).unwrap_or(0);
            write_timestamp(buf, secs, subsec_nanos);
        }
        TimeUnit::Millisecond => {
            let arr = col.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
            let millis = arr.value(row);
            let secs = millis.div_euclid(1_000);
            let subsec_nanos = u32::try_from(millis.rem_euclid(1_000) * 1_000_000).unwrap_or(0);
            write_timestamp(buf, secs, subsec_nanos);
        }
        TimeUnit::Second => {
            let arr = col.as_primitive::<arrow::datatypes::TimestampSecondType>();
            let secs = arr.value(row);
            write_timestamp(buf, secs, 0);
        }
    }
}

/// Writes a numeric Arrow column value using fast `itoa` or `ryu` formatting.
fn write_numeric_col(buf: &mut Vec<u8>, col: &dyn Array, row: usize, data_type: &DataType) {
    match data_type {
        DataType::Int8 => {
            let arr = col.as_primitive::<arrow::datatypes::Int8Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Int16 => {
            let arr = col.as_primitive::<arrow::datatypes::Int16Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Int32 => {
            let arr = col.as_primitive::<arrow::datatypes::Int32Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Int64 => {
            let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::UInt8 => {
            let arr = col.as_primitive::<arrow::datatypes::UInt8Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::UInt16 => {
            let arr = col.as_primitive::<arrow::datatypes::UInt16Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::UInt32 => {
            let arr = col.as_primitive::<arrow::datatypes::UInt32Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::UInt64 => {
            let arr = col.as_primitive::<arrow::datatypes::UInt64Type>();
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(arr.value(row)).as_bytes());
        }
        DataType::Float32 => {
            let arr = col.as_primitive::<arrow::datatypes::Float32Type>();
            let val = arr.value(row);
            if val.is_finite() {
                let mut ryu_buf = ryu::Buffer::new();
                buf.extend_from_slice(ryu_buf.format(val).as_bytes());
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        DataType::Float64 => {
            let arr = col.as_primitive::<arrow::datatypes::Float64Type>();
            let val = arr.value(row);
            if val.is_finite() {
                let mut ryu_buf = ryu::Buffer::new();
                buf.extend_from_slice(ryu_buf.format(val).as_bytes());
            } else {
                buf.extend_from_slice(b"null");
            }
        }
        _ => {}
    }
}

/// Writes a UTF-8 string Arrow column value, unpacking JSON objects when enabled.
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
        // Embed raw JSON directly without escaping
        buf.extend_from_slice(val.as_bytes());
    } else {
        write_escaped_string(buf, val);
    }
}

/// Writes a single Arrow value to the output buffer.
fn write_value(
    buf: &mut Vec<u8>,
    col: &dyn Array,
    row: usize,
    data_type: &DataType,
    unpack_json: bool,
) -> Result<(), ElasticsearchError> {
    match data_type {
        DataType::Timestamp(unit, _) => write_timestamp_col(buf, col, row, *unit),
        DataType::Utf8 | DataType::LargeUtf8 => {
            write_string_col(buf, col, row, data_type, unpack_json);
        }
        DataType::Boolean => {
            let arr = col.as_boolean();
            buf.extend_from_slice(if arr.value(row) { b"true" } else { b"false" });
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => {
            write_numeric_col(buf, col, row, data_type);
        }
        DataType::Null => {
            buf.extend_from_slice(b"null");
        }
        _ => {
            // Fallback: format as JSON string
            let display = arrow::util::display::ArrayFormatter::try_new(
                col,
                &arrow::util::display::FormatOptions::default(),
            )
            .map_err(|e| ElasticsearchError::Serialization(e.to_string()))?;
            write_escaped_string(buf, &display.value(row).to_string());
        }
    }
    Ok(())
}

/// Writes a JSON-escaped string to the buffer.
fn write_escaped_string(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for byte in s.bytes() {
        match byte {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX_CHARS[usize::from(b >> 4)]);
                buf.push(HEX_CHARS[usize::from(b & 0x0f)]);
            }
            _ => buf.push(byte),
        }
    }
    buf.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_log_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(StringArray::from(vec!["frontend"])),
                Arc::new(Int32Array::from(vec![9])),
                Arc::new(StringArray::from(vec!["Request processed"])),
                Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_serialize_produces_valid_ndjson() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "Expected 2 lines (action + document)");
        assert_eq!(lines[0], r#"{"create":{}}"#);
        assert!(lines[1].contains("@timestamp"));
        assert!(lines[1].contains("frontend"));
    }

    #[test]
    fn test_serialize_unpacks_attributes() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // Attributes should appear as native JSON object, not escaped string
        assert!(text.contains(r#""attributes":{"http.method":"GET"}"#));
    }

    #[test]
    fn test_serialize_preserves_attributes_as_string() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, false, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // Attributes should be an escaped JSON string
        assert!(text.contains(r#""attributes":"{\"http.method\":\"GET\"}"#));
    }

    #[test]
    fn test_serialize_empty_batch_returns_empty() {
        let batch = make_log_batch().slice(0, 0);
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_serialize_payload_too_large() {
        let batch = make_log_batch();
        let result = serialize_batch(&batch, true, 10); // 10 bytes limit
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ElasticsearchError::PayloadTooLarge { .. }
        ));
    }

    #[test]
    fn test_serialize_timestamp_formatting() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        // 1726500000 seconds = 2024-09-16T17:20:00Z
        assert!(
            text.contains("2024-09-16T"),
            "Timestamp must be ISO 8601: {text}"
        );
    }

    #[test]
    fn test_serialize_multi_row_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                    1_726_500_001_000_000_000i64,
                    1_726_500_002_000_000_000i64,
                ])),
                Arc::new(StringArray::from(vec!["srv1", "srv2", "srv3"])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 6, "Expected 6 lines for 3 rows");
        assert_eq!(lines[0], r#"{"create":{}}"#);
        assert!(lines[1].contains("srv1"));
        assert_eq!(lines[2], r#"{"create":{}}"#);
        assert!(lines[3].contains("srv2"));
        assert_eq!(lines[4], r#"{"create":{}}"#);
        assert!(lines[5].contains("srv3"));
    }

    #[test]
    fn test_serialize_unpacks_resource_attributes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("resource_attributes", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(StringArray::from(vec![r#"{"host.name":"node-1"}"#])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""resource_attributes":{"host.name":"node-1"}"#));

        let bytes_no_unpack = serialize_batch(&batch, false, 10_485_760).unwrap();
        let text_no_unpack = std::str::from_utf8(&bytes_no_unpack).unwrap();
        assert!(text_no_unpack.contains(r#""resource_attributes":"{\"host.name\":\"node-1\"}""#));
    }

    #[test]
    fn test_serialize_skips_null_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("optional_str", DataType::Utf8, true),
            Field::new("optional_int", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
                Arc::new(Int32Array::from(vec![Some(42)])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("optional_str"));
        assert!(text.contains(r#""optional_int":42"#));
    }

    #[test]
    fn test_serialize_all_numeric_types_and_booleans() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("b", DataType::Boolean, false),
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("u8", DataType::UInt8, false),
            Field::new("u16", DataType::UInt16, false),
            Field::new("u32", DataType::UInt32, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int8Array::from(vec![-8])),
                Arc::new(Int16Array::from(vec![-16])),
                Arc::new(Int32Array::from(vec![-32])),
                Arc::new(Int64Array::from(vec![-64])),
                Arc::new(UInt8Array::from(vec![8])),
                Arc::new(UInt16Array::from(vec![16])),
                Arc::new(UInt32Array::from(vec![32])),
                Arc::new(UInt64Array::from(vec![64])),
                Arc::new(Float32Array::from(vec![1.5])),
                Arc::new(Float64Array::from(vec![2.5])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""b":true"#));
        assert!(text.contains(r#""i8":-8"#));
        assert!(text.contains(r#""i16":-16"#));
        assert!(text.contains(r#""i32":-32"#));
        assert!(text.contains(r#""i64":-64"#));
        assert!(text.contains(r#""u8":8"#));
        assert!(text.contains(r#""u16":16"#));
        assert!(text.contains(r#""u32":32"#));
        assert!(text.contains(r#""u64":64"#));
        assert!(text.contains(r#""f32":1.5"#));
        assert!(text.contains(r#""f64":2.5"#));
    }

    #[test]
    fn test_serialize_string_escaping_control_chars() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("escaped_text", DataType::Utf8, false),
        ]));
        let tricky_string = "hello \"world\" \\\n\r\t\x00\x1f \u{1F680} 日本語";
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(StringArray::from(vec![tricky_string])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#"\"world\""#));
        assert!(text.contains(r"\\"));
        assert!(text.contains(r"\n"));
        assert!(text.contains(r"\r"));
        assert!(text.contains(r"\t"));
        assert!(text.contains(r"\u0000"));
        assert!(text.contains(r"\u001f"));
        assert!(text.contains("🚀"));
        assert!(text.contains("日本語"));
    }

    #[test]
    fn test_serialize_timestamp_time_units() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "ts_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("ts_sec", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_726_500_000_000_000i64,
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![1_726_500_000_000i64])),
                Arc::new(TimestampSecondArray::from(vec![1_726_500_000i64])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""ts_micro":"2024-09-16T"#));
        assert!(text.contains(r#""ts_milli":"2024-09-16T"#));
        assert!(text.contains(r#""ts_sec":"2024-09-16T"#));
    }

    #[test]
    fn test_serialize_large_utf8_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("attributes", DataType::LargeUtf8, false),
            Field::new("message", DataType::LargeUtf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(LargeStringArray::from(vec![r#"{"large.key":"large.val"}"#])),
                Arc::new(LargeStringArray::from(vec!["regular message"])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""attributes":{"large.key":"large.val"}"#));
        assert!(text.contains(r#""message":"regular message""#));
    }

    #[test]
    fn test_serialize_exact_payload_boundary() {
        let batch = make_log_batch();
        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let exact_len = bytes.len();

        // Exact length should succeed
        let exact_result = serialize_batch(&batch, true, exact_len);
        assert!(exact_result.is_ok());

        // One byte less should fail with PayloadTooLarge
        let fail_result = serialize_batch(&batch, true, exact_len - 1);
        assert!(fail_result.is_err());
        assert!(matches!(
            fail_result.unwrap_err(),
            ElasticsearchError::PayloadTooLarge {
                actual,
                limit
            } if actual == exact_len && limit == exact_len - 1
        ));
    }

    #[test]
    fn test_serialize_non_finite_floats_to_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("f64_nan", DataType::Float64, false),
            Field::new("f64_inf", DataType::Float64, false),
            Field::new("f64_neg_inf", DataType::Float64, false),
            Field::new("f32_nan", DataType::Float32, false),
            Field::new("f32_inf", DataType::Float32, false),
            Field::new("f32_neg_inf", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_726_500_000_000_000_000i64,
                ])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
                Arc::new(Float32Array::from(vec![f32::NAN])),
                Arc::new(Float32Array::from(vec![f32::INFINITY])),
                Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])),
            ],
        )
        .unwrap();

        let bytes = serialize_batch(&batch, true, 10_485_760).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains(r#""f64_nan":null"#));
        assert!(text.contains(r#""f64_inf":null"#));
        assert!(text.contains(r#""f64_neg_inf":null"#));
        assert!(text.contains(r#""f32_nan":null"#));
        assert!(text.contains(r#""f32_inf":null"#));
        assert!(text.contains(r#""f32_neg_inf":null"#));
    }
}
