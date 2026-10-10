//! In-memory Variant binary encoder and Arrow `RecordBatch` transformer.
//!
//! Encodes semi-structured JSON attribute columns into Arrow `StructArray`s
//! containing `metadata` (`DataType::Binary`) and `value` (`DataType::Binary`)
//! fields annotated with `ARROW:extension:name = "variant"`, conforming to the
//! Apache Parquet / Spark Variant specification.

use std::cell::RefCell;
use std::sync::Arc;

use arrow::array::{Array, BinaryBuilder, LargeStringArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use smallvec::SmallVec;

use crate::error::ParquetSinkError;

/// Type alias for owned `(metadata, value)` Variant binary vectors.
pub type OwnedVariantBytes = (Vec<u8>, Vec<u8>);

/// Type alias for borrowed `(metadata, value)` Variant binary slices.
pub type EncodedVariantBytes<'a> = (&'a [u8], &'a [u8]);

/// Static minimal metadata for an empty object: version 1 (0x01) followed by 1-byte 0 count, 1-byte 0 offset.
pub static STATIC_EMPTY_METADATA: [u8; 3] = [0x01, 0x00, 0x00];

/// Static minimal value for an empty object: Object basic type (0x02) with 0 count (0x00) and 0 offset (0x00).
pub static STATIC_EMPTY_VALUE: [u8; 3] = [0x02, 0x00, 0x00];

thread_local! {
    static LOCAL_ENCODER: RefCell<VariantEncoder> = RefCell::new(VariantEncoder::new());
}

/// Zero-allocation in-memory Variant binary encoder.
///
/// Encodes JSON strings into Apache Parquet / Spark Variant binary format using
/// thread-local or reusable instance scratch buffers.
pub struct VariantEncoder {
    meta_buf: SmallVec<[u8; 512]>,
    val_buf: SmallVec<[u8; 2048]>,
}

impl Default for VariantEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl VariantEncoder {
    /// Creates a new `VariantEncoder` with preallocated scratch buffers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            meta_buf: SmallVec::new(),
            val_buf: SmallVec::new(),
        }
    }

    /// Encodes a JSON string into owned `(metadata, value)` byte vectors.
    ///
    /// Fast-paths `""` and `"null"` to `None`.
    /// Fast-paths `"{}"` to a static minimal header.
    ///
    /// # Errors
    /// Returns `ParquetSinkError::VariantEncoding` if the JSON is malformed.
    pub fn encode_json_str(
        &mut self,
        json_str: &str,
    ) -> Result<Option<OwnedVariantBytes>, ParquetSinkError> {
        let trimmed = json_str.trim();
        if trimmed.is_empty() || trimmed == "null" {
            return Ok(None);
        }

        if trimmed == "{}" {
            return Ok(Some((
                STATIC_EMPTY_METADATA.to_vec(),
                STATIC_EMPTY_VALUE.to_vec(),
            )));
        }

        self.encode_json_internal(trimmed)?;
        Ok(Some((self.meta_buf.to_vec(), self.val_buf.to_vec())))
    }

    /// Encodes a JSON string into internal reusable scratch buffers without heap allocations.
    ///
    /// Fast-paths `""` and `"null"` to `None`.
    /// Fast-paths `"{}"` to static byte slices.
    ///
    /// # Errors
    /// Returns `ParquetSinkError::VariantEncoding` if the JSON is malformed.
    pub fn encode_to_scratch(
        &mut self,
        json_str: &str,
    ) -> Result<Option<EncodedVariantBytes<'_>>, ParquetSinkError> {
        let trimmed = json_str.trim();
        if trimmed.is_empty() || trimmed == "null" {
            return Ok(None);
        }

        if trimmed == "{}" {
            return Ok(Some((&STATIC_EMPTY_METADATA, &STATIC_EMPTY_VALUE)));
        }

        self.encode_json_internal(trimmed)?;
        Ok(Some((&self.meta_buf, &self.val_buf)))
    }

    /// Extracts dictionary keys from a Variant metadata binary payload in lexicographical order.
    #[must_use]
    pub fn extract_dictionary_keys(&self, metadata: &[u8]) -> Vec<String> {
        Self::parse_dictionary_keys(metadata)
    }

    /// Parses dictionary keys from a Variant metadata binary payload.
    #[must_use]
    pub fn parse_dictionary_keys(metadata: &[u8]) -> Vec<String> {
        if metadata.is_empty() {
            return Vec::new();
        }
        let header = metadata[0];
        // Header byte check: version 1 in lowest 4 bits
        if header & 0x0F != 1 {
            return Vec::new();
        }

        let offset_size = (((header >> 6) & 0x03) + 1) as usize;
        let mut pos = 1;

        if metadata.len() < pos + offset_size {
            return Vec::new();
        }
        let count = Self::read_int_le(metadata, pos, offset_size);
        pos += offset_size;

        if count == 0 {
            return Vec::new();
        }

        let mut offsets = Vec::with_capacity(count + 1);
        for _ in 0..=count {
            if metadata.len() < pos + offset_size {
                return Vec::new();
            }
            offsets.push(Self::read_int_le(metadata, pos, offset_size));
            pos += offset_size;
        }

        let mut keys = Vec::with_capacity(count);
        for i in 0..count {
            let start = pos + offsets[i];
            let end = pos + offsets[i + 1];
            if metadata.len() < end {
                return Vec::new();
            }
            if let Ok(s) = std::str::from_utf8(&metadata[start..end]) {
                keys.push(s.to_string());
            }
        }
        keys
    }

    fn read_int_le(bytes: &[u8], pos: usize, size: usize) -> usize {
        let mut buf = [0u8; 8];
        buf[..size].copy_from_slice(&bytes[pos..pos + size]);
        usize::try_from(u64::from_le_bytes(buf)).unwrap_or(0)
    }

    fn encode_json_internal(&mut self, trimmed: &str) -> Result<(), ParquetSinkError> {
        // If input is not valid JSON, check if it was intended as a JSON object/array or plain text
        let parsed: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                if trimmed.starts_with('{') || trimmed.starts_with('[') {
                    return Err(ParquetSinkError::VariantEncoding(format!(
                        "invalid JSON payload: {e}"
                    )));
                }
                // FAST PATH: Zero-allocation primitive string encoding
                self.meta_buf.clear();
                self.meta_buf.extend_from_slice(&STATIC_EMPTY_METADATA);
                self.val_buf.clear();

                let bytes = trimmed.as_bytes();
                if bytes.len() < 64 {
                    #[allow(clippy::cast_possible_truncation)]
                    let len_u8 = bytes.len() as u8;
                    self.val_buf.push((len_u8 << 2) | 0x01);
                } else {
                    self.val_buf.push(0x40);
                    let len_u32 = u32::try_from(bytes.len()).map_err(|_| {
                        ParquetSinkError::VariantEncoding(
                            "string length exceeds u32::MAX".to_string(),
                        )
                    })?;
                    self.val_buf.extend_from_slice(&len_u32.to_le_bytes());
                }
                self.val_buf.extend_from_slice(bytes);
                return Ok(());
            }
        };

        match parsed {
            serde_json::Value::Object(map) => {
                if map.is_empty() {
                    self.meta_buf.clear();
                    self.meta_buf.extend_from_slice(&STATIC_EMPTY_METADATA);
                    self.val_buf.clear();
                    self.val_buf.extend_from_slice(&STATIC_EMPTY_VALUE);
                    return Ok(());
                }

                // Recursively collect all keys across the JSON structure
                let mut all_keys: Vec<&str> = Vec::new();
                for (k, v) in &map {
                    all_keys.push(k.as_str());
                    collect_all_keys(v, &mut all_keys);
                }
                all_keys.sort_unstable();
                all_keys.dedup();

                self.encode_dictionary_metadata(&all_keys)?;
                self.val_buf.clear();
                encode_object_to_buf(&map, &all_keys, &mut self.val_buf)?;
                Ok(())
            }
            serde_json::Value::Array(arr) => {
                // Recursively collect dictionary keys across array elements
                let mut all_keys: Vec<&str> = Vec::new();
                for v in &arr {
                    collect_all_keys(v, &mut all_keys);
                }
                all_keys.sort_unstable();
                all_keys.dedup();

                self.encode_dictionary_metadata(&all_keys)?;
                self.val_buf.clear();
                let mut scratch: SmallVec<[u8; 256]> = SmallVec::new();
                encode_array_value(&arr, &all_keys, &mut scratch)?;
                self.val_buf.extend_from_slice(&scratch);
                Ok(())
            }
            primitive => {
                self.meta_buf.clear();
                self.meta_buf.extend_from_slice(&STATIC_EMPTY_METADATA);
                self.val_buf.clear();
                let mut scratch: SmallVec<[u8; 256]> = SmallVec::new();
                encode_json_value(&primitive, &[], &mut scratch)?;
                self.val_buf.extend_from_slice(&scratch);
                Ok(())
            }
        }
    }

    fn encode_dictionary_metadata(&mut self, keys: &[&str]) -> Result<(), ParquetSinkError> {
        self.meta_buf.clear();

        let count = keys.len();
        let mut total_len: usize = 0;
        for k in keys {
            total_len = total_len.checked_add(k.len()).ok_or_else(|| {
                ParquetSinkError::VariantEncoding("dictionary key offset overflow".to_string())
            })?;
        }

        let max_val = std::cmp::max(total_len, count);
        let offset_size = if u8::try_from(max_val).is_ok() {
            1
        } else if u16::try_from(max_val).is_ok() {
            2
        } else if max_val <= 0xFF_FFFF_usize {
            3
        } else {
            4
        };

        // version 1 (bits 0-3), is_sorted = 1 (bit 4), reserved (bit 5), offset_size_minus_1 (bits 6-7)
        #[allow(clippy::cast_possible_truncation)]
        let offset_size_minus_1 = (offset_size - 1) as u8;
        let header_byte = 1 | (1 << 4) | (offset_size_minus_1 << 6);
        self.meta_buf.push(header_byte);

        Self::write_int_le(&mut self.meta_buf, count, offset_size);

        let mut current_offset: usize = 0;
        Self::write_int_le(&mut self.meta_buf, current_offset, offset_size);
        for k in keys {
            current_offset += k.len();
            Self::write_int_le(&mut self.meta_buf, current_offset, offset_size);
        }

        for k in keys {
            self.meta_buf.extend_from_slice(k.as_bytes());
        }

        Ok(())
    }

    fn write_int_le(buf: &mut SmallVec<[u8; 512]>, val: usize, size: usize) {
        let bytes = val.to_le_bytes();
        buf.extend_from_slice(&bytes[..size]);
    }
}

fn collect_all_keys<'a>(val: &'a serde_json::Value, keys: &mut Vec<&'a str>) {
    match val {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                keys.push(k.as_str());
                collect_all_keys(v, keys);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_all_keys(v, keys);
            }
        }
        _ => {}
    }
}

fn encode_object_to_buf(
    map: &serde_json::Map<String, serde_json::Value>,
    dictionary: &[&str],
    buf: &mut SmallVec<[u8; 2048]>,
) -> Result<(), ParquetSinkError> {
    let mut entries: Vec<(&str, &serde_json::Value)> =
        map.iter().map(|(k, v)| (k.as_str(), v)).collect();
    entries.sort_unstable_by_key(|(k1, _)| *k1);
    entries.dedup_by(|(k1, _), (k2, _)| k1 == k2);

    let num_elements = entries.len();
    let mut encoded_values: SmallVec<[SmallVec<[u8; 256]>; 16]> =
        SmallVec::with_capacity(num_elements);
    let mut val_scratch: SmallVec<[u8; 256]> = SmallVec::new();

    for (_, v) in &entries {
        val_scratch.clear();
        encode_json_value(v, dictionary, &mut val_scratch)?;
        encoded_values.push(val_scratch.clone());
    }

    let mut total_val_len: usize = 0;
    let mut val_offsets: SmallVec<[usize; 17]> = SmallVec::with_capacity(num_elements + 1);
    val_offsets.push(0);

    for ev in &encoded_values {
        total_val_len = total_val_len.checked_add(ev.len()).ok_or_else(|| {
            ParquetSinkError::VariantEncoding("value offset overflow".to_string())
        })?;
        val_offsets.push(total_val_len);
    }

    let is_large = num_elements > 255;
    let field_id_size = if dictionary.len() <= 256 {
        1
    } else if dictionary.len() <= 65536 {
        2
    } else {
        4
    };
    let field_offset_size = if total_val_len <= 255 {
        1
    } else if total_val_len <= 65535 {
        2
    } else {
        4
    };

    let is_large_bit: u8 = u8::from(is_large);
    let field_id_size_minus_one: u8 = match field_id_size {
        1 => 0,
        2 => 1,
        _ => 3,
    };
    let field_offset_size_minus_one: u8 = match field_offset_size {
        1 => 0,
        2 => 1,
        _ => 3,
    };

    let object_header = (field_offset_size_minus_one & 0x03)
        | ((field_id_size_minus_one & 0x03) << 2)
        | ((is_large_bit & 0x01) << 4);
    let value_metadata = (object_header << 2) | 0x02; // basic_type = 2 (Object)

    buf.push(value_metadata);

    if is_large {
        let count_u32 = u32::try_from(num_elements).map_err(|_| {
            ParquetSinkError::VariantEncoding("element count exceeds u32::MAX".to_string())
        })?;
        buf.extend_from_slice(&count_u32.to_le_bytes());
    } else {
        #[allow(clippy::cast_possible_truncation)]
        buf.push(num_elements as u8);
    }

    // Field IDs referencing the dictionary
    for (k, _) in &entries {
        let id = dictionary
            .binary_search_by(|entry| entry.cmp(k))
            .map_err(|_| {
                ParquetSinkError::VariantEncoding(format!(
                    "dictionary key '{k}' missing during value encoding"
                ))
            })?;
        write_int_to_buf(buf, id, field_id_size)?;
    }

    // Field offsets
    for off in val_offsets {
        write_int_to_buf(buf, off, field_offset_size)?;
    }

    // Values payload
    for ev in encoded_values {
        buf.extend_from_slice(&ev);
    }

    Ok(())
}

fn write_int_to_buf(
    buf: &mut SmallVec<[u8; 2048]>,
    val: usize,
    size: usize,
) -> Result<(), ParquetSinkError> {
    match size {
        1 => {
            let v = u8::try_from(val).map_err(|_| {
                ParquetSinkError::VariantEncoding("offset/id exceeds 1 byte".to_string())
            })?;
            buf.push(v);
        }
        2 => {
            let v = u16::try_from(val).map_err(|_| {
                ParquetSinkError::VariantEncoding("offset/id exceeds 2 bytes".to_string())
            })?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        _ => {
            let v = u32::try_from(val).map_err(|_| {
                ParquetSinkError::VariantEncoding("offset/id exceeds 4 bytes".to_string())
            })?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }
    Ok(())
}

fn encode_json_value(
    val: &serde_json::Value,
    dictionary: &[&str],
    buf: &mut SmallVec<[u8; 256]>,
) -> Result<(), ParquetSinkError> {
    match val {
        serde_json::Value::Null => {
            buf.push(0x00); // primitive null
        }
        serde_json::Value::Bool(true) => {
            buf.push(0x04); // boolean_true: (1 << 2) | 0
        }
        serde_json::Value::Bool(false) => {
            buf.push(0x08); // boolean_false: (2 << 2) | 0
        }
        serde_json::Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                if let Ok(i8_val) = i8::try_from(i) {
                    buf.push(0x0C); // int8: (3 << 2) | 0
                    #[allow(clippy::cast_sign_loss)]
                    buf.push(i8_val as u8);
                } else if let Ok(i16_val) = i16::try_from(i) {
                    buf.push(0x10); // int16: (4 << 2) | 0
                    buf.extend_from_slice(&i16_val.to_le_bytes());
                } else if let Ok(i32_val) = i32::try_from(i) {
                    buf.push(0x14); // int32: (5 << 2) | 0
                    buf.extend_from_slice(&i32_val.to_le_bytes());
                } else {
                    buf.push(0x18); // int64: (6 << 2) | 0
                    buf.extend_from_slice(&i.to_le_bytes());
                }
            } else if let Some(u) = num.as_u64() {
                if let Ok(i64_val) = i64::try_from(u) {
                    buf.push(0x18); // int64
                    buf.extend_from_slice(&i64_val.to_le_bytes());
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    let f = u as f64;
                    buf.push(0x1C); // double: (7 << 2) | 0
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            } else if let Some(f) = num.as_f64() {
                buf.push(0x1C); // double: (7 << 2) | 0
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        serde_json::Value::String(s) => {
            let bytes = s.as_bytes();
            if bytes.len() < 64 {
                // Short string: basic_type = 1, value_header = length
                #[allow(clippy::cast_possible_truncation)]
                let len_u8 = bytes.len() as u8;
                buf.push((len_u8 << 2) | 0x01);
                buf.extend_from_slice(bytes);
                return Ok(());
            }
            // Long string: primitive_header = 16 -> (16 << 2) | 0 = 0x40
            buf.push(0x40);
            let len_u32 = u32::try_from(bytes.len()).map_err(|_| {
                ParquetSinkError::VariantEncoding("string length exceeds u32::MAX".to_string())
            })?;
            buf.extend_from_slice(&len_u32.to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        serde_json::Value::Array(arr) => {
            encode_array_value(arr, dictionary, buf)?;
        }
        serde_json::Value::Object(map) => {
            let mut obj_buf = SmallVec::<[u8; 2048]>::new();
            encode_object_to_buf(map, dictionary, &mut obj_buf)?;
            buf.extend_from_slice(&obj_buf);
        }
    }
    Ok(())
}

fn encode_array_value(
    arr: &[serde_json::Value],
    dictionary: &[&str],
    buf: &mut SmallVec<[u8; 256]>,
) -> Result<(), ParquetSinkError> {
    let num_elements = arr.len();
    let is_large = num_elements > 255;
    let mut elements: SmallVec<[SmallVec<[u8; 256]>; 8]> = SmallVec::with_capacity(num_elements);
    let mut elem_scratch: SmallVec<[u8; 256]> = SmallVec::new();

    for elem in arr {
        elem_scratch.clear();
        encode_json_value(elem, dictionary, &mut elem_scratch)?;
        elements.push(elem_scratch.clone());
    }

    let mut total_len: usize = 0;
    let mut offsets: SmallVec<[usize; 9]> = SmallVec::with_capacity(num_elements + 1);
    offsets.push(0);

    for el in &elements {
        total_len = total_len.checked_add(el.len()).ok_or_else(|| {
            ParquetSinkError::VariantEncoding("array offset overflow".to_string())
        })?;
        offsets.push(total_len);
    }

    let field_offset_size = if total_len <= 255 {
        1
    } else if total_len <= 65535 {
        2
    } else {
        4
    };

    let is_large_bit: u8 = u8::from(is_large);
    let field_offset_size_minus_one: u8 = match field_offset_size {
        1 => 0,
        2 => 1,
        _ => 3,
    };

    let array_header = (field_offset_size_minus_one & 0x03) | ((is_large_bit & 0x01) << 2);
    let value_metadata = (array_header << 2) | 0x03; // basic_type = 3 (Array)

    buf.push(value_metadata);
    if is_large {
        let count_u32 = u32::try_from(num_elements).map_err(|_| {
            ParquetSinkError::VariantEncoding("array element count exceeds u32::MAX".to_string())
        })?;
        buf.extend_from_slice(&count_u32.to_le_bytes());
    } else {
        #[allow(clippy::cast_possible_truncation)]
        buf.push(num_elements as u8);
    }

    for off in offsets {
        match field_offset_size {
            1 => {
                let v = u8::try_from(off).map_err(|_| {
                    ParquetSinkError::VariantEncoding("offset exceeds 1 byte".to_string())
                })?;
                buf.push(v);
            }
            2 => {
                let v = u16::try_from(off).map_err(|_| {
                    ParquetSinkError::VariantEncoding("offset exceeds 2 bytes".to_string())
                })?;
                buf.extend_from_slice(&v.to_le_bytes());
            }
            _ => {
                let v = u32::try_from(off).map_err(|_| {
                    ParquetSinkError::VariantEncoding("offset exceeds 4 bytes".to_string())
                })?;
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    for el in elements {
        buf.extend_from_slice(&el);
    }

    Ok(())
}

/// Transforms semi-structured JSON string columns within an Arrow `RecordBatch` into
/// Variant `StructArray`s.
#[derive(Debug, Clone, Default)]
pub struct VariantTransformer;

impl VariantTransformer {
    /// Creates a new `VariantTransformer`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Transforms specified string column names within the batch into Variant `StructArray` columns.
    ///
    /// The resulting `StructArray` contains `"metadata"` (`DataType::Binary`, not nullable)
    /// and `"value"` (`DataType::Binary`, not nullable), annotated with `ARROW:extension:name = "variant"`.
    ///
    /// # Errors
    /// Returns `ParquetSinkError` if a requested column cannot be found, is not string-typed,
    /// or encounters an Arrow array creation failure.
    pub fn transform_to_variant(
        &self,
        batch: &RecordBatch,
        column_names: &[&str],
    ) -> Result<RecordBatch, ParquetSinkError> {
        if column_names.is_empty() {
            return Ok(batch.clone());
        }

        let num_rows = batch.num_rows();
        let schema = batch.schema();

        // Validate all requested columns exist in the batch schema
        for req_col in column_names {
            if schema.field_with_name(req_col).is_err() {
                return Err(ParquetSinkError::VariantEncoding(format!(
                    "column '{req_col}' not found in record batch schema"
                )));
            }
        }

        let mut new_fields = Vec::with_capacity(schema.fields().len());
        let mut new_columns = Vec::with_capacity(batch.num_columns());

        let mut extension_meta = std::collections::HashMap::with_capacity(2);
        extension_meta.insert(
            "ARROW:extension:name".to_string(),
            "arrow.parquet.variant".to_string(),
        );
        extension_meta.insert("ARROW:extension:metadata".to_string(), String::new());

        let struct_fields = Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]);
        let struct_type = DataType::Struct(struct_fields.clone());

        for (idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(idx);
            if !column_names.contains(&field.name().as_str()) {
                new_fields.push(field.clone());
                new_columns.push(col.clone());
                continue;
            }

            let struct_array = match col.data_type() {
                DataType::Utf8 => {
                    let string_array =
                        col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                            ParquetSinkError::VariantEncoding(format!(
                                "failed downcasting column '{}' to StringArray",
                                field.name()
                            ))
                        })?;
                    Self::convert_string_column(num_rows, &struct_fields, |row| {
                        if string_array.is_null(row) {
                            None
                        } else {
                            Some(string_array.value(row))
                        }
                    })?
                }
                DataType::LargeUtf8 => {
                    let string_array =
                        col.as_any()
                            .downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| {
                                ParquetSinkError::VariantEncoding(format!(
                                    "failed downcasting column '{}' to LargeStringArray",
                                    field.name()
                                ))
                            })?;
                    Self::convert_string_column(num_rows, &struct_fields, |row| {
                        if string_array.is_null(row) {
                            None
                        } else {
                            Some(string_array.value(row))
                        }
                    })?
                }
                other => {
                    return Err(ParquetSinkError::VariantEncoding(format!(
                        "column '{}' is not a Utf8 or LargeUtf8 string array: {:?}",
                        field.name(),
                        other
                    )));
                }
            };

            let transformed_field =
                Field::new(field.name(), struct_type.clone(), field.is_nullable())
                    .with_metadata(extension_meta.clone());

            new_fields.push(Arc::new(transformed_field));
            new_columns.push(Arc::new(struct_array));
        }

        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, new_columns).map_err(ParquetSinkError::Arrow)
    }

    fn convert_string_column<'a, F>(
        num_rows: usize,
        struct_fields: &Fields,
        get_row: F,
    ) -> Result<StructArray, ParquetSinkError>
    where
        F: Fn(usize) -> Option<&'a str>,
    {
        LOCAL_ENCODER.with(|cell| -> Result<StructArray, ParquetSinkError> {
            let mut encoder = cell.borrow_mut();
            let mut meta_builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
            let mut val_builder = BinaryBuilder::with_capacity(num_rows, num_rows * 128);
            let mut validity = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                let (meta_slice, val_slice, is_valid) = if let Some(str_val) = get_row(row_idx) {
                    if let Some((m, v)) = encoder.encode_to_scratch(str_val)? {
                        (m, v, true)
                    } else {
                        (&[][..], &[][..], false)
                    }
                } else {
                    (&[][..], &[][..], false)
                };

                meta_builder.append_value(meta_slice);
                val_builder.append_value(val_slice);
                validity.push(is_valid);
            }

            let meta_array = meta_builder.finish();
            let val_array = val_builder.finish();
            let null_buffer = if validity.iter().all(|&v| v) {
                None
            } else {
                Some(NullBuffer::from(BooleanBuffer::from(validity)))
            };

            StructArray::try_new(
                struct_fields.clone(),
                vec![Arc::new(meta_array), Arc::new(val_array)],
                null_buffer,
            )
            .map_err(ParquetSinkError::Arrow)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_variant_encoder_handles_null_empty_and_lexicographical_keys() {
        let mut encoder = VariantEncoder::new();

        // 1. Null handling
        assert!(encoder.encode_json_str("").unwrap().is_none());

        // 2. Empty object handling
        let (empty_meta, _empty_val) = encoder.encode_json_str("{}").unwrap().unwrap();
        assert_eq!(empty_meta[0], 0x01); // version 1
        assert_eq!(empty_meta.len(), 3); // version + 1-byte count (0) + 1-byte offset

        // 3. Lexicographical sorting
        let (meta, _val) = encoder
            .encode_json_str(r#"{"z": 1, "a": 2, "m": 3}"#)
            .unwrap()
            .unwrap();
        let keys = encoder.extract_dictionary_keys(&meta);
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn test_variant_transformer_replaces_string_column_with_struct_array() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8, true),
        ]));
        let id_arr = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let attr_arr = Arc::new(StringArray::from(vec![
            Some(r#"{"service.name":"api"}"#),
            None,
            Some("{}"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, attr_arr]).unwrap();

        let xformer = VariantTransformer::new();
        let transformed = xformer
            .transform_to_variant(&batch, &["attributes"])
            .unwrap();

        let schema = transformed.schema();
        let field = schema.field_with_name("attributes").unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));
        assert_eq!(
            field.metadata().get("ARROW:extension:name"),
            Some(&"arrow.parquet.variant".to_string())
        );

        let struct_col = transformed
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_col.len(), 3);
        assert!(struct_col.is_valid(0));
        assert!(struct_col.is_null(1));
        assert!(struct_col.is_valid(2));
    }

    #[test]
    fn test_variant_encoder_invalid_json_returns_error() {
        let mut encoder = VariantEncoder::new();
        let res = encoder.encode_json_str("{unquoted_key: 123}");
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            ParquetSinkError::VariantEncoding(_)
        ));
    }

    #[test]
    fn test_variant_encoder_nested_objects_and_arrays() {
        let mut encoder = VariantEncoder::new();
        let (meta, val) = encoder
            .encode_json_str(r#"{"outer": {"inner": "val"}, "list": [10, 20]}"#)
            .unwrap()
            .unwrap();
        let keys = encoder.extract_dictionary_keys(&meta);
        assert_eq!(keys, vec!["inner", "list", "outer"]);
        assert!(!val.is_empty());
    }

    #[test]
    fn test_variant_encoder_unicode_keys_sorting() {
        let mut encoder = VariantEncoder::new();
        let (meta, _) = encoder
            .encode_json_str(r#"{"β": 1, "α": 2, "a": 3}"#)
            .unwrap()
            .unwrap();
        let keys = encoder.extract_dictionary_keys(&meta);
        assert_eq!(keys, vec!["a", "α", "β"]);
    }

    #[test]
    fn test_variant_transformer_column_not_found() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let id_arr = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_new(schema, vec![id_arr]).unwrap();

        let transformer = VariantTransformer::new();
        let res = transformer.transform_to_variant(&batch, &["non_existent"]);
        assert!(res.is_err());
    }

    #[test]
    fn test_variant_transformer_non_string_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let id_arr = Arc::new(Int64Array::from(vec![1]));
        let batch = RecordBatch::try_new(schema, vec![id_arr]).unwrap();

        let transformer = VariantTransformer::new();
        let res = transformer.transform_to_variant(&batch, &["id"]);
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            ParquetSinkError::VariantEncoding(_)
        ));
    }

    #[test]
    fn test_variant_transformer_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "attributes",
            DataType::Utf8,
            true,
        )]));
        let attr_arr = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let batch = RecordBatch::try_new(schema, vec![attr_arr]).unwrap();

        let xformer = VariantTransformer::new();
        let transformed = xformer
            .transform_to_variant(&batch, &["attributes"])
            .unwrap();
        assert_eq!(transformed.num_rows(), 0);
        let schema = transformed.schema();
        let field = schema.field_with_name("attributes").unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));
    }

    #[test]
    fn test_long_string_emits_header_0x40() {
        let mut encoder = VariantEncoder::new();
        let long_str = "x".repeat(70);
        let json = format!("\"{long_str}\"");
        let (_meta, val) = encoder.encode_json_str(&json).unwrap().unwrap();
        // Long string primitive type ID 16 -> (16 << 2) | 0 = 0x40
        assert_eq!(val[0], 0x40);
        let len_bytes: [u8; 4] = val[1..5].try_into().unwrap();
        assert_eq!(u32::from_le_bytes(len_bytes), 70);
        assert_eq!(&val[5..], long_str.as_bytes());
    }

    #[test]
    fn test_object_payload_greater_than_255_bytes_sets_offset_size_2() {
        let mut encoder = VariantEncoder::new();
        let s1 = "a".repeat(60);
        let s2 = "b".repeat(60);
        let s3 = "c".repeat(60);
        let s4 = "d".repeat(60);
        let s5 = "e".repeat(60);
        let json =
            format!(r#"{{"k1": "{s1}", "k2": "{s2}", "k3": "{s3}", "k4": "{s4}", "k5": "{s5}"}}"#);
        let (_meta, val) = encoder.encode_json_str(&json).unwrap().unwrap();
        let value_metadata = val[0];
        assert_eq!(value_metadata & 0x03, 0x02); // Object basic type
        let object_header = value_metadata >> 2;
        // field_offset_size_minus_one is in bits 1-0 of object_header
        // total payload is ~300 bytes (>255, <=65535) -> field_offset_size = 2 -> field_offset_size_minus_one = 1
        assert_eq!(
            object_header & 0x03,
            1,
            "bit 0 of object_header should be set for 2-byte offsets"
        );
    }

    #[test]
    fn test_static_empty_value_is_three_bytes() {
        assert_eq!(STATIC_EMPTY_VALUE, [0x02, 0x00, 0x00]);
        let mut encoder = VariantEncoder::new();
        let (_meta, val) = encoder.encode_json_str("{}").unwrap().unwrap();
        assert_eq!(val, vec![0x02, 0x00, 0x00]);
    }

    #[test]
    fn test_plain_text_log_body_encodes_as_variant_string() {
        let mut encoder = VariantEncoder::new();
        let res = encoder.encode_json_str("test raw log message body");
        assert!(res.is_ok());
        let (meta, val) = res.unwrap().unwrap();
        assert_eq!(meta[0] & 0x0F, 0x01); // Version 1
        // Short string has basic type 1: (len << 2) | 0x01
        assert_eq!(val[0] & 0x03, 0x01);
        assert_eq!(&val[1..], b"test raw log message body");
    }

    #[test]
    fn test_top_level_array_with_nested_objects_resolves_dictionary() {
        let mut encoder = VariantEncoder::new();
        let json = r#"[{"key1": "val1"}, {"key2": "val2"}]"#;
        let res = encoder.encode_json_str(json);
        assert!(
            res.is_ok(),
            "Top-level array with objects should encode: {:?}",
            res.err()
        );
        let (meta, val) = res.unwrap().unwrap();
        assert_eq!(meta[0] & 0x0F, 0x01);
        let keys = encoder.extract_dictionary_keys(&meta);
        assert_eq!(keys, vec!["key1", "key2"]);
        assert_eq!(val[0] & 0x03, 0x03); // Array type
    }
}
