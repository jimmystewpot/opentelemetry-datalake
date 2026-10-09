import re

with open("crates/parquet-sink/src/variant.rs", "r") as f:
    content = f.read()

# Replace encode_dictionary_metadata
encode_pattern = r"fn encode_dictionary_metadata\(&mut self, keys: &\[&str\]\) -> Result<\(\), ParquetSinkError> \{.*?Ok\(\(\)\)\n    \}"
new_encode = """fn encode_dictionary_metadata(&mut self, keys: &[&str]) -> Result<(), ParquetSinkError> {
        self.meta_buf.clear();

        let count = keys.len();
        let mut total_len: usize = 0;
        for k in keys {
            total_len = total_len.checked_add(k.len()).ok_or_else(|| {
                ParquetSinkError::VariantEncoding("dictionary key offset overflow".to_string())
            })?;
        }

        let max_val = std::cmp::max(total_len, count);
        let offset_size = if max_val <= u8::MAX as usize {
            1
        } else if max_val <= u16::MAX as usize {
            2
        } else if max_val <= 0xFF_FFFF as usize {
            3
        } else {
            4
        };

        // version 1 (bits 0-3), offset_size_minus_1 (bits 4-5), is_sorted = 1 (bit 6)
        #[allow(clippy::cast_possible_truncation)]
        let offset_size_minus_1 = (offset_size - 1) as u8;
        let header_byte = 1 | (offset_size_minus_1 << 4) | (1 << 6);
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
    }"""

content = re.sub(encode_pattern, new_encode, content, flags=re.DOTALL)

# Replace parse_dictionary_keys
parse_pattern = r"pub fn parse_dictionary_keys\(metadata: &\[u8\]\) -> Vec<String> \{.*?keys\n    \}"
new_parse = """pub fn parse_dictionary_keys(metadata: &[u8]) -> Vec<String> {
        if metadata.is_empty() {
            return Vec::new();
        }
        let header = metadata[0];
        // Header byte check: version 1 in lowest 4 bits
        if header & 0x0F != 1 {
            return Vec::new();
        }

        let offset_size = (((header >> 4) & 0x03) + 1) as usize;
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
    }"""

content = re.sub(parse_pattern, new_parse, content, flags=re.DOTALL)

with open("crates/parquet-sink/src/variant.rs", "w") as f:
    f.write(content)

