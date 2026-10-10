with open('crates/parquet-sink/src/variant.rs', 'r') as f:
    content = f.read()

# patch metadata header parsing
content = content.replace(
    'let offset_size = (((header >> 4) & 0x03) + 1) as usize;',
    'let offset_size = (((header >> 6) & 0x03) + 1) as usize;'
)

# patch metadata header encoding
content = content.replace(
    '// version 1 (bits 0-3), offset_size_minus_1 (bits 4-5), is_sorted = 1 (bit 6)\n        #[allow(clippy::cast_possible_truncation)]\n        let offset_size_minus_1 = (offset_size - 1) as u8;\n        let header_byte = 1 | (offset_size_minus_1 << 4) | (1 << 6);',
    '// version 1 (bits 0-3), is_sorted = 1 (bit 4), reserved (bit 5), offset_size_minus_1 (bits 6-7)\n        #[allow(clippy::cast_possible_truncation)]\n        let offset_size_minus_1 = (offset_size - 1) as u8;\n        let header_byte = 1 | (1 << 4) | (offset_size_minus_1 << 6);'
)

with open('crates/parquet-sink/src/variant.rs', 'w') as f:
    f.write(content)

