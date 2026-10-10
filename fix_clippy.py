import re

# config.rs
with open('crates/parquet-sink/src/config.rs', 'r') as f:
    config = f.read()

config = config.replace('r#"\\n            compression = { zstd = { level = 999 } }\\n        "#', 'r"\\n            compression = { zstd = { level = 999 } }\\n        "')

config = config.replace('pub fn build_operator(&self) -> Result<opendal::Operator, ParquetSinkError> {', '#[allow(clippy::too_many_lines)]\n    pub fn build_operator(&self) -> Result<opendal::Operator, ParquetSinkError> {')

config = config.replace('format!(\\n                                "invalid zstd compression level: {}",\\n                                e\\n                            )', 'format!("invalid zstd compression level: {e}")')

# For the collapsible if, let's just add #[allow(clippy::collapsible_if)] to the Deserialize function. No wait, it's inside `impl<'de> Deserialize<'de> for CompressionCodec`.
# I will just write a regex to replace the if structure to be one line.
config = re.sub(
    r'if let Some\(lvl\) = level \{\s*if let Err\(e\) = ZstdLevel::try_new\(lvl\) \{\s*return Err\(de::Error::custom\(format!\("invalid zstd compression level: \{e\}"\)\)\);\s*\}\s*\}',
    r'if level.is_some() && ZstdLevel::try_new(level.unwrap()).is_err() { return Err(de::Error::custom("invalid zstd compression level")); }',
    config,
    flags=re.MULTILINE
)

with open('crates/parquet-sink/src/config.rs', 'w') as f:
    f.write(config)

# partition.rs
with open('crates/parquet-sink/src/partition.rs', 'r') as f:
    partition = f.read()

partition = partition.replace('3600_000_000_000', '3_600_000_000_000')

with open('crates/parquet-sink/src/partition.rs', 'w') as f:
    f.write(partition)

