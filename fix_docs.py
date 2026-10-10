import re

with open('crates/parquet-sink/src/router.rs', 'r') as f:
    content = f.read()

content = content.replace(
    '/// When Variant encoding is enabled, semi-structured columns (`attributes`, `resource_attributes`,\n/// and `resource_attributes`) are converted into Parquet / Spark Variant binary `StructArray` columns.',
    '/// When Variant encoding is enabled, semi-structured columns (`attributes` and `resource_attributes`)\n/// are converted into Parquet / Spark Variant binary `StructArray` columns.'
)

with open('crates/parquet-sink/src/router.rs', 'w') as f:
    f.write(content)

