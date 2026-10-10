import re

with open('crates/parquet-sink/src/router.rs', 'r') as f:
    content = f.read()

def replacer(match):
    return match.group(0).replace('Field::new("attributes", DataType::Int64', 'Field::new("value", DataType::Int64')

# Just fix the specific test
content = content.replace('Field::new("attributes", DataType::Int64, true)', 'Field::new("value", DataType::Int64, true)')

with open('crates/parquet-sink/src/router.rs', 'w') as f:
    f.write(content)

