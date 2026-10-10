import re

with open('crates/parquet-sink/src/router.rs', 'r') as f:
    content = f.read()

content = content.replace('.field_with_name("datapoints")', '.field_with_name("attributes")')
content = content.replace('column \'datapoints\' is not a Utf8', 'column \'attributes\' is not a Utf8')
content = content.replace('test_route_metrics_without_datapoints', 'test_route_metrics_without_attributes')
# Restore the expected error string if needed.
content = content.replace('VariantEncoding("column \\\'attributes\\\' is not a Utf8 or LargeUtf8 string array: Int64")', 'VariantEncoding("column \\\'attributes\\\' is not a Utf8 or LargeUtf8 string array: Int64")')

with open('crates/parquet-sink/src/router.rs', 'w') as f:
    f.write(content)

