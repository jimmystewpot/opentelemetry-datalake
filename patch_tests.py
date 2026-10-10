import re

with open('crates/parquet-sink/src/router.rs', 'r') as f:
    content = f.read()

content = content.replace('test_route_metrics_non_string_datapoints_skipped', 'test_route_metrics_non_string_attributes_skipped')
content = content.replace('test_route_metrics_without_datapoints', 'test_route_metrics_graceful_missing_columns')
content = content.replace('test_route_metrics_variant_disabled', 'test_route_metrics_variant_disabled')
content = content.replace('Field::new("datapoints"', 'Field::new("attributes"')
content = content.replace('.expect("datapoints exists")', '.expect("attributes exists")')

with open('crates/parquet-sink/src/router.rs', 'w') as f:
    f.write(content)

