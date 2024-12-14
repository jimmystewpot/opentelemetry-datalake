CREATE TABLE otel_logs (
    timestamp TIMESTAMP,
    observed_timestamp TIMESTAMP,
    trace_id STRING,
    span_id STRING,
    trace_flags INT,
    severity_text STRING,
    severity_number INT,
    body STRING,
    service_name STRING,
    resource_attributes MAP<STRING, STRING>,
    scope_name STRING,
    scope_version STRING,
    log_attributes MAP<STRING, STRING>
)
USING iceberg
PARTITIONED BY (hours(timestamp))
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);
