CREATE TABLE otel_metrics (
    resource_attributes MAP<STRING, STRING>,
    resource_schema_url STRING,
    scope_name STRING,
    scope_version STRING,
    scope_attributes MAP<STRING, STRING>,
    scope_dropped_attr_count INT,
    scope_schema_url STRING,
    service_name STRING,
    metric_name STRING,
    metric_description STRING,
    metric_unit STRING,
    attributes MAP<STRING, STRING>,
    start_time_unix TIMESTAMP,
    time_unix TIMESTAMP,
    flags INT,
    metric_type STRING,
    value DOUBLE,
    count BIGINT,
    sum DOUBLE,
    bucket_counts LIST<BIGINT>,
    explicit_bounds LIST<DOUBLE>,
    scale INT,
    zero_count BIGINT,
    positive_offset INT,
    positive_bucket_counts LIST<BIGINT>,
    negative_offset INT,
    negative_bucket_counts LIST<BIGINT>,
    aggregation_temporality INT,
    is_monotonic BOOLEAN,
    min DOUBLE,
    max DOUBLE,
    exemplars LIST<STRUCT<
        filtered_attributes: MAP<STRING, STRING>,
        time_unix: TIMESTAMP,
        value: DOUBLE,
        span_id: STRING,
        trace_id: STRING
    >>
)
USING iceberg
PARTITIONED BY (hours(time_unix))
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);
