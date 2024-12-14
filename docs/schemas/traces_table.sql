CREATE TABLE otel_traces (
    timestamp TIMESTAMP,
    trace_id STRING,
    span_id STRING,
    parent_span_id STRING,
    trace_state STRING,
    span_name STRING,
    span_kind STRING,
    service_name STRING,
    resource_attributes MAP<STRING, STRING>,
    scope_name STRING,
    scope_version STRING,
    span_attributes MAP<STRING, STRING>,
    duration BIGINT,
    status_code STRING,
    status_message STRING,
    events LIST<STRUCT<
        timestamp: TIMESTAMP,
        name: STRING,
        attributes: MAP<STRING, STRING>
    >>,
    links LIST<STRUCT<
        trace_id: STRING,
        span_id: STRING,
        trace_state: STRING,
        attributes: MAP<STRING, STRING>
    >>
)
USING iceberg
PARTITIONED BY (hours(timestamp))
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);
