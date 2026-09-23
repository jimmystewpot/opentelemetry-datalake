use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::error::PipelineError;
use pipeline_core::sort::{
    BatchSorter, MissingColumnAction, SignalType, SortColumnDef, SortConfig,
};
use starrocks_sink::{
    StarRocksFormat, StarRocksSink, StarRocksSinkConfig, TableMapping, TransactionMode,
};
use std::sync::Arc;
use storage::iceberg::{IcebergSink, IcebergSinkConfig};

#[test]
fn test_e2e_sorter_with_interleaved_telemetry() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["web", "auth", "web", "auth"])),
            Arc::new(Int64Array::from(vec![9, 13, 5, 9])),
            Arc::new(Int64Array::from(vec![1000, 1050, 1010, 1040])),
        ],
    )
    .expect("batch creation failed");

    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("timestamp ASC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };

    let sorter = BatchSorter::from_config(&sort_config).expect("sorter creation");
    let output_batch = sorter.sort(&batch, SignalType::Logs).expect("sort failed");

    let services = output_batch
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let timestamps = output_batch
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(services.value(0), "auth");
    assert_eq!(timestamps.value(0), 1040);
    assert_eq!(services.value(1), "auth");
    assert_eq!(timestamps.value(1), 1050);
    assert_eq!(services.value(2), "web");
    assert_eq!(timestamps.value(2), 1000);
    assert_eq!(services.value(3), "web");
    assert_eq!(timestamps.value(3), 1010);
}

#[test]
fn test_e2e_kafka_partition_key_slicing_flow() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["payment", "checkout", "payment"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![200, 100, 150])),
        ],
    )
    .unwrap();

    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Error,
        logs: vec![SortColumnDef::Shorthand("timestamp ASC".to_string())],
        metrics: vec![],
        traces: vec![],
    };

    let sorter = BatchSorter::from_config(&sort_config).unwrap();
    let output_batch = sorter
        .sort_with_extra_lead_column(&batch, SignalType::Logs, Some("service_name"))
        .unwrap();

    let slices = kafka_sink::extract_partition_slices(&output_batch, "service_name").unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].0, "checkout");
    assert_eq!(slices[0].1.num_rows(), 1);
    assert_eq!(slices[1].0, "payment");
    assert_eq!(slices[1].1.num_rows(), 2);

    let payment_ts = slices[1]
        .1
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(payment_ts.value(0), 150);
    assert_eq!(payment_ts.value(1), 200);
}

#[test]
fn test_e2e_iceberg_sink_presorting_signals() {
    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("timestamp DESC".to_string()),
        ],
        metrics: vec![
            SortColumnDef::Shorthand("metric_name ASC".to_string()),
            SortColumnDef::Shorthand("value DESC".to_string()),
        ],
        traces: vec![SortColumnDef::Shorthand("trace_id ASC".to_string())],
    };

    let config = IcebergSinkConfig {
        order_by: Some(sort_config),
        ..Default::default()
    };
    let sink = IcebergSink::new(config);

    // 1. Logs pre-sorting
    let logs_schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let logs_batch = RecordBatch::try_new(
        logs_schema,
        vec![
            Arc::new(StringArray::from(vec!["api", "api", "auth", "auth"])),
            Arc::new(Int64Array::from(vec![100, 200, 50, 150])),
        ],
    )
    .unwrap();

    let sorted_logs = sink.sort_logs(&logs_batch).unwrap();
    let log_services = sorted_logs
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let log_timestamps = sorted_logs
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(log_services.value(0), "api");
    assert_eq!(log_timestamps.value(0), 200); // DESC
    assert_eq!(log_services.value(1), "api");
    assert_eq!(log_timestamps.value(1), 100);
    assert_eq!(log_services.value(2), "auth");
    assert_eq!(log_timestamps.value(2), 150); // DESC
    assert_eq!(log_services.value(3), "auth");
    assert_eq!(log_timestamps.value(3), 50);

    // 2. Metrics pre-sorting
    let metrics_schema = Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let metrics_batch = RecordBatch::try_new(
        metrics_schema,
        vec![
            Arc::new(StringArray::from(vec!["cpu", "memory", "cpu"])),
            Arc::new(Int64Array::from(vec![50, 10, 90])),
        ],
    )
    .unwrap();

    let sorted_metrics = sink.sort_metrics(&metrics_batch).unwrap();
    let metric_names = sorted_metrics
        .column_by_name("metric_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let metric_values = sorted_metrics
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(metric_names.value(0), "cpu");
    assert_eq!(metric_values.value(0), 90); // DESC
    assert_eq!(metric_names.value(1), "cpu");
    assert_eq!(metric_values.value(1), 50);
    assert_eq!(metric_names.value(2), "memory");
    assert_eq!(metric_values.value(2), 10);
}

#[test]
fn test_e2e_iceberg_sink_default_fallback() {
    let default_sink = IcebergSink::new(IcebergSinkConfig::default());
    let default_logs_schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let default_logs_batch = RecordBatch::try_new(
        default_logs_schema,
        vec![
            Arc::new(StringArray::from(vec!["auth", "auth", "api"])),
            Arc::new(StringArray::from(vec!["WARN", "INFO", "ERROR"])),
            Arc::new(Int64Array::from(vec![200, 100, 50])),
        ],
    )
    .unwrap();

    let sorted_default_logs = default_sink.sort_logs(&default_logs_batch).unwrap();
    let default_svc = sorted_default_logs
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let default_sev = sorted_default_logs
        .column_by_name("severity_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let default_ts = sorted_default_logs
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(default_svc.value(0), "api");
    assert_eq!(default_sev.value(0), "ERROR");
    assert_eq!(default_ts.value(0), 50);

    assert_eq!(default_svc.value(1), "auth");
    assert_eq!(default_sev.value(1), "INFO");
    assert_eq!(default_ts.value(1), 100);

    assert_eq!(default_svc.value(2), "auth");
    assert_eq!(default_sev.value(2), "WARN");
    assert_eq!(default_ts.value(2), 200);
}

#[test]
fn test_e2e_starrocks_sink_presorting() {
    let sort_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("timestamp DESC".to_string()),
            SortColumnDef::Shorthand("service_name ASC".to_string()),
        ],
        metrics: vec![SortColumnDef::Shorthand("metric_name ASC".to_string())],
        traces: vec![],
    };

    let config = StarRocksSinkConfig {
        frontend_urls: vec!["http://127.0.0.1:8030".to_string()],
        database: "telemetry".to_string(),
        username: "root".to_string(),
        password: None,
        format: StarRocksFormat::Ipc,
        transaction_mode: TransactionMode::V1,
        table_mapping: TableMapping::PerSignal {
            logs: "otel_logs".to_string(),
            metrics: "otel_metrics".to_string(),
            traces: "otel_traces".to_string(),
        },
        max_payload_bytes: 104_857_600,
        connect_timeout_secs: 10,
        request_timeout_secs: 600,
        max_retries: 3,
        retry_interval_secs: 1,
        order_by: Some(sort_config),
        batching: None,
        tls: pipeline_core::tls::TlsConfig::default(),
    };

    let sink = StarRocksSink::try_new(config).expect("StarRocksSink creation failed");

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("service_name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100, 200, 100])),
            Arc::new(StringArray::from(vec![
                "b_service",
                "a_service",
                "a_service",
            ])),
        ],
    )
    .unwrap();

    let output_batch = sink.sorter().sort(&batch, SignalType::Logs).unwrap();
    let ts_col = output_batch
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let svc_col = output_batch
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Sorted by timestamp DESC, then service_name ASC
    assert_eq!(ts_col.value(0), 200);
    assert_eq!(svc_col.value(0), "a_service");
    assert_eq!(ts_col.value(1), 100);
    assert_eq!(svc_col.value(1), "a_service");
    assert_eq!(ts_col.value(2), 100);
    assert_eq!(svc_col.value(2), "b_service");
}

#[test]
fn test_e2e_schema_tolerance_skip_and_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["b", "a"])),
            Arc::new(Int64Array::from(vec![20, 10])),
        ],
    )
    .unwrap();

    // 1. MissingColumnAction::Skip: missing columns are omitted, valid columns still sort
    let skip_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![
            SortColumnDef::Shorthand("missing_column ASC".to_string()),
            SortColumnDef::Shorthand("service_name ASC".to_string()),
            SortColumnDef::Shorthand("also_missing DESC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };
    let skip_engine = BatchSorter::from_config(&skip_config).unwrap();
    let output_skip = skip_engine.sort(&batch, SignalType::Logs).unwrap();

    let svc = output_skip
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(svc.value(0), "a");
    assert_eq!(svc.value(1), "b");

    // 2. MissingColumnAction::Skip when ALL configured columns are missing
    let all_missing_config = SortConfig {
        on_missing_column: MissingColumnAction::Skip,
        logs: vec![SortColumnDef::Shorthand(
            "completely_missing ASC".to_string(),
        )],
        metrics: vec![],
        traces: vec![],
    };
    let all_missing_engine = BatchSorter::from_config(&all_missing_config).unwrap();
    let output_untouched = all_missing_engine.sort(&batch, SignalType::Logs).unwrap();
    assert_eq!(output_untouched.num_rows(), 2);
    let orig_svc = output_untouched
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(orig_svc.value(0), "b"); // unchanged order

    // 3. MissingColumnAction::Error: fails fast if any column is missing
    let error_config = SortConfig {
        on_missing_column: MissingColumnAction::Error,
        logs: vec![
            SortColumnDef::Shorthand("missing_column ASC".to_string()),
            SortColumnDef::Shorthand("service_name ASC".to_string()),
        ],
        metrics: vec![],
        traces: vec![],
    };
    let error_engine = BatchSorter::from_config(&error_config).unwrap();
    let err = error_engine.sort(&batch, SignalType::Logs).unwrap_err();
    assert!(
        matches!(err, PipelineError::Arrow(_) | PipelineError::Internal(_)),
        "Expected error when sort column is missing under Error mode, got: {err}"
    );
}

#[test]
fn test_e2e_kafka_partition_key_slicing_edge_cases() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    // 1. Empty batch
    let empty_batch = RecordBatch::new_empty(schema.clone());
    let empty_slices = kafka_sink::extract_partition_slices(&empty_batch, "service_name").unwrap();
    assert!(empty_slices.is_empty());

    // 2. Single row batch
    let single_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["solo_service"])),
            Arc::new(Int64Array::from(vec![42])),
        ],
    )
    .unwrap();
    let single_slices =
        kafka_sink::extract_partition_slices(&single_batch, "service_name").unwrap();
    assert_eq!(single_slices.len(), 1);
    assert_eq!(single_slices[0].0, "solo_service");
    assert_eq!(single_slices[0].1.num_rows(), 1);

    // 3. Uniform partition batch (all rows have identical partition key)
    let uniform_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["auth", "auth", "auth"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let uniform_slices =
        kafka_sink::extract_partition_slices(&uniform_batch, "service_name").unwrap();
    assert_eq!(uniform_slices.len(), 1);
    assert_eq!(uniform_slices[0].0, "auth");
    assert_eq!(uniform_slices[0].1.num_rows(), 3);

    // 4. Missing partition key column error
    let missing_col_err =
        kafka_sink::extract_partition_slices(&uniform_batch, "nonexistent").unwrap_err();
    assert!(
        matches!(
            missing_col_err,
            PipelineError::Internal(ref msg) if msg.contains("Missing partition key column")
        ),
        "Expected missing partition key column error, got: {missing_col_err}"
    );

    // 5. Non-Utf8 partition key column error
    let non_utf8_err =
        kafka_sink::extract_partition_slices(&uniform_batch, "timestamp").unwrap_err();
    assert!(
        matches!(
            non_utf8_err,
            PipelineError::Internal(ref msg) if msg.contains("must be Utf8")
        ),
        "Expected non-Utf8 partition key column error, got: {non_utf8_err}"
    );
}
