use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_sink::{CompressionCodec, ParquetSink, ParquetSinkConfig};
use pipeline_core::pipeline::{SignalBatch, Sink};
use tokio::sync::mpsc;

fn find_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(find_parquet_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files
}

fn create_logs_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
                1_700_000_001_000_000_000,
                1_700_000_002_000_000_000,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"message":"log 1","level":"info"}"#,
                r#"{"message":"log 2","level":"warn"}"#,
                r#"{"message":"log 3","level":"error"}"#,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"user_id":100,"ip":"10.0.0.1"}"#,
                r#"{"user_id":101,"ip":"10.0.0.2"}"#,
                r#"{"user_id":102,"ip":"10.0.0.3"}"#,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"service.name":"auth-svc","env":"prod"}"#,
                r#"{"service.name":"billing-svc","env":"prod"}"#,
                r#"{"service.name":"gateway-svc","env":"staging"}"#,
            ])),
        ],
    )
    .expect("valid logs batch")
}

fn create_traces_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
                1_700_000_001_000_000_000,
            ])),
            Arc::new(StringArray::from(vec![
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "7c5414f52b7c4a16b9b3e120fcf95221",
            ])),
            Arc::new(StringArray::from(vec![
                "00f067aa0ba902b7",
                "5fb397be34d23b0f",
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"http.status_code":200,"http.method":"GET"}"#,
                r#"{"http.status_code":500,"http.method":"POST"}"#,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"service.name":"api-gateway"}"#,
                r#"{"service.name":"payment-service"}"#,
            ])),
        ],
    )
    .expect("valid traces batch")
}

fn create_metrics_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("datapoints", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
                1_700_000_001_000_000_000,
            ])),
            Arc::new(StringArray::from(vec![
                "system.cpu.utilization",
                "http.server.duration",
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"cpu.core":0,"state":"idle"}"#,
                r#"{"http.route":"/checkout"}"#,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"host.id":"i-12345"}"#,
                r#"{"host.id":"i-67890"}"#,
            ])),
            Arc::new(StringArray::from(vec![
                r#"{"value":0.82,"count":1}"#,
                r#"{"sum":142.5,"count":12}"#,
            ])),
        ],
    )
    .expect("valid metrics batch")
}

fn assert_variant_struct_field(field: &Field) {
    assert_eq!(
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("variant"),
        "Field '{}' must be annotated with ARROW:extension:name = variant",
        field.name()
    );

    match field.data_type() {
        DataType::Struct(child_fields) => {
            assert_eq!(
                child_fields.len(),
                2,
                "Variant struct must have exactly 2 child fields (metadata and value)"
            );
            assert_eq!(child_fields[0].name(), "metadata");
            assert_eq!(child_fields[0].data_type(), &DataType::Binary);
            assert_eq!(child_fields[1].name(), "value");
            assert_eq!(child_fields[1].data_type(), &DataType::Binary);
        }
        other => panic!(
            "Expected Struct DataType for variant field '{}', got {:?}",
            field.name(),
            other
        ),
    }
}

fn verify_logs_file(schema: &Schema, batches: &[RecordBatch]) {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows in logs parquet file");
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("body").is_ok());
    assert!(schema.field_with_name("attributes").is_ok());
    assert!(schema.field_with_name("resource_attributes").is_ok());

    assert_variant_struct_field(schema.field_with_name("attributes").unwrap());
    assert_variant_struct_field(schema.field_with_name("resource_attributes").unwrap());
}

fn verify_traces_file(schema: &Schema, batches: &[RecordBatch]) {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows in traces parquet file");
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("trace_id").is_ok());
    assert!(schema.field_with_name("span_id").is_ok());
    assert!(schema.field_with_name("attributes").is_ok());
    assert!(schema.field_with_name("resource_attributes").is_ok());

    assert_variant_struct_field(schema.field_with_name("attributes").unwrap());
    assert_variant_struct_field(schema.field_with_name("resource_attributes").unwrap());

    for batch in batches {
        let attr_col = batch
            .column_by_name("attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("attributes is StructArray");
        let meta_col = attr_col
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("metadata is BinaryArray");
        let val_col = attr_col
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("value is BinaryArray");

        for i in 0..batch.num_rows() {
            assert!(!meta_col.value(i).is_empty());
            assert!(!val_col.value(i).is_empty());
        }
    }
}

fn verify_metrics_file(schema: &Schema, batches: &[RecordBatch]) {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows in metrics parquet file");
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("metric_name").is_ok());
    assert!(schema.field_with_name("attributes").is_ok());
    assert!(schema.field_with_name("resource_attributes").is_ok());
    assert!(schema.field_with_name("datapoints").is_ok());

    assert_variant_struct_field(schema.field_with_name("attributes").unwrap());
    assert_variant_struct_field(schema.field_with_name("resource_attributes").unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_write_and_read_back_variant_parquet() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        compression: CompressionCodec::Zstd { level: Some(3) },
        variant_encoding: true,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).expect("sink try_new");
    let (tx, rx) = mpsc::channel(10);

    tx.send(SignalBatch::Logs(create_logs_batch()))
        .await
        .expect("send logs");
    tx.send(SignalBatch::Traces(create_traces_batch()))
        .await
        .expect("send traces");
    tx.send(SignalBatch::Metrics(create_metrics_batch()))
        .await
        .expect("send metrics");

    drop(tx);
    sink.run(rx).await.expect("sink run");

    let parquet_files = find_parquet_files(temp_dir.path());
    assert_eq!(
        parquet_files.len(),
        3,
        "Expected exactly 3 parquet files for logs, traces, and metrics"
    );

    let mut verified_logs = false;
    let mut verified_traces = false;
    let mut verified_metrics = false;

    for file_path in &parquet_files {
        let path_str = file_path.to_string_lossy();
        let file = File::open(file_path).expect("open parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("ParquetRecordBatchReaderBuilder");
        let schema = builder.schema().clone();
        let reader = builder.build().expect("build reader");
        let batches: Vec<RecordBatch> = reader.map(|r| r.expect("read batch")).collect();

        if path_str.contains("signal=logs") {
            verify_logs_file(&schema, &batches);
            verified_logs = true;
        } else if path_str.contains("signal=traces") {
            verify_traces_file(&schema, &batches);
            verified_traces = true;
        } else if path_str.contains("signal=metrics") {
            verify_metrics_file(&schema, &batches);
            verified_metrics = true;
        }
    }

    assert!(verified_logs, "Logs parquet file verified");
    assert!(verified_traces, "Traces parquet file verified");
    assert!(verified_metrics, "Metrics parquet file verified");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compression_codecs_roundtrip() {
    let codecs = [
        CompressionCodec::Zstd { level: Some(3) },
        CompressionCodec::Snappy,
        CompressionCodec::Lz4Raw,
        CompressionCodec::Uncompressed,
    ];

    for codec in codecs {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config = ParquetSinkConfig {
            storage_uri: format!("file://{}", temp_dir.path().display()),
            compression: codec,
            variant_encoding: true,
            ..Default::default()
        };

        let mut sink = ParquetSink::try_new(config).expect("sink try_new");
        let (tx, rx) = mpsc::channel(10);

        let batch = create_logs_batch();
        let expected_rows = batch.num_rows();

        tx.send(SignalBatch::Logs(batch)).await.expect("send batch");
        drop(tx);

        sink.run(rx).await.expect("sink run");

        let files = find_parquet_files(temp_dir.path());
        assert_eq!(
            files.len(),
            1,
            "Expected 1 parquet file for codec {codec:?}"
        );

        let file = File::open(&files[0]).expect("open parquet file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap_or_else(|e| panic!("Failed reading parquet file for codec {codec:?}: {e}"));
        let reader = builder.build().expect("build reader");

        let read_batches: Vec<RecordBatch> = reader.map(|r| r.expect("read batch")).collect();
        let total_rows: usize = read_batches.iter().map(RecordBatch::num_rows).sum();

        assert_eq!(
            total_rows, expected_rows,
            "Row count mismatch for codec {codec:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_path_partitioning_hierarchy() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        node_id: "test-node-01".to_string(),
        variant_encoding: false,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).expect("sink try_new");
    let (tx, rx) = mpsc::channel(10);

    // Two rows with timestamps in two separate hours:
    // 1_700_000_000_000_000_000 ns -> 2023-11-14 22:13:20 UTC
    // 1_700_003_600_000_000_000 ns -> 2023-11-14 23:13:20 UTC
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("msg", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
                1_700_003_600_000_000_000,
            ])),
            Arc::new(StringArray::from(vec!["hour22", "hour23"])),
        ],
    )
    .expect("batch");

    tx.send(SignalBatch::Logs(batch)).await.expect("send batch");
    drop(tx);

    sink.run(rx).await.expect("sink run");

    let files = find_parquet_files(temp_dir.path());
    assert_eq!(
        files.len(),
        2,
        "Expected 2 parquet files partitioned across two hours"
    );

    let mut found_hour_22 = false;
    let mut found_hour_23 = false;

    for path in &files {
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains("signal=logs/date=2023-11-14/hour=22")
                || path_str.contains("signal=logs/date=2023-11-14/hour=23"),
            "Path '{path_str}' must match partition pattern signal=logs/date=2023-11-14/hour=XX"
        );

        let filename = path.file_name().unwrap().to_string_lossy();
        assert!(
            filename.contains("test-node-01"),
            "Filename '{filename}' must contain configured node_id"
        );
        assert!(
            filename.ends_with(".parquet"),
            "Filename '{filename}' must end with .parquet"
        );

        if path_str.contains("hour=22") {
            found_hour_22 = true;
        }
        if path_str.contains("hour=23") {
            found_hour_23 = true;
        }
    }

    assert!(found_hour_22, "Hour 22 partition file found");
    assert!(found_hour_23, "Hour 23 partition file found");
}

#[test]
fn test_s3_compatible_storage_configuration() {
    let mut storage_options = HashMap::new();
    storage_options.insert("endpoint".to_string(), "http://127.0.0.1:9000".to_string());
    storage_options.insert("region".to_string(), "us-east-1".to_string());
    storage_options.insert("access_key_id".to_string(), "minioadmin".to_string());
    storage_options.insert("secret_access_key".to_string(), "minioadmin".to_string());

    let config = ParquetSinkConfig {
        storage_uri: "s3://telemetry-bucket/otlp-data".to_string(),
        storage_options,
        ..Default::default()
    };

    let sink_res = ParquetSink::try_new(config);
    assert!(
        sink_res.is_ok(),
        "S3-compatible sink initialization should succeed: {:?}",
        sink_res.err()
    );
}
