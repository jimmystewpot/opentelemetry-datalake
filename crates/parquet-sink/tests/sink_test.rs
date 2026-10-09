use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet_sink::{CompressionCodec, ParquetSink, ParquetSinkConfig};
use pipeline_core::pipeline::{SignalBatch, Sink};
use tokio::sync::mpsc;

fn find_parquet_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut parquet_files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                parquet_files.extend(find_parquet_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                parquet_files.push(path);
            }
        }
    }
    parquet_files
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parquet_sink_drains_on_channel_close() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        compression: CompressionCodec::Snappy,
        max_file_interval_sec: 10,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).unwrap();
    let (tx, rx) = mpsc::channel(10);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .unwrap();

    tx.send(SignalBatch::Logs(batch)).await.unwrap();
    drop(tx); // close channel

    // Run sink, must complete gracefully
    sink.run(rx).await.expect("Sink run failed");

    // Verify file written to temp_dir
    let files: Vec<_> = std::fs::read_dir(temp_dir.path()).unwrap().collect();
    assert!(!files.is_empty(), "Expected parquet files to be written");

    let parquet_files = find_parquet_files(temp_dir.path());
    assert!(
        !parquet_files.is_empty(),
        "Expected at least one .parquet file to be written"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parquet_sink_idle_sweeper_triggers_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        compression: CompressionCodec::Snappy,
        max_file_interval_sec: 1, // Sweep after 1s
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).unwrap();
    let (tx, rx) = mpsc::channel(10);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(StringArray::from(vec![r#"{"msg":"idle log test"}"#])),
        ],
    )
    .unwrap();

    tx.send(SignalBatch::Logs(batch)).await.unwrap();

    // Spawn sink in background
    let sink_handle = tokio::spawn(async move { sink.run(rx).await });

    // Wait 2.2s for the idle sweeper ticker to trigger and close the active writer
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // Check that parquet file was already created while channel is still open
    let parquet_files = find_parquet_files(temp_dir.path());
    assert!(
        !parquet_files.is_empty(),
        "Idle sweeper should have flushed writer even before channel closed"
    );

    // Now drop sender and ensure sink finishes
    drop(tx);
    sink_handle
        .await
        .unwrap()
        .expect("Sink run should finish Ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_parquet_sink_heterogeneous_signals() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = ParquetSinkConfig {
        storage_uri: format!("file://{}", temp_dir.path().display()),
        max_file_interval_sec: 10,
        ..Default::default()
    };

    let mut sink = ParquetSink::try_new(config).unwrap();
    let (tx, rx) = mpsc::channel(10);

    // 1. Logs
    let logs_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, false),
    ]));
    let logs_batch = RecordBatch::try_new(
        logs_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(StringArray::from(vec![r#"{"msg":"hello log"}"#])),
        ],
    )
    .unwrap();

    // 2. Metrics
    let metrics_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("metric_val", DataType::Float64, false),
    ]));
    let metrics_batch = RecordBatch::try_new(
        metrics_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(Float64Array::from(vec![42.0])),
        ],
    )
    .unwrap();

    // 3. Traces
    let traces_schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, false),
    ]));
    let traces_batch = RecordBatch::try_new(
        traces_schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
            Arc::new(StringArray::from(vec!["abc123trace"])),
        ],
    )
    .unwrap();

    tx.send(SignalBatch::Logs(logs_batch)).await.unwrap();
    tx.send(SignalBatch::Metrics(metrics_batch)).await.unwrap();
    tx.send(SignalBatch::Traces(traces_batch)).await.unwrap();
    drop(tx);

    sink.run(rx).await.expect("Sink run should succeed");

    let files = find_parquet_files(temp_dir.path());
    assert_eq!(
        files.len(),
        3,
        "Expected 3 distinct parquet files for logs, metrics, and traces"
    );
}

#[test]
fn test_parquet_sink_try_new_invalid_config() {
    let config = ParquetSinkConfig {
        storage_uri: "unsupported://bucket/path".to_string(),
        ..Default::default()
    };
    let res = ParquetSink::try_new(config);
    assert!(res.is_err(), "Invalid storage scheme should fail try_new");
}
