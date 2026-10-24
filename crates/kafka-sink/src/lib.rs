use arrow::array::Array;
use async_trait::async_trait;
use pipeline_core::buffer::SinkBuffer;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{SignalBatch, Sink};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::HashMap;
use std::time::Duration;

/// Default timeout in milliseconds when flushing the Kafka producer on shutdown.
pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 10_000;

/// Kafka message serialization format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Arrow IPC Streaming format.
    Ipc,
    /// Line-delimited JSON format.
    Json,
}

impl std::str::FromStr for SerializationFormat {
    type Err = PipelineError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ipc" => Ok(Self::Ipc),
            "json" => Ok(Self::Json),
            _ => Err(PipelineError::Internal(format!(
                "Unknown serialization format: {s}"
            ))),
        }
    }
}

/// Sink that writes Arrow record batches to Apache Kafka.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    format: SerializationFormat,
    sorter: pipeline_core::sort::BatchSorter,
    partition_key: Option<String>,
    flush_timeout: Duration,
}

/// Scans a contiguous sorted partition key column and slices the batch into sub-batches.
pub fn extract_partition_slices(
    batch: &arrow::record_batch::RecordBatch,
    key_column: &str,
) -> Result<Vec<(Option<String>, arrow::record_batch::RecordBatch)>, PipelineError> {
    let col = batch.column_by_name(key_column).ok_or_else(|| {
        PipelineError::Internal(format!("Missing partition key column '{key_column}'"))
    })?;

    let str_arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| {
            PipelineError::Internal(format!("Partition key column '{key_column}' must be Utf8"))
        })?;

    let mut slices = Vec::new();
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(slices);
    }

    let get_key = |i: usize| -> Option<&str> {
        if str_arr.is_null(i) {
            None
        } else {
            Some(str_arr.value(i))
        }
    };

    let mut start = 0;
    let mut current_val = get_key(0);

    for i in 1..num_rows {
        let val = get_key(i);
        if val != current_val {
            slices.push((
                current_val.map(str::to_owned),
                batch.slice(start, i - start),
            ));
            start = i;
            current_val = val;
        }
    }
    slices.push((
        current_val.map(str::to_owned),
        batch.slice(start, num_rows - start),
    ));

    Ok(slices)
}

impl KafkaSink {
    /// Creates a new `KafkaSink`.
    ///
    /// # Errors
    ///
    /// Returns ``PipelineError::Internal`` if the producer creation fails.
    pub fn try_new(
        brokers: &str,
        topic: &str,
        format: SerializationFormat,
        options: &HashMap<String, String>,
    ) -> Result<Self, PipelineError> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", brokers);
        for (k, v) in options {
            client_config.set(k, v);
        }

        let producer: FutureProducer = client_config.create().map_err(|e| {
            PipelineError::Internal(format!("Failed to create Kafka producer: {e}"))
        })?;

        let flush_timeout = options
            .get("delivery.timeout.ms")
            .or_else(|| options.get("message.timeout.ms"))
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map_or_else(
                || Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS),
                Duration::from_millis,
            );

        Ok(Self {
            producer,
            topic: topic.to_string(),
            format,
            sorter: pipeline_core::sort::BatchSorter::default(),
            partition_key: None,
            flush_timeout,
        })
    }

    #[must_use]
    pub fn with_sorting(
        mut self,
        sorter: pipeline_core::sort::BatchSorter,
        partition_key: Option<String>,
    ) -> Self {
        self.sorter = sorter;
        self.partition_key = partition_key;
        self
    }

    /// Overrides the shutdown flush timeout.
    #[must_use]
    pub fn with_flush_timeout(mut self, timeout: Duration) -> Self {
        self.flush_timeout = timeout;
        self
    }

    /// Returns the configured shutdown flush timeout.
    #[must_use]
    pub fn flush_timeout(&self) -> Duration {
        self.flush_timeout
    }

    /// Serializes an Arrow `RecordBatch` to the configured format.
    ///
    /// # Errors
    ///
    /// Returns ``PipelineError::Internal`` if serialization fails.
    pub fn serialize_batch(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        buf: &mut Vec<u8>,
    ) -> Result<(), PipelineError> {
        buf.clear();
        match self.format {
            SerializationFormat::Ipc => {
                {
                    let mut writer =
                        arrow::ipc::writer::StreamWriter::try_new(buf, &batch.schema())
                            .map_err(|e| PipelineError::Internal(e.to_string()))?;
                    writer
                        .write(batch)
                        .map_err(|e| PipelineError::Internal(e.to_string()))?;
                    writer
                        .finish()
                        .map_err(|e| PipelineError::Internal(e.to_string()))?;
                }
                Ok(())
            }
            SerializationFormat::Json => {
                {
                    let mut writer = arrow::json::LineDelimitedWriter::new(buf);
                    writer
                        .write(batch)
                        .map_err(|e| PipelineError::Internal(e.to_string()))?;
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn run(&mut self, mut buffer: SinkBuffer) -> Result<(), PipelineError> {
        let mut payload_buf = Vec::with_capacity(8192);
        while let Some(signal) = buffer.recv_passthrough().await? {
            let signal_type = match &signal {
                SignalBatch::Logs(_) => pipeline_core::sort::SignalType::Logs,
                SignalBatch::Metrics(_) => pipeline_core::sort::SignalType::Metrics,
                SignalBatch::Traces(_) => pipeline_core::sort::SignalType::Traces,
            };

            let batch = match signal {
                SignalBatch::Logs(b) | SignalBatch::Traces(b) | SignalBatch::Metrics(b) => b,
            };

            if batch.num_rows() == 0 {
                continue;
            }

            let sorted_batch = self.sorter.sort_with_extra_lead_column(
                &batch,
                signal_type,
                self.partition_key.as_deref(),
            )?;

            let batches_to_send = if let Some(ref p_key) = self.partition_key {
                extract_partition_slices(&sorted_batch, p_key)?
            } else {
                vec![(None, sorted_batch)]
            };

            for (key_opt, sub_batch) in batches_to_send {
                self.serialize_batch(&sub_batch, &mut payload_buf)?;
                let mut record = FutureRecord::to(&self.topic).payload(&payload_buf);
                if let Some(ref k) = key_opt {
                    record = record.key(k.as_str());
                }
                if let Err((e, _)) = self
                    .producer
                    .send(record, tokio::time::Duration::from_secs(5))
                    .await
                {
                    tracing::error!("Failed to send record to Kafka: {e}");
                    return Err(PipelineError::Internal(format!("Kafka send error: {e}")));
                }
            }
        }

        let producer = self.producer.clone();
        let timeout = self.flush_timeout;
        tokio::task::spawn_blocking(move || Producer::flush(&producer, timeout))
            .await
            .map_err(|e| PipelineError::Internal(format!("Kafka flush task join error: {e}")))?
            .map_err(|e| {
                tracing::error!("Failed to flush Kafka producer during shutdown: {e}");
                PipelineError::Storage(Box::new(e))
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_kafka_sink_serialization() {
        let options = HashMap::new();
        let sink = KafkaSink::try_new(
            "localhost:9092",
            "test-topic",
            SerializationFormat::Json,
            &options,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut buffer = Vec::new();

        // Verify JSON serialization
        sink.serialize_batch(&batch, &mut buffer).unwrap();
        let json_str = String::from_utf8(buffer.clone()).unwrap();
        assert!(json_str.contains("{\"f\":1}"));

        // Verify IPC serialization
        let ipc_sink = KafkaSink::try_new(
            "localhost:9092",
            "test-topic",
            SerializationFormat::Ipc,
            &options,
        )
        .unwrap();
        ipc_sink.serialize_batch(&batch, &mut buffer).unwrap();
        assert!(!buffer.is_empty());
    }

    /// "ipc", "json", and mixed-case variants must all parse successfully.
    #[test]
    fn test_serialization_format_from_str_valid() {
        use std::str::FromStr;
        assert!(matches!(
            SerializationFormat::from_str("ipc").unwrap(),
            SerializationFormat::Ipc
        ));
        assert!(matches!(
            SerializationFormat::from_str("json").unwrap(),
            SerializationFormat::Json
        ));
    }

    /// An unrecognised format string must return a `PipelineError::Internal` error.
    #[test]
    fn test_serialization_format_from_str_invalid() {
        use std::str::FromStr;
        let result = SerializationFormat::from_str("avro");
        assert!(
            result.is_err(),
            "Unknown format string must return an error"
        );
        assert!(
            matches!(result.unwrap_err(), PipelineError::Internal(_)),
            "Error must be PipelineError::Internal"
        );
    }

    /// Parsing must be case-insensitive ("IPC", "JSON" are valid).
    #[test]
    fn test_serialization_format_from_str_case_insensitive() {
        use std::str::FromStr;
        assert!(SerializationFormat::from_str("IPC").is_ok());
        assert!(SerializationFormat::from_str("JSON").is_ok());
        assert!(SerializationFormat::from_str("Ipc").is_ok());
    }

    /// IPC-serialized bytes must be readable back via Arrow's `StreamReader`,
    /// and the decoded schema and row count must match the original batch.
    #[test]
    fn test_kafka_sink_ipc_round_trip() {
        let options = HashMap::new();
        let sink = KafkaSink::try_new(
            "localhost:9092",
            "test-topic",
            SerializationFormat::Ipc,
            &options,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let mut buffer = Vec::new();
        sink.serialize_batch(&batch, &mut buffer).unwrap();

        // Decode the IPC stream back
        let cursor = std::io::Cursor::new(&buffer);
        let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let decoded = reader.next().unwrap().unwrap();

        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(*decoded.schema(), *schema);
    }

    #[test]
    fn test_find_contiguous_partition_slices() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "auth", "auth", "billing", "gateway",
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let slices = extract_partition_slices(&batch, "service_name").expect("slices");
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].0, Some("auth".to_string()));
        assert_eq!(slices[0].1.num_rows(), 2);
        assert_eq!(slices[1].0, Some("billing".to_string()));
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0, Some("gateway".to_string()));
        assert_eq!(slices[2].1.num_rows(), 1);

        // Test empty batch
        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(Vec::<&str>::new())),
                Arc::new(arrow::array::Int32Array::from(Vec::<i32>::new())),
            ],
        )
        .unwrap();
        let empty_slices = extract_partition_slices(&empty_batch, "service_name").expect("slices");
        assert!(empty_slices.is_empty());

        // Test single row batch
        let single_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["auth"])),
                Arc::new(arrow::array::Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let single_slices =
            extract_partition_slices(&single_batch, "service_name").expect("slices");
        assert_eq!(single_slices.len(), 1);
        assert_eq!(single_slices[0].0, Some("auth".to_string()));
        assert_eq!(single_slices[0].1.num_rows(), 1);
    }

    #[test]
    fn test_extract_partition_slices_with_null_keys() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("svc-a"),
                None,
                Some("svc-b"),
            ]))],
        )
        .unwrap();

        let slices = extract_partition_slices(&batch, "service_name").unwrap();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].0.as_deref(), Some("svc-a"));
        assert_eq!(slices[0].1.num_rows(), 1);
        assert_eq!(slices[1].0, None);
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0.as_deref(), Some("svc-b"));
        assert_eq!(slices[2].1.num_rows(), 1);
    }

    #[test]
    fn test_extract_partition_slices_all_nulls_and_single_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));

        let single_null = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None::<&str>,
            ]))],
        )
        .unwrap();
        let slices = extract_partition_slices(&single_null, "service_name").unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 1);

        let all_nulls = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None::<&str>,
                None::<&str>,
                None::<&str>,
            ]))],
        )
        .unwrap();
        let slices = extract_partition_slices(&all_nulls, "service_name").unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 3);
    }

    #[test]
    fn test_extract_partition_slices_distinct_empty_and_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec![
                None,
                Some(""),
                None,
                Some("auth"),
                Some("auth"),
                None,
                None,
            ]))],
        )
        .unwrap();

        let slices = extract_partition_slices(&batch, "service_name").unwrap();
        assert_eq!(slices.len(), 5);
        assert_eq!(slices[0].0, None);
        assert_eq!(slices[0].1.num_rows(), 1);
        assert_eq!(slices[1].0.as_deref(), Some(""));
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0, None);
        assert_eq!(slices[2].1.num_rows(), 1);
        assert_eq!(slices[3].0.as_deref(), Some("auth"));
        assert_eq!(slices[3].1.num_rows(), 2);
        assert_eq!(slices[4].0, None);
        assert_eq!(slices[4].1.num_rows(), 2);
    }

    #[test]
    fn test_kafka_sink_flush_timeout_resolution() {
        use std::time::Duration;

        // 1. Default 10s
        let options = HashMap::new();
        let sink =
            KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &options).unwrap();
        assert_eq!(
            sink.flush_timeout(),
            Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS)
        );

        // 2. From message.timeout.ms
        let mut opts_msg = HashMap::new();
        opts_msg.insert("message.timeout.ms".to_string(), "3000".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_msg)
            .unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(3000));

        // 3. delivery.timeout.ms takes precedence over message.timeout.ms
        let mut opts_both = HashMap::new();
        opts_both.insert("message.timeout.ms".to_string(), "3000".to_string());
        opts_both.insert("delivery.timeout.ms".to_string(), "4500".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_both)
            .unwrap();
        assert_eq!(sink.flush_timeout(), Duration::from_millis(4500));

        // 4. Zero ("0") falls back to default 10s
        let mut opts_zero = HashMap::new();
        opts_zero.insert("message.timeout.ms".to_string(), "0".to_string());
        let sink = KafkaSink::try_new("localhost:9092", "t", SerializationFormat::Json, &opts_zero)
            .unwrap();
        assert_eq!(
            sink.flush_timeout(),
            Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS)
        );

        // 5. Builder override wins
        let sink = sink.with_flush_timeout(Duration::from_millis(250));
        assert_eq!(sink.flush_timeout(), Duration::from_millis(250));
    }

    #[tokio::test]
    async fn test_kafka_sink_run_flushes_on_channel_close() {
        use std::time::Duration;
        let options = HashMap::new();

        // Case A: Clean empty-queue shutdown flush succeeds
        let mut sink_ok = KafkaSink::try_new(
            "127.0.0.1:1",
            "test-topic",
            SerializationFormat::Json,
            &options,
        )
        .unwrap()
        .with_flush_timeout(Duration::from_millis(50));

        let (tx, rx) = SinkBuffer::passthrough_pair();
        drop(tx);
        let res = sink_ok.run(rx).await;
        assert!(
            res.is_ok(),
            "Empty producer queue flush on EOF should succeed: {res:?}"
        );

        // Case B: Enqueued message to unreachable broker fails flush on EOF with PipelineError::Storage
        let mut sink_fail = KafkaSink::try_new(
            "127.0.0.1:1",
            "test-topic",
            SerializationFormat::Json,
            &options,
        )
        .unwrap()
        .with_flush_timeout(Duration::from_millis(50));

        // Enqueue directly into librdkafka C queue without awaiting delivery
        let _ = sink_fail
            .producer
            .send_result(FutureRecord::to("test-topic").payload(b"payload").key("k"));

        let (tx2, rx2) = SinkBuffer::passthrough_pair();
        drop(tx2);
        let err = sink_fail
            .run(rx2)
            .await
            .expect_err("Flush with undelivered message to 127.0.0.1:1 must time out");
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Expected PipelineError::Storage from failed flush, got: {err:?}"
        );
    }
}
