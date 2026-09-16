use async_trait::async_trait;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, SignalBatch, Sink};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;

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
}

/// Scans a contiguous sorted partition key column and slices the batch into sub-batches.
pub fn extract_partition_slices(
    batch: &arrow::record_batch::RecordBatch,
    key_column: &str,
) -> Result<Vec<(String, arrow::record_batch::RecordBatch)>, PipelineError> {
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
    if num_rows == 1 {
        return Ok(vec![(str_arr.value(0).to_string(), batch.clone())]);
    }

    let mut start = 0;
    let mut current_val = str_arr.value(0).to_string();

    for i in 1..num_rows {
        let val = str_arr.value(i);
        if val != current_val {
            slices.push((current_val, batch.slice(start, i - start)));
            start = i;
            current_val = val.to_string();
        }
    }
    slices.push((current_val, batch.slice(start, num_rows - start)));

    Ok(slices)
}

impl KafkaSink {
    /// Creates a new `KafkaSink`.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Internal` if the producer creation fails.
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

        Ok(Self {
            producer,
            topic: topic.to_string(),
            format,
            sorter: pipeline_core::sort::BatchSorter::default(),
            partition_key: None,
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

    /// Serializes an Arrow `RecordBatch` to the configured format.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Internal` if serialization fails.
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
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        let mut buffer = Vec::with_capacity(8192);
        while let Some(signal) = input.recv().await {
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
                vec![(String::new(), sorted_batch)]
            };

            for (key_str, sub_batch) in batches_to_send {
                self.serialize_batch(&sub_batch, &mut buffer)?;
                let record = FutureRecord::to(&self.topic).payload(&buffer).key(&key_str);
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

    /// An unrecognised format string must return a PipelineError::Internal error.
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

    /// IPC-serialized bytes must be readable back via Arrow's StreamReader,
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
        assert_eq!(slices[0].0, "auth");
        assert_eq!(slices[0].1.num_rows(), 2);
        assert_eq!(slices[1].0, "billing");
        assert_eq!(slices[1].1.num_rows(), 1);
        assert_eq!(slices[2].0, "gateway");
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
        assert_eq!(single_slices[0].0, "auth");
        assert_eq!(single_slices[0].1.num_rows(), 1);
    }
}
