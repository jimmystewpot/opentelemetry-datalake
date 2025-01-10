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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ipc" => Ok(Self::Ipc),
            "json" => Ok(Self::Json),
            _ => Err(format!("Unknown serialization format: {s}")),
        }
    }
}

/// Sink that writes Arrow record batches to Apache Kafka.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    format: SerializationFormat,
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
        })
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
            let batch = match signal {
                SignalBatch::Logs(b) | SignalBatch::Traces(b) | SignalBatch::Metrics(b) => b,
            };

            if batch.num_rows() == 0 {
                continue;
            }

            self.serialize_batch(&batch, &mut buffer)?;

            let record = FutureRecord::to(&self.topic).payload(&buffer).key("");

            if let Err((e, _)) = self
                .producer
                .send(record, tokio::time::Duration::from_secs(5))
                .await
            {
                tracing::error!("Failed to send record to Kafka: {e}");
                return Err(PipelineError::Internal(format!("Kafka send error: {e}")));
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
}
