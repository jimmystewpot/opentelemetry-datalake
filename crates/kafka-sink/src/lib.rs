use arrow::array::Array;
use async_trait::async_trait;
use pipeline_core::buffer::{BufferConfig, FailureClass, LegacyBatchingConfig, SinkBuffer};
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::Sink;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Default timeout in milliseconds when flushing the Kafka producer on shutdown.
pub const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 10_000;

/// Configuration for the Apache Kafka sink (`[kafka]`).
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct KafkaSinkConfig {
    /// Comma-separated list of Kafka bootstrap brokers.
    pub brokers: String,
    /// Kafka topic for log records.
    pub logs_topic: String,
    /// Kafka topic for trace records.
    pub traces_topic: String,
    /// Kafka topic for metric records.
    pub metrics_topic: String,
    /// Serialization format for logs (`"json"` or `"ipc"`).
    pub logs_format: String,
    /// Serialization format for traces (`"json"` or `"ipc"`).
    pub traces_format: String,
    /// Serialization format for metrics (`"json"` or `"ipc"`).
    pub metrics_format: String,
    /// Optional column name used to partition and key log batches.
    #[serde(default)]
    pub logs_partition_key: Option<String>,
    /// Optional column name used to partition and key metric batches.
    #[serde(default)]
    pub metrics_partition_key: Option<String>,
    /// Optional column name used to partition and key trace batches.
    #[serde(default)]
    pub traces_partition_key: Option<String>,
    /// Optional chronological pre-sorting columns per signal.
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,
    /// Optional shutdown flush timeout in milliseconds (must be > 0 when set).
    #[serde(default)]
    pub flush_timeout_ms: Option<u64>,
    /// Unified [`BufferConfig`] for the Kafka sink (`[kafka.buffer]`).
    #[serde(default)]
    pub buffer: Option<BufferConfig>,
    /// Deprecated legacy `[kafka.batching]` configuration alias.
    #[serde(default)]
    pub batching: Option<LegacyBatchingConfig>,
    /// Additional `librdkafka` client configuration options.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// Type alias for [`KafkaSinkConfig`].
pub type KafkaConfig = KafkaSinkConfig;

impl KafkaSinkConfig {
    /// Resolves the effective [`BufferConfig`] for this Kafka sink, defaulting to
    /// [`BufferConfig::passthrough`] when neither `[kafka.buffer]` nor `[kafka.batching]`
    /// is specified.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::Configuration`] if buffer validation fails (rules V1–V5).
    pub fn resolved_buffer_config(&self) -> Result<BufferConfig, PipelineError> {
        BufferConfig::resolve("kafka", self.buffer.clone(), self.batching.clone(), true)
    }

    /// Validates the Kafka sink configuration, including `flush_timeout_ms`, `order_by`,
    /// and `[kafka.buffer]` / `[kafka.batching]` constraints.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError`] if any configuration parameter is invalid.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if self.flush_timeout_ms == Some(0) {
            return Err(PipelineError::Internal(
                "kafka.flush_timeout_ms must be greater than 0".to_string(),
            ));
        }
        if let Some(ref sort_cfg) = self.order_by {
            pipeline_core::sort::BatchSorter::from_config(sort_cfg)?;
        }
        self.resolved_buffer_config()?;
        Ok(())
    }
}

/// A serialized Kafka message payload paired with its optional partition key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSlice {
    /// Optional Kafka partition/message key (`None` when no `partition_key` column is configured
    /// or when the slice's partition key value is SQL NULL).
    pub partition_key: Option<String>,
    /// Serialized Arrow IPC or line-delimited JSON bytes for this slice.
    pub payload: Vec<u8>,
}

impl PreparedSlice {
    /// Returns the optional partition key slice.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.partition_key.as_deref()
    }
}

/// Type alias for [`PreparedSlice`].
pub type PreparedKafkaSlice = PreparedSlice;

/// Classifies an [`rdkafka::error::KafkaError`] into a [`FailureClass`] for [`SinkBuffer`] lease handling.
#[must_use]
pub fn classify_kafka_error(err: &KafkaError) -> FailureClass {
    match err {
        KafkaError::ClientConfig(..)
        | KafkaError::ClientCreation(_)
        | KafkaError::MessageConsumptionFatal(_) => FailureClass::Fatal {
            reason: err.to_string(),
        },
        KafkaError::Transaction(txn_err) if txn_err.is_fatal() => FailureClass::Fatal {
            reason: err.to_string(),
        },
        KafkaError::Nul(_) => FailureClass::Rejected {
            reason: err.to_string(),
        },
        _ => match err.rdkafka_error_code() {
            Some(
                RDKafkaErrorCode::Authentication
                | RDKafkaErrorCode::SaslAuthenticationFailed
                | RDKafkaErrorCode::TopicAuthorizationFailed
                | RDKafkaErrorCode::GroupAuthorizationFailed
                | RDKafkaErrorCode::ClusterAuthorizationFailed
                | RDKafkaErrorCode::TransactionalIdAuthorizationFailed
                | RDKafkaErrorCode::DelegationTokenAuthorizationFailed
                | RDKafkaErrorCode::DelegationTokenAuthDisabled
                | RDKafkaErrorCode::DelegationTokenExpired
                | RDKafkaErrorCode::UnsupportedSASLMechanism
                | RDKafkaErrorCode::IllegalSASLState
                | RDKafkaErrorCode::SecurityDisabled
                | RDKafkaErrorCode::SSL
                | RDKafkaErrorCode::Fatal
                | RDKafkaErrorCode::Fenced
                | RDKafkaErrorCode::ProducerFenced
                | RDKafkaErrorCode::TransactionCoordinatorFenced
                | RDKafkaErrorCode::InvalidConfig
                | RDKafkaErrorCode::InvalidArgument
                | RDKafkaErrorCode::NotConfigured,
            ) => FailureClass::Fatal {
                reason: err.to_string(),
            },
            Some(
                RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::MessageBatchTooLarge
                | RDKafkaErrorCode::InvalidMessage
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::InvalidRecord
                | RDKafkaErrorCode::InvalidDifferentRecord
                | RDKafkaErrorCode::BadMessage
                | RDKafkaErrorCode::BadCompression
                | RDKafkaErrorCode::UnsupportedCompressionType
                | RDKafkaErrorCode::UnsupportedForMessageFormat
                | RDKafkaErrorCode::InvalidTimestamp
                | RDKafkaErrorCode::KeySerialization
                | RDKafkaErrorCode::ValueSerialization
                | RDKafkaErrorCode::PolicyViolation
                | RDKafkaErrorCode::InvalidTopic
                | RDKafkaErrorCode::UnknownTopic
                | RDKafkaErrorCode::UnknownPartition,
            ) => FailureClass::Rejected {
                reason: err.to_string(),
            },
            Some(
                RDKafkaErrorCode::QueueFull
                | RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::MessageTimedOut
                | RDKafkaErrorCode::OperationTimedOut
                | RDKafkaErrorCode::TimedOutQueue
                | RDKafkaErrorCode::RequestTimedOut
                | RDKafkaErrorCode::LeaderNotAvailable
                | RDKafkaErrorCode::NotLeaderForPartition
                | RDKafkaErrorCode::BrokerNotAvailable
                | RDKafkaErrorCode::ReplicaNotAvailable
                | RDKafkaErrorCode::NetworkException
                | RDKafkaErrorCode::NotEnoughReplicas
                | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
                | RDKafkaErrorCode::ThrottlingQuotaExceeded
                | _,
            )
            | None => FailureClass::Transient,
        },
    }
}

const fn failure_severity(class: &FailureClass) -> u8 {
    match class {
        FailureClass::Transient => 1,
        FailureClass::Rejected { .. } => 2,
        FailureClass::Fatal { .. } => 3,
    }
}

fn merge_failure_class(current: Option<FailureClass>, next: FailureClass) -> FailureClass {
    match current {
        Some(existing) if failure_severity(&existing) >= failure_severity(&next) => existing,
        _ => next,
    }
}

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

/// Serializes an Arrow `RecordBatch` into `buf` using `format`.
///
/// # Errors
///
/// Returns [`PipelineError::Internal`] if Arrow IPC or JSON serialization fails.
pub fn serialize_record_batch(
    batch: &arrow::record_batch::RecordBatch,
    buf: &mut Vec<u8>,
    format: SerializationFormat,
) -> Result<(), PipelineError> {
    buf.clear();
    match format {
        SerializationFormat::Ipc => {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(buf, &batch.schema())
                .map_err(|e| PipelineError::Internal(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| PipelineError::Internal(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| PipelineError::Internal(e.to_string()))?;
            Ok(())
        }
        SerializationFormat::Json => {
            let mut writer = arrow::json::LineDelimitedWriter::new(buf);
            writer
                .write(batch)
                .map_err(|e| PipelineError::Internal(e.to_string()))?;
            Ok(())
        }
    }
}

/// Sorts `batch` (putting `partition_key` first when configured), slices contiguous partition-key
/// runs safely (preserving `None` for SQL NULL keys), and serializes each slice into a [`PreparedSlice`].
///
/// Designed to run inside [`pipeline_core::buffer::Lease::prepare`] on the Tokio blocking pool.
///
/// # Errors
///
/// Returns [`PipelineError`] if sorting, partition-key extraction, or serialization fails.
pub fn prepare_kafka_slices(
    batch: &arrow::record_batch::RecordBatch,
    signal: pipeline_core::sort::SignalType,
    sorter: &pipeline_core::sort::BatchSorter,
    partition_key: Option<&str>,
    format: SerializationFormat,
) -> Result<Vec<PreparedSlice>, PipelineError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let sorted_batch = sorter.sort_with_extra_lead_column(batch, signal, partition_key)?;
    let raw_slices = if let Some(key_col) = partition_key {
        extract_partition_slices(&sorted_batch, key_col)?
    } else {
        vec![(None, sorted_batch)]
    };

    let mut prepared = Vec::with_capacity(raw_slices.len());
    for (key_opt, sub_batch) in raw_slices {
        let mut payload = Vec::with_capacity(1024);
        serialize_record_batch(&sub_batch, &mut payload, format)?;
        prepared.push(PreparedSlice {
            partition_key: key_opt,
            payload,
        });
    }

    Ok(prepared)
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

    /// Configures pre-sorting and an optional partition key column for the sink.
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
    pub const fn flush_timeout(&self) -> Duration {
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
        serialize_record_batch(batch, buf, self.format)
    }

    async fn send_prepared_slices(
        &self,
        slices: Vec<PreparedSlice>,
    ) -> Result<Option<FailureClass>, PipelineError> {
        let queue_timeout = self.flush_timeout;
        if slices.len() == 1 {
            let Some(slice) = slices.into_iter().next() else {
                return Ok(None);
            };
            let mut record = FutureRecord::to(&self.topic).payload(&slice.payload);
            if let Some(ref k) = slice.partition_key {
                record = record.key(k.as_str());
            }
            if let Err((e, _)) = self.producer.send(record, queue_timeout).await {
                tracing::error!("Failed to send record to Kafka: {e}");
                return Ok(Some(classify_kafka_error(&e)));
            }
            return Ok(None);
        }

        let mut join_set = tokio::task::JoinSet::new();
        for slice in slices {
            let producer = self.producer.clone();
            let topic = self.topic.clone();
            join_set.spawn(async move {
                let mut record = FutureRecord::to(&topic).payload(&slice.payload);
                if let Some(ref k) = slice.partition_key {
                    record = record.key(k.as_str());
                }
                producer
                    .send(record, queue_timeout)
                    .await
                    .map_err(|(e, _)| e)
            });
        }

        let mut failure: Option<FailureClass> = None;
        while let Some(res) = join_set.join_next().await {
            let delivery_res = res.map_err(|join_err| {
                PipelineError::Internal(format!("Kafka delivery task join error: {join_err}"))
            })?;
            if let Err(e) = delivery_res {
                tracing::error!("Failed to send record to Kafka: {e}");
                failure = Some(merge_failure_class(failure, classify_kafka_error(&e)));
            }
        }

        Ok(failure)
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn run(&mut self, mut buffer: SinkBuffer) -> Result<(), PipelineError> {
        while let Some(lease) = buffer.next_lease().await? {
            if lease.num_rows() == 0 {
                lease.ack().await?;
                continue;
            }

            let signal = lease.signal();
            let sorter = self.sorter.clone();
            let partition_key = self.partition_key.clone();
            let format = self.format;

            let prep_res = lease
                .prepare(move |batch| {
                    prepare_kafka_slices(&batch, signal, &sorter, partition_key.as_deref(), format)
                })
                .await;

            let slices = match prep_res {
                Ok(slices) if slices.is_empty() => {
                    lease.ack().await?;
                    continue;
                }
                Ok(slices) => slices,
                Err(PipelineError::Storage(e)) => {
                    return Err(PipelineError::Storage(e));
                }
                Err(err) if err.to_string().starts_with("Lease::prepare join error") => {
                    return Err(err);
                }
                Err(err) => {
                    tracing::warn!(
                        signal = signal.as_str(),
                        first_seq = lease.id().first_seq,
                        last_seq = lease.id().last_seq,
                        attempt = lease.attempt(),
                        error = %err,
                        "Kafka lease preparation failed; nacking as Rejected"
                    );
                    lease
                        .nack(FailureClass::Rejected {
                            reason: err.to_string(),
                        })
                        .await?;
                    continue;
                }
            };

            if let Some(class) = self.send_prepared_slices(slices).await? {
                lease.nack(class).await?;
            } else {
                lease.ack().await?;
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
    use pipeline_core::pipeline::SignalBatch;
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

    #[tokio::test]
    async fn test_prepare_kafka_slices_sorts_and_serializes_with_null_partition_keys() {
        use arrow::array::{Int64Array, StringArray};
        use pipeline_core::buffer::{BufferConfig, SignalKind};
        use pipeline_core::sort::{BatchSorter, MissingColumnAction, SortColumnDef, SortConfig};

        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("svc-b"),
                    None,
                    Some("svc-a"),
                    Some("svc-b"),
                ])),
                Arc::new(Int64Array::from(vec![200, 300, 100, 50])),
            ],
        )
        .unwrap();

        let sort_cfg = SortConfig {
            on_missing_column: MissingColumnAction::Error,
            logs: vec![SortColumnDef::Shorthand("timestamp ASC".to_string())],
            metrics: vec![],
            traces: vec![],
        };
        let sorter = BatchSorter::from_config(&sort_cfg).unwrap();

        let (tx, mut buf) =
            SinkBuffer::new("kafka_prep_test", BufferConfig::passthrough()).unwrap();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        let lease = buf.next_lease().await.unwrap().unwrap();

        let slices: Vec<PreparedSlice> = lease
            .prepare(move |combined| {
                prepare_kafka_slices(
                    &combined,
                    SignalKind::Logs,
                    &sorter,
                    Some("service_name"),
                    SerializationFormat::Json,
                )
            })
            .await
            .unwrap();

        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].partition_key.as_deref(), Some("svc-a"));
        assert!(String::from_utf8_lossy(&slices[0].payload).contains("\"timestamp\":100"));

        assert_eq!(slices[1].partition_key.as_deref(), Some("svc-b"));
        let svc_b_json = String::from_utf8_lossy(&slices[1].payload);
        let lines: Vec<&str> = svc_b_json.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"timestamp\":50"));
        assert!(lines[1].contains("\"timestamp\":200"));

        assert_eq!(slices[2].partition_key, None);
        assert!(String::from_utf8_lossy(&slices[2].payload).contains("\"timestamp\":300"));

        lease.ack().await.unwrap();
    }

    #[test]
    fn test_classify_kafka_error_distinguishes_rejected_and_transient() {
        use pipeline_core::buffer::FailureClass;
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};

        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageSizeTooLarge
            )),
            FailureClass::Rejected { .. }
        ));
        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::InvalidRecord
            )),
            FailureClass::Rejected { .. }
        ));
        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::Authentication
            )),
            FailureClass::Fatal { .. }
        ));
        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::SaslAuthenticationFailed
            )),
            FailureClass::Fatal { .. }
        ));
        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::TopicAuthorizationFailed
            )),
            FailureClass::Fatal { .. }
        ));
        assert_eq!(
            classify_kafka_error(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)),
            FailureClass::Transient
        );
        assert_eq!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::BrokerTransportFailure
            )),
            FailureClass::Transient
        );
        assert_eq!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageTimedOut
            )),
            FailureClass::Transient
        );
    }

    #[tokio::test]
    async fn test_prepare_kafka_slices_with_null_partition_keys_and_sorting() {
        use arrow::array::StringArray;
        use pipeline_core::buffer::{BufferConfig, SignalKind};
        use pipeline_core::sort::BatchSorter;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("svc-b"),
                None,
                Some("svc-a"),
            ]))],
        )
        .unwrap();

        let sorter = BatchSorter::default();
        let (tx, mut buf) =
            SinkBuffer::new("kafka_prep_brief_test", BufferConfig::passthrough()).unwrap();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();
        let lease = buf.next_lease().await.unwrap().unwrap();

        let slices = lease
            .prepare(move |combined| {
                prepare_kafka_slices(
                    &combined,
                    SignalKind::Logs,
                    &sorter,
                    Some("service_name"),
                    SerializationFormat::Json,
                )
            })
            .await
            .unwrap();

        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].key(), Some("svc-a"));
        assert_eq!(slices[1].key(), Some("svc-b"));
        assert_eq!(slices[2].key(), None);
        lease.ack().await.unwrap();
    }

    #[test]
    fn test_classify_kafka_error_maps_message_size_too_large_to_rejected() {
        use pipeline_core::buffer::FailureClass;
        use rdkafka::error::{KafkaError, RDKafkaErrorCode};

        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageSizeTooLarge
            )),
            FailureClass::Rejected { .. }
        ));
        assert!(matches!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::Authentication
            )),
            FailureClass::Fatal { .. }
        ));
        assert_eq!(
            classify_kafka_error(&KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)),
            FailureClass::Transient
        );
        assert_eq!(
            classify_kafka_error(&KafkaError::MessageProduction(
                RDKafkaErrorCode::BrokerTransportFailure
            )),
            FailureClass::Transient
        );
    }

    #[tokio::test]
    async fn test_kafka_sink_run_acks_and_nacks_leases() {
        use arrow::array::StringArray;
        use pipeline_core::buffer::{BufferConfig, SignalKind};

        // 1. Successful delivery via mock cluster acknowledges the lease
        let mut ok_options = HashMap::new();
        ok_options.insert("test.mock.num.brokers".to_string(), "1".to_string());
        let mut sink_ok = KafkaSink::try_new(
            "localhost:9092",
            "test-topic",
            SerializationFormat::Json,
            &ok_options,
        )
        .unwrap()
        .with_sorting(
            pipeline_core::sort::BatchSorter::default(),
            Some("service_name".to_string()),
        );

        let (tx_ok, mut rx_ok) =
            SinkBuffer::new("kafka_ack_test", BufferConfig::passthrough()).unwrap();
        let ctrl_ok = rx_ok.controller();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("svc-b"),
                None,
                Some("svc-a"),
            ]))],
        )
        .unwrap();

        // Verify direct lease prepare + send_prepared_slices + ack increments committed_records
        tx_ok.send(SignalBatch::Logs(batch.clone())).await.unwrap();
        let lease = rx_ok.next_lease().await.unwrap().unwrap();
        let sorter = sink_ok.sorter.clone();
        let slices = lease
            .prepare(move |b| {
                prepare_kafka_slices(
                    &b,
                    SignalKind::Logs,
                    &sorter,
                    Some("service_name"),
                    SerializationFormat::Json,
                )
            })
            .await
            .unwrap();
        assert!(
            sink_ok
                .send_prepared_slices(slices)
                .await
                .unwrap()
                .is_none()
        );
        lease.ack().await.unwrap();
        assert_eq!(rx_ok.committed_records(), 3);

        // Verify full Sink::run loop processes a second batch, acks, and flushes cleanly
        tx_ok.send(SignalBatch::Logs(batch)).await.unwrap();
        drop(tx_ok);

        sink_ok.run(rx_ok).await.unwrap();
        assert!(ctrl_ok.is_ready());

        // 2. Oversized message triggers MessageSizeTooLarge -> Rejected nack -> dead-lettered after max_attempts = 1
        let mut reject_options = HashMap::new();
        reject_options.insert("test.mock.num.brokers".to_string(), "1".to_string());
        reject_options.insert("message.max.bytes".to_string(), "1000".to_string());
        let mut sink_reject = KafkaSink::try_new(
            "localhost:9092",
            "test-topic",
            SerializationFormat::Json,
            &reject_options,
        )
        .unwrap();

        let dlq_dir = std::env::temp_dir().join(format!(
            "kafka_dlq_reject_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let reject_buf_cfg = BufferConfig {
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            max_attempts: 1,
            dead_letter_path: Some(dlq_dir.clone()),
            ..BufferConfig::default()
        };
        let (tx_rej, rx_rej) = SinkBuffer::new("kafka_rej_test", reject_buf_cfg).unwrap();
        let big_str = "x".repeat(2048);
        let big_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(big_str.as_str())]))],
        )
        .unwrap();

        tx_rej.send(SignalBatch::Logs(big_batch)).await.unwrap();
        drop(tx_rej);

        sink_reject.run(rx_rej).await.unwrap();
        assert!(
            dlq_dir.exists(),
            "Rejected Kafka lease must be quarantined to dead_letter_path"
        );
        let _ = std::fs::remove_dir_all(&dlq_dir);
    }

    #[test]
    fn test_kafka_sink_config_resolved_buffer_config() {
        let cfg = KafkaSinkConfig {
            brokers: "localhost:9092".to_string(),
            logs_topic: "logs".to_string(),
            traces_topic: "traces".to_string(),
            metrics_topic: "metrics".to_string(),
            logs_format: "json".to_string(),
            traces_format: "json".to_string(),
            metrics_format: "json".to_string(),
            ..Default::default()
        };
        let resolved = cfg.resolved_buffer_config().unwrap();
        assert_eq!(resolved, pipeline_core::buffer::BufferConfig::passthrough());
    }
}
