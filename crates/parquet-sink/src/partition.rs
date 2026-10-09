//! Vectorized partition management with composite keys and memory tracking.
//!
//! Provides [`PartitionManager`], which routes incoming Arrow [`RecordBatch`] payloads
//! to isolated [`PartitionWriter`] instances based on composite partition keys
//! ([`PartitionId`]), evaluates temporal partitioning vectorially via Arrow compute kernels,
//! maintains an LRU writer pool with bounded memory tracking, and handles rolling triggers.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, BooleanArray, Int32Array};
use arrow::compute::kernels::temporal::{DatePart, date_part};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Timelike};
use opendal::Operator;
use smallvec::SmallVec;

use crate::config::ParquetSinkConfig;
use crate::error::ParquetSinkError;
use crate::naming::FileNamer;
use crate::router::PreparedBatch;
use crate::uploader::{AsyncUploader, UploaderHandle};
use crate::writer::PartitionWriter;

/// Composite partition identifier ensuring signal-partition isolation.
///
/// Combines the telemetry signal identifier (e.g. `"logs"`, `"metrics"`, `"traces"`)
/// with the formatted directory partition path. Heterogeneous signals arriving within
/// the same temporal window route to distinct [`PartitionWriter`] instances, preventing
/// schema conflict failures.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionId {
    /// Telemetry signal identifier (`"logs"`, `"metrics"`, or `"traces"`).
    pub signal: &'static str,
    /// Directory partition prefix path (e.g. `"signal=logs/date=2026-10-09/hour=12"`).
    pub path: String,
}

impl PartitionId {
    /// Creates a new `PartitionId` with the given signal identifier and directory path.
    #[must_use]
    pub fn new(signal: &'static str, path: String) -> Self {
        Self { signal, path }
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.signal, self.path)
    }
}

/// An active in-progress Parquet writer and its associated upload handle and metadata.
#[derive(Debug)]
struct ActiveWriter {
    writer: PartitionWriter,
    uploader_handle: UploaderHandle,
    opened_at: Instant,
    buffered_memory: usize,
}

/// Extracted temporal components from an Arrow timestamp array.
struct TemporalComponents {
    years: Arc<Int32Array>,
    months: Arc<Int32Array>,
    days: Arc<Int32Array>,
    hours: Arc<Int32Array>,
}

/// Extracted partition key components for a single row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionKey {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
}

/// Safely extracts temporal year, month, day, and hour components from a column array.
fn extract_temporal_components(col: &Arc<dyn Array>) -> Option<TemporalComponents> {
    let timestamp_col: Arc<dyn Array> = match col.data_type() {
        DataType::Timestamp(_, _) => Arc::clone(col),
        DataType::Int64 => arrow::compute::cast(
            col.as_ref(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .ok()?,
        _ => return None,
    };

    let years_arr = date_part(timestamp_col.as_ref(), DatePart::Year).ok()?;
    let months_arr = date_part(timestamp_col.as_ref(), DatePart::Month).ok()?;
    let days_arr = date_part(timestamp_col.as_ref(), DatePart::Day).ok()?;
    let hours_arr = date_part(timestamp_col.as_ref(), DatePart::Hour).ok()?;

    let years = Arc::clone(&years_arr)
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()?;
    let months = Arc::clone(&months_arr)
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()?;
    let days = Arc::clone(&days_arr)
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()?;
    let hours = Arc::clone(&hours_arr)
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()?;

    Some(TemporalComponents {
        years: Arc::new(years),
        months: Arc::new(months),
        days: Arc::new(days),
        hours: Arc::new(hours),
    })
}

/// Formats a partition directory path using the template pattern and temporal values.
fn format_partition_path(
    pattern: &str,
    signal: &str,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
) -> String {
    let date_str = format!("{year:04}-{month:02}-{day:02}");
    let hour_str = format!("{hour:02}");
    let year_str = format!("{year:04}");
    let month_str = format!("{month:02}");
    let day_str = format!("{day:02}");

    pattern
        .replace("{signal}", signal)
        .replace("{date}", &date_str)
        .replace("{hour}", &hour_str)
        .replace("{year}", &year_str)
        .replace("{month}", &month_str)
        .replace("{day}", &day_str)
}

/// Manages vectorized partitioning, active Parquet writer lifecycles,
/// and memory accounting for incoming telemetry batches.
#[derive(Debug)]
pub struct PartitionManager {
    config: ParquetSinkConfig,
    operator: Operator,
    namer: FileNamer,
    writers: HashMap<PartitionId, ActiveWriter>,
    lru_order: VecDeque<PartitionId>,
    global_memory_bytes: AtomicUsize,
    file_sequence: u16,
    in_flight_uploads: Vec<tokio::task::JoinHandle<Result<(), ParquetSinkError>>>,
    error_sender: tokio::sync::mpsc::UnboundedSender<ParquetSinkError>,
    error_receiver: tokio::sync::mpsc::UnboundedReceiver<ParquetSinkError>,
}

impl PartitionManager {
    /// Creates a new `PartitionManager` with the specified configuration and storage operator.
    #[must_use]
    pub fn new(config: ParquetSinkConfig, operator: Operator) -> Self {
        let namer = FileNamer::new(config.node_id.clone());
        let (error_sender, error_receiver) = tokio::sync::mpsc::unbounded_channel();
        Self {
            config,
            operator,
            namer,
            writers: HashMap::new(),
            lru_order: VecDeque::new(),
            global_memory_bytes: AtomicUsize::new(0),
            file_sequence: 0,
            in_flight_uploads: Vec::new(),
            error_sender,
            error_receiver,
        }
    }

    /// Returns the number of currently active partition writers.
    #[must_use]
    pub fn active_writer_count(&self) -> usize {
        self.writers.len()
    }

    /// Returns the current estimated buffered memory usage across all active writers in bytes.
    #[must_use]
    pub fn current_memory_bytes(&self) -> usize {
        self.global_memory_bytes.load(Ordering::Relaxed)
    }

    /// Routes a prepared batch annotated with its signal type to target partitions.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if filtering, writing, or file creation fails.
    pub fn route_prepared_batch(
        &mut self,
        prepared: &PreparedBatch,
    ) -> Result<(), ParquetSinkError> {
        self.route_batch(&prepared.batch, prepared.signal)
    }

    /// Routes an Arrow [`RecordBatch`] to target partition writers based on temporal keys.
    ///
    /// Empty batches are treated as a no-op. Timestamp values are evaluated vectorially;
    /// rows missing timestamps or having null values fallback cleanly to current UTC time.
    /// If all rows belong to the same partition, the batch is written directly without
    /// allocating boolean filter masks.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if filtering, Parquet serialization, or storage I/O fails.
    pub fn route_batch(
        &mut self,
        batch: &RecordBatch,
        signal: &'static str,
    ) -> Result<(), ParquetSinkError> {
        self.check_background_errors()?;

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let now = chrono::Utc::now();
        let fallback_key = PartitionKey {
            year: now.year(),
            month: i32::try_from(now.month()).unwrap_or(1),
            day: i32::try_from(now.day()).unwrap_or(1),
            hour: i32::try_from(now.hour()).unwrap_or(0),
        };

        let temporal_components = batch
            .column_by_name("timestamp")
            .and_then(extract_temporal_components);

        let row_key = |i: usize| -> PartitionKey {
            if let Some(components) = &temporal_components {
                if components.years.is_valid(i) {
                    PartitionKey {
                        year: components.years.value(i),
                        month: components.months.value(i),
                        day: components.days.value(i),
                        hour: components.hours.value(i),
                    }
                } else {
                    fallback_key
                }
            } else {
                fallback_key
            }
        };

        let first_key = row_key(0);
        let mut is_homogeneous = true;
        for i in 1..num_rows {
            if row_key(i) != first_key {
                is_homogeneous = false;
                break;
            }
        }

        if is_homogeneous {
            let path = format_partition_path(
                &self.config.partition_pattern,
                signal,
                first_key.year,
                first_key.month,
                first_key.day,
                first_key.hour,
            );
            return self.route_sub_batch(PartitionId::new(signal, path), batch);
        }

        let mut row_keys = Vec::with_capacity(num_rows);
        let mut distinct_keys = SmallVec::<[PartitionKey; 4]>::new();

        for i in 0..num_rows {
            let k = row_key(i);
            row_keys.push(k);
            if !distinct_keys.contains(&k) {
                distinct_keys.push(k);
            }
        }

        for key in distinct_keys {
            let mask = BooleanArray::from_iter(row_keys.iter().map(|&k| Some(k == key)));
            let sub_batch = arrow::compute::filter_record_batch(batch, &mask)?;
            let path = format_partition_path(
                &self.config.partition_pattern,
                signal,
                key.year,
                key.month,
                key.day,
                key.hour,
            );
            self.route_sub_batch(PartitionId::new(signal, path), &sub_batch)?;
        }

        Ok(())
    }

    /// Routes a homogeneous sub-batch directly to the active writer for `partition_id`.
    fn route_sub_batch(
        &mut self,
        partition_id: PartitionId,
        batch: &RecordBatch,
    ) -> Result<(), ParquetSinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let batch_memory = batch.get_array_memory_size();
        if self.config.global_memory_limit_bytes > 0
            && batch_memory > self.config.global_memory_limit_bytes
        {
            return Err(ParquetSinkError::Config(format!(
                "RecordBatch size ({batch_memory} bytes) exceeds global memory limit ({} bytes)",
                self.config.global_memory_limit_bytes
            )));
        }

        self.global_memory_bytes
            .fetch_add(batch_memory, Ordering::Relaxed);

        // Evict oldest partition if memory limit exceeded
        while self.global_memory_bytes.load(Ordering::Relaxed)
            > self.config.global_memory_limit_bytes
            && !self.writers.is_empty()
        {
            if let Some(oldest_key) = self.lru_order.front().cloned() {
                self.close_writer(&oldest_key)?;
            } else {
                break;
            }
        }

        // If writer already exists, check rolling triggers
        if let Some(active_writer) = self.writers.get_mut(&partition_id) {
            let should_roll = (self.config.max_records > 0
                && active_writer.writer.records_written() >= self.config.max_records)
                || (self.config.max_file_size_bytes > 0
                    && active_writer.writer.bytes_written() >= self.config.max_file_size_bytes)
                || (self.config.max_file_interval_sec > 0
                    && active_writer.opened_at.elapsed().as_secs()
                        >= self.config.max_file_interval_sec);

            if should_roll {
                self.close_writer(&partition_id)?;
            } else {
                // Update LRU order
                if let Some(pos) = self.lru_order.iter().position(|k| k == &partition_id) {
                    self.lru_order.remove(pos);
                }
                self.lru_order.push_back(partition_id.clone());

                active_writer.buffered_memory += batch_memory;
                active_writer.writer.write_batch(batch)?;

                if (self.config.max_records > 0
                    && active_writer.writer.records_written() >= self.config.max_records)
                    || (self.config.max_file_size_bytes > 0
                        && active_writer.writer.bytes_written() >= self.config.max_file_size_bytes)
                {
                    self.close_writer(&partition_id)?;
                }
                return Ok(());
            }
        }

        // Evict LRU writer if pool reached max_open_partitions
        while self.writers.len() >= self.config.max_open_partitions && !self.writers.is_empty() {
            if let Some(oldest_key) = self.lru_order.front().cloned() {
                self.close_writer(&oldest_key)?;
            } else {
                break;
            }
        }

        // Open new writer
        let seq = self.next_sequence();
        let filename = self.namer.generate_filename(&partition_id.path, seq);
        let (uploader_sender, uploader_handle) = AsyncUploader::start(&self.operator, &filename)?;
        let mut partition_writer =
            PartitionWriter::try_new(batch.schema(), uploader_sender, &self.config)?;

        partition_writer.write_batch(batch)?;

        let active = ActiveWriter {
            writer: partition_writer,
            uploader_handle,
            opened_at: Instant::now(),
            buffered_memory: batch_memory,
        };

        if (self.config.max_records > 0
            && active.writer.records_written() >= self.config.max_records)
            || (self.config.max_file_size_bytes > 0
                && active.writer.bytes_written() >= self.config.max_file_size_bytes)
        {
            self.retire_active_writer(active)?;
            self.in_flight_uploads.retain(|jh| !jh.is_finished());
            self.check_background_errors()?;
        } else {
            self.writers.insert(partition_id.clone(), active);
            self.lru_order.push_back(partition_id);
        }

        Ok(())
    }

    /// Retires an active partition writer, finalizes the Parquet file, and spawns
    /// a tracked background task to commit the upload with error reporting.
    fn retire_active_writer(&mut self, active: ActiveWriter) -> Result<(), ParquetSinkError> {
        self.global_memory_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(active.buffered_memory))
            })
            .ok();

        active.writer.close()?;

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ParquetSinkError::Internal(
                "No active Tokio runtime available to spawn background upload completion"
                    .to_string(),
            )
        })?;

        let err_sender = self.error_sender.clone();
        let jh = handle.spawn(async move {
            let res = active.uploader_handle.wait_for_completion().await;
            if let Err(ref e) = res {
                tracing::error!("Background Parquet upload failed: {e}");
                let _ = err_sender.send(ParquetSinkError::Internal(format!(
                    "Background upload failed: {e}"
                )));
            }
            res
        });
        self.in_flight_uploads.push(jh);
        Ok(())
    }

    /// Closes and finalizes an active partition writer cleanly.
    fn close_writer(&mut self, partition_id: &PartitionId) -> Result<(), ParquetSinkError> {
        if let Some(pos) = self.lru_order.iter().position(|k| k == partition_id) {
            self.lru_order.remove(pos);
        }

        if let Some(active) = self.writers.remove(partition_id) {
            self.retire_active_writer(active)?;
        }

        self.in_flight_uploads.retain(|jh| !jh.is_finished());
        self.check_background_errors()?;
        Ok(())
    }

    /// Checks for any asynchronous upload failures encountered by background tasks.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if any background upload task failed.
    pub fn check_background_errors(&mut self) -> Result<(), ParquetSinkError> {
        if let Ok(err) = self.error_receiver.try_recv() {
            return Err(err);
        }
        Ok(())
    }

    /// Increments and returns the next monotonic file sequence counter.
    fn next_sequence(&mut self) -> u16 {
        let seq = self.file_sequence;
        self.file_sequence = self.file_sequence.wrapping_add(1);
        seq
    }

    /// Sweeps active partition writers, closing any whose wall-clock duration
    /// since creation exceeds `config.max_file_interval_sec`.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if closing or committing any expired writer fails.
    pub fn sweep_idle_writers(&mut self) -> Result<(), ParquetSinkError> {
        self.check_background_errors()?;
        let max_interval = Duration::from_secs(self.config.max_file_interval_sec);
        let idle_keys: Vec<PartitionId> = self
            .writers
            .iter()
            .filter(|(_, w)| w.opened_at.elapsed() >= max_interval)
            .map(|(k, _)| k.clone())
            .collect();

        for key in idle_keys {
            self.close_writer(&key)?;
        }
        self.check_background_errors()?;
        Ok(())
    }

    /// Closes and commits all active partition writers.
    ///
    /// Intended for pipeline shutdown or graceful drain cycles.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if closing or committing any writer fails.
    pub fn flush_all(&mut self) -> Result<(), ParquetSinkError> {
        self.check_background_errors()?;
        let all_keys: Vec<PartitionId> = self.writers.keys().cloned().collect();
        for key in all_keys {
            self.close_writer(&key)?;
        }
        self.check_background_errors()?;
        Ok(())
    }

    /// Awaits completion of all background uploads spawned during file commit.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if any background upload task failed.
    pub async fn wait_for_all_uploads(&mut self) -> Result<(), ParquetSinkError> {
        let mut first_error = None;
        for jh in self.in_flight_uploads.drain(..) {
            if let Err(e) = jh
                .await
                .map_err(|e| ParquetSinkError::Internal(e.to_string()))
                .and_then(|r| r)
            {
                tracing::error!("Background upload failed during shutdown: {e}");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        self.check_background_errors()?;
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use opendal::Operator;
    use opendal::services::Memory;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_heterogeneous_signals_routed_to_separate_writers_without_schema_conflict() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // Logs schema
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
                Arc::new(StringArray::from(vec!["hello log"])),
            ],
        )
        .unwrap();

        // Metrics schema (completely different fields)
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
                Arc::new(Float64Array::from(vec![std::f64::consts::PI])),
            ],
        )
        .unwrap();

        // Both route into the same hour without schema conflict
        assert!(manager.route_batch(&logs_batch, "logs").is_ok());
        assert!(manager.route_batch(&metrics_batch, "metrics").is_ok());
        assert_eq!(manager.active_writer_count(), 2);
    }

    #[tokio::test]
    async fn test_partition_routing_handles_empty_and_null_timestamps() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        let empty_batch = RecordBatch::new_empty(schema.clone());
        assert!(manager.route_batch(&empty_batch, "logs").is_ok());
        assert_eq!(manager.active_writer_count(), 0);

        let batch_with_null = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    None,
                    Some(1_700_000_000_000_000_000),
                ])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        assert!(manager.route_batch(&batch_with_null, "logs").is_ok());
        assert!(manager.active_writer_count() >= 1);
    }

    #[tokio::test]
    async fn test_lru_and_memory_eviction_closes_oldest_writer() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_open_partitions: 2,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        // Partition 1: 2023-11-14 22:13:20 UTC
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        manager.route_batch(&b1, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        // Partition 2: hour 23 (1_700_000_000 + 3600s)
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 3_600_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        manager.route_batch(&b2, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 2);

        // Partition 3: hour 00 next day (1_700_000_000 + 7200s) -> should evict partition 1
        let b3 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 7_200_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![3])),
            ],
        )
        .unwrap();
        manager.route_batch(&b3, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 2);
    }

    #[tokio::test]
    async fn test_sweep_idle_writers_and_flush_all() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_file_interval_sec: 1,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        manager.route_batch(&b1, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        // Sleep to let writer become idle (> 1s)
        tokio::time::sleep(Duration::from_millis(1100)).await;

        manager.sweep_idle_writers().unwrap();
        assert_eq!(manager.active_writer_count(), 0);
    }

    #[tokio::test]
    async fn test_flush_all_closes_all_writers() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        manager.route_batch(&b1, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        manager.flush_all().unwrap();
        assert_eq!(manager.active_writer_count(), 0);
    }

    #[tokio::test]
    async fn test_vectorized_partition_routing_splits_multiple_hours() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        // Batch containing 3 rows spanning 2 distinct hours (hour 22 and hour 23)
        let t1 = 1_700_000_000_000_000_000;
        let t2 = 1_700_000_000_000_000_000 + 100_000_000;
        let t3 = 1_700_000_000_000_000_000 + 3_600_000_000_000;

        let multi_hour_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![t1, t2, t3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        manager.route_batch(&multi_hour_batch, "logs").unwrap();
        // Should have created 2 writers for the 2 distinct hours
        assert_eq!(manager.active_writer_count(), 2);
    }

    #[tokio::test]
    async fn test_rolling_trigger_max_records() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_records: 2,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        // Batch of 2 rows reaches max_records immediately
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        manager.route_batch(&b1, "logs").unwrap();
        // Because max_records was 2 and 2 were written, it rolled and closed!
        assert_eq!(manager.active_writer_count(), 0);

        // Next batch opens fresh writer
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![3])),
            ],
        )
        .unwrap();
        manager.route_batch(&b2, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);
    }

    #[tokio::test]
    async fn test_global_memory_limit_eviction() {
        let op = Operator::new(Memory::default()).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        let mem1 = b1.get_array_memory_size();
        let config = crate::config::ParquetSinkConfig {
            global_memory_limit_bytes: mem1 + 10, // allows b1, but b1 + b2 will exceed limit
            max_open_partitions: 10,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 3_600_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();

        manager.route_batch(&b1, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        manager.route_batch(&b2, "logs").unwrap();
        // Memory limit exceeded -> oldest writer evicted
        assert_eq!(manager.active_writer_count(), 1);
    }

    #[tokio::test]
    async fn test_route_prepared_batch_and_missing_timestamp() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // Schema without timestamp column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["fallback test"]))],
        )
        .unwrap();

        let prepared = PreparedBatch {
            signal: "traces",
            batch,
        };

        // Missing timestamp column falls back cleanly to Utc::now()
        assert!(manager.route_prepared_batch(&prepared).is_ok());
        assert_eq!(manager.active_writer_count(), 1);

        manager.flush_all().unwrap();
        assert_eq!(manager.active_writer_count(), 0);
        manager.wait_for_all_uploads().await.unwrap();
    }

    #[tokio::test]
    async fn test_background_upload_failure_propagates_to_manager() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // Directly simulate an async background upload failure
        manager
            .error_sender
            .send(ParquetSinkError::Internal(
                "Simulated S3 failure".to_string(),
            ))
            .unwrap();

        let err = manager.check_background_errors();
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("Simulated S3 failure")
        );
    }

    #[tokio::test]
    async fn test_wait_for_all_uploads_does_not_short_circuit() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // First task fails
        manager.in_flight_uploads.push(tokio::spawn(async {
            Err(ParquetSinkError::Internal("First failed".to_string()))
        }));

        // Second task simulates long-running and succeeds
        let (tx, rx) = tokio::sync::oneshot::channel();
        manager.in_flight_uploads.push(tokio::spawn(async move {
            rx.await.unwrap();
            Ok(())
        }));

        // Send completion to second task after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tx.send(()).unwrap();
        });

        // wait_for_all_uploads should wait for the second task and return the first error
        let err = manager.wait_for_all_uploads().await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("First failed"));

        // Assert queue is empty (drained)
        assert!(manager.in_flight_uploads.is_empty());
    }
}
