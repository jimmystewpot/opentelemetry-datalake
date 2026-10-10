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
    upload_permit: Option<tokio::sync::OwnedSemaphorePermit>,
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
    use std::fmt::Write;

    // Fast path for canonical Hive template: "signal={signal}/date={date}/hour={hour}"
    if pattern == "signal={signal}/date={date}/hour={hour}" {
        let mut path = String::with_capacity(12 + signal.len() + 16 + 8);
        let _ = write!(
            path,
            "signal={signal}/date={year:04}-{month:02}-{day:02}/hour={hour:02}"
        );
        return path;
    }

    // General single-pass path: pre-allocated output buffer scanning tokens
    let mut result = String::with_capacity(pattern.len() + 32);
    let mut chars = pattern;
    while let Some(open) = chars.find('{') {
        result.push_str(&chars[..open]);
        let rest = &chars[open..];
        if let Some(close) = rest.find('}') {
            let token = &rest[1..close];
            match token {
                "signal" => result.push_str(signal),
                "date" => {
                    let _ = write!(result, "{year:04}-{month:02}-{day:02}");
                }
                "hour" => {
                    let _ = write!(result, "{hour:02}");
                }
                "year" => {
                    let _ = write!(result, "{year:04}");
                }
                "month" => {
                    let _ = write!(result, "{month:02}");
                }
                "day" => {
                    let _ = write!(result, "{day:02}");
                }
                _ => {
                    result.push_str(&rest[..=close]);
                }
            }
            chars = &rest[close + 1..];
        } else {
            result.push_str(rest);
            break;
        }
    }
    result.push_str(chars);
    result
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
    global_memory_bytes: Arc<AtomicUsize>,
    file_sequence: u16,
    in_flight_uploads: VecDeque<tokio::task::JoinHandle<Result<(), ParquetSinkError>>>,
    upload_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl PartitionManager {
    /// Creates a new `PartitionManager` with the specified configuration and storage operator.
    #[must_use]
    pub fn new(config: ParquetSinkConfig, operator: Operator) -> Self {
        let namer = FileNamer::new(config.node_id.clone());
        let upload_semaphore = if config.max_concurrent_uploads > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_uploads,
            )))
        } else {
            None
        };
        Self {
            config,
            operator,
            namer,
            writers: HashMap::new(),
            lru_order: VecDeque::new(),
            global_memory_bytes: Arc::new(AtomicUsize::new(0)),
            file_sequence: 0,
            in_flight_uploads: VecDeque::new(),
            upload_semaphore,
        }
    }

    /// Returns a clone of the shared global memory tracker.
    #[must_use]
    pub fn global_memory_tracker(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.global_memory_bytes)
    }

    /// Returns a clone of the shared upload concurrency limiter semaphore, if enabled.
    #[must_use]
    pub fn upload_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        self.upload_semaphore.as_ref().map(Arc::clone)
    }

    /// Shares aggregate memory tracking and upload concurrency limiters with another partition manager.
    pub fn share_limits_from(&mut self, other: &Self) {
        self.global_memory_bytes = Arc::clone(&other.global_memory_bytes);
        self.upload_semaphore = other.upload_semaphore.as_ref().map(Arc::clone);
    }

    /// Shares aggregate upload concurrency limiters with another partition manager,
    /// leaving memory tracking isolated per partition manager.
    pub fn share_upload_limits_from(&mut self, other: &Self) {
        self.upload_semaphore = other.upload_semaphore.as_ref().map(Arc::clone);
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
    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    fn route_sub_batch(
        &mut self,
        partition_id: PartitionId,
        batch: &RecordBatch,
    ) -> Result<(), ParquetSinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let total_rows = batch.num_rows();
        let total_batch_memory = batch.get_array_memory_size();

        if self.config.global_memory_limit_bytes > 0
            && total_batch_memory > self.config.global_memory_limit_bytes
        {
            return Err(ParquetSinkError::Config(format!(
                "RecordBatch size ({total_batch_memory} bytes) exceeds global memory limit ({} bytes)",
                self.config.global_memory_limit_bytes
            )));
        }

        // Evict oldest partition if adding this batch would exceed global memory limit
        while self.config.global_memory_limit_bytes > 0
            && self
                .global_memory_bytes
                .load(Ordering::Relaxed)
                .saturating_add(total_batch_memory)
                > self.config.global_memory_limit_bytes
            && !self.writers.is_empty()
        {
            let oldest_key = self.lru_order.front().cloned().ok_or_else(|| {
                ParquetSinkError::Internal("LRU order out of sync with active writers".to_string())
            })?;
            self.close_writer(&oldest_key)?;
        }

        let mut current_batch = batch.clone();

        while current_batch.num_rows() > 0 {
            let num_rows = current_batch.num_rows();
            let mut write_len = num_rows;

            if self.config.max_records > 0 {
                if let Some(active_writer) = self.writers.get(&partition_id) {
                    let written = active_writer.writer.records_written();
                    let remaining = self.config.max_records.saturating_sub(written);
                    if remaining == 0 {
                        self.close_writer(&partition_id)?;
                        continue;
                    }
                    if write_len > remaining {
                        write_len = remaining;
                    }
                } else if write_len > self.config.max_records {
                    write_len = self.config.max_records;
                }
            }

            let slice = if write_len == num_rows {
                current_batch.clone()
            } else {
                current_batch.slice(0, write_len)
            };

            let slice_memory = (total_batch_memory * write_len) / total_rows;

            if let Some(active_writer) = self.writers.get_mut(&partition_id) {
                let should_roll = (self.config.max_file_size_bytes > 0
                    && active_writer.writer.bytes_written() >= self.config.max_file_size_bytes)
                    || (self.config.max_file_interval_sec > 0
                        && active_writer.opened_at.elapsed().as_secs()
                            >= self.config.max_file_interval_sec);

                if should_roll {
                    self.close_writer(&partition_id)?;
                    continue;
                }

                if let Some(pos) = self.lru_order.iter().position(|k| k == &partition_id) {
                    self.lru_order.remove(pos);
                }
                self.lru_order.push_back(partition_id.clone());

                active_writer.writer.write_batch(&slice)?;
                active_writer.buffered_memory += slice_memory;
                self.global_memory_bytes
                    .fetch_add(slice_memory, Ordering::Relaxed);

                if (self.config.max_records > 0
                    && active_writer.writer.records_written() >= self.config.max_records)
                    || (self.config.max_file_size_bytes > 0
                        && active_writer.writer.bytes_written() >= self.config.max_file_size_bytes)
                {
                    self.close_writer(&partition_id)?;
                }
            } else {
                let effective_max_partitions = if self.config.max_concurrent_uploads > 0 {
                    self.config
                        .max_open_partitions
                        .min(self.config.max_concurrent_uploads)
                } else {
                    self.config.max_open_partitions
                };

                while self.writers.len() >= effective_max_partitions && !self.writers.is_empty() {
                    let oldest_key = self.lru_order.front().cloned().ok_or_else(|| {
                        ParquetSinkError::Internal(
                            "LRU order out of sync with active writers".to_string(),
                        )
                    })?;
                    self.close_writer(&oldest_key)?;
                }

                while self.config.max_concurrent_uploads > 0
                    && (self.writers.len() + self.in_flight_uploads.len())
                        >= self.config.max_concurrent_uploads
                {
                    if !self.in_flight_uploads.is_empty() {
                        self.wait_oldest_in_flight_upload()?;
                    } else if !self.writers.is_empty() {
                        let oldest_key = self.lru_order.front().cloned().ok_or_else(|| {
                            ParquetSinkError::Internal(
                                "LRU order out of sync with active writers".to_string(),
                            )
                        })?;
                        self.close_writer(&oldest_key)?;
                    } else {
                        break;
                    }
                }

                let permit = if let Some(sem) = self.upload_semaphore.as_ref().map(Arc::clone) {
                    loop {
                        match sem.clone().try_acquire_owned() {
                            Ok(p) => break Some(p),
                            Err(_) => {
                                if !self.in_flight_uploads.is_empty() {
                                    self.wait_oldest_in_flight_upload()?;
                                } else if !self.writers.is_empty() {
                                    let oldest_key =
                                        self.lru_order.front().cloned().ok_or_else(|| {
                                            ParquetSinkError::Internal(
                                                "LRU order out of sync with active writers"
                                                    .to_string(),
                                            )
                                        })?;
                                    self.close_writer(&oldest_key)?;
                                } else {
                                    let handle =
                                        tokio::runtime::Handle::try_current().map_err(|_| {
                                            ParquetSinkError::Internal(
                                                "No active Tokio runtime available to wait for upload permit"
                                                    .to_string(),
                                            )
                                        })?;
                                    if handle.runtime_flavor()
                                        != tokio::runtime::RuntimeFlavor::MultiThread
                                    {
                                        return Err(ParquetSinkError::Internal(
                                            "ParquetSink requires a multi-threaded Tokio runtime to wait for upload permits"
                                                .to_string(),
                                        ));
                                    }
                                    let sem_clone = Arc::clone(&sem);
                                    let p = tokio::task::block_in_place(|| {
                                        handle.block_on(sem_clone.acquire_owned())
                                    })
                                    .map_err(|e| {
                                        ParquetSinkError::Internal(format!("Semaphore closed: {e}"))
                                    })?;
                                    break Some(p);
                                }
                            }
                        }
                    }
                } else {
                    None
                };

                let seq = self.next_sequence();
                let filename = self.namer.generate_filename(&partition_id.path, seq);
                let (uploader_sender, uploader_handle) =
                    AsyncUploader::start(&self.operator, &filename)?;
                let mut partition_writer =
                    PartitionWriter::try_new(slice.schema(), uploader_sender, &self.config)?;

                partition_writer.write_batch(&slice)?;

                let active = ActiveWriter {
                    writer: partition_writer,
                    uploader_handle,
                    opened_at: Instant::now(),
                    buffered_memory: slice_memory,
                    upload_permit: permit,
                };
                self.global_memory_bytes
                    .fetch_add(slice_memory, Ordering::Relaxed);

                if (self.config.max_records > 0
                    && active.writer.records_written() >= self.config.max_records)
                    || (self.config.max_file_size_bytes > 0
                        && active.writer.bytes_written() >= self.config.max_file_size_bytes)
                {
                    self.retire_active_writer(active)?;
                    self.check_background_errors()?;
                } else {
                    self.writers.insert(partition_id.clone(), active);
                    self.lru_order.push_back(partition_id.clone());
                }
            }

            if write_len == num_rows {
                break;
            }
            current_batch = current_batch.slice(write_len, num_rows - write_len);
        }

        Ok(())
    }

    /// Waits for the oldest in-flight background upload task to finish and verifies its result.
    fn wait_oldest_in_flight_upload(&mut self) -> Result<(), ParquetSinkError> {
        let Some(oldest) = self.in_flight_uploads.pop_front() else {
            return Ok(());
        };

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ParquetSinkError::Internal(
                "No active Tokio runtime available to wait for background upload".to_string(),
            )
        })?;
        if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(ParquetSinkError::Internal(
                "ParquetSink requires a multi-threaded Tokio runtime to wait for pending uploads"
                    .to_string(),
            ));
        }
        let res = tokio::task::block_in_place(|| handle.block_on(oldest));
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(ParquetSinkError::Internal(format!(
                    "Background upload task panicked: {e}"
                )));
            }
        }
        self.check_background_errors()?;
        Ok(())
    }

    fn retire_active_writer(&mut self, active: ActiveWriter) -> Result<(), ParquetSinkError> {
        self.global_memory_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(active.buffered_memory))
            })
            .ok();

        let mut wait_err = None;
        while self.config.max_concurrent_uploads > 0
            && self.in_flight_uploads.len() >= self.config.max_concurrent_uploads
        {
            if let Err(e) = self.wait_oldest_in_flight_upload()
                && wait_err.is_none()
            {
                wait_err = Some(e);
            }
        }

        let close_res = active.writer.close();

        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ParquetSinkError::Internal(
                "No active Tokio runtime available to spawn background upload completion"
                    .to_string(),
            )
        })?;

        let permit = active.upload_permit;
        let jh = handle.spawn(async move {
            let _permit = permit;
            let res = active.uploader_handle.wait_for_completion().await;
            if let Err(ref e) = res {
                tracing::error!("Background Parquet upload failed: {e}");
            }
            res
        });
        self.in_flight_uploads.push_back(jh);

        close_res?;
        if let Some(err) = wait_err {
            return Err(err);
        }
        self.check_background_errors()?;
        Ok(())
    }

    /// Closes and finalizes an active partition writer cleanly.
    fn close_writer(&mut self, partition_id: &PartitionId) -> Result<(), ParquetSinkError> {
        if let Some(pos) = self.lru_order.iter().position(|k| k == partition_id) {
            self.lru_order.remove(pos);
        }

        let mut retire_res = Ok(());
        if let Some(active) = self.writers.remove(partition_id) {
            retire_res = self.retire_active_writer(active);
        }

        let bg_res = self.check_background_errors();
        retire_res.and(bg_res)
    }

    /// Checks for any asynchronous upload failures encountered by background tasks.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if any background upload task failed.
    pub fn check_background_errors(&mut self) -> Result<(), ParquetSinkError> {
        let mut i = 0;
        let mut first_error = None;
        while i < self.in_flight_uploads.len() {
            if self.in_flight_uploads[i].is_finished() {
                let Some(jh) = self.in_flight_uploads.remove(i) else {
                    continue;
                };
                let res =
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(jh));
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                    Err(e) => {
                        if first_error.is_none() {
                            first_error = Some(ParquetSinkError::Internal(format!(
                                "Background upload task panicked: {e}"
                            )));
                        }
                    }
                }
            } else {
                i += 1;
            }
        }

        if let Some(err) = first_error {
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
        let mut first_error = self.check_background_errors().err();
        if self.config.max_file_interval_sec == 0 {
            if let Some(err) = first_error {
                return Err(err);
            }
            return Ok(());
        }
        let max_interval = Duration::from_secs(self.config.max_file_interval_sec);
        let idle_keys: Vec<PartitionId> = self
            .writers
            .iter()
            .filter(|(_, w)| w.opened_at.elapsed() >= max_interval)
            .map(|(k, _)| k.clone())
            .collect();

        for key in idle_keys {
            if let Err(e) = self.close_writer(&key) {
                tracing::error!("Error closing idle partition writer: {e}");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if let Err(e) = self.check_background_errors()
            && first_error.is_none()
        {
            first_error = Some(e);
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    /// Closes and commits all active partition writers.
    ///
    /// Intended for pipeline shutdown or graceful drain cycles.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError`] if closing or committing any writer fails.
    pub fn flush_all(&mut self) -> Result<(), ParquetSinkError> {
        let mut first_error = self.check_background_errors().err();
        let all_keys: Vec<PartitionId> = self.writers.keys().cloned().collect();
        for key in all_keys {
            if let Err(e) = self.close_writer(&key) {
                tracing::error!("Error closing partition writer during flush_all: {e}");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if let Err(e) = self.check_background_errors()
            && first_error.is_none()
        {
            first_error = Some(e);
        }
        if let Some(err) = first_error {
            return Err(err);
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_background_upload_failure_propagates_to_manager() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // Directly simulate an async background upload failure
        let jh = tokio::spawn(async {
            Err(ParquetSinkError::Internal(
                "Simulated S3 failure".to_string(),
            ))
        });
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            while !jh.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        manager.in_flight_uploads.push_back(jh);

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
        manager.in_flight_uploads.push_back(tokio::spawn(async {
            Err(ParquetSinkError::Internal("First failed".to_string()))
        }));

        // Second task simulates long-running and succeeds
        let (tx, rx) = tokio::sync::oneshot::channel();
        manager
            .in_flight_uploads
            .push_back(tokio::spawn(async move {
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

    #[tokio::test]
    async fn test_zero_memory_limit_does_not_evict_all_writers() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            global_memory_limit_bytes: 0, // 0 means disabled
            max_open_partitions: 10,
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

        // Write a large batch
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
        manager.route_batch(&b2, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 2); // Still 2, no eviction happened due to memory
    }

    #[tokio::test]
    async fn test_zero_file_interval_disables_sweep() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_file_interval_sec: 0,
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

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        manager.sweep_idle_writers().unwrap();
        assert_eq!(manager.active_writer_count(), 1); // 0 means sweep disabled
    }

    #[tokio::test]
    async fn test_large_batch_is_sliced_across_multiple_files_on_max_records() {
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

        // Batch of 5 rows
        let b1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        manager.route_batch(&b1, "logs").unwrap();

        // 5 rows with max_records=2 means:
        // writer1 gets 2 rows (then closes)
        // writer2 gets 2 rows (then closes)
        // writer3 gets 1 row (stays open)
        assert_eq!(manager.active_writer_count(), 1);

        // The background tasks should be spawned for the 2 closed writers
        assert_eq!(manager.in_flight_uploads.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_in_flight_uploads_applies_backpressure_at_limit() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_concurrent_uploads: 1, // Only 1 background upload allowed
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

        let (tx, rx) = tokio::sync::oneshot::channel();
        manager
            .in_flight_uploads
            .push_back(tokio::spawn(async move {
                let _ = rx.await;
                Ok(())
            }));

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

        // Release the blocking in-flight task after a short delay so route_batch can acquire upload slot
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let _ = tx.send(());
        });

        manager.route_batch(&b1, "logs").unwrap();

        // flush_all should now gracefully wait for the slot and succeed without dropping files
        let res = manager.flush_all();
        assert!(
            res.is_ok(),
            "flush_all should wait for slot and succeed: {:?}",
            res.err()
        );
        assert_eq!(manager.in_flight_uploads.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_check_background_errors_detects_task_panic() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let panicked_jh = tokio::spawn(async {
            panic!("fatal background worker panic");
        });

        let _ = tokio::time::timeout(tokio::time::Duration::from_millis(2000), async {
            while !panicked_jh.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(panicked_jh.is_finished());

        manager.in_flight_uploads.push_back(panicked_jh);

        let res = manager.check_background_errors();
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("panicked"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_max_concurrent_uploads_strictly_bounds_active_plus_in_flight_uploads() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_open_partitions: 8,
            max_concurrent_uploads: 2,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));

        // Route batches to 4 different hours
        for h in 0..4 {
            let ts = 1_700_000_000_000_000_000 + i64::from(h) * 3600 * 1_000_000_000;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![ts])),
                    Arc::new(StringArray::from(vec![format!("msg-{h}").as_str()])),
                ],
            )
            .unwrap();

            manager.route_batch(&batch, "logs").unwrap();

            // At every step, active_writer_count + in_flight_uploads.len() MUST NOT exceed max_concurrent_uploads (2)
            let total_uploads = manager.active_writer_count() + manager.in_flight_uploads.len();
            assert!(
                total_uploads <= 2,
                "Total concurrent uploads {total_uploads} exceeded limit 2 at iteration {h}"
            );
        }

        manager.flush_all().unwrap();
        manager.wait_for_all_uploads().await.unwrap();
        assert_eq!(manager.active_writer_count(), 0);
        assert_eq!(manager.in_flight_uploads.len(), 0);
    }

    #[tokio::test]
    async fn test_current_memory_bytes_and_active_writer_count_accessors() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        assert_eq!(manager.active_writer_count(), 0);
        assert_eq!(manager.current_memory_bytes(), 0);

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
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();

        manager.route_batch(&batch, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);
        assert!(manager.current_memory_bytes() > 0);

        manager.flush_all().unwrap();
        assert_eq!(manager.active_writer_count(), 0);
        assert_eq!(manager.current_memory_bytes(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_all_uploads_reports_task_panic() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let panicked_jh = tokio::spawn(async {
            panic!("fatal worker panic during shutdown");
        });

        let _ = tokio::time::timeout(tokio::time::Duration::from_millis(2000), async {
            while !panicked_jh.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await;

        manager.in_flight_uploads.push_back(panicked_jh);

        let res = manager.wait_for_all_uploads().await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("panicked"));
    }

    #[test]
    fn test_format_partition_path_custom_tokens() {
        let pattern = "telemetry/{signal}/{year}-{month}-{day}/h_{hour}/{literal}";
        let path = format_partition_path(pattern, "traces", 2026, 10, 9, 14);
        assert_eq!(path, "telemetry/traces/2026-10-09/h_14/{literal}");

        let date_pattern = "{signal}/date={date}/hour={hour}";
        let date_path = format_partition_path(date_pattern, "logs", 2026, 5, 4, 8);
        assert_eq!(date_path, "logs/date=2026-05-04/hour=08");
    }

    #[tokio::test]
    async fn test_route_batch_with_heterogeneous_timestamps_in_single_batch() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));

        // Single batch with 3 rows spanning 3 different hours
        let ts_base = 1_700_000_000_000_000_000;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    ts_base,
                    ts_base + 3600 * 1_000_000_000,
                    ts_base + 7200 * 1_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["h0", "h1", "h2"])),
            ],
        )
        .unwrap();

        manager.route_batch(&batch, "logs").unwrap();
        // 3 separate partitions must be opened for the 3 hours
        assert_eq!(manager.active_writer_count(), 3);

        manager.flush_all().unwrap();
        manager.wait_for_all_uploads().await.unwrap();
        assert_eq!(manager.active_writer_count(), 0);
    }

    #[tokio::test]
    async fn test_route_batch_with_int64_and_null_timestamp_columns() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        // 1. Int64 timestamp column
        let schema_int64 = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let batch_int64 = RecordBatch::try_new(
            schema_int64,
            vec![
                Arc::new(Int64Array::from(vec![1_700_000_000_000_000_000])),
                Arc::new(StringArray::from(vec!["int64-ts"])),
            ],
        )
        .unwrap();
        assert!(manager.route_batch(&batch_int64, "traces").is_ok());

        // 2. Null timestamp values
        let schema_null = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let batch_null = RecordBatch::try_new(
            schema_null,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![None])),
                Arc::new(StringArray::from(vec!["null-ts"])),
            ],
        )
        .unwrap();
        assert!(manager.route_batch(&batch_null, "metrics").is_ok());

        // 3. Missing timestamp column altogether
        let schema_missing = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let batch_missing = RecordBatch::try_new(
            schema_missing,
            vec![Arc::new(StringArray::from(vec!["no-ts"]))],
        )
        .unwrap();
        assert!(manager.route_batch(&batch_missing, "logs").is_ok());

        manager.flush_all().unwrap();
        manager.wait_for_all_uploads().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_retire_active_writer_finalizes_when_previous_upload_failed() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op.clone());

        // Simulate an earlier background upload failure
        let failed_jh = tokio::spawn(async {
            Err(ParquetSinkError::Internal(
                "Simulated earlier upload failure".to_string(),
            ))
        });
        while !failed_jh.is_finished() {
            tokio::task::yield_now().await;
        }
        manager.in_flight_uploads.push_back(failed_jh);

        // Open an active writer and write a batch
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
                ])),
                Arc::new(StringArray::from(vec!["safe_record"])),
            ],
        )
        .unwrap();

        // Write batch - writer is opened
        let pid = PartitionId::new("logs", "signal=logs/date=2023-11-14/hour=22".to_string());
        manager.route_sub_batch(pid.clone(), &batch).unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        // Closing writer reports the background error, but MUST finalize and upload the active writer's file!
        let close_res = manager.close_writer(&pid);
        assert!(close_res.is_err());
        assert_eq!(manager.active_writer_count(), 0);

        // Await all uploads (will report the first error)
        let _ = manager.wait_for_all_uploads().await;

        // The safe record file MUST exist in storage and not be aborted
        let entries = op.list_with("signal=logs/").recursive(true).await.unwrap();
        assert!(
            !entries.is_empty(),
            "Healthy active writer must be finalized to storage!"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_all_finalizes_active_writers_when_previous_upload_failed() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op.clone());

        // Simulate an earlier background upload failure
        let failed_jh = tokio::spawn(async {
            Err(ParquetSinkError::Internal(
                "Simulated earlier upload failure".to_string(),
            ))
        });
        while !failed_jh.is_finished() {
            tokio::task::yield_now().await;
        }
        manager.in_flight_uploads.push_back(failed_jh);

        // Open two active writers across different partitions
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["safe_record_1"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_003_600_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["safe_record_2"])),
            ],
        )
        .unwrap();

        let pid1 = PartitionId::new("logs", "signal=logs/date=2023-11-14/hour=22".to_string());
        let pid2 = PartitionId::new("logs", "signal=logs/date=2023-11-14/hour=23".to_string());
        manager.route_sub_batch(pid1, &batch1).unwrap();
        manager.route_sub_batch(pid2, &batch2).unwrap();
        assert_eq!(manager.active_writer_count(), 2);

        // flush_all must report the background error, but MUST finalize and upload both active writers!
        let flush_res = manager.flush_all();
        assert!(flush_res.is_err());
        assert_eq!(manager.active_writer_count(), 0);

        // Await all uploads (will report the first error)
        let _ = manager.wait_for_all_uploads().await;

        // Files from both writers MUST exist in storage
        let entries = op.list_with("signal=logs/").recursive(true).await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "Both active writers must be finalized to storage!"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_permit_strictly_enforced_when_max_concurrent_uploads_configured() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_open_partitions: 4,
            max_concurrent_uploads: 2,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));

        // Route to partition 1
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["msg1"])),
            ],
        )
        .unwrap();
        manager.route_batch(&b1, "logs").unwrap();

        // Route to partition 2
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 3600 * 1_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["msg2"])),
            ],
        )
        .unwrap();
        manager.route_batch(&b2, "logs").unwrap();

        assert_eq!(manager.active_writer_count(), 2);
        // Verify every active writer has acquired an upload permit
        for (pid, writer) in &manager.writers {
            assert!(
                writer.upload_permit.is_some(),
                "Active writer for partition {pid} must hold an upload permit"
            );
        }

        // Route to partition 3, which must evict an active writer because max_concurrent_uploads is 2
        let b3 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 7200 * 1_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["msg3"])),
            ],
        )
        .unwrap();
        manager.route_batch(&b3, "logs").unwrap();

        // All active writers still strictly hold permits
        for (pid, writer) in &manager.writers {
            assert!(
                writer.upload_permit.is_some(),
                "Active writer for partition {pid} after eviction must hold an upload permit"
            );
        }

        manager.flush_all().unwrap();
        manager.wait_for_all_uploads().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_upload_queue_pop_and_drain_fifo_order() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig::default();
        let mut manager = PartitionManager::new(config, op);

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut senders = Vec::new();

        for i in 0..5 {
            let order_clone = Arc::clone(&order);
            let (tx, rx) = tokio::sync::oneshot::channel();
            senders.push(tx);
            manager
                .in_flight_uploads
                .push_back(tokio::spawn(async move {
                    let _ = rx.await;
                    order_clone.lock().unwrap().push(i);
                    Ok(())
                }));
        }

        assert_eq!(manager.in_flight_uploads.len(), 5);

        // Release the first task and wait oldest
        let _ = senders.remove(0).send(());
        manager.wait_oldest_in_flight_upload().unwrap();
        assert_eq!(manager.in_flight_uploads.len(), 4);

        // The first task (0) was waited on and completed
        {
            let guard = order.lock().unwrap();
            assert_eq!(guard.len(), 1);
            assert_eq!(guard[0], 0);
        }

        // Release remaining tasks
        for tx in senders {
            let _ = tx.send(());
        }

        // Drain the rest via wait_for_all_uploads
        manager.wait_for_all_uploads().await.unwrap();
        assert!(manager.in_flight_uploads.is_empty());
        assert_eq!(order.lock().unwrap().len(), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lru_out_of_sync_returns_internal_error() {
        let op = Operator::new(Memory::default()).unwrap();
        let config = crate::config::ParquetSinkConfig {
            max_open_partitions: 2,
            max_concurrent_uploads: 1,
            ..Default::default()
        };
        let mut manager = PartitionManager::new(config, op);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("msg", DataType::Utf8, false),
        ]));

        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["msg1"])),
            ],
        )
        .unwrap();
        manager.route_batch(&b1, "logs").unwrap();
        assert_eq!(manager.active_writer_count(), 1);

        // Artificially desynchronize lru_order while writers is non-empty
        manager.lru_order.clear();

        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000 + 3600 * 1_000_000_000,
                ])),
                Arc::new(StringArray::from(vec!["msg2"])),
            ],
        )
        .unwrap();

        // Routing should fail with Internal error reporting LRU order out of sync
        let res = manager.route_batch(&b2, "logs");
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("LRU order out of sync with active writers"),
            "Unexpected error: {err_msg}"
        );
    }
}
