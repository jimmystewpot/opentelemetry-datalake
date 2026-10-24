use crate::buffer::config::SignalKind;
use crate::error::PipelineError;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn storage_err(msg: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> PipelineError {
    PipelineError::Storage(msg.into())
}

pub(crate) fn current_unix_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Metadata manifest persisted as a JSON sidecar alongside a quarantined `.arrow` IPC file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterMetadata {
    /// Name of the sink that rejected the lease.
    pub sink: String,
    /// Telemetry signal of the quarantined batches.
    pub signal: SignalKind,
    /// Inclusive first sequence number of the quarantined lease.
    pub first_seq: u64,
    /// Inclusive last sequence number of the quarantined lease.
    pub last_seq: u64,
    /// Total number of rows across all quarantined batches.
    pub row_count: usize,
    /// Total delivery attempts made before quarantine.
    pub attempts: u32,
    /// Diagnostic rejection reason reported by the sink.
    pub reason: String,
    /// Unix epoch timestamp in milliseconds when the batch was quarantined.
    pub timestamp_unix_ms: u64,
}

/// Type alias for [`DeadLetterMetadata`] representing the JSON manifest sidecar file.
pub type DeadLetterManifest = DeadLetterMetadata;

/// Returns `true` if `val` is a safe, single relative path component without path traversal,
/// separators, or null bytes.
#[must_use]
pub fn is_safe_path_component(val: &str) -> bool {
    if val.is_empty() || val.contains('/') || val.contains('\\') || val.contains('\0') {
        return false;
    }
    let mut components = Path::new(val).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

/// Bounded local filesystem store for quarantining rejected Arrow `RecordBatch`es as IPC files
/// alongside JSON metadata manifests.
#[derive(Debug)]
pub struct DeadLetterStore {
    dir: PathBuf,
    max_bytes: u64,
    current_bytes: AtomicU64,
    file_seq: AtomicU64,
}

impl DeadLetterStore {
    /// Creates or opens a [`DeadLetterStore`] rooted at `dir` with a maximum quota of `max_bytes`.
    pub fn new(dir: PathBuf, max_bytes: u64) -> Result<Self, PipelineError> {
        std::fs::create_dir_all(&dir).map_err(|e| {
            storage_err(format!(
                "failed to create dead-letter directory '{}': {e}",
                dir.display()
            ))
        })?;
        let existing_bytes = Self::scan_existing_bytes(&dir)?;
        Ok(Self {
            dir,
            max_bytes,
            current_bytes: AtomicU64::new(existing_bytes),
            file_seq: AtomicU64::new(1),
        })
    }

    /// Returns the root directory of this dead-letter store.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the maximum allowed byte quota for this dead-letter store.
    #[must_use]
    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Returns the current tracked byte usage of this dead-letter store.
    #[must_use]
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Acquire)
    }

    /// Writes `batches` to `<dir>/<sink>/<signal>_<first_seq>_<last_seq>_<timestamp_unix_ms>.arrow`
    /// and `meta` to the matching `.json` sidecar file, enforcing `max_bytes`.
    pub fn write_quarantine(
        &self,
        meta: &DeadLetterMetadata,
        batches: &[RecordBatch],
    ) -> Result<PathBuf, PipelineError> {
        if !is_safe_path_component(&meta.sink) {
            return Err(storage_err(format!(
                "dead-letter sink name '{}' contains unsafe path components",
                meta.sink
            )));
        }
        if batches.is_empty() {
            return Err(storage_err(
                "cannot quarantine empty RecordBatch slice to dead-letter store",
            ));
        }

        let mut arrow_bytes = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(
                &mut arrow_bytes,
                batches[0].schema().as_ref(),
            )
            .map_err(PipelineError::Arrow)?;
            for batch in batches {
                writer.write(batch).map_err(PipelineError::Arrow)?;
            }
            writer.finish().map_err(PipelineError::Arrow)?;
        }

        let json_bytes = serde_json::to_vec_pretty(meta)
            .map_err(|e| storage_err(format!("failed to serialize dead-letter manifest: {e}")))?;

        let added_bytes = (arrow_bytes.len() as u64).saturating_add(json_bytes.len() as u64);

        self.reserve_bytes(added_bytes)?;

        match self.persist_pair(meta, &arrow_bytes, &json_bytes) {
            Ok(arrow_path) => Ok(arrow_path),
            Err(err) => {
                self.current_bytes.fetch_sub(added_bytes, Ordering::AcqRel);
                Err(err)
            }
        }
    }

    fn reserve_bytes(&self, added_bytes: u64) -> Result<(), PipelineError> {
        let mut current = self.current_bytes.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(added_bytes);
            if next > self.max_bytes {
                return Err(storage_err(format!(
                    "dead-letter store quota exceeded in '{}': adding {added_bytes} bytes to {current} bytes exceeds max_dead_letter_bytes ({})",
                    self.dir.display(),
                    self.max_bytes
                )));
            }
            match self.current_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    fn persist_pair(
        &self,
        meta: &DeadLetterMetadata,
        arrow_bytes: &[u8],
        json_bytes: &[u8],
    ) -> Result<PathBuf, PipelineError> {
        let sink_dir = self.dir.join(&meta.sink);
        std::fs::create_dir_all(&sink_dir).map_err(|e| {
            storage_err(format!(
                "failed to create sink dead-letter directory '{}': {e}",
                sink_dir.display()
            ))
        })?;

        let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
        let base_stem = format!(
            "{}_{}_{}_{}",
            meta.signal.as_str(),
            meta.first_seq,
            meta.last_seq,
            meta.timestamp_unix_ms
        );
        let stem = if sink_dir.join(format!("{base_stem}.arrow")).exists() {
            format!("{base_stem}_{seq}")
        } else {
            base_stem
        };

        let arrow_path = sink_dir.join(format!("{stem}.arrow"));
        let json_path = sink_dir.join(format!("{stem}.json"));

        Self::write_file_atomic(&sink_dir, &arrow_path, arrow_bytes, seq)?;
        if let Err(err) = Self::write_file_atomic(&sink_dir, &json_path, json_bytes, seq) {
            let _ = std::fs::remove_file(&arrow_path);
            return Err(err);
        }
        Self::sync_dir(&sink_dir);
        Ok(arrow_path)
    }

    fn write_file_atomic(
        dir: &Path,
        final_path: &Path,
        bytes: &[u8],
        seq: u64,
    ) -> Result<(), PipelineError> {
        let file_name = final_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("dlq");
        let tmp_path = dir.join(format!(".tmp_{file_name}_{}_{seq}", std::process::id()));

        let mut tmp_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| {
                storage_err(format!(
                    "failed to create temporary dead-letter file '{}': {e}",
                    tmp_path.display()
                ))
            })?;

        if let Err(e) = tmp_file.write_all(bytes) {
            drop(tmp_file);
            let _ = std::fs::remove_file(&tmp_path);
            return Err(storage_err(format!(
                "failed to write temporary dead-letter file '{}': {e}",
                tmp_path.display()
            )));
        }

        if let Err(e) = tmp_file.sync_all() {
            drop(tmp_file);
            let _ = std::fs::remove_file(&tmp_path);
            return Err(storage_err(format!(
                "failed to sync temporary dead-letter file '{}': {e}",
                tmp_path.display()
            )));
        }
        drop(tmp_file);

        if let Err(e) = std::fs::rename(&tmp_path, final_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(storage_err(format!(
                "failed to rename '{}' to '{}': {e}",
                tmp_path.display(),
                final_path.display()
            )));
        }

        Ok(())
    }

    fn sync_dir(dir: &Path) {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_data();
        }
    }

    fn scan_existing_bytes(dir: &Path) -> Result<u64, PipelineError> {
        let mut total = 0u64;
        let entries = std::fs::read_dir(dir).map_err(|e| {
            storage_err(format!(
                "failed to scan dead-letter directory '{}': {e}",
                dir.display()
            ))
        })?;
        for entry_res in entries {
            let entry = entry_res.map_err(|e| {
                storage_err(format!("failed to read dead-letter directory entry: {e}"))
            })?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with(".tmp_")
            {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            let meta = entry.metadata().map_err(|e| {
                storage_err(format!(
                    "failed to stat dead-letter entry '{}': {e}",
                    path.display()
                ))
            })?;
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            } else if meta.is_dir() {
                total = total.saturating_add(Self::scan_subdir_bytes(&path)?);
            }
        }
        Ok(total)
    }

    fn scan_subdir_bytes(dir: &Path) -> Result<u64, PipelineError> {
        let mut total = 0u64;
        let entries = std::fs::read_dir(dir).map_err(|e| {
            storage_err(format!(
                "failed to scan sink dead-letter directory '{}': {e}",
                dir.display()
            ))
        })?;
        for entry_res in entries {
            let entry = entry_res
                .map_err(|e| storage_err(format!("failed to read sink dead-letter entry: {e}")))?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with(".tmp_")
            {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            let meta = entry.metadata().map_err(|e| {
                storage_err(format!(
                    "failed to stat sink dead-letter entry '{}': {e}",
                    path.display()
                ))
            })?;
            if meta.is_file() {
                total = total.saturating_add(meta.len());
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferConfig, BufferMode, FailureClass, SignalKind, SinkBuffer};
    use crate::pipeline::SignalBatch;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader;
    use std::fs::File;
    use std::sync::Arc;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        let arr = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    static TEST_DLQ_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempDlqDir {
        path: PathBuf,
    }

    impl TempDlqDir {
        fn new(prefix: &str) -> Self {
            let id = TEST_DLQ_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let path = std::env::temp_dir().join(format!("otel-dl-dlq-{prefix}-{pid}-{id}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDlqDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn test_rejected_nack_exhausts_max_attempts_and_writes_dead_letter_ipc_and_manifest() {
        let tmp_dir = TempDlqDir::new("rejected");
        let dlq_root = tmp_dir.path.join("dlq");
        let config = BufferConfig {
            max_attempts: 3,
            dead_letter_path: Some(dlq_root.clone()),
            max_dead_letter_bytes: 10 * 1024 * 1024,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("elasticsearch", config).unwrap();

        tx.send(SignalBatch::Logs(make_batch(&[10, 20, 30])))
            .await
            .unwrap();
        drop(tx);

        // Attempt 1 (attempt == 0 -> next_attempt == 1 < 3): requeues immediately
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1.attempt(), 0);
        assert_eq!(lease1.id().first_seq, 1);
        assert_eq!(lease1.id().last_seq, 1);
        lease1
            .nack(FailureClass::Rejected {
                reason: "mapper_parsing_exception".into(),
            })
            .await
            .unwrap();
        assert_eq!(buf.committed_seq(SignalKind::Logs), 0);

        // Attempt 2 (attempt == 1 -> next_attempt == 2 < 3): requeues immediately
        let lease2 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease2.attempt(), 1);
        lease2
            .nack(FailureClass::Rejected {
                reason: "mapper_parsing_exception".into(),
            })
            .await
            .unwrap();
        assert_eq!(buf.committed_seq(SignalKind::Logs), 0);

        // Attempt 3 (attempt == 2 -> next_attempt == 3 >= 3): exhausts max_attempts, writes DLQ, acks
        let lease3 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease3.attempt(), 2);
        lease3
            .nack(FailureClass::Rejected {
                reason: "mapper_parsing_exception".into(),
            })
            .await
            .unwrap();

        assert_eq!(buf.committed_seq(SignalKind::Logs), 1);
        assert_eq!(buf.dead_letter_drops(), 3);
        assert!(buf.next_lease().await.unwrap().is_none());

        // Verify <dead_letter_path>/elasticsearch/logs_1_1_<epoch_ms>.arrow and .json
        let sink_dlq_dir = dlq_root.join("elasticsearch");
        assert!(
            sink_dlq_dir.exists(),
            "Expected sink dead-letter directory {} to exist",
            sink_dlq_dir.display()
        );

        let mut arrow_files = Vec::new();
        let mut json_files = Vec::new();
        for entry in std::fs::read_dir(&sink_dlq_dir).unwrap() {
            let path = entry.unwrap().path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("arrow") => arrow_files.push(path),
                Some("json") => json_files.push(path),
                _ => {}
            }
        }
        assert_eq!(arrow_files.len(), 1, "Expected exactly 1 .arrow file");
        assert_eq!(json_files.len(), 1, "Expected exactly 1 .json file");

        let arrow_name = arrow_files[0].file_name().unwrap().to_str().unwrap();
        assert!(
            arrow_name.starts_with("logs_1_1_"),
            "Unexpected arrow filename: {arrow_name}"
        );

        // Read back the Arrow IPC file using FileReader and verify rows match original unmutated batch
        let arrow_file = File::open(&arrow_files[0]).unwrap();
        let reader = FileReader::try_new(arrow_file, None).unwrap();
        let recovered_batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(recovered_batches.len(), 1);
        assert_eq!(recovered_batches[0].num_rows(), 3);
        let col = recovered_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[10, 20, 30]);

        // Read back the JSON manifest and verify fields
        let json_bytes = std::fs::read(&json_files[0]).unwrap();
        let manifest: DeadLetterManifest = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(manifest.sink, "elasticsearch");
        assert_eq!(manifest.signal, SignalKind::Logs);
        assert_eq!(manifest.first_seq, 1);
        assert_eq!(manifest.last_seq, 1);
        assert_eq!(manifest.row_count, 3);
        assert_eq!(manifest.attempts, 3);
        assert_eq!(manifest.reason, "mapper_parsing_exception");
        assert!(manifest.timestamp_unix_ms > 0);
    }

    #[tokio::test]
    async fn test_rejected_lease_retries_up_to_max_attempts_then_writes_dlq_and_acks() {
        let tmp_dir = TempDlqDir::new("rejected_alias");
        let dlq_root = tmp_dir.path.join("dlq_alias");
        let config = BufferConfig {
            max_attempts: 3,
            dead_letter_path: Some(dlq_root.clone()),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("starrocks", config).unwrap();
        tx.send(SignalBatch::Logs(make_batch(&[42]))).await.unwrap();
        drop(tx);

        for expected_attempt in 0..3 {
            let lease = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease.attempt(), expected_attempt);
            lease
                .nack(FailureClass::Rejected {
                    reason: "too many filtered rows".into(),
                })
                .await
                .unwrap();
        }
        assert_eq!(buf.committed_seq(SignalKind::Logs), 1);
        assert!(buf.next_lease().await.unwrap().is_none());
        assert!(dlq_root.join("starrocks").exists());
    }

    #[test]
    fn test_dead_letter_store_enforces_max_dead_letter_bytes_quota() {
        let tmp_dir = TempDlqDir::new("quota");
        let dlq_dir = tmp_dir.path.join("dlq_quota");
        let store = DeadLetterStore::new(dlq_dir.clone(), 100).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let large_strings: Vec<String> = (0..32)
            .map(|i| format!("row-{i:04}-padding-data"))
            .collect();
        let arr = Arc::new(StringArray::from(large_strings));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        assert!(batch.get_array_memory_size() >= 500);

        let meta = DeadLetterMetadata {
            sink: "iceberg".into(),
            signal: SignalKind::Logs,
            first_seq: 1,
            last_seq: 1,
            row_count: batch.num_rows(),
            attempts: 3,
            reason: "schema mismatch".into(),
            timestamp_unix_ms: 1_700_000_000_000,
        };

        let err = store.write_quarantine(&meta, &[batch]).unwrap_err();
        assert!(matches!(err, PipelineError::Storage(_)));
        assert_eq!(store.current_bytes(), 0);

        let sink_dir = dlq_dir.join("iceberg");
        if sink_dir.exists() {
            let count = std::fs::read_dir(&sink_dir).unwrap().count();
            assert_eq!(
                count, 0,
                "No files should be written when quota is exceeded"
            );
        }
    }

    #[tokio::test]
    async fn test_sink_buffer_quarantine_batches_writes_slice_and_allows_lease_ack() {
        let tmp_dir = TempDlqDir::new("slice");
        let dlq_root = tmp_dir.path.join("dlq_slice");
        let config = BufferConfig {
            dead_letter_path: Some(dlq_root),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("elasticsearch", config).unwrap();
        let batch = make_batch(&[100, 200, 300, 400]);
        tx.send(SignalBatch::Logs(batch.clone())).await.unwrap();
        drop(tx);

        let lease = buf.next_lease().await.unwrap().unwrap();
        let rejected_slice = batch.slice(2, 1);
        let arrow_path = buf
            .quarantine_batches(
                lease.signal(),
                lease.id().first_seq,
                lease.id().last_seq,
                1,
                "mapper_parsing_exception".into(),
                vec![rejected_slice],
            )
            .await
            .unwrap()
            .expect("Expected dead-letter file path");
        assert!(arrow_path.exists());
        assert_eq!(buf.dead_letter_drops(), 1);

        lease.ack().await.unwrap();
        assert_eq!(buf.committed_seq(SignalKind::Logs), 1);
    }

    #[tokio::test]
    async fn test_rejected_nack_in_disk_mode_reads_frame_and_writes_dead_letter() {
        let tmp_dir = TempDlqDir::new("disk_rejected");
        let disk_dir = tmp_dir.path.join("disk");
        let dlq_root = tmp_dir.path.join("dlq");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(disk_dir),
            max_attempts: 2,
            dead_letter_path: Some(dlq_root.clone()),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("starrocks", config).unwrap();
        tx.send(SignalBatch::Logs(make_batch(&[7, 8])))
            .await
            .unwrap();
        drop(tx);

        // Attempt 1 requeues
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        lease1
            .nack(FailureClass::Rejected {
                reason: "filtered".into(),
            })
            .await
            .unwrap();

        // Attempt 2 quarantines on-disk payload to DLQ and acks
        let lease2 = buf.next_lease().await.unwrap().unwrap();
        lease2
            .nack(FailureClass::Rejected {
                reason: "filtered".into(),
            })
            .await
            .unwrap();

        assert_eq!(buf.committed_seq(SignalKind::Logs), 1);
        assert_eq!(buf.dead_letter_drops(), 2);
        assert!(buf.next_lease().await.unwrap().is_none());
        assert!(dlq_root.join("starrocks").exists());
    }

    #[tokio::test]
    async fn test_rejected_nack_returns_err_and_retains_lease_when_dead_letter_quota_exceeded() {
        let tmp_dir = TempDlqDir::new("nack_quota_err");
        let dlq_root = tmp_dir.path.join("dlq");
        let config = BufferConfig {
            max_attempts: 1,
            dead_letter_path: Some(dlq_root),
            max_dead_letter_bytes: 50, // Too small for any Arrow IPC file + JSON manifest
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("iceberg", config).unwrap();
        tx.send(SignalBatch::Logs(make_batch(&[1, 2, 3, 4])))
            .await
            .unwrap();

        let lease = buf.next_lease().await.unwrap().unwrap();
        let err = lease
            .nack(FailureClass::Rejected {
                reason: "schema mismatch".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, PipelineError::Storage(_)));
        assert_eq!(
            buf.committed_seq(SignalKind::Logs),
            0,
            "Watermark must not advance when dead-letter write fails"
        );
        assert_eq!(buf.dead_letter_drops(), 0);
    }
}
