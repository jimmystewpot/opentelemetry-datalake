use crate::buffer::BufferInner;
use crate::buffer::config::SignalKind;
use crate::buffer::dead_letter::{DeadLetterMetadata, current_unix_ms};
use crate::buffer::memory::{BatchPayload, LeasedBatchMeta};
use crate::error::PipelineError;
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Unique identifier for a leased range of batches within a signal ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseId {
    /// Durable or per-process buffer instance identifier scoping sequence numbers.
    pub instance_id: u64,
    /// Telemetry signal type of the leased batches.
    pub signal: SignalKind,
    /// Inclusive first sequence number in the lease.
    pub first_seq: u64,
    /// Inclusive last sequence number in the lease.
    pub last_seq: u64,
}

/// Sink-reported failure classification when nacking a [`Lease`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    /// Retriable infrastructure, network, rate-limit, or catalog conflict error.
    Transient,
    /// Data-specific rejection (e.g. HTTP 400, schema mismatch, oversized payload).
    Rejected {
        /// Human-readable diagnostic reason for the rejection.
        reason: String,
    },
    /// Unrecoverable sink or configuration fault (e.g. HTTP 401/403 auth failure).
    Fatal {
        /// Human-readable diagnostic reason for the fatal error.
        reason: String,
    },
}

/// Transactional handle to one or more coalesced `RecordBatch`es leased from a [`crate::buffer::SinkBuffer`].
///
/// Dropping a `Lease` without calling [`Lease::ack`] or [`Lease::nack`] synchronously
/// requeues its batches at the head of the signal ring with `attempt + 1`.
#[derive(Debug)]
pub struct Lease {
    pub(crate) inner: Arc<BufferInner>,
    pub(crate) id: LeaseId,
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) payloads: Vec<BatchPayload>,
    pub(crate) meta: Vec<LeasedBatchMeta>,
    pub(crate) num_rows: usize,
    pub(crate) byte_size: usize,
    pub(crate) attempt: u32,
    pub(crate) leased_at: Instant,
    pub(crate) completed: bool,
}

impl Lease {
    /// Returns the [`LeaseId`] identifying the signal and sequence range of this lease.
    #[must_use]
    pub const fn id(&self) -> LeaseId {
        self.id
    }

    /// Returns the [`SignalKind`] of this lease.
    #[must_use]
    pub const fn signal(&self) -> SignalKind {
        self.id.signal
    }

    /// Returns a slice of the in-memory `RecordBatch`es belonging to this lease.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns the total row count across all batches in this lease.
    #[must_use]
    pub const fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the total byte size charged for this lease.
    #[must_use]
    pub const fn byte_size(&self) -> usize {
        self.byte_size
    }

    /// Returns the delivery attempt counter (`0` on first lease, incremented on each retry).
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Offloads on-disk frame decoding (when in `Disk` mode), batch concatenation (bypassed
    /// when the lease holds a single batch), and sink-specific preparation work `f` onto a
    /// single `tokio::task::spawn_blocking` call.
    pub async fn prepare<F, T>(&self, f: F) -> Result<T, PipelineError>
    where
        F: FnOnce(RecordBatch) -> Result<T, PipelineError> + Send + 'static,
        T: Send + 'static,
    {
        if self.payloads.is_empty() {
            return Err(PipelineError::Internal(
                "Lease::prepare invoked on empty lease".into(),
            ));
        }
        let payloads = self.payloads.clone();
        let disk_path = self.inner.config.disk_path.clone();
        let max_payload_len = self.inner.config.max_disk_frame_payload_bytes();
        tokio::task::spawn_blocking(move || {
            let mut decoded = Vec::with_capacity(payloads.len());
            for payload in payloads {
                match payload {
                    BatchPayload::InMemory(batch) => decoded.push(batch),
                    BatchPayload::OnDisk {
                        segment_id,
                        offset,
                        len,
                        ..
                    } => {
                        let base_dir = disk_path.as_ref().ok_or_else(|| {
                            PipelineError::Storage(
                                "OnDisk payload encountered without configured disk_path".into(),
                            )
                        })?;
                        let batch = crate::buffer::disk::read_frame_at(
                            base_dir,
                            segment_id,
                            offset,
                            len,
                            max_payload_len,
                        )?;
                        decoded.push(batch);
                    }
                }
            }
            let combined = if decoded.len() == 1 {
                decoded
                    .into_iter()
                    .next()
                    .ok_or_else(|| PipelineError::Internal("empty lease batches".into()))?
            } else {
                let schema = decoded[0].schema();
                arrow::compute::concat_batches(&schema, &decoded).map_err(PipelineError::Arrow)?
            };
            f(combined)
        })
        .await
        .map_err(|e| PipelineError::Internal(format!("Lease::prepare join error: {e}")))?
    }

    /// Quarantines a rejected subset of batches belonging to this lease (e.g. item-level HTTP 400
    /// rows) to the configured [`crate::buffer::DeadLetterStore`] inside `tokio::task::spawn_blocking`.
    pub async fn quarantine_batches(
        &self,
        reason: String,
        batches: Vec<RecordBatch>,
    ) -> Result<Option<PathBuf>, PipelineError> {
        self.inner
            .quarantine_batches(
                self.id.signal,
                self.id.first_seq,
                self.id.last_seq,
                self.attempt.saturating_add(1),
                reason,
                batches,
            )
            .await
    }

    /// Commits the lease, freeing its byte budget and advancing the contiguous watermark.
    pub async fn ack(mut self) -> Result<(), PipelineError> {
        self.completed = true;
        let flush_duration = self.leased_at.elapsed();
        let acked_rows = self
            .inner
            .ack_lease(
                self.id.signal,
                &self.meta,
                self.byte_size,
                self.num_rows,
                true,
            )
            .await?;
        if acked_rows > 0 {
            self.inner.metrics.record_committed(
                self.id.signal,
                u64::try_from(acked_rows).unwrap_or(u64::MAX),
                flush_duration,
            );
        }
        Ok(())
    }

    /// Rejects the lease with the given [`FailureClass`], either scheduling a backoff retry,
    /// quarantining after `max_attempts`, or failing the buffer on `Fatal`.
    pub async fn nack(mut self, class: FailureClass) -> Result<(), PipelineError> {
        let signal = self.id.signal;
        let next_attempt = self.attempt.saturating_add(1);

        match class {
            FailureClass::Transient => {
                self.completed = true;
                self.batches.clear();
                let payloads = std::mem::take(&mut self.payloads);
                let mut meta = std::mem::take(&mut self.meta);
                self.inner.metrics.record_nack(signal, "transient");
                let now = Instant::now();
                let now_ms = current_unix_ms().max(1);
                let ring_state = &self.inner.rings[signal.as_u8() as usize];
                let _ = ring_state.first_transient_failure_unix_ms.compare_exchange(
                    0,
                    now_ms,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                for item in &mut meta {
                    if item.first_failed_at.is_none() {
                        item.first_failed_at = Some(now);
                    }
                }

                if self.inner.config.max_transient_duration_sec > 0 {
                    let max_dur = Duration::from_secs(self.inner.config.max_transient_duration_sec);
                    let first_fail_ms = ring_state
                        .first_transient_failure_unix_ms
                        .load(Ordering::Acquire);
                    let streak_expired = first_fail_ms > 0
                        && now_ms.saturating_sub(first_fail_ms)
                            >= self
                                .inner
                                .config
                                .max_transient_duration_sec
                                .saturating_mul(1_000);
                    let item_expired = meta.iter().any(|m| {
                        m.first_failed_at
                            .is_some_and(|t| now.saturating_duration_since(t) >= max_dur)
                    });
                    if streak_expired || item_expired {
                        let reason = format!(
                            "transient retry duration exceeded {}s on sink '{}'",
                            self.inner.config.max_transient_duration_sec, self.inner.sink_name
                        );
                        self.inner.fail_lease(signal, payloads, meta, reason);
                        return Ok(());
                    }
                }

                let shift = self.attempt.min(16);
                let factor = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
                let max_backoff_ms = self.inner.config.max_backoff_sec.saturating_mul(1_000);
                let backoff_ms = self
                    .inner
                    .config
                    .initial_backoff_ms
                    .saturating_mul(factor)
                    .min(max_backoff_ms);
                let ready_at = if backoff_ms > 0 {
                    Some(now + Duration::from_millis(backoff_ms))
                } else {
                    None
                };
                self.inner
                    .requeue_lease(signal, payloads, meta, next_attempt, ready_at);
            }
            FailureClass::Rejected { reason } => {
                self.inner.metrics.record_nack(signal, "rejected");
                if next_attempt < self.inner.config.max_attempts {
                    self.completed = true;
                    self.batches.clear();
                    let payloads = std::mem::take(&mut self.payloads);
                    let meta = std::mem::take(&mut self.meta);
                    self.inner
                        .requeue_lease(signal, payloads, meta, next_attempt, None);
                } else {
                    self.nack_rejected_exhausted(signal, next_attempt, reason)
                        .await?;
                }
            }
            FailureClass::Fatal { reason } => {
                self.completed = true;
                self.batches.clear();
                let payloads = std::mem::take(&mut self.payloads);
                let meta = std::mem::take(&mut self.meta);
                self.inner.metrics.record_nack(signal, "fatal");
                self.inner.fail_lease(signal, payloads, meta, reason);
            }
        }
        Ok(())
    }

    async fn nack_rejected_exhausted(
        &mut self,
        signal: SignalKind,
        next_attempt: u32,
        reason: String,
    ) -> Result<(), PipelineError> {
        let dlq = Arc::clone(&self.inner.dead_letter);
        let disk_path = self.inner.config.disk_path.clone();
        let max_payload_len = self.inner.config.max_disk_frame_payload_bytes();
        let dlq_meta = DeadLetterMetadata {
            sink: self.inner.sink_name.clone(),
            signal,
            first_seq: self.id.first_seq,
            last_seq: self.id.last_seq,
            row_count: self.num_rows,
            attempts: next_attempt,
            reason: reason.clone(),
            timestamp_unix_ms: current_unix_ms(),
        };
        let payloads_for_dlq = self.payloads.clone();

        let write_res = tokio::task::spawn_blocking(move || -> Result<PathBuf, PipelineError> {
            let mut decoded = Vec::with_capacity(payloads_for_dlq.len());
            for payload in &payloads_for_dlq {
                match payload {
                    BatchPayload::InMemory(batch) => {
                        decoded.push(batch.clone());
                    }
                    BatchPayload::OnDisk {
                        segment_id,
                        offset,
                        len,
                        ..
                    } => {
                        let base_dir = disk_path.as_ref().ok_or_else(|| {
                            PipelineError::Storage(
                                "OnDisk payload encountered without configured disk_path".into(),
                            )
                        })?;
                        let batch = crate::buffer::disk::read_frame_at(
                            base_dir,
                            *segment_id,
                            *offset,
                            *len,
                            max_payload_len,
                        )?;
                        decoded.push(batch);
                    }
                }
            }
            dlq.write_quarantine(&dlq_meta, &decoded)
        })
        .await
        .map_err(|e| PipelineError::Internal(format!("Lease::nack dead-letter join error: {e}")))?;

        self.completed = true;
        self.batches.clear();
        let payloads = std::mem::take(&mut self.payloads);
        let meta = std::mem::take(&mut self.meta);

        if let Err(err) = write_res {
            self.inner
                .fail_lease(signal, payloads, meta, err.to_string());
            return Err(err);
        }

        let dropped_rows = u64::try_from(self.num_rows).unwrap_or(u64::MAX);
        self.inner.rings[signal.as_u8() as usize]
            .dead_letter_drops
            .fetch_add(dropped_rows, Ordering::Relaxed);
        self.inner
            .metrics
            .record_dropped(signal, dropped_rows, "dead_letter");

        tracing::error!(
            sink = %self.inner.sink_name,
            signal = signal.as_str(),
            first_seq = self.id.first_seq,
            last_seq = self.id.last_seq,
            attempts = next_attempt,
            reason = %reason,
            "Rejected lease exhausted max_attempts; quarantined and advancing watermark"
        );
        self.inner
            .ack_lease(signal, &meta, self.byte_size, self.num_rows, false)
            .await?;
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if !self.completed {
            self.batches.clear();
            let payloads = std::mem::take(&mut self.payloads);
            let mut meta = std::mem::take(&mut self.meta);
            let next_attempt = self.attempt.saturating_add(1);
            let now = Instant::now();
            let now_ms = current_unix_ms().max(1);
            let ring_state = &self.inner.rings[self.id.signal.as_u8() as usize];
            let _ = ring_state.first_transient_failure_unix_ms.compare_exchange(
                0,
                now_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            for item in &mut meta {
                if item.first_failed_at.is_none() {
                    item.first_failed_at = Some(now);
                }
            }
            tracing::warn!(
                sink = %self.inner.sink_name,
                signal = self.id.signal.as_str(),
                first_seq = self.id.first_seq,
                last_seq = self.id.last_seq,
                attempt = next_attempt,
                "Lease dropped without ack or nack; synchronously requeuing batches at head"
            );
            self.inner
                .requeue_lease(self.id.signal, payloads, meta, next_attempt, None);
        }
    }
}
