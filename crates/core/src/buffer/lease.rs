use crate::buffer::BufferInner;
use crate::buffer::config::SignalKind;
use crate::buffer::memory::LeasedBatchMeta;
use crate::error::PipelineError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Unique identifier for a leased range of batches within a signal ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseId {
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
    pub(crate) meta: Vec<LeasedBatchMeta>,
    pub(crate) num_rows: usize,
    pub(crate) byte_size: usize,
    pub(crate) attempt: u32,
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

    /// Returns a slice of the raw `RecordBatch`es belonging to this lease.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns the total row count across all batches in this lease.
    #[must_use]
    pub const fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns the total in-memory byte size charged for this lease.
    #[must_use]
    pub const fn byte_size(&self) -> usize {
        self.byte_size
    }

    /// Returns the delivery attempt counter (`0` on first lease, incremented on each retry).
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Offloads batch concatenation (bypassed when the lease holds a single batch) and
    /// sink-specific preparation work `f` onto a single `tokio::task::spawn_blocking` call.
    pub async fn prepare<F, T>(&self, f: F) -> Result<T, PipelineError>
    where
        F: FnOnce(RecordBatch) -> Result<T, PipelineError> + Send + 'static,
        T: Send + 'static,
    {
        if self.batches.is_empty() {
            return Err(PipelineError::Internal(
                "Lease::prepare invoked on empty lease".into(),
            ));
        }
        let batches = self.batches.clone();
        tokio::task::spawn_blocking(move || {
            let combined = if batches.len() == 1 {
                batches
                    .into_iter()
                    .next()
                    .ok_or_else(|| PipelineError::Internal("empty lease batches".into()))?
            } else {
                let schema = batches[0].schema();
                arrow::compute::concat_batches(&schema, &batches).map_err(PipelineError::Arrow)?
            };
            f(combined)
        })
        .await
        .map_err(|e| PipelineError::Internal(format!("Lease::prepare join error: {e}")))?
    }

    /// Commits the lease, freeing its byte budget and advancing the contiguous watermark.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn ack(mut self) -> Result<(), PipelineError> {
        self.completed = true;
        self.inner
            .ack_lease(self.id.signal, &self.meta, self.byte_size, self.num_rows);
        Ok(())
    }

    /// Rejects the lease with the given [`FailureClass`], either scheduling a backoff retry,
    /// quarantining after `max_attempts`, or failing the buffer on `Fatal`.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn nack(mut self, class: FailureClass) -> Result<(), PipelineError> {
        self.completed = true;
        let signal = self.id.signal;
        let next_attempt = self.attempt.saturating_add(1);
        let batches = std::mem::take(&mut self.batches);
        let meta = std::mem::take(&mut self.meta);

        match class {
            FailureClass::Transient => {
                if self.inner.config.max_transient_duration_sec > 0 {
                    let max_dur = Duration::from_secs(self.inner.config.max_transient_duration_sec);
                    let expired = meta.iter().any(|m| m.enqueued_at.elapsed() >= max_dur);
                    if expired {
                        let reason = format!(
                            "transient retry duration exceeded {}s on sink '{}'",
                            self.inner.config.max_transient_duration_sec, self.inner.sink_name
                        );
                        self.inner.fail_lease(signal, batches, meta, reason);
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
                    Some(Instant::now() + Duration::from_millis(backoff_ms))
                } else {
                    None
                };
                self.inner
                    .requeue_lease(signal, batches, meta, next_attempt, ready_at);
            }
            FailureClass::Rejected { reason } => {
                if next_attempt < self.inner.config.max_attempts {
                    self.inner
                        .requeue_lease(signal, batches, meta, next_attempt, None);
                } else {
                    tracing::error!(
                        sink = %self.inner.sink_name,
                        signal = signal.as_str(),
                        first_seq = self.id.first_seq,
                        last_seq = self.id.last_seq,
                        attempts = next_attempt,
                        reason = %reason,
                        "Rejected lease exhausted max_attempts; dropping and advancing watermark"
                    );
                    self.inner
                        .ack_lease(signal, &meta, self.byte_size, self.num_rows);
                }
            }
            FailureClass::Fatal { reason } => {
                self.inner.fail_lease(signal, batches, meta, reason);
            }
        }
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if !self.completed {
            let batches = std::mem::take(&mut self.batches);
            let meta = std::mem::take(&mut self.meta);
            let next_attempt = self.attempt.saturating_add(1);
            tracing::warn!(
                sink = %self.inner.sink_name,
                signal = self.id.signal.as_str(),
                first_seq = self.id.first_seq,
                last_seq = self.id.last_seq,
                attempt = next_attempt,
                "Lease dropped without ack or nack; synchronously requeuing batches at head"
            );
            self.inner
                .requeue_lease(self.id.signal, batches, meta, next_attempt, None);
        }
    }
}
