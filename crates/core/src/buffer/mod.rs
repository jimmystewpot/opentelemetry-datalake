pub mod config;
pub mod dead_letter;
pub mod disk;
pub mod lease;
pub mod memory;
pub mod metrics;

pub use crate::buffer::config::{
    BufferConfig, BufferMode, DrainTimeoutPolicy, LegacyBatchingConfig, OverflowPolicy, SignalKind,
};
pub use crate::buffer::dead_letter::{
    DeadLetterManifest, DeadLetterMetadata, DeadLetterStore, is_safe_path_component,
};
pub use crate::buffer::disk::{
    DiskWatermark, FRAME_HEADER_LEN, FRAME_MAGIC, FRAME_VERSION, FrameDecodeError, FrameHeader,
    decode_frame, encode_frame,
};
pub use crate::buffer::lease::{FailureClass, Lease, LeaseId};
pub use crate::buffer::metrics::{BufferGaugeState, BufferMetrics, SignalMetricsState};

use crate::buffer::disk::DiskBackend;
use crate::buffer::memory::{
    BatchPayload, LeaseSlotState, LeasedBatchMeta, QueuedBatch, SignalRing, SignalRingState,
};
use crate::error::PipelineError;
use crate::pipeline::SignalBatch;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;
use tokio::sync::Notify;

const ALL_SIGNALS: [SignalKind; 3] = [SignalKind::Logs, SignalKind::Metrics, SignalKind::Traces];

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Generates a non-zero 64-bit buffer instance identifier combining OS CSPRNG state,
/// wall-clock nanoseconds, process ID, and a monotonic counter mixed via SplitMix64.
#[must_use]
pub(crate) fn generate_instance_id() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::AtomicU64;
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    let pid = u64::from(std::process::id());

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(nanos);
    hasher.write_u64(pid);
    hasher.write_u64(seq);
    let mut z = hasher
        .finish()
        .wrapping_add(seq.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    let out = z ^ (z >> 31);
    if out == 0 { 1 } else { out }
}

/// Shared state backing a [`SinkBuffer`] and its [`BufferSender`] handles.
#[derive(Debug)]
pub(crate) struct BufferInner {
    pub(crate) sink_name: String,
    pub(crate) instance_id: u64,
    pub(crate) config: BufferConfig,
    pub(crate) rings: [SignalRingState; 3],
    pub(crate) disk: Option<DiskBackend>,
    pub(crate) dead_letter: Option<Arc<DeadLetterStore>>,
    pub(crate) metrics: BufferMetrics,
    pub(crate) any_data_ready_notify: Notify,
    pub(crate) fatal_error: Mutex<Option<String>>,
}

impl BufferInner {
    fn new(sink_name: &str, config: BufferConfig) -> Result<Arc<Self>, PipelineError> {
        let rings = [
            SignalRingState::new(),
            SignalRingState::new(),
            SignalRingState::new(),
        ];
        let (disk, instance_id) = if config.mode == BufferMode::Disk {
            let disk = DiskBackend::open_and_recover(sink_name, &config, &rings)?;
            let id = disk.instance_id();
            (Some(disk), id)
        } else {
            (None, generate_instance_id())
        };
        let dead_letter = if let Some(ref dl_path) = config.dead_letter_path {
            Some(Arc::new(DeadLetterStore::new(
                dl_path.clone(),
                config.max_dead_letter_bytes,
            )?))
        } else {
            None
        };
        let metrics = BufferMetrics::new(sink_name, &config);
        for sig in ALL_SIGNALS {
            let ring_state = &rings[sig.as_u8() as usize];
            let replayed = ring_state.replayed_records.load(Ordering::Acquire);
            if replayed > 0 {
                metrics.record_replayed(sig, replayed);
            }
            let corrupted = ring_state.corruption_drops.load(Ordering::Acquire);
            if corrupted > 0 {
                metrics.record_dropped(sig, corrupted, "corruption");
            }
            let ring = lock_unpoisoned(&ring_state.ring);
            metrics.update_gauges(
                sig,
                ring.rows_in_flight,
                ring.bytes_in_flight,
                ring.inflight_leases,
                ring.oldest_enqueued_unix_ms(),
            );
        }
        Ok(Arc::new(Self {
            sink_name: sink_name.to_string(),
            instance_id,
            config,
            rings,
            disk,
            dead_letter,
            metrics,
            any_data_ready_notify: Notify::new(),
            fatal_error: Mutex::new(None),
        }))
    }

    fn new_passthrough() -> Arc<Self> {
        let config = BufferConfig::passthrough();
        let metrics = BufferMetrics::new("passthrough", &config);
        Arc::new(Self {
            sink_name: "passthrough".to_string(),
            instance_id: generate_instance_id(),
            config,
            rings: [
                SignalRingState::new(),
                SignalRingState::new(),
                SignalRingState::new(),
            ],
            disk: None,
            dead_letter: None,
            metrics,
            any_data_ready_notify: Notify::new(),
            fatal_error: Mutex::new(None),
        })
    }

    pub(crate) fn sync_ring_metrics(&self, signal: SignalKind, ring: &SignalRing) {
        self.metrics.update_gauges(
            signal,
            ring.rows_in_flight,
            ring.bytes_in_flight,
            ring.inflight_leases,
            ring.oldest_enqueued_unix_ms(),
        );
    }

    pub(crate) fn fatal_reason(&self) -> Option<String> {
        lock_unpoisoned(&self.fatal_error).clone()
    }

    pub(crate) fn is_ready(&self) -> bool {
        if self.fatal_reason().is_some() {
            return false;
        }
        let now_ms = crate::buffer::dead_letter::current_unix_ms();
        let stall_threshold_ms = self.config.readiness_stall_sec.saturating_mul(1_000);
        let transient_threshold_ms = self.config.max_transient_duration_sec.saturating_mul(1_000);

        for sig in ALL_SIGNALS {
            let ring_state = &self.rings[sig.as_u8() as usize];
            if stall_threshold_ms > 0 {
                let blocked_since = ring_state.blocked_since_unix_ms.load(Ordering::Acquire);
                if blocked_since > 0 && now_ms.saturating_sub(blocked_since) >= stall_threshold_ms {
                    return false;
                }
            }
            if transient_threshold_ms > 0 {
                let transient_since = ring_state
                    .first_transient_failure_unix_ms
                    .load(Ordering::Acquire);
                if transient_since > 0
                    && now_ms.saturating_sub(transient_since) >= transient_threshold_ms
                {
                    return false;
                }
            }
        }
        true
    }

    pub(crate) fn finalize_shutdown_timeout(&self) -> Result<(), PipelineError> {
        match self.config.effective_drain_policy() {
            DrainTimeoutPolicy::Persist => {
                if let Some(disk) = &self.disk {
                    let wm = DiskWatermark::from_rings(self.instance_id, &self.rings);
                    disk.flush_and_sync_watermark(wm)?;
                }
                Ok(())
            }
            DrainTimeoutPolicy::Discard => {
                let mut total_dropped: u64 = 0;
                for sig in ALL_SIGNALS {
                    let ring_state = &self.rings[sig.as_u8() as usize];
                    let dropped_rows = {
                        let mut ring = lock_unpoisoned(&ring_state.ring);
                        let rows = u64::try_from(ring.rows_in_flight).unwrap_or(u64::MAX);
                        ring.queue.clear();
                        ring.bytes_in_flight = 0;
                        ring.rows_in_flight = 0;
                        ring.inflight_leases = 0;
                        ring.slots.clear();
                        self.sync_ring_metrics(sig, &ring);
                        rows
                    };
                    if dropped_rows > 0 {
                        ring_state
                            .shutdown_timeout_drops
                            .fetch_add(dropped_rows, Ordering::Release);
                        self.metrics
                            .record_dropped(sig, dropped_rows, "shutdown_timeout");
                        total_dropped = total_dropped.saturating_add(dropped_rows);
                    }
                    ring_state.space_available_notify.notify_waiters();
                    ring_state.data_ready_notify.notify_waiters();
                }
                self.any_data_ready_notify.notify_waiters();

                tracing::error!(
                    sink = %self.sink_name,
                    dropped_records = total_dropped,
                    "Graceful shutdown drain timed out under DrainTimeoutPolicy::Discard; discarded remaining unacknowledged records"
                );

                Err(PipelineError::Storage(
                    format!(
                        "sink '{}' shutdown drain timed out under Discard policy ({total_dropped} records dropped)",
                        self.sink_name
                    )
                    .into(),
                ))
            }
        }
    }

    pub(crate) fn notify_data_ready(&self, signal: SignalKind) {
        self.rings[signal.as_u8() as usize]
            .data_ready_notify
            .notify_waiters();
        self.any_data_ready_notify.notify_waiters();
    }

    pub(crate) async fn ack_lease(
        &self,
        signal: SignalKind,
        meta: &[LeasedBatchMeta],
        byte_size: usize,
        num_rows: usize,
    ) -> Result<(), PipelineError> {
        let ring_state = &self.rings[signal.as_u8() as usize];
        ring_state
            .first_transient_failure_unix_ms
            .store(0, Ordering::Release);
        {
            let mut ring = lock_unpoisoned(&ring_state.ring);
            ring.ack_entries(meta, byte_size, num_rows, &ring_state.committed_seq);
            self.sync_ring_metrics(signal, &ring);
        }
        ring_state.space_available_notify.notify_waiters();
        self.notify_data_ready(signal);

        if let Some(disk) = &self.disk {
            let wm = DiskWatermark::from_rings(self.instance_id, &self.rings);
            disk.update_watermark(wm).await?;
        }
        Ok(())
    }

    pub(crate) fn requeue_lease(
        &self,
        signal: SignalKind,
        payloads: Vec<BatchPayload>,
        meta: Vec<LeasedBatchMeta>,
        next_attempt: u32,
        ready_at: Option<Instant>,
    ) {
        let ring_state = &self.rings[signal.as_u8() as usize];
        {
            let mut ring = lock_unpoisoned(&ring_state.ring);
            ring.requeue_at_head(payloads, meta, next_attempt, ready_at);
            self.sync_ring_metrics(signal, &ring);
        }
        self.notify_data_ready(signal);
    }

    pub(crate) fn fail_lease(
        &self,
        signal: SignalKind,
        payloads: Vec<BatchPayload>,
        meta: Vec<LeasedBatchMeta>,
        reason: String,
    ) {
        {
            let mut fatal = lock_unpoisoned(&self.fatal_error);
            if fatal.is_none() {
                *fatal = Some(reason.clone());
            }
        }
        let ring_state = &self.rings[signal.as_u8() as usize];
        {
            let mut ring = lock_unpoisoned(&ring_state.ring);
            ring.requeue_at_head(payloads, meta, 0, None);
            if ring.fatal_error.is_none() {
                ring.fatal_error = Some(reason);
            }
            self.sync_ring_metrics(signal, &ring);
        }
        for sig in ALL_SIGNALS {
            self.rings[sig.as_u8() as usize]
                .space_available_notify
                .notify_waiters();
            self.rings[sig.as_u8() as usize]
                .data_ready_notify
                .notify_waiters();
        }
        self.any_data_ready_notify.notify_waiters();
    }

    pub(crate) fn record_storage_failure(&self, signal: SignalKind, reason: String) {
        {
            let mut fatal = lock_unpoisoned(&self.fatal_error);
            if fatal.is_none() {
                *fatal = Some(reason.clone());
            }
        }
        {
            let mut ring = lock_unpoisoned(&self.rings[signal.as_u8() as usize].ring);
            if ring.fatal_error.is_none() {
                ring.fatal_error = Some(reason);
            }
        }
        for sig in ALL_SIGNALS {
            self.rings[sig.as_u8() as usize]
                .space_available_notify
                .notify_waiters();
            self.rings[sig.as_u8() as usize]
                .data_ready_notify
                .notify_waiters();
        }
        self.any_data_ready_notify.notify_waiters();
    }

    pub(crate) async fn quarantine_batches(
        &self,
        signal: SignalKind,
        first_seq: u64,
        last_seq: u64,
        attempts: u32,
        reason: String,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<Option<std::path::PathBuf>, PipelineError> {
        let row_count: usize = batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum();
        if batches.is_empty() || row_count == 0 {
            return Ok(None);
        }

        let written_path = if let Some(ref dlq) = self.dead_letter {
            let dlq = Arc::clone(dlq);
            let meta = DeadLetterMetadata {
                sink: self.sink_name.clone(),
                signal,
                first_seq,
                last_seq,
                row_count,
                attempts,
                reason: reason.clone(),
                timestamp_unix_ms: crate::buffer::dead_letter::current_unix_ms(),
            };
            let path = tokio::task::spawn_blocking(move || dlq.write_quarantine(&meta, &batches))
                .await
                .map_err(|e| {
                    PipelineError::Internal(format!("dead-letter write join error: {e}"))
                })??;
            Some(path)
        } else {
            None
        };

        let dropped_rows = u64::try_from(row_count).unwrap_or(u64::MAX);
        self.rings[signal.as_u8() as usize]
            .dead_letter_drops
            .fetch_add(dropped_rows, Ordering::Relaxed);
        self.metrics
            .record_dropped(signal, dropped_rows, "dead_letter");

        tracing::error!(
            sink = %self.sink_name,
            signal = signal.as_str(),
            first_seq,
            last_seq,
            rows = row_count,
            attempts,
            reason = %reason,
            "Quarantined rejected batch rows to dead-letter store"
        );

        Ok(written_path)
    }

    fn increment_senders(&self, scope: Option<SignalKind>) {
        match scope {
            Some(sig) => {
                self.rings[sig.as_u8() as usize]
                    .active_senders
                    .fetch_add(1, Ordering::AcqRel);
            }
            None => {
                for sig in ALL_SIGNALS {
                    self.rings[sig.as_u8() as usize]
                        .active_senders
                        .fetch_add(1, Ordering::AcqRel);
                }
            }
        }
    }

    fn decrement_senders(&self, scope: Option<SignalKind>) {
        let signals: &[SignalKind] = match scope {
            Some(ref sig) => std::slice::from_ref(sig),
            None => &ALL_SIGNALS,
        };
        for &sig in signals {
            let ring_state = &self.rings[sig.as_u8() as usize];
            let prev = ring_state.active_senders.fetch_sub(1, Ordering::AcqRel);
            if prev == 1 {
                {
                    let mut ring = lock_unpoisoned(&ring_state.ring);
                    ring.closed = true;
                }
                self.notify_data_ready(sig);
            }
        }
    }

    fn try_pop_signal_lease(
        self: &Arc<Self>,
        signal: SignalKind,
        now: Instant,
    ) -> (Option<Lease>, Option<Instant>, bool) {
        let ring_state = &self.rings[signal.as_u8() as usize];
        let mut ring = lock_unpoisoned(&ring_state.ring);

        if ring.inflight_leases >= self.config.max_inflight_leases {
            let is_drained = ring.closed && ring.queue.is_empty() && ring.inflight_leases == 0;
            return (None, None, is_drained);
        }

        let Some(front) = ring.queue.front() else {
            let is_drained = ring.closed && ring.inflight_leases == 0;
            return (None, None, is_drained);
        };

        if let Some(ready_at) = front.ready_at
            && ready_at > now
        {
            return (None, Some(ready_at), false);
        }

        let first_attempt = front.attempt;
        let first_schema = front.payload.schema();
        let first_enqueued_at = front.enqueued_at;
        let mut check_last_seq = front.seq_id;
        let mut check_bytes = front.byte_size;
        let mut check_rows = front.row_count;
        let mut coalesce_count = 1usize;
        let mut hit_coalesce_boundary = false;

        for next in ring.queue.iter().skip(1) {
            if next.ready_at.is_some_and(|t| t > now)
                || next.attempt != first_attempt
                || next.seq_id != check_last_seq.saturating_add(1)
                || next.payload.schema() != first_schema
                || check_bytes.saturating_add(next.byte_size) > self.config.max_batch_size_bytes
                || self.config.max_batch_records.is_some_and(|max_rec| {
                    max_rec <= 1 || check_rows.saturating_add(next.row_count) > max_rec
                })
            {
                hit_coalesce_boundary = true;
                break;
            }
            check_last_seq = next.seq_id;
            check_bytes = check_bytes.saturating_add(next.byte_size);
            check_rows = check_rows.saturating_add(next.row_count);
            coalesce_count = coalesce_count.saturating_add(1);
        }

        let interval_zero = self.config.max_batch_interval_sec == 0;
        let bytes_reached = check_bytes >= self.config.max_batch_size_bytes;
        let records_reached = self
            .config
            .max_batch_records
            .is_some_and(|max_rec| check_rows >= max_rec);
        let sender_blocked = ring_state.blocked_senders.load(Ordering::Acquire) > 0;

        if !interval_zero
            && !ring.closed
            && first_attempt == 0
            && !bytes_reached
            && !records_reached
            && !hit_coalesce_boundary
            && !sender_blocked
        {
            let flush_deadline = first_enqueued_at
                + std::time::Duration::from_secs(self.config.max_batch_interval_sec);
            if flush_deadline > now {
                return (None, Some(flush_deadline), false);
            }
        }

        let Some(first) = ring.queue.pop_front() else {
            let is_drained = ring.closed && ring.inflight_leases == 0;
            return (None, None, is_drained);
        };

        ring.slots.insert(first.seq_id, LeaseSlotState::InFlight);
        let first_seq = first.seq_id;
        let mut last_seq = first.seq_id;
        let attempt = first.attempt;
        let mut total_bytes = first.byte_size;
        let mut total_rows = first.row_count;
        let mut batches = Vec::new();
        if let BatchPayload::InMemory(ref b) = first.payload {
            batches.push(b.clone());
        }
        let mut payloads = Vec::with_capacity(coalesce_count);
        payloads.push(first.payload);
        let mut meta = Vec::with_capacity(coalesce_count);
        meta.push(LeasedBatchMeta {
            seq_id: first.seq_id,
            byte_size: first.byte_size,
            row_count: first.row_count,
            enqueued_at: first.enqueued_at,
        });

        for _ in 1..coalesce_count {
            let Some(popped) = ring.queue.pop_front() else {
                break;
            };
            ring.slots.insert(popped.seq_id, LeaseSlotState::InFlight);
            last_seq = popped.seq_id;
            total_bytes = total_bytes.saturating_add(popped.byte_size);
            total_rows = total_rows.saturating_add(popped.row_count);
            if let BatchPayload::InMemory(ref b) = popped.payload {
                batches.push(b.clone());
            }
            payloads.push(popped.payload);
            meta.push(LeasedBatchMeta {
                seq_id: popped.seq_id,
                byte_size: popped.byte_size,
                row_count: popped.row_count,
                enqueued_at: popped.enqueued_at,
            });
        }

        ring.inflight_leases = ring.inflight_leases.saturating_add(1);
        self.sync_ring_metrics(signal, &ring);
        let lease = Lease {
            inner: Arc::clone(self),
            id: LeaseId {
                instance_id: self.instance_id,
                signal,
                first_seq,
                last_seq,
            },
            batches,
            payloads,
            meta,
            num_rows: total_rows,
            byte_size: total_bytes,
            attempt,
            leased_at: now,
            completed: false,
        };
        (Some(lease), None, false)
    }
}

struct BlockedSendGuard<'a> {
    ring_state: &'a SignalRingState,
    active: bool,
}

impl<'a> BlockedSendGuard<'a> {
    const fn new(ring_state: &'a SignalRingState) -> Self {
        Self {
            ring_state,
            active: false,
        }
    }

    fn mark_blocked(&mut self) {
        if !self.active {
            let prev = self
                .ring_state
                .blocked_senders
                .fetch_add(1, Ordering::AcqRel);
            if prev == 0 {
                let now_ms = crate::buffer::dead_letter::current_unix_ms().max(1);
                self.ring_state
                    .blocked_since_unix_ms
                    .store(now_ms, Ordering::Release);
            }
            self.active = true;
        }
    }

    fn mark_admitted(&mut self) {
        if self.active {
            let prev = self
                .ring_state
                .blocked_senders
                .fetch_sub(1, Ordering::AcqRel);
            if prev <= 1 {
                self.ring_state
                    .blocked_since_unix_ms
                    .store(0, Ordering::Release);
            } else {
                let now_ms = crate::buffer::dead_letter::current_unix_ms().max(1);
                self.ring_state
                    .blocked_since_unix_ms
                    .store(now_ms, Ordering::Release);
            }
            self.active = false;
        }
    }
}

impl Drop for BlockedSendGuard<'_> {
    fn drop(&mut self) {
        if self.active {
            let prev = self
                .ring_state
                .blocked_senders
                .fetch_sub(1, Ordering::AcqRel);
            if prev <= 1 {
                self.ring_state
                    .blocked_since_unix_ms
                    .store(0, Ordering::Release);
            }
            self.active = false;
        }
    }
}

/// Cloneable producer handle used by transformers to enqueue [`SignalBatch`]es into a [`SinkBuffer`].
#[derive(Debug)]
pub struct BufferSender {
    inner: Arc<BufferInner>,
    signal_scope: Option<SignalKind>,
}

impl BufferSender {
    fn new(inner: Arc<BufferInner>, signal_scope: Option<SignalKind>) -> Self {
        inner.increment_senders(signal_scope);
        Self {
            inner,
            signal_scope,
        }
    }

    /// Enqueues a [`SignalBatch`] into the target signal ring, applying the configured
    /// [`OverflowPolicy`] when the per-signal byte budget is reached.
    pub async fn send(&self, batch: SignalBatch) -> Result<(), PipelineError> {
        let (signal, record_batch) = match batch {
            SignalBatch::Logs(b) => (SignalKind::Logs, b),
            SignalBatch::Metrics(b) => (SignalKind::Metrics, b),
            SignalBatch::Traces(b) => (SignalKind::Traces, b),
        };

        let batch_bytes = record_batch.get_array_memory_size();
        let batch_rows = record_batch.num_rows();
        let budget = self.inner.config.per_signal_budget();
        let ring_state = &self.inner.rings[signal.as_u8() as usize];
        let mut blocked_guard = BlockedSendGuard::new(ring_state);

        let reserved_seq_id = loop {
            if let Some(reason) = self.inner.fatal_reason() {
                return Err(PipelineError::Storage(reason.into()));
            }

            let notified = ring_state.space_available_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut ring = lock_unpoisoned(&ring_state.ring);
                if let Some(reason) = &ring.fatal_error {
                    return Err(PipelineError::Storage(reason.clone().into()));
                }
                if ring.receiver_dropped || ring.closed {
                    return Err(PipelineError::DownstreamClosed);
                }

                let mut evicted_rows: u64 = 0;
                if self.inner.config.overflow_policy == OverflowPolicy::DropOldest {
                    while !Self::can_admit(&ring, batch_bytes, budget) {
                        let Some(evicted) = ring.queue.pop_front() else {
                            break;
                        };
                        ring.bytes_in_flight =
                            ring.bytes_in_flight.saturating_sub(evicted.byte_size);
                        ring.rows_in_flight = ring.rows_in_flight.saturating_sub(evicted.row_count);
                        let rows = u64::try_from(evicted.row_count).unwrap_or(u64::MAX);
                        ring.evicted_records = ring.evicted_records.saturating_add(rows);
                        evicted_rows = evicted_rows.saturating_add(rows);
                        ring.slots.insert(evicted.seq_id, LeaseSlotState::Acked);
                        ring.advance_watermark(&ring_state.committed_seq);
                    }
                }

                if evicted_rows > 0 {
                    self.inner
                        .metrics
                        .record_dropped(signal, evicted_rows, "overflow");
                    if let Some(disk) = &self.inner.disk {
                        disk.update_watermark_nonblocking(DiskWatermark::from_rings(
                            self.inner.instance_id,
                            &self.inner.rings,
                        ));
                    }
                }

                if Self::can_admit(&ring, batch_bytes, budget) {
                    let seq_id = ring.next_seq;
                    ring.next_seq = ring.next_seq.saturating_add(1);
                    ring.bytes_in_flight = ring.bytes_in_flight.saturating_add(batch_bytes);
                    ring.rows_in_flight = ring.rows_in_flight.saturating_add(batch_rows);
                    ring.slots.insert(seq_id, LeaseSlotState::Queued);

                    if self.inner.disk.is_none() {
                        ring.queue.push_back(QueuedBatch {
                            seq_id,
                            payload: BatchPayload::InMemory(record_batch),
                            byte_size: batch_bytes,
                            row_count: batch_rows,
                            enqueued_at: Instant::now(),
                            attempt: 0,
                            ready_at: None,
                        });
                        self.inner.sync_ring_metrics(signal, &ring);
                        drop(ring);
                        blocked_guard.mark_admitted();
                        self.inner.notify_data_ready(signal);
                        return Ok(());
                    }

                    self.inner.sync_ring_metrics(signal, &ring);
                    blocked_guard.mark_admitted();
                    break seq_id;
                }
            }

            blocked_guard.mark_blocked();
            self.inner.notify_data_ready(signal);
            notified.await;
        };

        self.finish_disk_send(
            signal,
            reserved_seq_id,
            record_batch,
            batch_bytes,
            batch_rows,
        )
        .await
    }

    async fn finish_disk_send(
        &self,
        signal: SignalKind,
        reserved_seq_id: u64,
        record_batch: arrow::record_batch::RecordBatch,
        batch_bytes: usize,
        batch_rows: usize,
    ) -> Result<(), PipelineError> {
        let ring_state = &self.inner.rings[signal.as_u8() as usize];
        let Some(disk) = &self.inner.disk else {
            return Err(PipelineError::Internal(
                "disk backend missing on disk send path".into(),
            ));
        };
        let schema = record_batch.schema();
        match disk
            .append_frame(signal, reserved_seq_id, record_batch)
            .await
        {
            Ok(written) => {
                let actual_bytes = written.len as usize;
                {
                    let mut ring = lock_unpoisoned(&ring_state.ring);
                    ring.bytes_in_flight = ring
                        .bytes_in_flight
                        .saturating_sub(batch_bytes)
                        .saturating_add(actual_bytes);
                    ring.enqueue_ordered(QueuedBatch {
                        seq_id: reserved_seq_id,
                        payload: BatchPayload::OnDisk {
                            segment_id: written.segment_id,
                            offset: written.offset,
                            len: written.len,
                            rows: u32::try_from(batch_rows).unwrap_or(u32::MAX),
                            schema,
                        },
                        byte_size: actual_bytes,
                        row_count: batch_rows,
                        enqueued_at: Instant::now(),
                        attempt: 0,
                        ready_at: None,
                    });
                    self.inner.sync_ring_metrics(signal, &ring);
                }
                if actual_bytes < batch_bytes {
                    ring_state.space_available_notify.notify_waiters();
                }
                self.inner.notify_data_ready(signal);
                Ok(())
            }
            Err(err) => {
                {
                    let mut ring = lock_unpoisoned(&ring_state.ring);
                    ring.bytes_in_flight = ring.bytes_in_flight.saturating_sub(batch_bytes);
                    ring.rows_in_flight = ring.rows_in_flight.saturating_sub(batch_rows);
                    ring.slots.remove(&reserved_seq_id);
                    self.inner.sync_ring_metrics(signal, &ring);
                }
                self.inner.record_storage_failure(signal, err.to_string());
                Err(err)
            }
        }
    }

    fn can_admit(ring: &SignalRing, batch_bytes: usize, budget: u64) -> bool {
        if ring.queue.is_empty() && ring.bytes_in_flight == 0 {
            return true;
        }
        let in_flight_u64 = u64::try_from(ring.bytes_in_flight).unwrap_or(u64::MAX);
        let batch_u64 = u64::try_from(batch_bytes).unwrap_or(u64::MAX);
        in_flight_u64.saturating_add(batch_u64) <= budget
    }
}

impl Clone for BufferSender {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.inner), self.signal_scope)
    }
}

impl Drop for BufferSender {
    fn drop(&mut self) {
        self.inner.decrement_senders(self.signal_scope);
    }
}

/// Cloneable administrative handle for inspecting readiness and finalizing bounded shutdown drains
/// on a [`SinkBuffer`] without holding a consumer receiver handle.
#[derive(Debug, Clone)]
pub struct BufferController {
    inner: Arc<BufferInner>,
}

impl BufferController {
    /// Returns the buffer's 64-bit instance identifier.
    #[must_use]
    pub fn instance_id(&self) -> u64 {
        self.inner.instance_id
    }

    /// Returns `true` if the buffer is healthy and not stalled beyond `readiness_stall_sec`
    /// or `max_transient_duration_sec`.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    /// Returns the configured graceful shutdown drain timeout in seconds.
    #[must_use]
    pub fn drain_timeout_sec(&self) -> u64 {
        self.inner.config.drain_timeout_sec
    }

    /// Executes the configured [`DrainTimeoutPolicy`] (`Persist` or `Discard`) when graceful
    /// shutdown exceeds `drain_timeout_sec`.
    pub fn finalize_shutdown_timeout(&self) -> Result<(), PipelineError> {
        self.inner.finalize_shutdown_timeout()
    }
}

/// Consumer handle owned by a sink task to acquire transactional [`Lease`]s.
#[derive(Debug)]
pub struct SinkBuffer {
    inner: Arc<BufferInner>,
    signal_scope: Option<SignalKind>,
    rr_cursor: usize,
}

impl SinkBuffer {
    /// Creates a new [`BufferSender`] and [`SinkBuffer`] pair from validated configuration.
    pub fn new(
        sink_name: &str,
        config: BufferConfig,
    ) -> Result<(BufferSender, Self), PipelineError> {
        config.validate(sink_name)?;
        let inner = BufferInner::new(sink_name, config)?;
        let sender = BufferSender::new(Arc::clone(&inner), None);
        Ok((
            sender,
            Self {
                inner,
                signal_scope: None,
                rr_cursor: 0,
            },
        ))
    }

    /// Creates an infallible in-memory passthrough `(BufferSender, SinkBuffer)` pair.
    #[must_use]
    pub fn passthrough_pair() -> (BufferSender, Self) {
        let inner = BufferInner::new_passthrough();
        let sender = BufferSender::new(Arc::clone(&inner), None);
        (
            sender,
            Self {
                inner,
                signal_scope: None,
                rr_cursor: 0,
            },
        )
    }

    /// Creates three signal-scoped `(BufferSender, SinkBuffer)` pairs (`[Logs, Metrics, Traces]`)
    /// sharing a single underlying buffer budget and state.
    pub fn new_signal_triad(
        sink_name: &str,
        config: BufferConfig,
    ) -> Result<[(BufferSender, Self); 3], PipelineError> {
        config.validate(sink_name)?;
        let inner = BufferInner::new(sink_name, config)?;
        Ok(ALL_SIGNALS.map(|sig| {
            let sender = BufferSender::new(Arc::clone(&inner), Some(sig));
            (
                sender,
                Self {
                    inner: Arc::clone(&inner),
                    signal_scope: Some(sig),
                    rr_cursor: sig.as_u8() as usize,
                },
            )
        }))
    }

    /// Returns a cloneable [`BufferController`] handle for readiness probes and shutdown drain orchestration.
    #[must_use]
    pub fn controller(&self) -> BufferController {
        BufferController {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Returns the buffer's 64-bit instance identifier.
    #[must_use]
    pub fn instance_id(&self) -> u64 {
        self.inner.instance_id
    }

    /// Returns `true` if the buffer is healthy and not stalled beyond `readiness_stall_sec`
    /// or `max_transient_duration_sec`.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    /// Returns the configured graceful shutdown drain timeout in seconds.
    #[must_use]
    pub fn drain_timeout_sec(&self) -> u64 {
        self.inner.config.drain_timeout_sec
    }

    /// Executes the configured [`DrainTimeoutPolicy`] (`Persist` or `Discard`) when graceful
    /// shutdown exceeds `drain_timeout_sec`.
    pub fn finalize_shutdown_timeout(&self) -> Result<(), PipelineError> {
        self.inner.finalize_shutdown_timeout()
    }

    /// Returns a reference to the buffer's [`BufferMetrics`] instruments and state.
    #[must_use]
    pub fn metrics(&self) -> &BufferMetrics {
        &self.inner.metrics
    }

    /// Returns the cumulative number of dropped records for `reason`
    /// (`"overflow"`, `"dead_letter"`, `"corruption"`, or `"shutdown_timeout"`).
    #[must_use]
    pub fn dropped_records(&self, reason: &str) -> u64 {
        self.inner
            .metrics
            .dropped_records(self.signal_scope, reason)
    }

    /// Returns the cumulative number of records discarded on shutdown drain timeout (`DrainTimeoutPolicy::Discard`).
    #[must_use]
    pub fn shutdown_timeout_drops(&self) -> u64 {
        match self.signal_scope {
            Some(sig) => self.inner.rings[sig.as_u8() as usize]
                .shutdown_timeout_drops
                .load(Ordering::Acquire),
            None => ALL_SIGNALS.iter().fold(0u64, |acc, &sig| {
                acc.saturating_add(
                    self.inner.rings[sig.as_u8() as usize]
                        .shutdown_timeout_drops
                        .load(Ordering::Acquire),
                )
            }),
        }
    }

    /// Returns the cumulative number of records dropped due to `OverflowPolicy::DropOldest`.
    #[must_use]
    pub fn overflow_drops(&self) -> u64 {
        self.dropped_records("overflow")
    }

    /// Returns the cumulative number of records committed via [`Lease::ack`].
    #[must_use]
    pub fn committed_records(&self) -> u64 {
        self.inner.metrics.committed_records(self.signal_scope)
    }

    /// Returns the cumulative number of lease nacks for `failure_class` (`"transient"`, `"rejected"`, `"fatal"`).
    #[must_use]
    pub fn lease_nacks(&self, failure_class: &str) -> u64 {
        self.inner
            .metrics
            .lease_nacks(self.signal_scope, failure_class)
    }

    /// Waits for the next coalesced [`Lease`] to become ready, or returns `Ok(None)` when
    /// all senders have closed and the buffer is completely drained.
    pub async fn next_lease(&mut self) -> Result<Option<Lease>, PipelineError> {
        loop {
            if let Some(reason) = self.inner.fatal_reason() {
                return Err(PipelineError::Storage(reason.into()));
            }

            let now = Instant::now();

            if let Some(sig) = self.signal_scope {
                let ring_state = &self.inner.rings[sig.as_u8() as usize];
                let notified = ring_state.data_ready_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if let Some(reason) = self.inner.fatal_reason() {
                    return Err(PipelineError::Storage(reason.into()));
                }

                let (lease_opt, ready_at_opt, is_drained) =
                    self.inner.try_pop_signal_lease(sig, now);
                if let Some(lease) = lease_opt {
                    return Ok(Some(lease));
                }
                if is_drained {
                    return Ok(None);
                }

                if let Some(deadline) = ready_at_opt {
                    tokio::select! {
                        () = notified => {}
                        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                    }
                } else {
                    notified.await;
                }
            } else {
                let notified = self.inner.any_data_ready_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if let Some(reason) = self.inner.fatal_reason() {
                    return Err(PipelineError::Storage(reason.into()));
                }

                let mut earliest_deadline: Option<Instant> = None;
                let mut all_drained = true;

                for offset in 0..ALL_SIGNALS.len() {
                    let idx = (self.rr_cursor + offset) % ALL_SIGNALS.len();
                    let sig = ALL_SIGNALS[idx];
                    let (lease_opt, ready_at_opt, is_drained) =
                        self.inner.try_pop_signal_lease(sig, now);
                    if let Some(lease) = lease_opt {
                        self.rr_cursor = (idx + 1) % ALL_SIGNALS.len();
                        return Ok(Some(lease));
                    }
                    if !is_drained {
                        all_drained = false;
                    }
                    if let Some(deadline) = ready_at_opt {
                        earliest_deadline =
                            Some(earliest_deadline.map_or(deadline, |prev| prev.min(deadline)));
                    }
                }

                if all_drained {
                    return Ok(None);
                }

                if let Some(deadline) = earliest_deadline {
                    tokio::select! {
                        () = notified => {}
                        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                    }
                } else {
                    notified.await;
                }
            }
        }
    }

    /// Passthrough helper that acquires the next lease, immediately acknowledges it, and
    /// returns the underlying [`SignalBatch`].
    pub async fn recv_passthrough(&mut self) -> Result<Option<SignalBatch>, PipelineError> {
        let Some(lease) = self.next_lease().await? else {
            return Ok(None);
        };
        let signal = lease.signal();
        let batch = if lease.batches().len() == 1 {
            lease.batches()[0].clone()
        } else if !lease.batches().is_empty() {
            let schema = lease.batches()[0].schema();
            arrow::compute::concat_batches(&schema, lease.batches())
                .map_err(PipelineError::Arrow)?
        } else {
            lease.prepare(Ok).await?
        };
        lease.ack().await?;
        let signal_batch = match signal {
            SignalKind::Logs => SignalBatch::Logs(batch),
            SignalKind::Metrics => SignalBatch::Metrics(batch),
            SignalKind::Traces => SignalBatch::Traces(batch),
        };
        Ok(Some(signal_batch))
    }

    /// Returns the highest contiguous sequence number acknowledged for `signal`.
    #[must_use]
    pub fn committed_seq(&self, signal: SignalKind) -> u64 {
        self.inner.rings[signal.as_u8() as usize]
            .committed_seq
            .load(Ordering::Acquire)
    }

    /// Returns the cumulative number of records dropped due to disk frame corruption during recovery.
    #[must_use]
    pub fn corruption_drops(&self) -> u64 {
        match self.signal_scope {
            Some(sig) => self.inner.rings[sig.as_u8() as usize]
                .corruption_drops
                .load(Ordering::Acquire),
            None => ALL_SIGNALS.iter().fold(0u64, |acc, &sig| {
                acc.saturating_add(
                    self.inner.rings[sig.as_u8() as usize]
                        .corruption_drops
                        .load(Ordering::Acquire),
                )
            }),
        }
    }

    /// Returns the cumulative number of records replayed from disk during startup recovery.
    #[must_use]
    pub fn replayed_records(&self) -> u64 {
        match self.signal_scope {
            Some(sig) => self.inner.rings[sig.as_u8() as usize]
                .replayed_records
                .load(Ordering::Acquire),
            None => ALL_SIGNALS.iter().fold(0u64, |acc, &sig| {
                acc.saturating_add(
                    self.inner.rings[sig.as_u8() as usize]
                        .replayed_records
                        .load(Ordering::Acquire),
                )
            }),
        }
    }

    /// Flushes pending disk writes and durably syncs `watermark.json` (`BufferMode::Disk`).
    pub fn flush_and_sync_watermark(&self) -> Result<(), PipelineError> {
        if let Some(disk) = &self.inner.disk {
            let wm = DiskWatermark::from_rings(self.inner.instance_id, &self.inner.rings);
            disk.flush_and_sync_watermark(wm)?;
        }
        Ok(())
    }

    /// Returns the cumulative number of records quarantined to the dead-letter store.
    #[must_use]
    pub fn dead_letter_drops(&self) -> u64 {
        match self.signal_scope {
            Some(sig) => self.inner.rings[sig.as_u8() as usize]
                .dead_letter_drops
                .load(Ordering::Acquire),
            None => ALL_SIGNALS.iter().fold(0u64, |acc, &sig| {
                acc.saturating_add(
                    self.inner.rings[sig.as_u8() as usize]
                        .dead_letter_drops
                        .load(Ordering::Acquire),
                )
            }),
        }
    }

    /// Returns a reference to the underlying [`DeadLetterStore`] if `dead_letter_path` is configured.
    #[must_use]
    pub fn dead_letter_store(&self) -> Option<&Arc<DeadLetterStore>> {
        self.inner.dead_letter.as_ref()
    }

    /// Quarantines a rejected subset of batches (e.g. item-level HTTP 400 rows) to the
    /// configured [`DeadLetterStore`] inside `tokio::task::spawn_blocking`.
    pub async fn quarantine_batches(
        &self,
        signal: SignalKind,
        first_seq: u64,
        last_seq: u64,
        attempts: u32,
        reason: String,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<Option<std::path::PathBuf>, PipelineError> {
        self.inner
            .quarantine_batches(signal, first_seq, last_seq, attempts, reason, batches)
            .await
    }
}

impl Drop for SinkBuffer {
    fn drop(&mut self) {
        let signals: &[SignalKind] = match self.signal_scope {
            Some(ref sig) => std::slice::from_ref(sig),
            None => &ALL_SIGNALS,
        };
        for &sig in signals {
            let ring_state = &self.inner.rings[sig.as_u8() as usize];
            {
                let mut ring = lock_unpoisoned(&ring_state.ring);
                ring.receiver_dropped = true;
            }
            ring_state.space_available_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::time::Duration;

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        let arr = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[tokio::test]
    async fn test_memory_ring_isolates_per_signal_budgets() {
        let sample = make_batch(&[1, 2, 3, 4]);
        let batch_bytes = sample.get_array_memory_size();
        // Per-signal budget fits exactly 1 batch
        let config = BufferConfig {
            max_buffer_bytes: (batch_bytes as u64) * 3,
            max_batch_size_bytes: batch_bytes,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_iso", config).unwrap();

        // Fill Logs ring to its per-signal budget
        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();

        // Second Logs send should block
        let tx_logs2 = tx.clone();
        let sample_logs2 = sample.clone();
        let mut logs2_handle =
            tokio::spawn(async move { tx_logs2.send(SignalBatch::Logs(sample_logs2)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !logs2_handle.is_finished(),
            "Second Logs send must block when Logs ring is full"
        );

        // Metrics and Traces sends must succeed immediately without blocking
        tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(SignalBatch::Metrics(sample.clone())),
        )
        .await
        .expect("Metrics send should not block")
        .unwrap();

        tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(SignalBatch::Traces(sample.clone())),
        )
        .await
        .expect("Traces send should not block")
        .unwrap();

        // Drain and ack Logs batch 1 to unblock logs2_handle
        let lease = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease.signal(), SignalKind::Logs);
        lease.ack().await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), &mut logs2_handle)
            .await
            .expect("Second Logs send should unblock after ack")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_oversize_single_batch_admitted_when_ring_empty() {
        // max_buffer_bytes = 3000 -> per_signal_budget = 1000, max_batch_size_bytes = 500
        let config = BufferConfig {
            max_buffer_bytes: 3000,
            max_batch_size_bytes: 500,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_oversize", config).unwrap();

        // Construct a single RecordBatch whose get_array_memory_size() > 1000
        let big_values: Vec<i64> = (0..256).collect();
        let oversize_batch = make_batch(&big_values);
        assert!(
            oversize_batch.get_array_memory_size() > 1000,
            "Batch memory size ({}) must exceed per_signal_budget (1000)",
            oversize_batch.get_array_memory_size()
        );

        // Sending oversize batch into an empty ring must succeed immediately
        tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(SignalBatch::Logs(oversize_batch)),
        )
        .await
        .expect("Oversize batch into empty ring must not deadlock")
        .unwrap();

        // A second send must block while the oversize batch is in flight
        let small_batch = make_batch(&[42]);
        let tx2 = tx.clone();
        let mut second_send =
            tokio::spawn(async move { tx2.send(SignalBatch::Logs(small_batch)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !second_send.is_finished(),
            "Second send must block while oversize batch occupies the ring"
        );

        // Acquiring and acknowledging the oversize lease must unblock the second send
        let lease = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease.num_rows(), 256);
        lease.ack().await.unwrap();

        tokio::time::timeout(Duration::from_millis(100), &mut second_send)
            .await
            .expect("Second send must unblock after oversize batch is acked")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_overflow_policy_drop_oldest_evicts_only_queued_never_leased() {
        let b1 = make_batch(&[10, 11]);
        let batch_bytes = b1.get_array_memory_size();
        // Per-signal budget holds at most 3 batches
        let config = BufferConfig {
            max_buffer_bytes: (batch_bytes as u64) * 9, // per_signal_budget = 3 * batch_bytes
            max_batch_size_bytes: batch_bytes,
            max_batch_records: Some(2),
            max_batch_interval_sec: 0,
            max_inflight_leases: 3,
            overflow_policy: OverflowPolicy::DropOldest,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_drop_oldest", config).unwrap();

        // Send batch 1 and lease it (InFlight)
        tx.send(SignalBatch::Logs(make_batch(&[1, 1])))
            .await
            .unwrap();
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1.id().first_seq, 1);

        // Queue batch 2 and batch 3 (now 1 InFlight + 2 Queued = 3 batches = full budget)
        tx.send(SignalBatch::Logs(make_batch(&[2, 2])))
            .await
            .unwrap();
        tx.send(SignalBatch::Logs(make_batch(&[3, 3])))
            .await
            .unwrap();

        // Send batch 4 exceeding budget: must evict batch 2 (oldest Queued), keeping batch 1 (InFlight) and batch 3
        tx.send(SignalBatch::Logs(make_batch(&[4, 4])))
            .await
            .unwrap();

        // Next lease must be batch 3 (seq 3), proving batch 2 (seq 2) was evicted
        let lease3 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease3.id().first_seq, 3);
        let lease4 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease4.id().first_seq, 4);

        // Now all 3 slots (lease1, lease3, lease4) are InFlight and queue is empty.
        // Sending batch 5 must block until an InFlight lease is acked!
        let tx5 = tx.clone();
        let mut send5 =
            tokio::spawn(async move { tx5.send(SignalBatch::Logs(make_batch(&[5, 5]))).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !send5.is_finished(),
            "DropOldest must block when all budget is held by InFlight leases"
        );

        lease1.ack().await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), &mut send5)
            .await
            .expect("send5 should unblock once lease1 acks")
            .unwrap()
            .unwrap();
        lease3.ack().await.unwrap();
        lease4.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_lease_drop_without_ack_requeues_batches_at_head() {
        let b1 = make_batch(&[1, 2]);
        let b2 = make_batch(&[3, 4]);
        let config = BufferConfig {
            max_batch_size_bytes: b1.get_array_memory_size() * 4,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_drop_requeue", config).unwrap();

        tx.send(SignalBatch::Logs(b1)).await.unwrap();
        tx.send(SignalBatch::Logs(b2)).await.unwrap();

        let lease1 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1.id().first_seq, 1);
        assert_eq!(lease1.id().last_seq, 2);
        assert_eq!(lease1.attempt(), 0);

        // Drop lease1 without calling ack() or nack()
        drop(lease1);

        let lease2 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease2.id().first_seq, 1);
        assert_eq!(lease2.id().last_seq, 2);
        assert_eq!(lease2.attempt(), 1);
        assert_eq!(lease2.num_rows(), 4);
        lease2.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_out_of_order_lease_ack_watermark_progression() {
        let config = BufferConfig {
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            max_inflight_leases: 2,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_ooo_ack", config).unwrap();

        tx.send(SignalBatch::Logs(make_batch(&[10]))).await.unwrap();
        tx.send(SignalBatch::Logs(make_batch(&[20]))).await.unwrap();

        let lease1 = buf.next_lease().await.unwrap().unwrap();
        let lease2 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1.id().first_seq, 1);
        assert_eq!(lease1.id().last_seq, 1);
        assert_eq!(lease2.id().first_seq, 2);
        assert_eq!(lease2.id().last_seq, 2);

        // Acknowledge lease2 first (out of order) -> watermark must stay at 0
        lease2.ack().await.unwrap();
        assert_eq!(buf.committed_seq(SignalKind::Logs), 0);

        // Acknowledge lease1 second -> watermark must jump to 2
        lease1.ack().await.unwrap();
        assert_eq!(buf.committed_seq(SignalKind::Logs), 2);
    }

    #[tokio::test]
    async fn test_lease_prepare_bypasses_concat_on_single_batch_and_concats_multiple() {
        let b1 = make_batch(&[1, 2]);
        let b2 = make_batch(&[3, 4, 5]);
        let config = BufferConfig {
            max_batch_size_bytes: b1.get_array_memory_size() * 4,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_prepare", config).unwrap();

        // Single batch lease: verify zero-copy buffer pointer identity and row count
        let expected_ptr = b1.column(0).to_data().buffers()[0].as_ptr() as usize;
        tx.send(SignalBatch::Logs(b1)).await.unwrap();
        let single_lease = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(single_lease.batches().len(), 1);
        let (rows, actual_ptr) = single_lease
            .prepare(|batch| {
                let ptr = batch.column(0).to_data().buffers()[0].as_ptr() as usize;
                Ok((batch.num_rows(), ptr))
            })
            .await
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            actual_ptr, expected_ptr,
            "Single-batch prepare must bypass concat_batches"
        );
        single_lease.ack().await.unwrap();

        // Multi-batch lease: verify concat_batches combines rows
        tx.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();
        tx.send(SignalBatch::Logs(b2)).await.unwrap();
        let multi_lease = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(multi_lease.batches().len(), 2);
        let total_rows = multi_lease
            .prepare(|batch| Ok(batch.num_rows()))
            .await
            .unwrap();
        assert_eq!(total_rows, 5);
        multi_lease.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_transient_applies_backoff_and_increments_attempt() {
        let config = BufferConfig {
            initial_backoff_ms: 30,
            max_backoff_sec: 1,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_transient", config).unwrap();

        tx.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1.attempt(), 0);

        let start = std::time::Instant::now();
        lease1.nack(FailureClass::Transient).await.unwrap();

        let lease2 = buf.next_lease().await.unwrap().unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(25),
            "Transient nack must wait for initial_backoff_ms before re-leasing"
        );
        assert_eq!(lease2.id().first_seq, 1);
        assert_eq!(lease2.attempt(), 1);
        lease2.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_nack_fatal_fails_buffer_and_next_lease_returns_err() {
        let config = BufferConfig::passthrough();
        let (tx, mut buf) = SinkBuffer::new("test_fatal", config).unwrap();

        tx.send(SignalBatch::Logs(make_batch(&[1]))).await.unwrap();
        let lease = buf.next_lease().await.unwrap().unwrap();
        lease
            .nack(FailureClass::Fatal {
                reason: "401 Unauthorized".into(),
            })
            .await
            .unwrap();

        let err = buf.next_lease().await.unwrap_err();
        assert!(matches!(err, PipelineError::Storage(_)));
        assert!(err.to_string().contains("401"));
    }

    #[tokio::test]
    async fn test_passthrough_pair_triad_and_clean_eof() {
        let (tx, mut buf) = SinkBuffer::passthrough_pair();
        tx.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();
        tx.send(SignalBatch::Metrics(make_batch(&[3])))
            .await
            .unwrap();
        drop(tx);

        let b1 = buf.recv_passthrough().await.unwrap().unwrap();
        assert!(matches!(b1, SignalBatch::Logs(ref r) if r.num_rows() == 2));
        let b2 = buf.recv_passthrough().await.unwrap().unwrap();
        assert!(matches!(b2, SignalBatch::Metrics(ref r) if r.num_rows() == 1));
        assert!(buf.recv_passthrough().await.unwrap().is_none());

        let [
            (logs_tx, mut logs_buf),
            (metrics_tx, mut metrics_buf),
            (traces_tx, mut traces_buf),
        ] = SinkBuffer::new_signal_triad("triad_test", BufferConfig::passthrough()).unwrap();
        logs_tx
            .send(SignalBatch::Logs(make_batch(&[10])))
            .await
            .unwrap();
        metrics_tx
            .send(SignalBatch::Metrics(make_batch(&[20])))
            .await
            .unwrap();
        traces_tx
            .send(SignalBatch::Traces(make_batch(&[30])))
            .await
            .unwrap();
        drop(logs_tx);
        drop(metrics_tx);
        drop(traces_tx);

        assert!(matches!(
            logs_buf.recv_passthrough().await.unwrap(),
            Some(SignalBatch::Logs(_))
        ));
        assert!(logs_buf.recv_passthrough().await.unwrap().is_none());
        assert!(matches!(
            metrics_buf.recv_passthrough().await.unwrap(),
            Some(SignalBatch::Metrics(_))
        ));
        assert!(metrics_buf.recv_passthrough().await.unwrap().is_none());
        assert!(matches!(
            traces_buf.recv_passthrough().await.unwrap(),
            Some(SignalBatch::Traces(_))
        ));
        assert!(traces_buf.recv_passthrough().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_buffer_is_not_ready_when_send_stalled_beyond_readiness_stall_sec() {
        let sample = make_batch(&[1, 2, 3, 4]);
        let batch_bytes = sample.get_array_memory_size();
        let config = BufferConfig {
            max_buffer_bytes: (batch_bytes as u64) * 3,
            max_batch_size_bytes: batch_bytes,
            max_batch_interval_sec: 0,
            overflow_policy: OverflowPolicy::Block,
            readiness_stall_sec: 1,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_ready_stall", config).unwrap();
        assert!(buf.is_ready());

        // Fill Logs ring to capacity
        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();
        assert!(buf.is_ready());

        // Spawn a blocked send
        let tx2 = tx.clone();
        let sample2 = sample.clone();
        let blocked_send = tokio::spawn(async move { tx2.send(SignalBatch::Logs(sample2)).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!blocked_send.is_finished());
        assert!(
            buf.is_ready(),
            "Buffer should still be ready before readiness_stall_sec elapses"
        );

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(
            !buf.is_ready(),
            "Buffer must report not-ready once send has been blocked beyond readiness_stall_sec"
        );

        // Ack the first batch to unblock send and restore readiness
        let lease = buf.next_lease().await.unwrap().unwrap();
        lease.ack().await.unwrap();
        blocked_send.await.unwrap().unwrap();
        assert!(
            buf.is_ready(),
            "Buffer must return to ready once blocked send completes"
        );
    }

    #[tokio::test]
    async fn test_is_ready_returns_false_when_transient_retry_exceeds_max_transient_duration_sec() {
        let config = BufferConfig {
            initial_backoff_ms: 10,
            max_backoff_sec: 1,
            max_transient_duration_sec: 1,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_transient_ready", config).unwrap();
        assert!(buf.is_ready());

        tx.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        lease1.nack(FailureClass::Transient).await.unwrap();
        assert!(buf.is_ready());

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(
            !buf.is_ready(),
            "Buffer must report not-ready when transient retry streak exceeds max_transient_duration_sec"
        );

        let lease2 = buf.next_lease().await.unwrap().unwrap();
        lease2.nack(FailureClass::Transient).await.unwrap();
        assert!(!buf.is_ready());
    }

    #[tokio::test]
    async fn test_shutdown_drain_timeout_discard_drops_remaining_and_increments_metric() {
        let config = BufferConfig {
            mode: BufferMode::Memory,
            on_drain_timeout: Some(DrainTimeoutPolicy::Discard),
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, buf) = SinkBuffer::new("test_discard_timeout", config).unwrap();

        tx.send(SignalBatch::Logs(make_batch(&[10, 20, 30])))
            .await
            .unwrap();
        tx.send(SignalBatch::Metrics(make_batch(&[40, 50])))
            .await
            .unwrap();
        drop(tx);

        let err = buf.finalize_shutdown_timeout().unwrap_err();
        assert!(matches!(err, PipelineError::Storage(_)));
        assert_eq!(buf.dropped_records("shutdown_timeout"), 5);
        assert_eq!(buf.shutdown_timeout_drops(), 5);
    }

    #[tokio::test]
    async fn test_shutdown_drain_timeout_persist_flushes_disk_writer_and_preserves_segments() {
        let pid = std::process::id();
        let disk_dir = std::env::temp_dir().join(format!("otel-dl-persist-timeout-{pid}"));
        let _ = std::fs::remove_dir_all(&disk_dir);

        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(disk_dir.clone()),
            on_drain_timeout: Some(DrainTimeoutPolicy::Persist),
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        {
            let (tx, mut buf) = SinkBuffer::new("test_persist_timeout", config.clone()).unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[1, 2])))
                .await
                .unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[3, 4, 5])))
                .await
                .unwrap();
            drop(tx);

            // Ack batch 1, leave batch 2 unacked when drain timeout fires
            let lease1 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease1.id().first_seq, 1);
            lease1.ack().await.unwrap();

            buf.finalize_shutdown_timeout().unwrap();
        }

        {
            let (tx, mut reopened) =
                SinkBuffer::new("test_persist_timeout", config.clone()).unwrap();
            drop(tx);
            assert_eq!(reopened.replayed_records(), 3);

            let lease2 = reopened.next_lease().await.unwrap().unwrap();
            assert_eq!(lease2.id().first_seq, 2);
            let batch2 = lease2.prepare(Ok).await.unwrap();
            assert_eq!(batch2, make_batch(&[3, 4, 5]));
            lease2.ack().await.unwrap();
            assert!(reopened.next_lease().await.unwrap().is_none());
        }

        let _ = std::fs::remove_dir_all(&disk_dir);
    }

    #[tokio::test]
    async fn test_shutdown_drain_timeout_discard_and_persist_policies() {
        let config = BufferConfig {
            mode: BufferMode::Memory,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, buf) = SinkBuffer::new("test_both_policies", config).unwrap();
        tx.send(SignalBatch::Traces(make_batch(&[7, 8, 9, 10])))
            .await
            .unwrap();
        drop(tx);

        assert!(buf.finalize_shutdown_timeout().is_err());
        assert_eq!(buf.dropped_records("shutdown_timeout"), 4);
    }

    #[tokio::test]
    async fn test_buffer_metrics_gauges_counters_and_drop_reasons() {
        let sample = make_batch(&[1, 2, 3]);
        let batch_bytes = sample.get_array_memory_size() as u64;
        let config = BufferConfig {
            mode: BufferMode::Memory,
            max_buffer_bytes: batch_bytes * 3,
            max_batch_size_bytes: batch_bytes as usize,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            overflow_policy: OverflowPolicy::DropOldest,
            initial_backoff_ms: 1,
            max_backoff_sec: 1,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_metrics_sink", config).unwrap();

        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();
        // Trigger DropOldest eviction of the first batch (3 rows)
        tx.send(SignalBatch::Logs(sample)).await.unwrap();
        assert_eq!(buf.overflow_drops(), 3);
        assert_eq!(buf.dropped_records("overflow"), 3);

        let lease = buf.next_lease().await.unwrap().unwrap();
        assert_eq!(
            buf.metrics()
                .lease_nacks(Some(SignalKind::Logs), "transient"),
            0
        );
        lease.nack(FailureClass::Transient).await.unwrap();
        assert_eq!(buf.lease_nacks("transient"), 1);

        let lease2 = buf.next_lease().await.unwrap().unwrap();
        lease2.ack().await.unwrap();
        assert_eq!(buf.committed_records(), 3);
    }

    #[tokio::test]
    async fn test_memory_buffers_assign_distinct_instance_ids_and_preserve_across_retries() {
        let config = BufferConfig {
            initial_backoff_ms: 1,
            max_backoff_sec: 1,
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx1, mut buf1) = SinkBuffer::new("inst_test_1", config.clone()).unwrap();
        let (tx2, mut buf2) = SinkBuffer::new("inst_test_2", config).unwrap();

        let id1 = buf1.instance_id();
        let id2 = buf2.instance_id();
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
        assert_ne!(
            id1, id2,
            "Distinct memory buffer instances must receive distinct instance_ids"
        );
        assert_eq!(buf1.controller().instance_id(), id1);

        tx1.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();
        tx2.send(SignalBatch::Logs(make_batch(&[1, 2])))
            .await
            .unwrap();

        let lease1_first = buf1.next_lease().await.unwrap().unwrap();
        let lease2_first = buf2.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1_first.id().instance_id, id1);
        assert_eq!(lease2_first.id().instance_id, id2);
        assert_ne!(lease1_first.id(), lease2_first.id());

        // Nack lease1 and verify retry preserves the exact same LeaseId (including instance_id)
        let original_lease1_id = lease1_first.id();
        lease1_first.nack(FailureClass::Transient).await.unwrap();
        let lease1_retry = buf1.next_lease().await.unwrap().unwrap();
        assert_eq!(lease1_retry.id(), original_lease1_id);
        lease1_retry.ack().await.unwrap();
        lease2_first.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_next_lease_waits_for_batch_interval_or_threshold_and_flushes_early_on_boundary_or_close()
     {
        let sample = make_batch(&[1, 2]);
        let batch_bytes = sample.get_array_memory_size();

        // 1. Partial batch below max_batch_size_bytes and max_batch_records must wait for max_batch_interval_sec
        let config_interval = BufferConfig {
            max_batch_size_bytes: batch_bytes * 10,
            max_batch_records: Some(20),
            max_batch_interval_sec: 1,
            ..BufferConfig::default()
        };
        let (tx, mut buf) = SinkBuffer::new("test_interval_wait", config_interval).unwrap();
        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();

        // Must NOT pop within 80ms while sender is open and thresholds are not met
        let early = tokio::time::timeout(Duration::from_millis(80), buf.next_lease()).await;
        assert!(
            early.is_err(),
            "Partial batch must wait for batching trigger while sender is open"
        );

        // Sending 9 more batches reaches max_batch_records (10 * 2 = 20 rows) -> pops immediately without waiting full 1s!
        for _ in 0..9 {
            tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();
        }
        let threshold_lease = tokio::time::timeout(Duration::from_millis(100), buf.next_lease())
            .await
            .expect("Reaching max_batch_records must trigger immediate lease")
            .unwrap()
            .unwrap();
        assert_eq!(threshold_lease.num_rows(), 20);
        assert_eq!(threshold_lease.id().first_seq, 1);
        assert_eq!(threshold_lease.id().last_seq, 10);
        threshold_lease.ack().await.unwrap();

        // 2. Partial batch pops once max_batch_interval_sec (1s) elapses
        let start = Instant::now();
        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();
        let timer_lease = tokio::time::timeout(Duration::from_millis(1_500), buf.next_lease())
            .await
            .expect("Interval timer must flush partial batch")
            .unwrap()
            .unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(850),
            "Partial batch must wait for max_batch_interval_sec before flushing"
        );
        assert_eq!(timer_lease.num_rows(), 2);
        timer_lease.ack().await.unwrap();

        // 3. Schema boundary queued behind front batch flushes front batch immediately
        let alt_schema = Arc::new(Schema::new(vec![Field::new(
            "other_col",
            DataType::Int64,
            false,
        )]));
        let alt_batch =
            RecordBatch::try_new(alt_schema, vec![Arc::new(Int64Array::from(vec![99]))]).unwrap();
        tx.send(SignalBatch::Logs(sample.clone())).await.unwrap();
        tx.send(SignalBatch::Logs(alt_batch)).await.unwrap();
        let boundary_lease = tokio::time::timeout(Duration::from_millis(100), buf.next_lease())
            .await
            .expect("Coalescing boundary must flush front batch immediately")
            .unwrap()
            .unwrap();
        assert_eq!(boundary_lease.num_rows(), 2);
        boundary_lease.ack().await.unwrap();

        // 4. Dropping sender (ring.closed) flushes remaining partial batch immediately
        drop(tx);
        let closed_lease = tokio::time::timeout(Duration::from_millis(100), buf.next_lease())
            .await
            .expect("Closing sender must flush remaining batch immediately")
            .unwrap()
            .unwrap();
        assert_eq!(closed_lease.num_rows(), 1);
        closed_lease.ack().await.unwrap();
        assert!(buf.next_lease().await.unwrap().is_none());
    }
}
