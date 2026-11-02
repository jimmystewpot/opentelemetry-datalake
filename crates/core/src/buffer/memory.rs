use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

/// State of an active (unacknowledged) sequence number slot in a signal ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseSlotState {
    /// Sequence number is reserved for an in-flight disk append and not yet in `queue`.
    Reserved,
    /// Batch is waiting in the ring queue to be leased.
    Queued,
    /// Batch is currently leased to a sink task.
    InFlight,
}

/// Storage payload for a queued or leased batch inside a [`SignalRing`].
#[derive(Debug, Clone)]
pub enum BatchPayload {
    /// In-memory Arrow `RecordBatch` (`BufferMode::Memory`).
    InMemory(RecordBatch),
    /// On-disk frame pointer (`BufferMode::Disk`).
    OnDisk {
        /// Segment file identifier (`segment-<segment_id:020>.bin`).
        segment_id: u64,
        /// Byte offset of the 28-byte `OTLB` frame header within the segment file.
        offset: u64,
        /// Total frame length in bytes (`28 + payload_len`).
        len: u32,
        /// Number of rows in the encoded `RecordBatch`.
        rows: u32,
        /// Cached Arrow schema used for lease coalescing homogeneity checks.
        schema: SchemaRef,
    },
}

impl BatchPayload {
    /// Returns the Arrow [`SchemaRef`] of this payload.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::InMemory(batch) => batch.schema(),
            Self::OnDisk { schema, .. } => Arc::clone(schema),
        }
    }
}

/// Metadata and payload for a queued Arrow `RecordBatch` inside a [`SignalRing`].
#[derive(Debug, Clone)]
pub struct QueuedBatch {
    /// Monotonically increasing per-signal sequence number (`>= 1`).
    pub seq_id: u64,
    /// In-memory or on-disk batch payload.
    pub payload: BatchPayload,
    /// Memory footprint or on-disk frame size charged to the signal ring budget.
    pub byte_size: usize,
    /// Number of rows in the batch.
    pub row_count: usize,
    /// Timestamp when the batch first entered the buffer.
    pub enqueued_at: Instant,
    /// Timestamp when the batch first experienced a transient delivery failure (`None` while healthy).
    pub first_failed_at: Option<Instant>,
    /// Number of previous delivery attempts (`0` on initial enqueue).
    pub attempt: u32,
    /// Optional backoff deadline before which this batch cannot be leased.
    pub ready_at: Option<Instant>,
}

/// Per-batch metadata retained inside an active [`crate::buffer::Lease`] for requeue or ack.
#[derive(Debug, Clone)]
pub struct LeasedBatchMeta {
    /// Sequence number of the batch.
    pub seq_id: u64,
    /// Byte size charged to the signal ring for this batch.
    pub byte_size: usize,
    /// Number of rows in this batch.
    pub row_count: usize,
    /// Original enqueue timestamp of the batch.
    pub enqueued_at: Instant,
    /// Timestamp when the batch first experienced a transient delivery failure (`None` while healthy).
    pub first_failed_at: Option<Instant>,
}

/// Mutable state for a single telemetry signal ring (`Logs`, `Metrics`, or `Traces`).
#[derive(Debug)]
pub struct SignalRing {
    /// FIFO queue of unleased batches.
    pub queue: VecDeque<QueuedBatch>,
    /// Sequence tracking table for out-of-order watermark progression.
    pub slots: BTreeMap<u64, LeaseSlotState>,
    /// Original enqueue timestamps for currently `InFlight` sequences so `oldest_item_age_seconds`
    /// remains accurate while batches are leased.
    pub inflight_enqueued_at: BTreeMap<u64, Instant>,
    /// Next sequence number to assign on `send()` (starts at `1`).
    pub next_seq: u64,
    /// Total bytes currently held in `Queued` and `InFlight` batches.
    pub bytes_in_flight: usize,
    /// Total rows currently held in `Queued` and `InFlight` batches.
    pub rows_in_flight: usize,
    /// Number of currently active `InFlight` leases for this signal.
    pub inflight_leases: usize,
    /// Cumulative number of rows evicted under `OverflowPolicy::DropOldest`.
    pub evicted_records: u64,
    /// On-disk frame bytes evicted out of order while an earlier slot is still `InFlight` or `Reserved`
    /// and therefore not yet reclaimable by segment GC until `committed_seq` advances past their `seq_id`.
    pub evicted_unreclaimed_bytes: BTreeMap<u64, usize>,
    /// Sum of bytes in `evicted_unreclaimed_bytes`.
    pub unreclaimed_disk_bytes: usize,
    /// Whether all `BufferSender`s for this signal have been dropped.
    pub closed: bool,
    /// Whether the `SinkBuffer` consumer for this signal has been dropped.
    pub receiver_dropped: bool,
    /// Fatal error message if the ring has transitioned to `Failed`.
    pub fatal_error: Option<String>,
}

impl Default for SignalRing {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalRing {
    /// Creates an empty [`SignalRing`] starting at sequence number `1`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            slots: BTreeMap::new(),
            inflight_enqueued_at: BTreeMap::new(),
            next_seq: 1,
            bytes_in_flight: 0,
            rows_in_flight: 0,
            inflight_leases: 0,
            evicted_records: 0,
            evicted_unreclaimed_bytes: BTreeMap::new(),
            unreclaimed_disk_bytes: 0,
            closed: false,
            receiver_dropped: false,
            fatal_error: None,
        }
    }

    /// Inserts a [`QueuedBatch`] into `queue` preserving ascending `seq_id` order.
    pub fn enqueue_ordered(&mut self, item: QueuedBatch) {
        if self
            .queue
            .back()
            .is_none_or(|back| back.seq_id < item.seq_id)
        {
            self.queue.push_back(item);
        } else if self
            .queue
            .front()
            .is_some_and(|front| item.seq_id < front.seq_id)
        {
            self.queue.push_front(item);
        } else {
            let pos = self
                .queue
                .iter()
                .position(|existing| existing.seq_id > item.seq_id)
                .unwrap_or(self.queue.len());
            self.queue.insert(pos, item);
        }
    }

    /// Advances `committed_seq` to the highest contiguous sequence number below the
    /// earliest active slot in `slots` (or `next_seq - 1` when no active slots remain),
    /// and releases any evicted on-disk byte charges that are now `<= watermark`.
    pub fn advance_watermark(&mut self, committed_seq: &AtomicU64) {
        let candidate = self.slots.first_key_value().map_or_else(
            || self.next_seq.saturating_sub(1),
            |(&first_unacked, _)| first_unacked.saturating_sub(1),
        );
        let prev = committed_seq.fetch_max(candidate, Ordering::AcqRel);
        let watermark = prev.max(candidate);
        while let Some((&seq, _)) = self.evicted_unreclaimed_bytes.first_key_value() {
            if seq <= watermark {
                if let Some(bytes) = self.evicted_unreclaimed_bytes.remove(&seq) {
                    self.unreclaimed_disk_bytes = self.unreclaimed_disk_bytes.saturating_sub(bytes);
                }
            } else {
                break;
            }
        }
    }

    /// Records the eviction of `evicted` from `queue`, retaining its byte charge in
    /// `unreclaimed_disk_bytes` when `evicted.payload` is `OnDisk` and an earlier slot
    /// still prevents the contiguous watermark from advancing past `evicted.seq_id`.
    pub fn record_evicted_batch(
        &mut self,
        evicted: &QueuedBatch,
        committed_seq: &AtomicU64,
    ) -> u64 {
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(evicted.byte_size);
        self.rows_in_flight = self.rows_in_flight.saturating_sub(evicted.row_count);
        let rows = u64::try_from(evicted.row_count).unwrap_or(u64::MAX);
        self.evicted_records = self.evicted_records.saturating_add(rows);
        self.slots.remove(&evicted.seq_id);
        if matches!(evicted.payload, BatchPayload::OnDisk { .. }) {
            self.evicted_unreclaimed_bytes
                .insert(evicted.seq_id, evicted.byte_size);
            self.unreclaimed_disk_bytes = self
                .unreclaimed_disk_bytes
                .saturating_add(evicted.byte_size);
        }
        self.advance_watermark(committed_seq);
        rows
    }

    /// Requeues leased batch payloads into `queue` in ascending `seq_id` order, ignoring any
    /// slots that were already discarded by `finalize_shutdown_timeout`.
    pub fn requeue_at_head(
        &mut self,
        payloads: Vec<BatchPayload>,
        meta: Vec<LeasedBatchMeta>,
        next_attempt: u32,
        ready_at: Option<Instant>,
    ) {
        let mut any_requeued = false;
        for (payload, item) in payloads.into_iter().zip(meta).rev() {
            self.inflight_enqueued_at.remove(&item.seq_id);
            if self.slots.get(&item.seq_id) != Some(&LeaseSlotState::InFlight) {
                continue;
            }
            any_requeued = true;
            self.slots.insert(item.seq_id, LeaseSlotState::Queued);
            self.enqueue_ordered(QueuedBatch {
                seq_id: item.seq_id,
                payload,
                byte_size: item.byte_size,
                row_count: item.row_count,
                enqueued_at: item.enqueued_at,
                first_failed_at: item.first_failed_at,
                attempt: next_attempt,
                ready_at,
            });
        }
        if any_requeued {
            self.inflight_leases = self.inflight_leases.saturating_sub(1);
        }
    }

    /// Removes all acknowledged batch sequence numbers in `meta` from `slots`, releases
    /// their byte/row budget, advances the contiguous watermark, and returns the number
    /// of active in-flight rows acknowledged.
    pub fn ack_entries(
        &mut self,
        meta: &[LeasedBatchMeta],
        _byte_size: usize,
        _num_rows: usize,
        committed_seq: &AtomicU64,
    ) -> usize {
        let mut acked_bytes = 0usize;
        let mut acked_rows = 0usize;
        let mut any_acked = false;
        for item in meta {
            self.inflight_enqueued_at.remove(&item.seq_id);
            if self.slots.remove(&item.seq_id) == Some(LeaseSlotState::InFlight) {
                any_acked = true;
                acked_bytes = acked_bytes.saturating_add(item.byte_size);
                acked_rows = acked_rows.saturating_add(item.row_count);
            }
        }
        if any_acked {
            self.bytes_in_flight = self.bytes_in_flight.saturating_sub(acked_bytes);
            self.rows_in_flight = self.rows_in_flight.saturating_sub(acked_rows);
            self.inflight_leases = self.inflight_leases.saturating_sub(1);
            self.advance_watermark(committed_seq);
        }
        acked_rows
    }

    /// Returns the approximate Unix epoch millisecond timestamp of the oldest queued or
    /// in-flight batch, or `0` if the buffer ring is completely empty.
    #[must_use]
    pub fn oldest_enqueued_unix_ms(&self) -> u64 {
        let oldest_queued = self.queue.front().map(|b| b.enqueued_at);
        let oldest_inflight = self.inflight_enqueued_at.values().copied().min();
        let oldest = match (oldest_queued, oldest_inflight) {
            (Some(q), Some(i)) => q.min(i),
            (Some(q), None) => q,
            (None, Some(i)) => i,
            (None, None) => return 0,
        };
        let elapsed_ms = u64::try_from(oldest.elapsed().as_millis()).unwrap_or(u64::MAX);
        crate::buffer::dead_letter::current_unix_ms()
            .saturating_sub(elapsed_ms)
            .max(1)
    }
}

/// Synchronized state and notifications for one signal ring.
#[derive(Debug)]
pub struct SignalRingState {
    /// Mutex-protected signal ring queue and lease slot map.
    pub ring: Mutex<SignalRing>,
    /// Notifies blocked `BufferSender::send` calls when byte budget is freed or state changes.
    pub space_available_notify: Notify,
    /// Notifies waiting `SinkBuffer::next_lease` calls when batches arrive or lease slots free.
    pub data_ready_notify: Notify,
    /// Contiguous low-watermark sequence number committed for this signal.
    pub committed_seq: AtomicU64,
    /// Number of active `BufferSender` handles capable of sending to this signal ring.
    pub active_senders: AtomicU64,
    /// Cumulative number of records dropped due to on-disk frame corruption during recovery.
    pub corruption_drops: AtomicU64,
    /// Cumulative number of records replayed from disk during startup recovery.
    pub replayed_records: AtomicU64,
    /// Cumulative number of records quarantined or dropped after exhausting rejected retries.
    pub dead_letter_drops: AtomicU64,
    /// Cumulative number of records discarded on shutdown drain timeout (`DrainTimeoutPolicy::Discard`).
    pub shutdown_timeout_drops: AtomicU64,
    /// Number of `BufferSender::send` futures currently blocked waiting for ring capacity.
    pub blocked_senders: AtomicU64,
    /// Unix epoch timestamp in milliseconds when the ring first entered a continuous blocked-send state (`0` when unblocked).
    pub blocked_since_unix_ms: AtomicU64,
    /// Unix epoch timestamp in milliseconds when the current continuous `Transient` retry streak started (`0` when healthy).
    pub first_transient_failure_unix_ms: AtomicU64,
}

impl Default for SignalRingState {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalRingState {
    /// Creates a new [`SignalRingState`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            ring: Mutex::new(SignalRing::new()),
            space_available_notify: Notify::new(),
            data_ready_notify: Notify::new(),
            committed_seq: AtomicU64::new(0),
            active_senders: AtomicU64::new(0),
            corruption_drops: AtomicU64::new(0),
            replayed_records: AtomicU64::new(0),
            dead_letter_drops: AtomicU64::new(0),
            shutdown_timeout_drops: AtomicU64::new(0),
            blocked_senders: AtomicU64::new(0),
            blocked_since_unix_ms: AtomicU64::new(0),
            first_transient_failure_unix_ms: AtomicU64::new(0),
        }
    }
}
