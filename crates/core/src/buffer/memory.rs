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
}

/// Mutable state for a single telemetry signal ring (`Logs`, `Metrics`, or `Traces`).
#[derive(Debug)]
pub struct SignalRing {
    /// FIFO queue of unleased batches.
    pub queue: VecDeque<QueuedBatch>,
    /// Sequence tracking table for out-of-order watermark progression.
    pub slots: BTreeMap<u64, LeaseSlotState>,
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
            next_seq: 1,
            bytes_in_flight: 0,
            rows_in_flight: 0,
            inflight_leases: 0,
            evicted_records: 0,
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
    /// earliest active slot in `slots` (or `next_seq - 1` when no active slots remain).
    pub fn advance_watermark(&mut self, committed_seq: &AtomicU64) {
        let candidate = self.slots.first_key_value().map_or_else(
            || self.next_seq.saturating_sub(1),
            |(&first_unacked, _)| first_unacked.saturating_sub(1),
        );
        committed_seq.fetch_max(candidate, Ordering::AcqRel);
    }

    /// Requeues leased batch payloads at the front of `queue` in their original sequence order.
    pub fn requeue_at_head(
        &mut self,
        payloads: Vec<BatchPayload>,
        meta: Vec<LeasedBatchMeta>,
        next_attempt: u32,
        ready_at: Option<Instant>,
    ) {
        for (payload, item) in payloads.into_iter().zip(meta).rev() {
            self.slots.insert(item.seq_id, LeaseSlotState::Queued);
            self.queue.push_front(QueuedBatch {
                seq_id: item.seq_id,
                payload,
                byte_size: item.byte_size,
                row_count: item.row_count,
                enqueued_at: item.enqueued_at,
                attempt: next_attempt,
                ready_at,
            });
        }
        self.inflight_leases = self.inflight_leases.saturating_sub(1);
    }

    /// Removes all acknowledged batch sequence numbers in `meta` from `slots`, releases
    /// their byte/row budget, and advances the contiguous watermark.
    pub fn ack_entries(
        &mut self,
        meta: &[LeasedBatchMeta],
        byte_size: usize,
        num_rows: usize,
        committed_seq: &AtomicU64,
    ) {
        for item in meta {
            self.slots.remove(&item.seq_id);
        }
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(byte_size);
        self.rows_in_flight = self.rows_in_flight.saturating_sub(num_rows);
        self.inflight_leases = self.inflight_leases.saturating_sub(1);
        self.advance_watermark(committed_seq);
    }

    /// Returns the approximate Unix epoch millisecond timestamp of the oldest queued batch,
    /// or `0` if the queue is empty.
    #[must_use]
    pub fn oldest_enqueued_unix_ms(&self) -> u64 {
        let Some(front) = self.queue.front() else {
            return 0;
        };
        let elapsed_ms = u64::try_from(front.enqueued_at.elapsed().as_millis()).unwrap_or(u64::MAX);
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
