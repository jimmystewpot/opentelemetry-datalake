# Buffer Architecture & Specification

This document specifies data buffering, batch coalescing, durability, retry and backpressure behavior for `opentelemetry-datalake`.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

For general codebase engineering rules (zero-panic policy, Arrow memory layout, async/blocking isolation), see [AGENTS.md](../AGENTS.md).

---

## 0. Status & Conventions

> [!IMPORTANT]
> **Sections 1 and 2 describe current behavior. Sections 3–10 are a target specification that is NOT implemented.** No `[<sink>.buffer]` configuration key, `SinkBuffer` type, or `BufferSender` type exists in the codebase today. Do not configure or depend on them until the corresponding phase in §9 has landed. Other documents in `docs/` continue to describe current behavior and are updated by the implementing change (see §10).

Every normative section is labeled **Current** or **Target**. Open questions are collected in §11 and MUST be closed before the implementation plan is written.

### 0.1 Decision Log

| ID | Decision | Rationale |
| :--- | :--- | :--- |
| D1 | The `Sink` and `Transform` traits are changed for all five sinks in one change (no parallel trait). Behavioral adoption is phased (§9): sinks not yet migrated receive a *passthrough* buffer and keep their internal accumulation. | Single trait surface; avoids a long-lived dual-trait `main.rs`. Phasing preserves the "not every sink at once" goal. |
| D2 | A batch that fails permanently is quarantined to a local Arrow IPC dead-letter directory, acked, and the ring continues. | Prevents head-of-line blocking without silent loss. Reuses the existing `FileDlqSink` pattern. |
| D3 | Graceful shutdown **drains to the sink within a timeout**, then applies an operator-selected `on_drain_timeout` of `persist` (disk mode only) or `discard`. | Bounded termination; operator chooses between durability and loss. |
| D4 | Delivery semantics are **at-least-once**. Exactly-once is out of scope; duplicate windows are enumerated in §7. | Honest contract; idempotency is a later phase. |

---

## 1. Pipeline Channel Buffering (Source → Transform)

### 1.1 Current behavior

*   **Fixed capacity**: Six channels (`logs_tx`, `traces_tx`, `metrics_tx` source→transformer; `logs_sink_tx`, `traces_sink_tx`, `metrics_sink_tx` transformer→sink) are created in [`src/main.rs`](../src/main.rs) with a hard-coded capacity of **1,000** `SignalBatch`es. No configuration knob exists (`worker_channel_capacity` applies only to the WASM transformer).
*   **WASM worker queues**: With `[wasm_transformer]` enabled, [`WasmDispatcher::spawn_workers`](../crates/wasm-transformer/src/dispatcher.rs) adds `3 × concurrency` further `mpsc::channel(worker_channel_capacity)` queues (default `3 × 4 × 64 = 768` slots).
*   **Unbounded bytes**: `tokio::sync::mpsc` bounds message count, not bytes. With 5–25 MiB batches, the six primary channels alone can hold `6 × 1,000 × (5–25 MiB) = 30–150 GiB` (#125).
*   **Backpressure**: A full channel leaves `.send().await` pending indefinitely. `OtlpReceiverSource` returns `503` / gRPC `UNAVAILABLE` only when a channel is **closed** (#142).
*   **Ingress limits**: Listeners enforce no explicit body-size, decompression, or concurrency limits (#125, #140).

### 1.2 Target

*   Source→transformer channel capacity **MUST** be configurable and default to **64** batches (#125).
*   **Configuration location**: `PipelineConfig` is `#[serde(flatten)]`-ed into `AppConfig`, so its fields are **top-level** TOML keys (as `[telemetry]` is). The key is therefore `channel_capacity` at top level. A `[pipeline]` table would be ignored. Issue #125's `[pipeline] channel_capacity` wording MUST be corrected.
*   The transformer→sink hop is no longer an `mpsc` channel; it is the `SinkBuffer` (§3).
*   When a downstream queue stays full beyond a bounded timeout, receivers **MUST** shed with HTTP `503` / gRPC `UNAVAILABLE` and increment `datalake.receiver.requests_shed_total` (#142).
*   HTTP listeners **MUST** apply `DefaultBodyLimit::max` and `ConcurrencyLimitLayer`; gRPC services **MUST** apply `max_decoding_message_size` (decompressed) and a concurrency limit (#125, #140).

---

## 2. Sink Batching (Current)

[`src/main.rs`](../src/main.rs) spawns **three independent sink instances** per configured backend (logs, traces, metrics).

### 2.1 Per-sink matrix

| Sink | Internal buffer | When `batching` is omitted | Default thresholds (per signal instance) | Process-wide bound (3 instances) |
| :--- | :--- | :--- | :--- | :--- |
| `IcebergSink` (`storage`) | `HashMap<String, TableBuffer>` | **Buffers** using fallback constants in `should_flush` | 128 MiB (`134,217,728`), 60 s, `max_batch_records` optional | 384 MiB |
| `StarRocksSink` | `BufferState` ×3 | **Flushes every batch immediately** | 50 MiB (`52,428,800`), 30 s, `max_batch_records` optional. `max_payload_bytes` defaults to 100 MiB (`104,857,600`) | 150 MiB (when configured) |
| `ElasticsearchSink` | `BufferState` ×3 | **Flushes every batch immediately** | 10 MiB (`10,485,760`), 50,000 records, 10 s | 30 MiB (when configured) |
| `KafkaSink` | none (`librdkafka` `FutureProducer` queue) | No Arrow accumulation; slices by `partition_key` and sends | `librdkafka` `options` (`queue.buffering.max.kbytes`, `linger.ms`) | `librdkafka` queue × 3 |
| `ParquetSink` | `PartitionManager` writer LRU | Always streams into open partition writers | 64 MiB, 60 s, 500,000 records, 16 open partitions | `global_memory_limit_bytes` (1 GiB) `/ 3` in `main.rs`; up to 48 open partitions |

### 2.2 Known anomalies

1.  **Premature buffer clearing (#127)**: `IcebergSink::flush_table` and `ElasticsearchSink::flush_buffer` `std::mem::take` their batches *before* concat, upload and commit. `StarRocksSink`/`ElasticsearchSink` keep per-signal buffers as `run()` locals that are dropped when `flush_buffer(...).await?` propagates an error. `ElasticsearchSink::flush_buffer` returns before its spawned bulk requests complete.
2.  **CPU on async threads (#132)**: `IcebergSink`, `StarRocksSink`, `KafkaSink`, and `OtlpReceiverSource` run concat, sort, partition, serialization, or decode inline. `WasmDispatcher` calls `WasmWorker::rejuvenate()` on the async task after a trap. `ElasticsearchSink::flush_buffer` is already compliant.
3.  **Unused per-signal buffers**: each of the three sink instances allocates logs/metrics/traces buffers but receives one signal.
4.  **Error semantics**: `StarRocksSink` maps every SDK failure to `PipelineError::DownstreamClosed`; any sink error ends the task and [`supervise_sink_exit`](../src/main.rs) shuts the whole pipeline down. There is no retry, classification, or quarantine.

---

## 3. Target Architecture: `SinkBuffer` (`pipeline_core::buffer`)

### 3.1 Topology

```text
 [ Source ] ──mpsc(channel_capacity)──► [ Transform ] ──BufferSender::send──►
        ┌────────────────────────────────────────────────────────────────────┐
        │ SinkBuffer  (one per signal per sink; owns the ring + counters)    │
        │   Backend: MemoryRing | DiskRing (NVMe segmented log)              │
        │   Policy : thresholds, overflow, retry/backoff, dead-letter        │
        └──────────────────────────────┬─────────────────────────────────────┘
                                       │ next_lease()  →  Lease
                                       ▼
                     [ Sink ]  lease.prepare(closure)   (one spawn_blocking)
                          │            │
                          │     lease.ack()  /  lease.nack(FailureClass)
                          ▼
                    destination
```

### 3.2 Trait changes (D1)

The `Source` trait is unchanged. `Transform` and `Sink` change as follows (signatures illustrative; final shapes are fixed in the implementation plan):

```rust
/// Producer handle returned to a Transform. Cloneable. Replaces PipelineSender on the
/// transformer -> sink hop only; the source -> transformer hop and DLQ senders stay mpsc.
pub struct BufferSender { /* Arc<shared> */ }
impl BufferSender {
    /// Waits (bounded by overflow policy) for capacity. Err = buffer closed or failed.
    pub async fn send(&self, batch: SignalBatch) -> Result<(), PipelineError>;
}

#[async_trait]
pub trait Transform: Send + Sync {
    async fn transform(&mut self, input: PipelineReceiver, output: BufferSender)
        -> Result<(), PipelineError>;
}

pub struct SinkBuffer { /* consumer handle */ }
impl SinkBuffer {
    /// Resolves with the next lease once a flush trigger fires and an in-flight slot is free.
    /// Ok(None) = all senders dropped AND the ring is fully drained (clean EOF).
    pub async fn next_lease(&mut self) -> Result<Option<Lease>, PipelineError>;
}

/// Owned (holds an Arc), so it can be moved into a spawned task (Elasticsearch).
pub struct Lease { /* … */ }
impl Lease {
    pub fn signal(&self) -> SignalType;
    pub fn rows(&self) -> usize;
    pub fn bytes(&self) -> usize;
    pub fn seq_range(&self) -> std::ops::RangeInclusive<u64>; // stable commit-token material
    pub fn attempt(&self) -> u32;
    /// Decode (disk) + concat_batches (bypassed when one batch) + `f`, in ONE spawn_blocking.
    pub async fn prepare<T, F>(&self, f: F) -> Result<T, PipelineError>
    where F: FnOnce(RecordBatch) -> Result<T, PipelineError> + Send + 'static, T: Send + 'static;
    pub async fn ack(self) -> Result<(), PipelineError>;
    pub async fn nack(self, class: FailureClass) -> Result<(), PipelineError>;
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn run(&mut self, input: SinkBuffer) -> Result<(), PipelineError>;
}
```

Consequences: `noop-transformer` and `wasm-transformer` change their `output` type; all five `Sink` implementations change their `run` signature. `WasmDispatcher`'s `DlqOutput::Sender(PipelineSender)` is unaffected.

**Passthrough buffer (Phase 1).** `BufferConfig::passthrough()` yields memory mode where each lease is exactly one queued batch (no coalescing, no time trigger). A sink not yet migrated MUST use it and **acks on receipt**, then keeps its existing internal accumulation. Such a sink remains non-compliant with §4 buffer retention (#127) until migrated, and its tracking issue MUST say so.

### 3.3 Task and concurrency model

*   The buffer **MUST NOT** spawn detached tasks (AGENTS.md §5). Flush timers are driven inside `next_lease()`, which the sink awaits in its own supervised `tokio::select!` loop.
*   Ring metadata (queue indices, lease table, acked-range set) is guarded by a `std::sync::Mutex` that is held only for O(1) operations and **never across an `.await`**. Push rate is per-batch (not per-record). Occupancy and counters used for telemetry and readiness are `AtomicU64` (AGENTS.md §13.3).
*   `Lease::drop` without `ack`/`nack` performs a **synchronous** requeue as `FailureClass::Transient` and logs `WARN` (async work in `Drop` is not possible).
*   The disk backend owns a dedicated OS writer thread (group commit), created by the buffer and **joined in `close()`**; its failures surface as `Err` on the next `send`/`next_lease` (never swallowed).

### 3.4 Lease state machine

```mermaid
stateDiagram-v2
    [*] --> Queued: BufferSender::send
    Queued --> Evicted: overflow_policy = drop_oldest
    Queued --> Leased: trigger fired and in-flight slot free
    Leased --> Committed: ack()
    Leased --> Queued: nack(Transient) after backoff
    Leased --> Queued: nack(Rejected), attempt < max_attempts
    Leased --> DeadLettered: nack(Rejected), attempts exhausted
    Leased --> Failed: nack(Fatal)
    DeadLettered --> Committed: dead-letter written then acked
    Committed --> [*]
    Evicted --> [*]
    Failed --> [*]
```

*   Entries in `Queued` **and** `Leased` count against the byte budget (§5.2).
*   `max_inflight_leases` bounds concurrent leases (default `1`; Elasticsearch sets it to its `max_concurrent_requests`).
*   **Ack granularity is the lease.** A sink that splits a lease into several sub-requests (Elasticsearch `serialize_batch_chunks`, Kafka partition slices) **MUST** join all sub-requests and ack only if every one succeeded; otherwise it nacks the whole lease.
*   **Out-of-order acks.** Leases may be acked out of order. The buffer keeps an in-memory set of acked seq ranges and advances a **contiguous low-watermark**. Only the low-watermark is persisted (§6.4). After a crash, any lease acked above the watermark is replayed (a duplicate window, §7).

### 3.5 Prepare hook (sink-specific CPU work)

The buffer owns leasing and coalescing; **each sink owns its pre-processing**, supplied as the closure passed to `Lease::prepare`. This is required because the order of operations differs per sink:

| Sink | Operations inside `prepare` (single `spawn_blocking`) | Notes |
| :--- | :--- | :--- |
| `IcebergSink` | `apply_schema_mode` → `BatchSorter::sort` → `partition_batch` | The catalog table is loaded (`async`) **between** `next_lease()` and `prepare`. Sorting must follow schema mode because `Catalog` mode drops and casts columns. |
| `StarRocksSink` | `BatchSorter::sort` → `inject_signal_type_column` → `serialize` (IPC/NDJSON/CSV) | Payload size check (`max_payload_bytes`) is a `Rejected` failure. |
| `ElasticsearchSink` | `BatchSorter::sort` → `serialize_batch_chunks` | Already implemented this way today. |
| `KafkaSink` | `sort_with_extra_lead_column` → `extract_partition_slices` → per-slice serialize | Default passthrough lease (no accumulation) preserves today's latency. |

---

## 4. Failure Handling, Retry and Dead-Letter (D2)

### 4.1 Failure classes

A sink classifies every failed lease:

| `FailureClass` | Meaning | Buffer behavior |
| :--- | :--- | :--- |
| `Transient` | Outage-like: network error, 5xx, 429, catalog commit conflict, I/O timeout | Requeue with exponential backoff (`initial_backoff_ms`, `multiplier`, `max_backoff_sec`, full jitter). **Retried indefinitely**; never dead-lettered. The ring provides durability and backpressure during the outage. Optional `max_transient_duration_sec` (default `0` = unlimited) converts a prolonged outage into `Fatal`. |
| `Rejected` | Data-specific and not fixable by retry: schema mismatch, HTTP 400, oversized payload, serialization failure | Retry up to `max_attempts` (default `3`, absorbing misclassification), then dead-letter and ack. |
| `Fatal` | Unrecoverable sink or config fault: authentication/authorization failure, invalid configuration | Lease is retained; the sink returns `Err`; the supervisor shuts the pipeline down (existing behavior). |

Backoff scheduling lives in the buffer (`next_lease` waits on `not_before`); sinks do not sleep.

### 4.2 Initial classification (to be finalized per sink issue)

| Sink | Transient | Rejected | Fatal |
| :--- | :--- | :--- | :--- |
| Iceberg | catalog/REST 5xx, commit conflict (OCC), object-store I/O | `apply_schema_mode` mismatch (`PipelineError::Internal`), missing sort column with `on_missing_column = error` | 401/403 |
| StarRocks | Stream Load failure after SDK retries (today: all mapped to `DownstreamClosed`; MUST be refined) | `max_payload_bytes` exceeded | auth failure |
| Elasticsearch | bulk transport error, 429/503 after in-client retries | item-level 400 (today logged and dropped in-client; MUST be routed to dead-letter) | 401/403 |
| Kafka | retriable delivery error, queue full | `MessageSizeTooLarge` | SASL/auth failure |

### 4.3 Dead-letter store

*   Quarantined data is the lease's **original, unprepared** batches written as Arrow IPC (atomic temp-file + rename + directory fsync, the pattern in `FileDlqSink::persist_ipc_payload`), so it can be reprocessed after a fix.
*   `dead_letter_path` defaults to `<disk_path>/dead-letter/<sink>/<signal>` (disk mode) or `./dlq/buffer/<sink>/<signal>` (memory mode, consistent with the existing relative `dlq/` convention).
*   `max_dead_letter_bytes` (default 1 GiB) bounds the store. If a dead-letter write fails or the bound is exceeded the sink **MUST** return `Err` (data is never silently discarded here).
*   The shared writer and path-component validation (`is_safe_path_component`) **MUST** move from the binary (`src/main.rs`) into `pipeline_core::buffer::dead_letter`; `FileDlqSink` delegates to it.
*   Metric: `datalake.sink.records_dropped_total{reason="dead_letter"}` and an `ERROR` log with lease range and failure reason.

---

## 5. Capacity, Memory Accounting and Overflow

### 5.1 Configuration scope

`max_buffer_bytes` is the **total for one sink configuration** and is divided equally among the signal buffers it owns (three), mirroring the existing `parquet.global_memory_limit_bytes / 3` precedent. This removes the 3× multiplier described in §2.2.

### 5.2 Accounting

*   **Memory mode**: an entry costs `RecordBatch::get_array_memory_size()`. This is an **upper bound** (it counts whole parent buffers for sliced batches) and the over-count is accepted for safety.
*   **Disk mode**: an entry costs its on-disk frame size.
*   **Transient working set**: preparing a lease holds the source batches plus the concatenated copy plus the prepared copy(ies), approximately `2–3 ×` lease bytes, **in addition to** the ring budget. Worst-case resident bytes per signal buffer:

$$
\text{budget}_{signal} + \text{max\_inflight\_leases} \times 3 \times \text{max\_batch\_size\_bytes}
$$

    Operators MUST size container memory limits against this formula (jemalloc fragmentation is additional).

### 5.3 Startup validation (all MUST fail startup with a clear error)

| Rule | Condition |
| :--- | :--- |
| V1 | `max_batch_size_bytes × (1 + max_inflight_leases) ≤ max_buffer_bytes / 3` |
| V2 | `mode = "disk"` requires `disk_path` and a writable directory with free space ≥ `max_buffer_bytes` |
| V3 | `on_drain_timeout = "persist"` requires `mode = "disk"` |
| V4 | `[<sink>.buffer]` and legacy `[<sink>.batching]` MUST NOT both be set |
| V5 | `segment_size_bytes ≤ max_buffer_bytes / 3 / 2` |

V1 rationale: if a batch trigger exceeds the budget, only the interval trigger would ever fire, producing degenerate small flushes (and, for Iceberg, the small-files problem). It is a stall and efficiency hazard, not a permanent deadlock.

### 5.4 Overflow policy

*   **`block` (default)**: `send` waits until acks free capacity. Backpressure reaches the receiver via the bounded send timeout (#142), which sheds with `503` / `UNAVAILABLE`.
*   **`drop_oldest`**: evicts the oldest **unleased** entries (disk: the oldest segment containing no leased frames; if none exists, `send` blocks). Opt-in only; it contradicts at-least-once.
    *   Reconciliation with [components.md](components.md) "MUST NOT drop batches silently": a drop is *not silent* when it is counted in `datalake.sink.records_dropped_total{reason="overflow"}`, logged at `WARN` (rate-limited, with cumulative counts), and visible in `/debug/vars`.

---

## 6. Storage Backends

### 6.1 `memory` (default)

`VecDeque<RingEntry>` holding `Arc`-backed `RecordBatch`es: zero copies, zero serialization before coalescing. A process crash loses all buffered data.

### 6.2 `disk` (local NVMe)

Directory layout: `<disk_path>/<sink>/<signal>/` containing `LOCK`, `segment_<first_seq:020>.bin`, and `watermark.bin`.

*   **Exclusive ownership**: the buffer takes an advisory exclusive lock on `LOCK`; a second process using the same directory fails startup.
*   **Frame format** (28-byte fixed header, little-endian, followed by the Arrow IPC stream payload):

| Offset | Size | Field |
| ---: | ---: | :--- |
| 0 | 4 | Magic `OTLB` (`0x4F544C42`) |
| 4 | 1 | Format version (`1`) |
| 5 | 1 | Signal type |
| 6 | 2 | Reserved (`0`) |
| 8 | 8 | Sequence ID (`u64`, monotonic per buffer, persisted across restarts) |
| 16 | 4 | Row count (`u32`) |
| 20 | 4 | Payload length (`u32`) |
| 24 | 4 | CRC32 of payload |
| 28 | N | Arrow IPC stream payload |

*   **No `mmap`, no `unsafe`** (AGENTS.md §7). IPC encode/decode uses `arrow::ipc` inside the writer thread or `Lease::prepare`.
*   **Write path**: a dedicated writer thread performs group commit (several queued batches per `write` + optional `sync_data`). One `spawn_blocking` hop per `send` is explicitly rejected as too costly.
*   **Durability**: `sync_on_write = false` (default) survives process crash and restart on the same volume; it does **not** survive host power loss or node loss. `true` calls `sync_data` before `send` returns. Segment create, unlink, and watermark rename **MUST** be followed by a directory `fsync` (precedent: `FileDlqSink::persist_ipc_payload`).
*   **Segments**: roll at `segment_size_bytes` (default 64 MiB); a sealed segment is unlinked once every frame in it is at or below the watermark.
*   **Dependencies**: `crc32fast` is already in `Cargo.lock` (1.5.2, transitive via parquet/flate2). It is promoted to a direct dependency of `pipeline-core`. The advisory-lock mechanism (std `File::try_lock` vs a crate) is decided against the workspace MSRV in the implementation plan.

### 6.3 Errors and disk-full behavior

*   `ENOSPC`: treated as capacity exhaustion; behavior follows `overflow_policy` (`block` waits for acks to free segments).
*   Any other I/O error (`EIO`, corrupted writer state): the buffer enters `Failed`; the next `send`/`next_lease` returns `Err(PipelineError::Storage)`; the sink exits and the supervisor shuts down.

### 6.4 Recovery on startup

1.  Take the directory lock; read `watermark.bin` (the contiguous low-watermark).
2.  Scan segments in order; skip frames with `seq ≤ watermark`.
3.  Validate magic, version, length, and CRC. A torn tail frame (partial write) is truncated. A frame that fails validation **mid-segment**, or whose IPC payload cannot be decoded, is copied to the dead-letter store and counted in `records_dropped_total{reason="corruption"}`; recovery continues.
4.  Remaining frames become `Queued` ahead of new data (FIFO). The receiver MAY start accepting traffic once the scan completes; replay drains through normal leasing and is subject to the same backpressure.
5.  Replayed record counts are reported in `datalake.sink.buffer_replayed_records_total`.

### 6.5 Operational requirements (disk mode)

*   Use a local NVMe volume. Network filesystems and throttled network block storage are unsupported.
*   In Kubernetes use a PersistentVolume (StatefulSet) or hostPath tied to a **stable node identity**. An `emptyDir` is lost with the pod; a buffer on a terminated node is **not** adopted by other nodes (operators must reattach the volume; adoption tooling is out of scope).
*   Dirty page-cache memory is charged to the container cgroup; account for it in memory limits when `sync_on_write = false`.
*   Write volume is approximately `1 ×` ingest bytes (uncompressed IPC) plus replay reads; size NVMe endurance accordingly.

---

## 7. Delivery Semantics and Idempotency (D4)

The contract is **at-least-once**. Duplicate windows:

| Window | Backend | Cause |
| :--- | :--- | :--- |
| W1 | disk | Crash after the sink committed but before the watermark advanced |
| W2 | disk | Leases acked above the contiguous watermark at crash time |
| W3 | any | Sub-request partially succeeded, lease nacked and retried (Elasticsearch chunks, Kafka slices) |
| W4 | memory | Not a duplicate window: a crash **loses** buffered data (no replay) |

Idempotency by sink (scope and phase):

| Sink | Mechanism | Scope / limitation |
| :--- | :--- | :--- |
| StarRocks | Stream Load **label derived from the lease** (`otel-{signal}-{buffer_id}-{first_seq}-{last_seq}`), replacing `Uuid::now_v7()` in `make_label`. | Dedupes in-process retries of the same lease. After a restart lease boundaries may differ, so replay can still duplicate. Phase 3. |
| Iceberg | Record the lease's seq ranges in snapshot summary properties (the "batch delivery token" required by [lakehouse.md §2.2](lakehouse.md)); on replay skip frames whose seq is already covered. | **lakehouse.md §2.2 is a MUST that is not implemented today**; tracked separately. Phase 3. |
| Elasticsearch | Optional deterministic `_id` (`{buffer_id}-{seq}-{row}`) so retries return `409` (treated as success). | Costs the append-only fast path; opt-in, deferred. |
| Kafka | Idempotent producer (`enable.idempotence`) | Producer-session scoped only. |

Current known gaps (unchanged by this spec until implemented): only SIGINT triggers drain, not SIGTERM, and the drain join is unbounded (#126); Iceberg and Elasticsearch drop buffered batches on flush failure (#127); `KafkaSink` does not flush its producer queue on shutdown (#130). In-memory data is lost on `kill -9` or OOM.

---

## 8. Async / Blocking Isolation Contract (#132)

CPU-bound work MUST NOT run on Tokio worker threads (AGENTS.md §3).

| Component | Operation | Mechanism | Status |
| :--- | :--- | :--- | :--- |
| `SinkBuffer` | IPC decode, `concat_batches` (bypassed when one batch), sink `prepare` closure | One `spawn_blocking` per lease via `Lease::prepare` | Target |
| `SinkBuffer` (disk) | IPC encode, CRC, file append | Dedicated owned writer thread (AGENTS.md permits "dedicated CPU thread pools") | Target |
| `ElasticsearchSink` | concat, sort, NDJSON chunking | `spawn_blocking` | **Compliant today** |
| `IcebergSink` | concat, `apply_schema_mode`, sort, `partition_batch` | Move into `prepare` | Non-compliant today |
| `StarRocksSink` | concat, sort, signal-column injection, serialize | Move into `prepare` | Non-compliant today |
| `KafkaSink` | sort, slice, serialize | Move into `prepare` | Non-compliant today |
| `OtlpReceiverSource` (HTTP) | `decode_http_body` + `arrow_codec::decode_*` | `spawn_blocking` when `body.len() > 64 KiB` (length is free) | Non-compliant today |
| `OtlpReceiverSource` (gRPC) | `arrow_codec::decode_*` | `spawn_blocking` when estimated record count exceeds a threshold (sum of record vectors across resource/scope entries; O(resources × scopes), no message walk) | Non-compliant today |
| `WasmDispatcher` | `WasmWorker::rejuvenate()` after trap | Move inside the existing `spawn_blocking` closure | Non-compliant today |

**Limitation (explicit).** `tonic` decodes the protobuf **before** the handler runs, so gRPC protobuf decoding itself cannot be moved off the async worker with the current service wiring. Only the Arrow conversion is offloaded. Satisfying AGENTS.md's ">64 KB protobuf decode" rule for gRPC requires a custom bytes codec; this is recorded as open item O1 (§11), not assumed solved.

---

## 9. Rollout, Sequencing and Verification

### 9.1 Phases

| Phase | Scope | Notes |
| :--- | :--- | :--- |
| 0 | This document; correct pre-existing defects (§10.2) | No code behavior change |
| 0.5 | **#127 lands first** (Iceberg buffer retention, partition paths) | #127's comment requires this: #127 and #132 both rewrite `flush_table`, and the buffer replaces it a third time |
| 1 | `pipeline_core::buffer` memory backend, trait change (D1), passthrough buffer for all five sinks, `channel_capacity`, #132 fixes outside sinks (`otlp-receiver`, `wasm-transformer`) | Un-migrated sinks ack on receipt (§3.2) |
| 2 | Disk backend, dead-letter store, readiness predicate, shutdown policy | |
| 3 | Per-sink migration to real leasing, one issue each: StarRocks (reference), Elasticsearch, Iceberg, Kafka; idempotency items (§7) | Each also closes its `prepare`-based #132 item |
| 4 | `ParquetSink` | Deferred; see §9.3 |

### 9.2 Tracking issues

1.  **#153** — `feat(core): SinkBuffer memory backend, BufferSender/Lease API, and Sink/Transform trait migration` (Phase 1: PR 1A core memory buffer + PR 1B mechanical trait migration & passthrough wiring)
2.  **#154** — `feat(core): SinkBuffer disk (NVMe) backend, crash recovery, dead-letter store, and readiness/shutdown policy` (Phase 2: PR 2A disk backend + PR 2B dead-letter store + PR 2C telemetry, `/readyz`, and shutdown drain)
3.  **#155** — `feat(starrocks-sink): Migrate StarRocksSink to SinkBuffer leasing and lease-derived Stream Load labels` (Phase 3 — parallel sink PR; also closes `StarRocksSink` portion of #132)
4.  **#156** — `feat(elasticsearch-sink): Migrate ElasticsearchSink to SinkBuffer leasing (chunk-join ack & dead-letter item 400s)` (Phase 3 — parallel sink PR; coordinates with #138 and #133)
5.  **#157** — `feat(storage): Migrate IcebergSink to SinkBuffer leasing and snapshot-property delivery tokens` (Phase 3 — parallel sink PR; sequenced after #127; closes `IcebergSink` portion of #132)
6.  **#158** — `feat(kafka-sink): Migrate KafkaSink to SinkBuffer leasing` (Phase 3 — parallel sink PR; coordinates with #130; closes `KafkaSink` portion of #132)
7.  **#159** — `design(parquet-sink): Define SinkBuffer ack-to-file mapping for streaming partition writers` (Phase 4 — coordinates with #143)
8.  **#160** — `fix(config): Support nested __ environment variable overrides and top-level channel_capacity` (§10.2 D-1/D-3 and #125 alignment)

### 9.3 Why Parquet is deferred

`ParquetSink` routes each batch into up to 16 open per-partition writers that stay open up to `max_file_interval_sec`, then upload asynchronously. A lease therefore maps to **many files across many partitions**, and the point at which it is safely persisted (all touched files rolled and uploaded) is undefined by the current design. "Streaming directly" and "leasing" contradict each other. Until an ack-to-file mapping is designed (issue 7), Parquet keeps its own memory accounting (`global_memory_limit_bytes`) and uses the passthrough buffer.

### 9.4 Verification plan

*   **Unit/model tests**: lease state machine, out-of-order acks, nack/backoff, `Drop` requeue, overflow policies, validation rules V1–V5 (property-based testing requires dependency approval).
*   **Crash-recovery tests** with injected kill points: after append before `sync_data`; after sink commit before watermark write; mid-segment torn write; corrupted CRC.
*   **Fault injection**: `ENOSPC` on a small tmpfs; `EIO`; slow/failed sink (`Transient`), poison batch (`Rejected`), auth failure (`Fatal`).
*   **Shutdown tests**: drain success, drain timeout with `persist`, drain timeout with `discard`, bounded join.
*   **Benchmarks**: baseline current mpsc path, then memory and disk backends. Acceptance thresholds are set from the measured baseline; **this document deliberately states no throughput numbers**.

---

## 10. Configuration, Telemetry, Readiness, Shutdown and Documentation Impact

### 10.1 Configuration schema (Target)

Keys live under `[<sink>.buffer]` for `iceberg`, `starrocks`, `elasticsearch`, `kafka`, and later `parquet`. Defaults marked *sink* take the sink's current default so upgrading changes no behavior.

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `mode` | `"memory"` \| `"disk"` | `"memory"` | Backend |
| `disk_path` | String | none | Required for `disk` |
| `segment_size_bytes` | Integer | `67108864` | Disk segment roll size |
| `sync_on_write` | Boolean | `false` | `sync_data` per append |
| `max_buffer_bytes` | Integer | `805306368` (768 MiB) | Total across the sink's three signal buffers (§5.1) |
| `overflow_policy` | `"block"` \| `"drop_oldest"` | `"block"` | §5.4 |
| `max_batch_size_bytes` | Integer | *sink* | Flush trigger |
| `max_batch_records` | Integer | *sink* | Flush trigger (optional) |
| `max_batch_interval_sec` | Integer | *sink* | Flush trigger |
| `max_inflight_leases` | Integer | `1` (Elasticsearch: `max_concurrent_requests`) | Concurrent leases per signal |
| `initial_backoff_ms` | Integer | `500` | `Transient` backoff start |
| `max_backoff_sec` | Integer | `60` | `Transient` backoff cap |
| `max_attempts` | Integer | `3` | `Rejected` attempts before dead-letter |
| `max_transient_duration_sec` | Integer | `0` | `0` = unlimited |
| `dead_letter_path` | String | see §4.3 | Quarantine directory |
| `max_dead_letter_bytes` | Integer | `1073741824` | Quarantine bound |
| `drain_timeout_sec` | Integer | `30` | Shutdown drain bound |
| `on_drain_timeout` | `"persist"` \| `"discard"` | `persist` (disk) / `discard` (memory) | §10.4 |
| `readiness_stall_sec` | Integer | `10` | §10.3 |

*   **Legacy `[<sink>.batching]`** maps onto the three trigger keys with `mode = "memory"`, and is rejected if `[<sink>.buffer]` is also present (V4). Omitting both preserves today's behavior per sink: Iceberg buffers with its defaults; StarRocks, Elasticsearch, and Kafka use the passthrough buffer.
*   **Environment overrides**: nested keys (for example `…__BUFFER__DISK_PATH`) depend on fixing §10.2 D-1 first.

### 10.2 Pre-existing defects found while writing this spec (not fixed here)

| ID | Defect | Evidence |
| :--- | :--- | :--- |
| D-1 | Nested environment overrides cannot work as documented. [`src/main.rs`](../src/main.rs) builds `Env::prefixed("OTEL_DATALAKE_")` with **no `.split(...)`**, yet [starrocks.md](starrocks.md), [configuration.md](configuration.md), and `starrocks-sink` docs advertise `OTEL_DATALAKE_STARROCKS__PASSWORD`. (`crates/core/src/config.rs` uses a different prefix, `DATALAKE_`, with `.split("_")`.) | Figment keys are flat unless `split` is set |
| D-2 | Elasticsearch docs and examples sort metrics on `metric_name`; the Arrow column is `name`. With `on_missing_column = "skip"` the sort silently does nothing. | [elasticsearch.md](elasticsearch.md), [configuration.md](configuration.md) vs `arrow-codec` schema |
| D-3 | `docs/configuration.md` states a different env-var convention (`OTEL_DATALAKE_SERVER_GRPC_ADDR`, single underscore) from `starrocks.md` (`__`). | Same as D-1 |

### 10.3 Telemetry and readiness

New and amended metrics (names per [instrumentation.md](instrumentation.md); one instrument type each; all attributes are bounded enums):

| Metric | Instrument | Unit | Attributes |
| :--- | :--- | :--- | :--- |
| `datalake.sink.buffer_records` | ObservableGauge (`AtomicU64`) | `{record}` | `signal`, `sink`, `buffer_mode` |
| `datalake.sink.buffer_bytes` | ObservableGauge (`AtomicU64`) | `By` | `signal`, `sink`, `buffer_mode` |
| `datalake.sink.buffer_oldest_age_seconds` | ObservableGauge | `s` | `signal`, `sink` |
| `datalake.sink.buffer_leases_inflight` | ObservableGauge | `{lease}` | `signal`, `sink` |
| `datalake.sink.records_committed_total` | Counter | `{record}` | `signal`, `sink` |
| `datalake.sink.records_dropped_total` | Counter | `{record}` | `signal`, `sink`, `reason` ∈ {`overflow`, `dead_letter`, `corruption`, `shutdown_timeout`} |
| `datalake.sink.lease_nacks_total` | Counter | `{lease}` | `signal`, `sink`, `failure_class` |
| `datalake.sink.buffer_replayed_records_total` | Counter | `{record}` | `signal`, `sink` |
| `datalake.sink.flush_duration_seconds` | Histogram | `s` | `signal`, `sink` (from `next_lease` to `ack`) |

Suggested alerts: `buffer_oldest_age_seconds > 2 × max_batch_interval_sec`; `buffer_bytes / budget > 0.8` for 5 min; any `records_dropped_total{reason="dead_letter"|"corruption"}`; sustained `lease_nacks_total{failure_class="transient"}` rate; `requests_shed_total > 0`.

**Readiness** ([administration.md §2.2](administration.md)): `/readyz` returns `503` when any buffer has had a `send` blocked continuously for `readiness_stall_sec`, is in `Failed`, or (disk) has less free space than one segment.

### 10.4 Graceful shutdown (D3)

1.  **Ingress stop** on SIGTERM or SIGINT; receivers drop their senders (SIGTERM wiring is #126).
2.  **Source then transformer drain**; transformers drop their `BufferSender`s, which closes each buffer to new data.
3.  **Drain**: each sink keeps leasing until `next_lease()` yields `Ok(None)` or `drain_timeout_sec` elapses. The overall drain join in `main.rs` MUST be bounded by the same timeout (today it is not). `drain_timeout_sec` MUST be less than the platform termination grace period (for example Kubernetes `terminationGracePeriodSeconds`) with margin.
4.  **On timeout**: `persist` (disk only) flushes and fsyncs the writer thread, leaves unacked segments for replay, and exits `0`. `discard` drops the remainder, logs `ERROR` with counts, increments `records_dropped_total{reason="shutdown_timeout"}`, and exits non-zero.
5.  **Producer queues**: `KafkaSink` MUST `flush()` `librdkafka` before exit (#130). **Telemetry**: `TelemetryGuard::drop` shuts down providers (#124).

### 10.5 Documentation impact (applied by the implementing changes, not now)

| Document | Change at implementation |
| :--- | :--- |
| [components.md](components.md) | `Sink`/`Transform` trait snippets (§3.2); §1 backpressure; §4 buffer retention; "silently" definition (§5.4); async-isolation list |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Topology diagram; "default capacity 1000" (line 82); per-sink buffering bullets |
| [instrumentation.md](instrumentation.md) | Add `buffer_mode`, `reason`, `failure_class` attributes; new metrics; "in memory" wording in descriptions |
| [administration.md](administration.md) | `/readyz` predicate (§10.3) |
| [lakehouse.md](lakehouse.md) | §7 `batching:` → `buffer` mapping and precedence; §2.2 token requirement cross-reference |
| [configuration.md](configuration.md) | `[<sink>.buffer]` reference, `channel_capacity`, env-override fix |
| [starrocks.md](starrocks.md), [elasticsearch.md](elasticsearch.md) | Buffer, retry, dead-letter sections |
| [README.md](../README.md) | Channel diagram; `[elasticsearch.batching]` example |

---

## 11. Open Items

| ID | Question | Needed before |
| :--- | :--- | :--- |
| O1 | gRPC protobuf decode >64 KB cannot be offloaded without a custom bytes codec. Implement the codec or record an accepted AGENTS.md deviation? | Phase 1 plan |
| O2 | Advisory directory lock: std `File::try_lock` vs a crate (depends on workspace MSRV) | Phase 2 plan |
| O3 | Property-testing dependency approval (AGENTS.md dependency discipline) | Phase 1 plan |
| O4 | StarRocks error classification requires inspecting `starrocks-stream-load` error types; confirm they expose HTTP status | Issue 3 |
| O5 | Deterministic Elasticsearch `_id` trade-off (loses append-only fast path) | Deferred |
