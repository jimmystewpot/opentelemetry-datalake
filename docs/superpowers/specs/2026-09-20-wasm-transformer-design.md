# Design Specification: WebAssembly (WASM) Whole-Batch Arrow Transformer

**Date**: 2026-09-20  
**Status**: Final Approved Spec (Incorporating Trait Integration, Three-Tier Immutability, Dispatcher Fan-Out, and K8s Sizing)  
**Target Crates**:
- `crates/wasm-transformer` (Host runtime implementing `pipeline_core::pipeline::Transform`)
- `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`, standalone publishable SDK)
- `crates/wasm-cli` (`datalake-wasm-tool`, developer testing and validation CLI)

---

## 1. Overview & Objectives

This specification defines the architecture, ABI, guest SDK, and verification tooling for executing WebAssembly (WASM) transformations on Apache Arrow `RecordBatch` payloads in `opentelemetry-datalake`.

### Key Architectural Decisions
* **Whole-Batch Transformations**: Users can inspect, enrich, mask, filter rows, or drop/nullify fields inside a sandboxed WASM environment and emit `0..N` transformed `RecordBatch`es.
* **Three-Tier Telemetry Immutability**: Core OpenTelemetry identity fields (`trace_id`, `span_id` in traces; metric `name`, `type` in metrics; `timestamp` in logs) are protected across three tiers:
  1. *Host Runtime*: $O(1)$ structural integrity & non-nullity check (`col.null_count() == col.len()`).
  2. *Guest SDK*: Call-site fail-fast via immutable column checks in helper methods.
  3. *CLI Conformance*: Full value-level byte integrity assertions in `datalake-wasm test`.
  Auxiliary fields (`attributes`, `scope_attributes`, `resource_attributes`, `body`, `exemplars`) remain fully mutable and nullable.
* **Noise Discard vs. Audit Rejection**: Distinguishes intentional noise filtering/sampling (`TransformResult::Discard`, which drops silently without rerouting) from compliance/validation rejections (`TransformResult::Reject { reason }`, which routes to `<component_id>._reroute_rejected` if configured). Guest unrecoverable code errors/traps set `Error` (status 3) and route to `<component_id>._reroute_errored`.
* **Symmetric Topological DLQ Policies (`on_error` & `on_reject`)**: Clean, uniform configuration surface:
  - `on_error = "reroute" | "drop" | "passthrough"` (routes failures to `<component_id>._reroute_errored`)
  - `on_reject = "reroute" | "drop"` (routes policy rejections to `<component_id>._reroute_rejected`)
  The topology builder strictly enforces at startup that subscribed sinks exist for any enabled reroute streams.
* **Lock-Free Dispatcher Multi-Worker Architecture**: A lightweight async dispatcher task reads from the single `input: PipelineReceiver` and distributes batches across $N$ worker channels, completely avoiding mutex contention on the input receiver while preserving natural backpressure.
* **Seamless `Transform` Trait Integration**: `WasmTransformer` implements the standard `pipeline_core::pipeline::Transform` trait without modifying its single-output method signature. Secondary reroute channels (`_reroute_errored` and `_reroute_rejected`) are held as internal struct state injected at construction time.
* **Full IEEE 754 `f64` Gauge Precision via Bitcast**: The controlled host metric ABI passes gauges as IEEE 754 64-bit float bit patterns (`f64::to_bits()` / `f64::from_bits()`), supporting fractional, positive, negative, and zero values. The host uses a concurrent read-optimized registry (`DashMap`) to avoid hot-path lock contention.
* **Graceful Shutdown with Drain Guards**: On `SIGTERM` / `SIGINT`, upstream closes ingress, workers drain all queued batches in channel buffers, complete in-flight batches up to `max_execution_duration`, and exit cleanly. Secondary reroute sends use a bounded timeout (`shutdown_reroute_timeout = 2s`) to prevent stalled dead-letter sinks from blocking process termination.
* **Kubernetes Sizing Guidance**: Explicit sizing formulas for virtual memory pooling allocations and RSS footprint with `madvise(MADV_DONTNEED)` page resets.
* **Defined 0-Batch Success Semantics**: Emitting `TransformResult::Success(vec![])` (via `TransformResult::ok_empty()`) is explicitly supported as a successful no-op/buffering outcome: incoming rows are recorded, 0 outgoing rows emitted, no dead-letter streams invoked, and schema guard is bypassed.
* **Resilient Instance Rejuvenation**: Fresh WASM instances are fully instantiated and initialized via `datalake_init` *before* retiring active stores. If `datalake_init` fails during periodic rejuvenation, the worker retries with exponential backoff and continues serving on its current instance, preventing worker death.
* **Vector-Aligned Component Observability**: Component logs and standard metrics are strictly labeled with `component_id`, `component_type = "wasm"`, and `component_kind = "transform"`. Custom guest metrics are restricted to a dedicated namespace (`datalake_transformers_<component_id>_*`) via a controlled host API (`Counter`, `Gauge` as `f64`, and `Duration` standardizing strictly on nanoseconds and automatically mapped to Prometheus Histograms).
* **Zero-Trust Environment Variable Whitelisting**: Strict zero-trust environment variable isolation. The WASI sandbox inherits zero ambient host environment variables by default. Only explicitly configured keys in `env_whitelist` or explicit values in `[pipeline.transforms.env]` are exposed to the guest. Static config values always override host environment values.
* **Deterministic Hot-Reload Generation Fencing**: A monotonic `module_generation` counter is verified by workers at every batch boundary. When a hot-reload occurs, workers finish their active batch, detect the generation mismatch, and immediately drain and reload their instance in $\approx 10\,\mu\text{s}$, eliminating zombie worker drift.
* **Pure Unordered Concurrency**: In alignment with distributed OpenTelemetry principles, batches are processed concurrently without artificial inter-batch FIFO sequencing, eliminating head-of-line blocking and reorder buffer stalls. In-batch record sorting is handled downstream by `crates/core/src/sort.rs` and sink partitioners.
* **Bounded Invariant Guard & Upstream Accumulator**: The WASM transformer enforces a strict `max_batch_rows` ceiling (e.g. 5,000 rows). Batches exceeding this threshold are rejected at ingestion; batch coalescing, timeout flushing, and upstream splitting are delegated to a dedicated upstream `AccumulatorTransformer` (specified in a companion spec).
* **Memory Safety via Wasmtime Pooling Allocator**: Uses pre-allocated virtual memory slots with microsecond physical page resets via `madvise(MADV_DONTNEED)`. Dual-trigger rejuvenation (soft memory threshold + batch count ceiling) guarantees zero memory leaks or fragmentation bloat.
* **Init-Time Capability Negotiation**: Capabilities (e.g. GeoIP, cache, secrets) are queried exclusively during `datalake_init` and cached in guest memory. The host uses explicit return codes (`0 = unavailable`, `1 = available`) and logs diagnostic warnings for unrecognized capability names to prevent silent typos.
* **Enterprise Governance & Atomic Hot-Reloading**: In-memory single-read SHA-256 verification (closing TOCTOU vulnerabilities) and atomic zero-downtime hot-reloading (`POST /api/v1/transforms/wasm/reload` or `SIGHUP`) without dropping active gRPC/HTTP ingestion streams.

---

## 2. Workspace Crate Architecture

```text
opentelemetry-datalake/
├── crates/
│   ├── wasm-transformer/             # Host runtime transformer implementing Transform
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # WasmTransformer struct & Transform impl with internal reroute channels
│   │       ├── config.rs              # TOML deserialization (paths, limits, concurrency, sha256, env, on_error, on_reject)
│   │       ├── engine.rs              # Wasmtime Engine & compiled Module cache (Arc<Module>, module_generation)
│   │       ├── pool.rs                # Pooling instance allocator & soft rejuvenation lifecycle
│   │       ├── dispatcher.rs          # Lock-free single-receiver fan-out to N worker channels
│   │       ├── guard.rs               # Invariant validation, O(1) structural integrity & schema defense
│   │       ├── wasi_env.rs            # Zero-trust WASI context builder & env whitelist filter
│   │       ├── host_calls.rs          # Versioned host imports with concurrent DashMap metric registry
│   │       ├── reload.rs              # Atomic zero-downtime hot-reloader with generation fencing
│   │       └── error.rs               # WasmTransformError (Timeout, Trap, OOM, IPC, ShaMismatch)
│   │
│   ├── wasm-sdk/                      # Standalone, publishable guest SDK
│   │   │                              # (opentelemetry-datalake-wasm-sdk)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # Public prelude, BatchTransformer trait, TransformResult
│   │       ├── abi.rs                 # Low-level FFI exports & memory protocol (with manual escape hatch)
│   │       ├── ipc.rs                 # Arrow IPC stream reading & writing in guest
│   │       ├── helpers.rs             # Client-side checked column nullification, projection, filtering
│   │       ├── metrics.rs             # Controlled guest metric API (counter, f64 gauge, duration in nanos)
│   │       ├── panic.rs               # Custom std::panic hook forwarding to datalake_host_log
│   │       ├── logger.rs              # Guest tracing/log forwarder using HostLogRecord
│   │       └── testing.rs             # Mock batch generators & WasmHarness test runner
│   │
│   ├── wasm-cli/                      # Developer CLI & validation tool
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs                # CLI commands: validate, test (with value immutability check), bench
│   │
│   └── core/                          # pipeline_core config with WasmTransformerConfig and DLQ validation
```

---

## 3. Host ↔ Guest ABI Specification (v1)

The ABI contract is a versioned, low-overhead C-compatible interface operating across WebAssembly linear memory (`wasm32`).

### 3.1 ABI Versioning Handshake

1. **Mandatory Export**: Every conformant module must export:
   ```c
   uint32_t datalake_abi_version(void);
   ```
   Modules conforming to this specification must return `1`. On load, the host validates this export; if missing or not equal to `1`, instantiation aborts with `WasmTransformError::AbiVersionMismatch`.
2. **Namespaced Imports**: All host imports live under versioned namespaces (`datalake_host_v1`).

### 3.2 Exported Guest Functions (Required)

```c
// 1. ABI Version Handshake (must return 1 for v1)
uint32_t datalake_abi_version(void);

// 2. Allocate buffer of `size` bytes in guest linear memory.
// Returns 32-bit pointer (offset in WASM memory), or 0 on failure.
uint32_t datalake_alloc(uint32_t size);

// 3. Deallocate buffer previously allocated by datalake_alloc.
void datalake_dealloc(uint32_t ptr, uint32_t size);

// 4. Optional one-time configuration initialization.
// Returns 0 on success, non-zero on failure.
int32_t datalake_init(uint32_t cfg_ptr, uint32_t cfg_len);

// 5. Transform incoming batch.
// signal_type: 0 = Logs, 1 = Metrics, 2 = Traces
// ipc_ptr / ipc_len: input Arrow IPC stream bytes
// Returns a packed 64-bit value: (response_ptr << 32) | response_len
uint64_t datalake_transform(uint32_t signal_type, uint32_t ipc_ptr, uint32_t ipc_len);
```

### 3.3 Response Header Format

```rust
#[repr(C)]
pub struct TransformResponseHeader {
    /// ABI version (must be 1)
    pub abi_version: u32,

    /// 0 = Success (emit batches downstream)
    /// 1 = Discard (noise drop / sampling, silently dropped, never rerouted)
    /// 2 = Reject (business validation drop, routed to _reroute_rejected if on_reject = "reroute")
    /// 3 = Error (execution failure, panic, unhandled error, routed to _reroute_errored if on_error = "reroute")
    pub status: u32,

    /// Number of emitted Arrow IPC batches (0..N)
    pub batch_count: u32,

    /// Pointer to array of `BatchDescriptor { ptr: u32, len: u32 }`
    pub batches_ptr: u32,

    /// Pointer to optional UTF-8 string (reject reason or error message)
    pub message_ptr: u32,
    pub message_len: u32,
}

#[repr(C)]
pub struct BatchDescriptor {
    pub ptr: u32,
    pub len: u32,
}
```

### 3.4 Imported Host Functions (Module: `datalake_host_v1`)

```rust
#[repr(C)]
pub struct HostLogRecord {
    pub level: u32,       // 1 = Error, 2 = Warn, 3 = Info, 4 = Debug, 5 = Trace
    pub msg_ptr: u32,
    pub msg_len: u32,
    pub target_ptr: u32,  // 0 if unused
    pub target_len: u32,
    pub file_ptr: u32,    // 0 if unused
    pub file_len: u32,
    pub line: u32,        // 0 if unused
}
```

```c
// Emits structured log event. Host enriches with component_id, component_type, component_kind, signal, and instance_id.
void datalake_host_log(uint32_t record_ptr);

// Emits a controlled custom metric counter, gauge, or duration (histogram).
// metric_type: 0 = Counter, 1 = Gauge, 2 = Duration
//
// Value interpretation by metric_type:
// - Counter (0): u64 integer count to increment by.
// - Gauge (1): IEEE 754 64-bit float bitcast (f64::to_bits() in guest, f64::from_bits() on host).
//              Allows full negative, fractional, and positive float representation.
// - Duration (2): u64 integer duration in NANOSECONDS.
//                 Host automatically converts to seconds (value / 1e9) and records into Prometheus Histogram.
//
// name_ptr / name_len: relative metric name (e.g. "pii_redacted")
void datalake_host_metric_emit(uint32_t metric_type, uint32_t name_ptr, uint32_t name_len, uint64_t value);

// Dynamic capability query for optional host features (cache, geoip, secrets).
// CONTRACT: Must be queried during `datalake_init` and cached. Querying in `transform` is prohibited.
// Returns: 1 = Available, 0 = Unavailable. Unrecognized capability names log a diagnostic warning.
uint32_t datalake_host_has_capability(uint32_t cap_name_ptr, uint32_t cap_name_len);
```

### 3.5 Latency Overhead Budget

- For a standard batch of 2,000 rows (~500 KB uncompressed IPC stream), the round-trip boundary overhead (excluding user business logic) must satisfy:
  - **$p95 \le 1.5\text{ms}$**
  - **$p99 \le 3.0\text{ms}$**
- Validated via automated CI benchmark: `benches/wasm_boundary_bench.rs`.

---

## 4. Guest SDK (`opentelemetry-datalake-wasm-sdk`)

Designed for publishability on `crates.io`, this crate provides idiomatic, safe abstractions for writing transforms.

### 4.1 Trait Definition & Semantic Error Guidance

```rust
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Logs = 0,
    Metrics = 1,
    Traces = 2,
}

pub enum TransformResult {
    /// Emit 0..N transformed RecordBatches downstream to primary sinks.
    /// Note: Emitting vec![] (via ok_empty()) is a valid success outcome: marks batch consumed, 
    /// emits 0 rows, bypasses schema guard, and does not trigger error/reject handling.
    Success(Vec<RecordBatch>),

    /// Intentional noise filtering / sampling. Silently discarded from memory, counted in metrics, NEVER rerouted.
    Discard,

    /// Business-logic rejection (e.g. validation failure, security policy).
    /// Routed to <component_id>._reroute_rejected if on_reject = "reroute".
    Reject { reason: String },
}

impl TransformResult {
    pub fn ok(batch: RecordBatch) -> Self {
        Self::Success(vec![batch])
    }

    pub fn ok_multiple(batches: Vec<RecordBatch>) -> Self {
        Self::Success(batches)
    }

    pub fn ok_empty() -> Self {
        Self::Success(vec![])
    }

    pub fn discard() -> Self {
        Self::Discard
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject { reason: reason.into() }
    }
}

pub trait BatchTransformer: Default + Send + Sync + 'static {
    /// One-time initialization hook. Query host capabilities, read whitelisted env vars, and parse config here.
    fn init(&mut self, _config: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    /// Core transform entrypoint.
    ///
    /// ## Guidance on Error vs Reject:
    /// - Use `Ok(TransformResult::Reject { reason })` for expected data validation failures,
    ///   malformed payload data, or business rule rejections (routed to `_reroute_rejected`).
    /// - Only return `Err(...)` for unrecoverable, catastrophic code failures or internal
    ///   inconsistencies (routed to `_reroute_errored` as status = 3).
    fn transform(
        &mut self,
        signal: SignalType,
        batch: RecordBatch,
    ) -> Result<TransformResult, Box<dyn std::error::Error>>;
}
```

### 4.2 Entry Point Registration & Panic Hook

The `export_transformer!(MyType)` macro generates the FFI exports and automatically installs a panic hook:

```rust
// Automatically sets std::panic::set_hook to capture panic location & message,
// routing it through datalake_host_log before returning status = 3 (Error).
export_transformer!(MyCustomTransformer);
```

### 4.3 Manual FFI Escape Hatch (No Macro Requirement)

Advanced users can bypass macros and export standard C functions directly using SDK low-level primitives:
```rust
#[no_mangle]
pub extern "C" fn datalake_abi_version() -> u32 { 1 }

#[no_mangle]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    opentelemetry_datalake_wasm_sdk::abi::raw_alloc(size)
}

#[no_mangle]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    opentelemetry_datalake_wasm_sdk::abi::raw_dealloc(ptr, size);
}

#[no_mangle]
pub extern "C" fn datalake_init(cfg_ptr: u32, cfg_len: u32) -> i32 { 0 }

#[no_mangle]
pub extern "C" fn datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64 {
    opentelemetry_datalake_wasm_sdk::abi::dispatch_transform::<MyCustomTransformer>(
        signal_type, ipc_ptr, ipc_len
    )
}
```

### 4.4 Guest Custom Metrics & Call-Site Immutability Guards

The SDK provides safe wrappers for metrics, logging, environment access, and helpers that **fail fast at the call site** if an immutable field is targeted:

```rust
// Client-side fail-fast: returns Err(SdkError::ImmutableFieldViolation("trace_id")) immediately!
sdk::helpers::nullify_column(&batch, "trace_id")?; 

// Valid operations on mutable auxiliary fields
sdk::helpers::nullify_column(&batch, "scope_attributes")?;
sdk::helpers::filter_batch(&batch, &boolean_mask)?;
sdk::helpers::redact_column_regex(&batch, "body", &regex, "[REDACTED]")?;

// Custom metrics (automatically namespaced to datalake_transformers_<component_id>_*)
sdk::metrics::counter("pii_redacted", 1);

// Gauge takes f64 (supports negative, fractional, and positive values via IEEE 754 bitcast)
sdk::metrics::gauge("cpu_usage_ratio", 0.742);
sdk::metrics::gauge("ambient_temp_celsius", -4.5);

// Duration helper: extracts nanoseconds (u64) from std::time::Duration
sdk::metrics::duration("lookup_duration", elapsed_duration);

// Environment access
let env = std::env::var("APP_ENV").unwrap_or_else(|_| "unknown".to_string());
```

---

## 5. Host Runtime & Execution Engine (`crates/wasm-transformer`)

The host transformer integrates into the pipeline via `pipeline_core::pipeline::Transform`.

### 5.1 Configuration (`pipeline.toml`)

The configuration uses idiomatic TOML table nesting, scoping per-transform options directly under the specific array entry:

```toml
[[pipeline.transforms]]
id = "pii_scrubber"
type = "wasm"
module_path = "transforms/enrichment.wasm"
sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" # Optional integrity verification
max_execution_duration = "500ms"
drain_timeout = "10s"                # Maximum time to drain in-flight batches during graceful shutdown
max_batch_rows = 5000                # Invariant boundary limit; reject if exceeded
concurrency = 4                      # Number of worker tasks / pooling slots
max_memory = "64MiB"                 # Virtual memory slot size (recommended: 64MiB)
rejuvenate_threshold = "16MiB"       # Soft memory limit for instant page reclamation
rejuvenate_batches = 10000           # Maximum batches before hygiene refresh
init_timeout = "2s"                  # Maximum duration for datalake_init

# Symmetric Routing Policies ("reroute" | "drop" | "passthrough")
on_error = "reroute"                 # "reroute" requires a sink subscribed to <id>._reroute_errored
allow_unmasked_passthrough = false   # Required only if on_error = "passthrough"

on_reject = "reroute"                # "reroute" requires a sink subscribed to <id>._reroute_rejected
                                     # "drop" silently discards rejects with metrics increment

# Schema Guard Mode: "defensive" (backfill typed nulls) or "strict" (fail on missing columns)
schema_guard = "defensive"

# Whitelisted host environment variables passed to the WASM sandbox
env_whitelist = [
    "APP_ENV",
    "CLUSTER_ID",
    "REGION"
]

# Explicit static environment variables injected into this WASM sandbox (overrides host env)
[pipeline.transforms.env]
LOG_LEVEL = "info"
TRANSFORM_VERSION = "1.2.0"

# Optional config block passed to datalake_init
[pipeline.transforms.config]
environment = "production"
redact_sensitive_fields = true
```

### 5.2 Lock-Free Dispatcher & Multi-Worker Concurrency

Because `tokio::sync::mpsc::Receiver` is not `Clone`, sharing it across worker tasks with an `Arc<Mutex<Receiver>>` would create an unacceptable serialization bottleneck. Instead, `WasmTransformer` implements a **lock-free single-receiver fan-out pattern**:

```rust
pub struct WasmTransformer {
    config: WasmTransformerConfig,
    engine: Arc<EngineCache>,
    reroute_errored_tx: Option<PipelineSender>,
    reroute_rejected_tx: Option<PipelineSender>,
}

#[async_trait]
impl Transform for WasmTransformer {
    async fn transform(
        &mut self,
        mut input: PipelineReceiver,
        output: PipelineSender,
    ) -> Result<(), PipelineError> {
        let concurrency = self.config.concurrency;
        let mut worker_txs = Vec::with_capacity(concurrency);

        // 1. Spawn N independent worker tasks, each with its own instance and bounded channel
        for worker_id in 0..concurrency {
            let (worker_tx, mut worker_rx) = mpsc::channel::<SignalBatch>(2);
            worker_txs.push(worker_tx);

            let worker_output = output.clone();
            let worker_error = self.reroute_errored_tx.clone();
            let worker_reject = self.reroute_rejected_tx.clone();
            let worker_engine = Arc::clone(&self.engine);
            let worker_config = self.config.clone();

            tokio::spawn(async move {
                Self::run_worker(
                    worker_id,
                    worker_rx,
                    worker_output,
                    worker_error,
                    worker_reject,
                    worker_engine,
                    worker_config,
                ).await;
            });
        }

        // 2. Dispatcher loop: read from input, distribute round-robin across worker channels
        let mut next_worker = 0;
        while let Some(batch) = input.recv().await {
            worker_txs[next_worker]
                .send(batch)
                .await
                .map_err(|_| PipelineError::DownstreamClosed)?;
            next_worker = (next_worker + 1) % concurrency;
        }

        // 3. Upstream input closed; drop worker senders to signal drain
        drop(worker_txs);
        Ok(())
    }
}
```

- Each worker holds a clone of `output` (`mpsc::Sender` is `Clone`) and internal reroute senders (`_reroute_errored`, `_reroute_rejected`).
- Backpressure propagates naturally: if workers are saturated, `worker_tx.send(batch).await` pauses the dispatcher, which pauses reading from `input`.

### 5.3 Failure Policies & Backpressure Semantics

1. **`on_error = "reroute"` (Default Recommended)**:
   - When a batch execution fails (timeout, panic, trap, IPC decode error, or unhandled guest `Err`), the pristine original batch is routed to `<component_id>._reroute_errored`.
   - The topology builder asserts at startup that a sink is subscribed to `inputs = ["<component_id>._reroute_errored"]`. If missing, startup fails immediately.
2. **`on_error = "passthrough"` (Fail-Open)**:
   - Requires `allow_unmasked_passthrough = true` in configuration.
   - When a transform fails, the worker **preserves the original batch in memory** and forwards it to the primary `output.send(batch).await`.
   - The worker **blocks under backpressure** until downstream sinks accept the batch. If downstream is stalled, backpressure propagates upstream to the OTLP receiver, which returns `503 Service Unavailable` to the caller. No data is dropped.
3. **`on_error = "drop"` (Fail-Closed)**:
   - Failed batches are discarded from memory, and `component_errors_total` is incremented.

### 5.4 Audit Rejection vs. Noise Discard Semantics

1. **`TransformResult::Discard` (Noise Filtering / Sampling)**:
   - Telemetry that the module intentionally filters out (e.g. noisy health checks).
   - Dropped directly from memory. Never sent to any sink.
   - Increments `component_discarded_rows_total{component_id="...", reason="discard"}`.
2. **`TransformResult::Reject { reason }` (Audit Policy Drop)**:
   - Telemetry dropped due to business validation failure or security rule.
   - If `on_reject = "reroute"`, the original batch is routed to `<component_id>._reroute_rejected`.
   - The topology builder requires a sink subscribed to `inputs = ["<component_id>._reroute_rejected"]`.
   - If `on_reject = "drop"`, the batch is discarded from memory.
   - Increments `component_discarded_rows_total{component_id="...", reason="reject"}`.

### 5.5 Three-Tier Telemetry Immutability & O(1) Schema Guard

The OpenTelemetry data model relies on immutable trace and metric identities:
* **Traces**: `trace_id`, `span_id`.
* **Logs**: `timestamp` (or `observed_timestamp`).
* **Metrics**: Metric `name`, metric `type`.

To prevent silent corruption without incurring per-record byte comparison penalties in the hot path, immutability is guaranteed across three tiers:

1. **Host Hot-Path Structural Integrity Check ($O(1)$)**:
   - Asserts mandatory columns exist in the Arrow schema.
   - Evaluates:
     ```rust
     let is_all_null = col.null_count() == col.len() && col.len() > 0;
     ```
     Because `null_count` is cached metadata in Arrow's `ArrayData`, this is strictly **$O(1)$ integer arithmetic**. If an immutable column is omitted or `is_all_null` is true while the input had valid non-null entries, the batch is rejected with `WasmTransformError::ImmutableFieldViolation { column }` and routed to `on_error`.
2. **Guest SDK Call-Site Fail-Fast**:
   - `sdk::helpers::nullify_column` and `redact_column_regex` inspect the target column against static immutable lists, returning `Err(SdkError::ImmutableFieldViolation)` immediately in local guest unit tests.
3. **Developer CLI Value Conformance (`datalake-wasm test`)**:
   - The CLI test runner feeds synthetic batches with known IDs and explicitly checks byte-for-byte that the values of `trace_id` and `span_id` were not overwritten or scrambled by the module.

#### Schema Guard for Mutable Fields (Defensive vs. Strict)
For mutable auxiliary fields (`attributes`, `scope_attributes`, `resource_attributes`, `body`, `exemplars`):
* **`schema_guard = "defensive"` (Default)**:
  - If a guest omits a mutable canonical column, the host backfills it using `arrow::array::new_null_array(field.data_type(), num_rows)`.
  - Increments counter: `datalake_wasm_schema_columns_backfilled_total{component_id, signal, column}`.
  - Logs a rate-limited `WARN` (at most once every 60s per column) to ensure visibility of guest bugs.
* **`schema_guard = "strict"`**:
  - Missing mutable columns are treated as an error, rejecting the batch and triggering `on_error`.

### 5.6 Resilient Rejuvenation & Lifecycle

When a worker instance rejuvenates (due to `rejuvenate_threshold = 16MiB` or `rejuvenate_batches = 10000`):
1. **Non-Destructive Instance Swap**:
   - The worker initializes a candidate `Store` from the active `Arc<Module>` and invokes `datalake_init`.
   - The old instance is **not discarded** until `datalake_init` succeeds.
2. **Retry with Exponential Backoff**:
   - If `datalake_init` returns non-zero or times out (`init_timeout = 2s`), the worker logs an error and retries up to 3 times with exponential backoff (100ms, 200ms, 400ms).
   - If all retries fail, the worker keeps serving on the existing store (avoiding permanent worker slot death), logs a critical warning, and attempts rejuvenation again after 100 batches.

### 5.7 Vector-Aligned Observability & Concurrent Metrics Registry

1. **Uniform Component Identification**:
   - All host logs and metrics emitted by the transformer include the standardized Vector-style labels:
     - `component_id = "<id>"`
     - `component_type = "wasm"`
     - `component_kind = "transform"`
     - `signal = "logs|metrics|traces"`
2. **Standard Pipeline Metrics**:
   - `component_received_rows_total`
   - `component_sent_rows_total`
   - `component_discarded_rows_total{reason="discard|reject"}`
   - `component_errors_total`
   - `component_execution_duration_seconds` (Histogram)
3. **Controlled Custom Metric Injection with Lock-Free Registry**:
   - Custom guest metrics are restricted to:
     ```text
     datalake_transformers_<component_id>_<metric_name>{component_id="...", signal="..."}
     ```
   - Type mapping:
     - `metric_type = 0` (Counter): Prometheus counter.
     - `metric_type = 1` (Gauge): IEEE 754 64-bit float bitcast (`f64::from_bits(value)`), recorded into Prometheus Gauge.
     - `metric_type = 2` (Duration): Value in nanoseconds (`u64`), converted to seconds (`f64 / 1e9`) and recorded into a Prometheus Histogram (`..._duration_seconds`).
   - **Concurrency Safety**:
     - Metric handles are cached in a read-optimized concurrent map (`DashMap<String, MetricHandle>`).
     - In steady-state execution, looking up handles is lock-free and $O(1)$.
     - Insertions only take place on the first encounter of a metric name, strictly bounded by the 50-metric cardinality ceiling.
   - Hard limits:
     - Metric names: ASCII alphanumeric + `_`, max 64 characters.
     - Cardinality ceiling: Maximum 50 distinct custom metric names per `component_id`.

### 5.8 Kubernetes Sizing & Memory Guidance

When running in containerized environments (Kubernetes pods), operators must account for Wasmtime's virtual memory pooling allocator:

$$\text{Virtual Memory Reserved} = \text{concurrency} \times \text{max\_memory}$$

* **Recommended Edge Configuration**: `concurrency = 4`, `max_memory = "64MiB"`, `rejuvenate_threshold = "16MiB"`. This requires $256\text{MiB}$ of virtual address space.
* **Host Physical RAM (RSS)**: Thanks to `madvise(MADV_DONTNEED)`, actual physical resident set size (RSS) stays around $\text{concurrency} \times \text{rejuvenate\_threshold}$ ($\approx 64\text{MiB}$).
* **Container Limits**: Ensure pod `resources.limits.memory` is at least $2\times$ the expected RSS, and the host OS `vm.max_map_count` is sufficient (Linux default of 65,530 is plenty for standard pools).

### 5.9 Environment Variable Sandboxing & Zero-Trust WASI

1. **Default Deny**: By default, `wasmtime_wasi::WasiCtxBuilder` does not inherit host environment variables.
2. **Precedence Rule**:
   - `[pipeline.transforms.env]` (static explicit injection) **strictly overrides** variables resolved from `env_whitelist`.
   - If a key exists in both, the static value is used and an informational notice is logged.
3. **Audit Logging**: At startup and reload, the host logs the list of permitted variable names (values are masked) for compliance auditability.

### 5.10 Graceful Shutdown & Pipeline Drain

During rolling deployments or process termination (`SIGTERM` / `SIGINT`):
1. The upstream OTLP receiver shuts down its listening port and ceases accepting new traffic.
2. The receiver completes in-flight requests and drops its channel senders (`input` on `WasmTransformer` observes EOF `None`).
3. **Worker Drain Loop**:
   - The dispatcher loop exits, closing worker channels (`worker_txs`).
   - Workers finish their current in-flight batch (bounded by `max_execution_duration`).
   - Workers drain any remaining batches buffered in their `worker_rx` queues, executing transforms and forwarding outputs downstream.
4. **Shutdown Reroute Timeout Guard**:
   - During shutdown drain, secondary reroute channel sends use a bounded timeout (`shutdown_reroute_timeout = 2s`):
     ```rust
     tokio::select! {
         res = reroute_tx.send(batch) => { ... },
         _ = tokio::time::sleep(Duration::from_secs(2)) => {
             tracing::warn!("Reroute channel full during shutdown; dropping batch to prevent hang");
             metrics::counter!("component_errors_total", "error_type" => "shutdown_reroute_timeout");
         }
     }
     ```
   - This prevents stalled dead-letter sinks from holding the shutdown process hostage.
5. **Drain Timeout Guard**:
   - The overall shutdown process enforces `drain_timeout` (default: 10s). If draining exceeds this threshold, worker tasks are aborted, logging:
     ```text
     WARN wasm_host: Shutdown drain timeout reached (10s), aborting remaining workers [component_id="..."]
     ```
6. **Clean Downstream Cascade**:
   - Once workers terminate, all output channels (`output`, `_reroute_errored`, `_reroute_rejected`) drop, allowing downstream sinks to finish commits and flush Parquet files cleanly without truncated writes.

### 5.11 Deterministic Hot-Reloading & Generation Fencing

1. **Single-Read In-Memory Compilation**:
   ```rust
   let wasm_bytes = tokio::fs::read(&module_path).await?;
   if let Some(expected_sha) = &config.sha256 {
       let actual_sha = hex::encode(Sha256::digest(&wasm_bytes));
       if &actual_sha != expected_sha {
           return Err(WasmTransformError::Sha256Mismatch { expected, actual });
       }
   }
   let new_module = wasmtime::Module::from_binary(&engine, &wasm_bytes)?;
   ```
2. **Atomic Swap & Generation Increment**:
   - The host swaps `Arc<Module>` and increments an atomic `module_generation: AtomicU64`.
3. **Generation Fencing at Worker Boundary**:
   - At the beginning of processing each batch, workers check:
     ```rust
     if self.local_generation != global_engine.module_generation() {
         self.reload_instance(&global_engine)?; // Non-destructive reload from new module
     }
     ```
   - Maximum lag before running new code is exactly 1 batch per worker. Zero zombie drift.

---

## 6. Observability & Metrics Specification

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `component_received_rows_total` | Counter | `component_id`, `component_type`, `component_kind`, `signal` | Incoming rows. |
| `component_sent_rows_total` | Counter | `component_id`, `component_type`, `component_kind`, `signal` | Outgoing rows after mutations. |
| `component_discarded_rows_total` | Counter | `component_id`, `component_type`, `component_kind`, `signal`, `reason` (`discard`, `reject`) | Dropped rows by discard category. |
| `component_errors_total` | Counter | `component_id`, `component_type`, `component_kind`, `signal`, `error_type` | Total execution failures. |
| `component_execution_duration_seconds` | Histogram | `component_id`, `component_type`, `component_kind`, `signal` | Boundary latency per batch. |
| `datalake_wasm_memory_bytes` | Gauge | `component_id`, `instance_id` | Current linear memory consumption per instance. |
| `datalake_wasm_rejuvenations_total` | Counter | `component_id`, `instance_id`, `reason` | Total instance pool resets. |
| `datalake_wasm_module_info` | Gauge | `component_id`, `sha256`, `generation`, `abi_version` | Active module audit information. |
| `datalake_wasm_schema_columns_backfilled_total` | Counter | `component_id`, `signal`, `column` | Missing mutable columns backfilled with nulls. |
| `datalake_transformers_<component_id>_<name>` | Counter / Gauge | `component_id`, `signal` | Guest custom counter or gauge (f64). |
| `datalake_transformers_<component_id>_<name>_duration_seconds` | Histogram | `component_id`, `signal` | Guest custom duration histogram (fed by nanoseconds). |

### Logging Standards

* **Panic & Guest Log Forwarding**: Host intercepts `datalake_host_log` (`HostLogRecord`) and emits structured logs:
  ```text
  ERROR wasm_guest: Guest panic in transforms/pii.rs:42: called `Option::unwrap()` on a `None` value [component_id="pii_scrubber", component_type="wasm", component_kind="transform", signal="logs", instance_id=2]
  ```

---

## 7. Conformity & Testing Tooling (`crates/wasm-cli`)

Dedicated CLI utility (`datalake-wasm`) verifies compiled `.wasm` modules:

1. **`datalake-wasm validate <path.wasm>`**:
   - Verifies `datalake_abi_version()` returns `1`.
   - Checks exports (`datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
   - Rejects forbidden WASI syscalls (raw sockets/files).
   - Validates memory limits and initialization behavior.
2. **`datalake-wasm test <path.wasm> [--signal logs|metrics|traces] [--input sample.ipc] [--env KEY=VAL]`**:
   - Executes module against synthetic or user-provided Arrow IPC streams.
   - **Value Immutability Conformance Suite**: Feeds synthetic batches with known IDs and verifies that the output batch's `trace_id` / `span_id` bytes match the input byte-for-byte. Emits explicit failure if illegal mutation occurred.
   - Injects test environment variables into guest WASI context.
   - Enables DWARF debug info for full guest stack traces on panics.
   - Verifies handling of 0-row batches, Discard, Reject, and canonical schema invariance.
3. **`datalake-wasm bench <path.wasm> [--rows 10000] [--concurrency 4]`**:
   - Measures batch transformation latency distribution (p50, p95, p99), throughput (rows/sec), and peak memory.

---

## 8. Language-Agnostic Evolution Path

1. **Standardized C-ABI v1**: TinyGo, C/C++, and Zig can immediately produce conformant WASM blobs by implementing the 5 exported functions (`datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
2. **Component Model / WASI 0.2 WIT**: A future `.wit` interface definition (`datalake:transformer/transform@0.1.0`) can wrap this same engine, enabling WIT-based bindings without breaking the underlying Arrow IPC memory exchange.
