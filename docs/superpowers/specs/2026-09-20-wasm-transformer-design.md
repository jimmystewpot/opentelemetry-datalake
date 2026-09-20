# Design Specification: WebAssembly (WASM) Whole-Batch Arrow Transformer

**Date**: 2026-09-20  
**Status**: Final Approved Spec (Incorporating Full Architecture, Telemetry Immutability, Noise Discard vs Audit Reject, and DLQ Topology Reviews)  
**Target Crates**:
- `crates/wasm-transformer` (Host runtime implementing `pipeline_core::pipeline::Transform`)
- `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`, standalone publishable SDK)
- `crates/wasm-cli` (`datalake-wasm-tool`, developer testing and validation CLI)

---

## 1. Overview & Objectives

This specification defines the architecture, ABI, guest SDK, and verification tooling for executing WebAssembly (WASM) transformations on Apache Arrow `RecordBatch` payloads in `opentelemetry-datalake`.

### Key Architectural Decisions
* **Whole-Batch Transformations**: Users can inspect, enrich, mask, filter rows, or drop/nullify fields inside a sandboxed WASM environment and emit `0..N` transformed `RecordBatch`es.
* **Core Telemetry Immutability**: Core OpenTelemetry identity fields (`trace_id`, `span_id` in traces; metric `name`, `type` in metrics; `timestamp` in logs) are strictly **immutable**. Dropping or nullifying immutable fields triggers a schema invariant violation. Only auxiliary fields (`attributes`, `scope_attributes`, `resource_attributes`, `body`, `exemplars`) can be nullified or redacted.
* **Noise Discard vs. Audit Rejection**: Distinguishes intentional noise filtering/sampling (`TransformResult::Discard`, which drops silently without rerouting) from compliance/validation rejections (`TransformResult::Reject { reason }`, which routes to `<component_id>._reroute_aborted` if configured).
* **Unified Error Policy & Topological DLQ (`on_error`)**: Configured via a single enum: `on_error = "drop" | "reroute" | "passthrough"`. If `"reroute"`, the topology builder strictly enforces at startup that a sink is subscribed to `<component_id>._reroute_errored`. If `"passthrough"`, the worker preserves the original batch and forwards it downstream under standard channel backpressure (requiring `allow_unmasked_passthrough = true`).
* **Resilient Instance Rejuvenation**: Fresh WASM instances are fully instantiated and initialized via `datalake_init` *before* retiring active stores. If `datalake_init` fails during periodic rejuvenation, the worker retries with exponential backoff and continues serving on its current instance, preventing worker death.
* **Vector-Aligned Component Observability**: Component logs and standard metrics are strictly labeled with `component_id`, `component_type = "wasm"`, and `component_kind = "transform"`. Custom guest metrics are restricted to a dedicated namespace (`datalake_transformers_<component_id>_*`) via a controlled host API (`Counter`, `Gauge`, and `Duration` standardizing strictly on nanoseconds and automatically mapped to Prometheus Histograms).
* **Zero-Trust Environment Variable Whitelisting**: Strict zero-trust environment variable isolation. The WASI sandbox inherits zero ambient host environment variables by default. Only explicitly configured keys in `env_whitelist` or explicit values in `[pipeline.transform.wasm.env]` are exposed to the guest. Static config values always override host environment values.
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
│   ├── wasm-transformer/             # Host runtime transformer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # WasmTransformer implementing pipeline_core::Transform
│   │       ├── config.rs              # TOML deserialization (paths, limits, concurrency, sha256, env, on_error)
│   │       ├── engine.rs              # Wasmtime Engine & compiled Module cache (Arc<Module>, module_generation)
│   │       ├── pool.rs                # Pooling instance allocator & soft rejuvenation lifecycle
│   │       ├── guard.rs               # Invariant validation, immutability checks & schema defense
│   │       ├── wasi_env.rs            # Zero-trust WASI context builder & env whitelist filter
│   │       ├── host_calls.rs          # Versioned host imports (datalake_host_v1)
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
│   │       ├── helpers.rs             # Column nullification, projection, filtering utilities
│   │       ├── metrics.rs             # Controlled guest metric API (counter, gauge, duration in nanos)
│   │       ├── panic.rs               # Custom std::panic hook forwarding to datalake_host_log
│   │       ├── logger.rs              # Guest tracing/log forwarder using HostLogRecord
│   │       └── testing.rs             # Mock batch generators & WasmHarness test runner
│   │
│   ├── wasm-cli/                      # Developer CLI & validation tool
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs                # CLI commands: validate, test, bench
│   │
│   └── core/                          # Updated pipeline_core config with WasmTransformerConfig and DLQ validation
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
    /// 2 = Reject (business validation drop, routed to _reroute_aborted if enabled)
    /// 3 = Error (execution failure, panic, or unhandled error, routed to _reroute_errored if enabled)
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
// For metric_type = 2 (Duration), value is strictly in NANOSECONDS (u64).
// Host automatically converts nanoseconds to seconds and records into Prometheus Histogram.
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

### 4.1 Trait Definition

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
    Success(Vec<RecordBatch>),
    /// Intentional noise filtering / sampling. Silently discarded from memory, counted in metrics, NEVER rerouted.
    Discard,
    /// Business-logic rejection (e.g. validation failure, security policy).
    /// Routed to <component_id>._reroute_aborted if reroute_on_reject = true.
    Reject { reason: String },
}

impl TransformResult {
    pub fn ok(batch: RecordBatch) -> Self {
        Self::Success(vec![batch])
    }

    pub fn ok_multiple(batches: Vec<RecordBatch>) -> Self {
        Self::Success(batches)
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

### 4.4 Guest Custom Metrics & Environment Helpers

The SDK provides safe wrappers for metrics, logging, and environment access:
```rust
// Custom metrics (automatically namespaced to datalake_transformers_<component_id>_*)
sdk::metrics::counter("pii_redacted", 1);
sdk::metrics::gauge("cache_size", 1024);

// Duration helper: takes std::time::Duration, automatically extracts nanoseconds (u64)
// and host converts to seconds in Prometheus Histogram
sdk::metrics::duration("lookup_duration", elapsed_duration);

// Canonical schema helpers (only valid for mutable fields)
sdk::helpers::nullify_column(&batch, "scope_attributes")?;
sdk::helpers::filter_batch(&batch, &boolean_mask)?;
sdk::helpers::redact_column_regex(&batch, "body", &regex, "[REDACTED]")?;

// Environment access
let env = std::env::var("APP_ENV").unwrap_or_else(|_| "unknown".to_string());
```

---

## 5. Host Runtime & Execution Engine (`crates/wasm-transformer`)

The host transformer integrates into the pipeline via `pipeline_core::pipeline::Transform`.

### 5.1 Configuration (`pipeline.toml`)

```toml
[[pipeline.transforms]]
id = "pii_scrubber"
type = "wasm"
module_path = "transforms/enrichment.wasm"
sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" # Optional integrity verification
max_execution_duration = "500ms"
max_batch_rows = 5000                # Invariant boundary limit; reject if exceeded
concurrency = 4                      # Number of worker tasks / pooling slots
max_memory = "64MiB"                 # Virtual memory slot size (recommended: 64MiB)
rejuvenate_threshold = "16MiB"       # Soft memory limit for instant page reclamation
rejuvenate_batches = 10000           # Maximum batches before hygiene refresh
init_timeout = "2s"                  # Maximum duration for datalake_init

# Failure Policy (Unified Enum: "drop" | "reroute" | "passthrough")
on_error = "reroute"                 # "reroute" requires a sink subscribed to <id>._reroute_errored
allow_unmasked_passthrough = false   # Required only if on_error = "passthrough"

# Audit Rejection Routing (TransformResult::Reject)
reroute_on_reject = true             # Routes TransformResult::Reject to <id>._reroute_aborted

# Schema Guard Mode: "defensive" (backfill typed nulls) or "strict" (fail on missing columns)
schema_guard = "defensive"

# Whitelisted host environment variables passed to the WASM sandbox
env_whitelist = [
    "APP_ENV",
    "CLUSTER_ID",
    "REGION"
]

# Explicit static environment variables injected into the WASM sandbox (overrides host env)
[pipeline.transform.wasm.env]
LOG_LEVEL = "info"
TRANSFORM_VERSION = "1.2.0"

# Optional config block passed to datalake_init
[pipeline.transform.wasm.config]
environment = "production"
mask_credit_cards = true
```

### 5.2 Failure Policies & Backpressure Semantics

1. **`on_error = "reroute"` (Default Recommended)**:
   - When a batch execution fails (timeout, panic, trap, IPC decode error), the pristine original batch is routed to the virtual stream `<component_id>._reroute_errored`.
   - The topology builder asserts at startup that a sink is subscribed to `inputs = ["<component_id>._reroute_errored"]`. If missing, startup fails immediately.
2. **`on_error = "passthrough"` (Fail-Open)**:
   - Requires `allow_unmasked_passthrough = true` in configuration.
   - When a transform fails, the worker **preserves the original batch in memory** and forwards it to the primary `output.send(batch).await`.
   - The worker **blocks under backpressure** until downstream sinks accept the batch. If downstream is stalled, backpressure propagates upstream to the OTLP receiver, which returns `503 Service Unavailable` to the caller. No data is dropped.
3. **`on_error = "drop"` (Fail-Closed)**:
   - Failed batches are discarded from memory, and `component_errors_total` is incremented.

### 5.3 Audit Rejection vs. Noise Discard Semantics

1. **`TransformResult::Discard` (Noise Filtering / Sampling)**:
   - Telemetry that the module intentionally filters out (e.g. noisy health checks).
   - Dropped directly from memory. Never sent to any sink.
   - Increments `component_discarded_rows_total{component_id="...", reason="discard"}`.
2. **`TransformResult::Reject { reason }` (Audit Policy Drop)**:
   - Telemetry dropped due to business validation failure or security rule.
   - If `reroute_on_reject = true`, the original batch is routed to `<component_id>._reroute_aborted`.
   - The topology builder requires a sink subscribed to `inputs = ["<component_id>._reroute_aborted"]`.
   - Increments `component_discarded_rows_total{component_id="...", reason="reject"}`.

### 5.4 Core Telemetry Immutability & Schema Guard

#### 5.4.1 Mandatory Immutable Core Fields
The OpenTelemetry data model relies on immutable trace and metric identities. The host enforces that these fields cannot be dropped or nullified:
* **Traces**: `trace_id`, `span_id`.
* **Logs**: `timestamp` (or `observed_timestamp`).
* **Metrics**: Metric `name`, metric `type`.

If an outgoing batch drops an immutable column or contains all-null values for an immutable column where the input had valid IDs, the host rejects the batch with `WasmTransformError::ImmutableFieldViolation { column }` and triggers the configured `on_error` policy.

#### 5.4.2 Schema Guard (Defensive vs. Strict)
For mutable fields (`attributes`, `scope_attributes`, `resource_attributes`, `body`, `exemplars`):
* **`schema_guard = "defensive"` (Default)**:
  - If a guest omits a mutable canonical column, the host backfills it using `arrow::array::new_null_array(field.data_type(), num_rows)`.
  - Increments counter: `datalake_wasm_schema_columns_backfilled_total{component_id, signal, column}`.
  - Logs a rate-limited `WARN` (at most once every 60s per column) to ensure visibility of guest bugs.
* **`schema_guard = "strict"`**:
  - Missing mutable columns are treated as an error, rejecting the batch and triggering `on_error`.

### 5.5 Resilient Rejuvenation & Lifecycle

When a worker instance rejuvenates (due to `rejuvenate_threshold = 16MiB` or `rejuvenate_batches = 10000`):
1. **Non-Destructive Instance Swap**:
   - The worker initializes a candidate `Store` from the active `Arc<Module>` and invokes `datalake_init`.
   - The old instance is **not discarded** until `datalake_init` succeeds.
2. **Retry with Exponential Backoff**:
   - If `datalake_init` returns non-zero or times out (`init_timeout = 2s`), the worker logs an error and retries up to 3 times with exponential backoff (100ms, 200ms, 400ms).
   - If all retries fail, the worker keeps serving on the existing store (avoiding permanent worker slot death), logs a critical warning, and attempts rejuvenation again after 100 batches.

### 5.6 Vector-Aligned Observability & Controlled Metrics

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
3. **Controlled Custom Metric Injection**:
   - Custom guest metrics are restricted to:
     ```text
     datalake_transformers_<component_id>_<metric_name>{component_id="...", signal="..."}
     ```
   - Type mapping:
     - `metric_type = 0` (Counter): Prometheus counter.
     - `metric_type = 1` (Gauge): Prometheus gauge.
     - `metric_type = 2` (Duration): Value in nanoseconds (`u64`), converted to seconds (`f64 / 1e9`) and recorded into a Prometheus Histogram (`..._duration_seconds`).
   - Hard limits:
     - Metric names: ASCII alphanumeric + `_`, max 64 characters.
     - Cardinality ceiling: Maximum 50 distinct custom metric names per `component_id`.

### 5.7 Environment Variable Sandboxing & Zero-Trust WASI

1. **Default Deny**: By default, `wasmtime_wasi::WasiCtxBuilder` does not inherit host environment variables.
2. **Precedence Rule**:
   - `[pipeline.transform.wasm.env]` (static explicit injection) **strictly overrides** variables resolved from `env_whitelist`.
   - If a key exists in both, the static value is used and an informational notice is logged.
3. **Audit Logging**: At startup and reload, the host logs the list of permitted variable names (values are masked) for compliance auditability.

### 5.8 Kubernetes Sizing & Memory Guidance

$$\text{Virtual Memory Reserved} = \text{concurrency} \times \text{max\_memory}$$

* **Recommended Edge Configuration**: `concurrency = 4`, `max_memory = "64MiB"`, `rejuvenate_threshold = "16MiB"`. Requires $256\text{MiB}$ of virtual address space.
* **Host Physical RAM**: Actual physical RSS stays around $\text{concurrency} \times \text{rejuvenate\_threshold}$ ($\approx 64\text{MiB}$).
* **Container Limits**: Ensure pod `resources.limits.memory` is at least $2\times$ the expected RSS, and the host OS `vm.max_map_count` is sufficient.

### 5.9 Deterministic Hot-Reloading & Generation Fencing

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
| `datalake_transformers_<component_id>_<name>` | Counter / Gauge | `component_id`, `signal` | Guest custom counter or gauge. |
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
   - Injects test environment variables into guest WASI context.
   - Enables DWARF debug info for full guest stack traces on panics.
   - Verifies handling of 0-row batches, Discard, Reject, and canonical schema invariance.
3. **`datalake-wasm bench <path.wasm> [--rows 10000] [--concurrency 4]`**:
   - Measures batch transformation latency distribution (p50, p95, p99), throughput (rows/sec), and peak memory.

---

## 8. Language-Agnostic Evolution Path

1. **Standardized C-ABI v1**: TinyGo, C/C++, and Zig can immediately produce conformant WASM blobs by implementing the 5 exported functions (`datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
2. **Component Model / WASI 0.2 WIT**: A future `.wit` interface definition (`datalake:transformer/transform@0.1.0`) can wrap this same engine, enabling WIT-based bindings without breaking the underlying Arrow IPC memory exchange.
