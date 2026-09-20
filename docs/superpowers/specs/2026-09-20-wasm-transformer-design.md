# Design Specification: WebAssembly (WASM) Whole-Batch Arrow Transformer

**Date**: 2026-09-20  
**Status**: Revised Draft (Incorporating Principal Architect & Technical PM Review)  
**Target Crates**:
- `crates/wasm-transformer` (Host runtime implementing `pipeline_core::pipeline::Transform`)
- `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`, standalone publishable SDK)
- `crates/wasm-cli` (`datalake-wasm-tool`, developer testing and validation CLI)

---

## 1. Overview & Objectives

This specification defines the architecture, ABI, guest SDK, and verification tooling for executing WebAssembly (WASM) transformations on Apache Arrow `RecordBatch` payloads in `opentelemetry-datalake`.

### Key Objectives
* **Whole-Batch Transformations**: Users can inspect, enrich, mask, filter rows, drop columns, or split batches inside a sandboxed WASM environment and emit `0..N` transformed `RecordBatch`es.
* **Explicit Drop/Abort with Fail-Closed Safety**: WASM transforms can deliberately abort or drop batches (e.g. invalid data, test traffic, compliance filters) with full observability into reasons and counts. By default, failures fail-closed to protect against data leakage.
* **Deterministic Batch Ordering**: An async worker pool allows high-concurrency execution while a bounded sequence re-orderer preserves strict FIFO batch ordering for downstream ACID sinks.
* **Strict Sandboxing & Resilience**: Guaranteed zero-panic host safety. Guest crashes, panics, memory exhaustion, or infinite loops are cleanly contained and recovered without crashing the host process.
* **Intuitive Resource Limits**: Configurable wall-clock duration timeouts via epoch interruption and hard linear memory ceilings.
* **Dual-Layer Testing & Conformity**: Fast native unit testing (`cargo test`) via an idiomatic Rust trait, paired with a WASM binary conformance harness and standalone CLI tool (`datalake-wasm validate|test|bench`).
* **Versioned, Language-Agnostic ABI**: Standard C-ABI with explicit version handshakes and structured imports, exchanging standard Apache Arrow IPC byte streams.

---

## 2. Workspace Crate Architecture

The system is decomposed into three focused crates to ensure clean separation of concerns and independent versioning:

```text
opentelemetry-datalake/
├── crates/
│   ├── wasm-transformer/             # Host runtime transformer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # WasmTransformer implementing pipeline_core::Transform
│   │       ├── config.rs              # TOML deserialization (paths, limits, concurrency, ordering)
│   │       ├── engine.rs              # Wasmtime Engine & compiled Module cache
│   │       ├── instance.rs            # Store<HostState>, ResourceLimiter, memory bounds
│   │       ├── worker.rs              # Concurrent worker tasks with sequence-tagged execution
│   │       ├── reorder.rs             # Bounded re-sequencing buffer for FIFO batch ordering
│   │       ├── host_calls.rs          # Versioned host imports (logging, metrics, capability probe)
│   │       └── error.rs               # WasmTransformError (Timeout, Trap, OOM, IPC, VersionMismatch)
│   │
│   ├── wasm-sdk/                      # Standalone, publishable guest SDK
│   │   │                              # (opentelemetry-datalake-wasm-sdk)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # Public prelude, BatchTransformer trait, TransformResult
│   │       ├── abi.rs                 # Low-level FFI exports & memory protocol (with manual escape hatch)
│   │       ├── ipc.rs                 # Arrow IPC stream reading & writing in guest
│   │       ├── helpers.rs             # Column dropping, projection, filtering utilities
│   │       ├── logger.rs              # Guest tracing/log forwarder using structured HostLogRecord
│   │       └── testing.rs             # Mock batch generators & WasmHarness test runner
│   │
│   ├── wasm-cli/                      # Developer CLI & validation tool
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs                # CLI commands: validate, test, bench
│   │
│   └── core/                          # Updated pipeline_core config with WasmTransformerConfig
```

---

## 3. Host ↔ Guest ABI Specification

The ABI contract is a versioned, low-overhead, C-compatible interface operating on WebAssembly linear memory (`wasm32`).

### 3.1 ABI Versioning Handshake

To prevent version skew between host runtimes and guest WASM blobs:

1. **Mandatory Export**: Every conformant module must export:
   ```c
   uint32_t datalake_abi_version(void);
   ```
   Modules conforming to this specification must return `1`. If the host loads a module whose `datalake_abi_version` does not match, module instantiation immediately halts with `WasmTransformError::AbiVersionMismatch`.

2. **Namespaced Imports**: All host imports live under versioned namespaces (e.g. `datalake_host_v1`).

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

The packed `uint64_t` returned by `datalake_transform` points to a versioned response header in guest memory:

```rust
#[repr(C)]
pub struct TransformResponseHeader {
    /// ABI version (must be 1)
    pub abi_version: u32,

    /// 0 = Success (emit batches)
    /// 1 = Abort/Drop (deliberately drop payload)
    /// 2 = Error (execution failure)
    pub status: u32,

    /// Number of emitted Arrow IPC batches (0..N)
    pub batch_count: u32,

    /// Pointer to array of `BatchDescriptor { ptr: u32, len: u32 }`
    pub batches_ptr: u32,

    /// Pointer to optional UTF-8 string (abort reason or error message)
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

To avoid clumsy 8-parameter FFI signatures, log events pass a single pointer to a structured record:

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
// Emits structured log event. Host enriches with instance_id and signal_type.
void datalake_host_log(uint32_t record_ptr);

// Increments a telemetry metric counter on the host.
void datalake_host_metric_inc(uint32_t name_ptr, uint32_t name_len, uint64_t value);

// Dynamic capability query for future extensions (cache, geoip, secrets).
// Returns 1 if supported, 0 otherwise.
uint32_t datalake_host_has_capability(uint32_t cap_name_ptr, uint32_t cap_name_len);
```

### 3.5 Performance Envelope & Latency Overhead Budget

Crossing the WASM boundary with Arrow IPC involves four distinct phases:
1. Host serialization: `RecordBatch` $\to$ IPC stream buffer.
2. Host $\to$ Guest `memcpy` into WASM linear memory.
3. Guest deserialization: IPC stream buffer $\to$ guest `RecordBatch`.
4. The reverse sequence for output batch emission.

**Baseline Latency Budget**:
- For a standard batch of 2,000 rows (~500 KB uncompressed IPC stream), the round-trip boundary overhead (excluding user business logic) must satisfy:
  - **$p95 \le 1.5\text{ms}$**
  - **$p99 \le 3.0\text{ms}$**
- An automated benchmark (`benches/wasm_boundary_bench.rs`) compares `NoopTransformer` directly against a no-op WASM module to detect any serialization regressions in CI.
- **Future Fast Path**: If pipeline throughput exceeds 50,000 rows/sec per core and profiling indicates IPC overhead is a primary bottleneck, the architecture reserves a feature flag (`zero-copy-c-abi`) to transition to Arrow C Data Interface pointer passing across shared memory pages.

---

## 4. Guest SDK (`opentelemetry-datalake-wasm-sdk`)

Designed for publication on `crates.io`, this crate allows users to author WASM transformers using safe, idiomatic Rust.

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
    /// Emit 0..N transformed RecordBatches downstream.
    Success(Vec<RecordBatch>),
    /// Deliberately drop/abort the payload with a reason.
    Drop { reason: Option<String> },
}

impl TransformResult {
    /// 1:1 transformation convenience helper.
    pub fn ok(batch: RecordBatch) -> Self {
        Self::Success(vec![batch])
    }

    /// 1:N multi-batch convenience helper.
    pub fn ok_multiple(batches: Vec<RecordBatch>) -> Self {
        Self::Success(batches)
    }

    /// Abort/drop convenience helper.
    pub fn drop_payload(reason: impl Into<String>) -> Self {
        Self::Drop { reason: Some(reason.into()) }
    }
}

pub trait BatchTransformer: Default + Send + Sync + 'static {
    /// One-time initialization with raw configuration bytes.
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

### 4.2 Entry Point Registration & Manual Escape Hatch

#### Option A: Declarative Macro (Default)
The SDK provides `export_transformer!(MyType)`, a `macro_rules!` macro with clear compiler diagnostics:
```rust
export_transformer!(MyCustomTransformer);
```

#### Option B: Manual FFI Escape Hatch (No Magic)
For advanced users needing bespoke initialization, static state, or fine-grained allocator control, the SDK exposes the low-level processing pipeline directly:
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
pub extern "C" fn datalake_init(cfg_ptr: u32, cfg_len: u32) -> i32 {
    // Custom manual initialization logic
    0
}

#[no_mangle]
pub extern "C" fn datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64 {
    opentelemetry_datalake_wasm_sdk::abi::dispatch_transform::<MyCustomTransformer>(
        signal_type, ipc_ptr, ipc_len
    )
}
```

### 4.3 Compiler Target & Build Guidance
The SDK includes clear documentation and diagnostics for compiler targets:
- Target: `wasm32-wasip1` (recommended) or `wasm32-unknown-unknown`.
- Cargo build configuration (`.cargo/config.toml` template provided in SDK):
  ```toml
  [target.wasm32-wasip1]
  runner = "datalake-wasm test"
  ```
- Clear compiler error messages if non-WASM compatible crates (e.g. `tokio`, `std::net`) are pulled in transitively.

---

## 5. Host Runtime & Execution Engine (`crates/wasm-transformer`)

The host transformer integrates into the pipeline via `pipeline_core::pipeline::Transform`.

### 5.1 Configuration (`pipeline.toml`)

```toml
[pipeline.transform.wasm]
module_path = "transforms/enrichment.wasm"
max_execution_duration = "500ms"
max_memory = "256MiB"
concurrency = 4
ordered = true                       # Preserve strict FIFO arrival order downstream
on_error = "drop"                    # "drop", "quarantine", or "passthrough"
allow_unmasked_passthrough = false   # Required if on_error = "passthrough"

# Optional config block passed to datalake_init
[pipeline.transform.wasm.config]
environment = "production"
mask_credit_cards = true
```

### 5.2 Concurrency & Bounded FIFO Re-Sequencing

To guarantee maximum throughput while preserving batch sequence integrity for downstream ACID sinks:

```text
┌─────────────────┐      Tag: seq_id      ┌─────────────────────────┐
│ PipelineReceiver│ ────────────────────► │ Worker Dispatcher       │
└─────────────────┘                       └───────────┬─────────────┘
                                                      │
                       ┌──────────────────────────────┼──────────────────────────────┐
                       ▼                              ▼                              ▼
             ┌───────────────────┐          ┌───────────────────┐          ┌───────────────────┐
             │ Worker 1 (Store)  │          │ Worker 2 (Store)  │          │ Worker N (Store)  │
             └─────────┬─────────┘          └─────────┬─────────┘          └─────────┬─────────┘
                       │                              │                              │
                       └──────────────────────────────┼──────────────────────────────┘
                                                      ▼
                                          ┌─────────────────────────┐
                                          │ Re-Sequencing Buffer    │ (Bounded Priority Queue)
                                          │ Emits strictly in seq_id│
                                          └───────────┬─────────────┘
                                                      ▼
                                          ┌─────────────────────────┐
                                          │ PipelineSender (Sink)   │
                                          └─────────────────────────┘
```

1. **Dispatcher**: Incoming `SignalBatch`es are stamped with a monotonic `seq_id: u64`.
2. **Worker Pool**: Workers process batches concurrently across independent `wasmtime::Store` instances.
3. **Re-Sequencing Buffer (`ordered = true`)**:
   - Completed batches enter a bounded priority queue indexed by `seq_id`.
   - The buffer immediately flushes all contiguous completed sequence IDs downstream to `PipelineSender`.
   - `ordering_window_size` (default: 128) prevents unbounded memory growth if a single batch stalls.
4. **Unordered Mode (`ordered = false`)**: For telemetry sinks where order is irrelevant (e.g. Elasticsearch log streams), workers forward directly to `output` with zero buffering overhead.

### 5.3 Resource Bounds & Wall-Clock Interruption

* **Wall-Clock Interruption**: A background ticker advances the engine epoch every 10ms. Each batch invocation sets an epoch deadline calculated from `max_execution_duration`. When expired, the instance is immediately interrupted with `Trap::Interrupt`.
* **Memory Limiter**: A `wasmtime::ResourceLimiter` implementation enforces `max_memory` (default: 256 MiB).
* **Fault Isolation**: Traps return `Result::Err(WasmTransformError)`. The contaminated `Store` is discarded and cleanly rebuilt from the precompiled `Module`.

### 5.4 Security Threat Model & Failure Policies (`on_error`)

Transform failures (timeouts, traps, memory ceiling exceeded) represent critical decision points:

1. **`on_error = "drop"` (Default / Fail-Closed)**:
   - Batch is discarded. Drop reason and error context are recorded in logs and metrics.
   - Recommended for compliance, PII scrubbing, and security filtering.

2. **`on_error = "quarantine"`**:
   - Failed batches are routed to a dedicated dead-letter channel/quarantine table with error metadata attached.
   - Prevents data loss without poisoning the primary production sink.

3. **`on_error = "passthrough"` (Fail-Open / High-Availability)**:
   - Forwards the raw, un-transformed batch downstream.
   - **Mandatory Safety Check**: In configuration, `allow_unmasked_passthrough = true` must be explicitly specified. If missing, config validation aborts at startup with:
     ```text
     FATAL: on_error='passthrough' allows unmasked or non-compliant telemetry to reach storage sinks.
     You must set allow_unmasked_passthrough=true to explicitly acknowledge this security risk.
     ```

---

## 6. Observability & Metrics

All metrics strictly adhere to `docs/instrumentation.md` naming conventions:

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `datalake_wasm_batches_processed_total` | Counter | `signal`, `status` (`success`, `dropped`, `error`) | Total batches evaluated. |
| `datalake_wasm_batches_dropped_total` | Counter | `signal`, `reason` | Deliberately aborted/dropped batches. |
| `datalake_wasm_batches_emitted_total` | Counter | `signal` | Emitted batches downstream. |
| `datalake_wasm_rows_in_total` | Counter | `signal` | Incoming rows. |
| `datalake_wasm_rows_out_total` | Counter | `signal` | Outgoing rows after mutations. |
| `datalake_wasm_errors_total` | Counter | `signal`, `error_type` (`timeout`, `oom`, `trap`, `ipc_decode`) | Total execution failures. |
| `datalake_wasm_duration_seconds` | Histogram | `signal` | Execution latency per batch. |
| `datalake_wasm_memory_bytes` | Gauge | `instance_id` | Current linear memory consumption per instance. |
| `datalake_wasm_reorder_queue_depth` | Gauge | `signal` | Current queued batches waiting in re-sequencer. |

### Logging

* **Guest Log Forwarding**: Host intercepts `datalake_host_log` (`HostLogRecord`) and emits a `tracing::event!` with target `"wasm_guest"` including:
  - `instance_id`: Worker instance identifier
  - `signal`: Current signal being transformed
  - `file`, `line`, `target`: Guest source code location
* **Structured Abort Events**:
  ```rust
  tracing::info!(
      signal = ?signal,
      rows = batch.num_rows(),
      reason = %reason,
      "WASM transform aborted and dropped batch"
  );
  ```

---

## 7. Conformity & Testing Tooling (`crates/wasm-cli`)

A dedicated CLI utility (`datalake-wasm`) verifies compiled `.wasm` modules prior to deployment.

### Subcommands:

1. **`datalake-wasm validate <path.wasm>`**:
   - Verifies `datalake_abi_version()` returns `1`.
   - Inspects exports (`datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
   - Rejects forbidden WASI imports (e.g. raw filesystem/socket calls).
   - Validates memory limits and initialization behavior.

2. **`datalake-wasm test <path.wasm> [--signal logs|metrics|traces] [--input sample.ipc]`**:
   - Executes module against synthetic or user-provided Arrow IPC streams.
   - Verifies handling of 0-row batches, normal batches, and deliberate drops.
   - Detects memory leaks inside guest memory across successive calls.

3. **`datalake-wasm bench <path.wasm> [--rows 10000] [--concurrency 4]`**:
   - Measures batch transformation latency distribution (p50, p95, p99), throughput (rows/sec), and peak memory.

---

## 8. Language-Agnostic Evolution Path

While Phase 1 targets Rust guests via `opentelemetry-datalake-wasm-sdk`, the architecture guarantees future multi-language compatibility:
1. **Standardized C-ABI v1**: TinyGo, C/C++, and Zig can target the exact same FFI exports (`datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`) and read/write standard Arrow IPC streams without host modifications.
2. **Component Model / WASI 0.2 WIT**: A future `.wit` interface definition (`datalake:transformer/transform@0.1.0`) can wrap this same engine, enabling WIT-based bindings without breaking the underlying Arrow IPC memory exchange.
