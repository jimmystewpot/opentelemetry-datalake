# Design Specification: WebAssembly (WASM) Whole-Batch Arrow Transformer

**Date**: 2026-09-20  
**Status**: Final Approved Spec (Incorporating Architectural Review & Brainstorming Refinements)  
**Target Crates**:
- `crates/wasm-transformer` (Host runtime implementing `pipeline_core::pipeline::Transform`)
- `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`, standalone publishable SDK)
- `crates/wasm-cli` (`datalake-wasm-tool`, developer testing and validation CLI)

---

## 1. Overview & Objectives

This specification defines the architecture, ABI, guest SDK, and verification tooling for executing WebAssembly (WASM) transformations on Apache Arrow `RecordBatch` payloads in `opentelemetry-datalake`.

### Key Architectural Decisions
* **Whole-Batch Transformations**: Users can inspect, enrich, mask, filter rows, or drop/nullify fields inside a sandboxed WASM environment and emit `0..N` transformed `RecordBatch`es.
* **Pure Unordered Concurrency**: In alignment with distributed OpenTelemetry principles, batches are processed concurrently without artificial inter-batch FIFO sequencing, eliminating head-of-line blocking and reorder buffer stalls.
* **Memory Safety via Wasmtime Pooling Allocator**: Uses pre-allocated virtual memory slots with microsecond physical page resets via `madvise(MADV_DONTNEED)`. Dual-trigger rejuvenation (soft memory threshold + batch count ceiling) guarantees zero memory leaks or fragmentation bloat.
* **Pre-Transform Zero-Copy Batch Slicing**: Slices oversized batches natively using `RecordBatch::slice()` before entering the WASM sandbox, preserving dictionary arrays without copies and bounding memory usage.
* **Canonical OpenTelemetry Schema Invariance**: Telemetry entering and exiting the transformer always adheres to the canonical OpenTelemetry Arrow schema for that signal type. Stripped fields are represented as nulls or empty structures. The host automatically backfills any missing canonical columns with typed null arrays, guaranteeing downstream sinks (Iceberg, StarRocks, Elasticsearch) never experience schema failure.
* **Actionable Panic Diagnostics**: The SDK installs a standard WASM panic hook capturing source file, line, and message, emitted directly into host structured logs.
* **Enterprise Governance & Hot-Reloading**: Optional SHA-256 checksum verification and atomic zero-downtime hot-reloading (`POST /api/v1/transforms/wasm/reload` or `SIGHUP`) without dropping active gRPC/HTTP ingestion streams.

---

## 2. Workspace Crate Architecture

```text
opentelemetry-datalake/
├── crates/
│   ├── wasm-transformer/             # Host runtime transformer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # WasmTransformer implementing pipeline_core::Transform
│   │       ├── config.rs              # TOML deserialization (paths, limits, concurrency, sha256)
│   │       ├── engine.rs              # Wasmtime Engine & compiled Module cache (Arc<Module>)
│   │       ├── pool.rs                # Pooling instance allocator & soft rejuvenation lifecycle
│   │       ├── splitter.rs            # Zero-copy pre-transform batch slicer (RecordBatch::slice)
│   │       ├── schema_guard.rs        # Canonical OTel schema validation & null backfiller
│   │       ├── host_calls.rs          # Versioned host imports (datalake_host_v1)
│   │       ├── reload.rs              # Atomic zero-downtime hot-reloader
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
│   │       ├── panic.rs               # Custom std::panic hook forwarding to datalake_host_log
│   │       ├── logger.rs              # Guest tracing/log forwarder using HostLogRecord
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

    /// 0 = Success (emit batches)
    /// 1 = Abort/Drop (deliberately drop payload)
    /// 2 = Error (execution failure, panic, or unhandled error)
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
    /// Emit 0..N transformed RecordBatches downstream.
    Success(Vec<RecordBatch>),
    /// Deliberately drop/abort the payload with a reason.
    Drop { reason: Option<String> },
}

impl TransformResult {
    pub fn ok(batch: RecordBatch) -> Self {
        Self::Success(vec![batch])
    }

    pub fn ok_multiple(batches: Vec<RecordBatch>) -> Self {
        Self::Success(batches)
    }

    pub fn drop_payload(reason: impl Into<String>) -> Self {
        Self::Drop { reason: Some(reason.into()) }
    }
}

pub trait BatchTransformer: Default + Send + Sync + 'static {
    fn init(&mut self, _config: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

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
// routing it through datalake_host_log before returning status = 2.
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

### 4.4 Canonical Schema Helpers

The SDK provides zero-copy helpers for stripping data while maintaining canonical schema invariance:
- `sdk::helpers::nullify_column(&batch, "scope_attributes")`
- `sdk::helpers::filter_batch(&batch, &boolean_mask)`
- `sdk::helpers::redact_column_regex(&batch, "body", &regex, "[REDACTED]")`

---

## 5. Host Runtime & Execution Engine (`crates/wasm-transformer`)

The host transformer integrates into the pipeline via `pipeline_core::pipeline::Transform`.

### 5.1 Configuration (`pipeline.toml`)

```toml
[pipeline.transform.wasm]
module_path = "transforms/enrichment.wasm"
sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" # Optional integrity verification
max_execution_duration = "500ms"
max_batch_rows = 5000                # Pre-transform batch slicer threshold
concurrency = 4                      # Number of worker tasks / pooling slots
max_memory = "128MiB"                # Hard virtual memory slot size
rejuvenate_threshold = "32MiB"       # Soft memory limit for instant page reclamation
rejuvenate_batches = 10000           # Maximum batches before hygiene refresh
on_error = "drop"                    # "drop", "quarantine", or "passthrough"
allow_unmasked_passthrough = false   # Required if on_error = "passthrough"

# Optional config block passed to datalake_init
[pipeline.transform.wasm.config]
environment = "production"
mask_credit_cards = true
```

### 5.2 Concurrency & Worker Model

* **Lock-Free Concurrency**: $N$ independent worker tasks pull directly from `input: PipelineReceiver` and emit directly to `output: PipelineSender`. No inter-batch reorder buffer, no head-of-line blocking.
* **Pre-Transform Batch Slicer**: If an incoming batch exceeds `max_batch_rows`, the host splits it into zero-copy slices via `batch.slice(offset, len)` before dispatching to WASM. Dictionary arrays are shared via `Arc` without copying.

### 5.3 Memory Management: Wasmtime Pooling Allocator & Rejuvenation

* **Pooling Instance Allocator**: Pre-allocates $N$ memory slots in virtual memory at startup (`PoolingAllocationConfig`).
* **Microsecond Resets (`MADV_DONTNEED`)**: When an instance is refreshed, Wasmtime issues `MADV_DONTNEED` to reclaim physical RAM pages and zero the memory in $\approx 5\text{--}10\,\mu\text{s}$.
* **Rejuvenation Triggers**:
  1. *Soft Memory Cap*: If linear memory $> 32\text{MiB}$ after a batch, the `Store` is reset.
  2. *Hygiene Trigger*: Every $10,000$ batches, the `Store` is reset.
  3. *Trap Recovery*: If an instance traps, the contaminated `Store` is immediately discarded and replaced.

### 5.4 Canonical OTel Schema Invariance & Defense Guard

* Telemetry entering and exiting the WASM boundary must conform to the canonical OpenTelemetry Arrow schema for that `SignalType`.
* **Defensive Backfill**: If a guest module emits a batch omitting a canonical column (e.g. user completely dropped `scope_attributes`), `schema_guard` automatically backfills the missing column with a typed `NullArray` of length `num_rows()`. Downstream sinks (Iceberg, StarRocks, Elasticsearch) are 100% immune to schema corruption.

### 5.5 Failure Policies & Security Safeguard

* **`on_error = "drop"` (Default / Fail-Closed)**: Discards failed batches, incrementing `datalake_wasm_errors_total`.
* **`on_error = "quarantine"`**: Routes failed batches to dead-letter storage.
* **`on_error = "passthrough"` (Fail-Open)**: Forwards un-transformed raw batches. Requires `allow_unmasked_passthrough = true` in config; otherwise startup halts with a fatal security error.

### 5.6 Zero-Downtime Hot-Reloading

1. Admin endpoint `POST /api/v1/transforms/wasm/reload` or `SIGHUP` triggers reload.
2. Host reads file, verifies SHA-256 hash (if configured), and compiles the new `wasmtime::Module` in the background.
3. Host atomically replaces `Arc<Module>`.
4. In-flight worker batches finish on the old module. The next time a worker takes an instance from the pool, it instantiates from the new module.
5. Ingestion gRPC/HTTP endpoints experience zero connection resets.

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
| `datalake_wasm_rejuvenations_total` | Counter | `instance_id`, `reason` (`memory_threshold`, `batch_count`, `trap`) | Total instance pool resets. |
| `datalake_wasm_module_info` | Gauge | `sha256`, `abi_version` | Active module audit information. |

### Logging

* **Panic & Guest Log Forwarding**: Host intercepts `datalake_host_log` (`HostLogRecord`) and emits structured logs:
  ```text
  ERROR wasm_guest: Guest panic in transforms/pii.rs:42: called `Option::unwrap()` on a `None` value [instance_id=2, signal=Logs]
  ```

---

## 7. Conformity & Testing Tooling (`crates/wasm-cli`)

Dedicated CLI utility (`datalake-wasm`) verifies compiled `.wasm` modules:

1. **`datalake-wasm validate <path.wasm>`**:
   - Verifies `datalake_abi_version()` returns `1`.
   - Checks exports (`datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
   - Rejects forbidden WASI syscalls (raw sockets/files).
   - Validates memory limits and initialization behavior.
2. **`datalake-wasm test <path.wasm> [--signal logs|metrics|traces] [--input sample.ipc]`**:
   - Executes module against synthetic or user-provided Arrow IPC streams.
   - Enables DWARF debug info for full guest stack traces on panics.
   - Verifies handling of 0-row batches, drops, and canonical schema invariance.
3. **`datalake-wasm bench <path.wasm> [--rows 10000] [--concurrency 4]`**:
   - Measures batch transformation latency distribution (p50, p95, p99), throughput (rows/sec), and peak memory.

---

## 8. Language-Agnostic Evolution Path

1. **Standardized C-ABI v1**: TinyGo, C/C++, and Zig can immediately produce conformant WASM blobs by implementing the 5 exported functions (`datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
2. **Component Model / WASI 0.2 WIT**: A future `.wit` interface definition (`datalake:transformer/transform@0.1.0`) can wrap this same engine, enabling WIT-based bindings without breaking the underlying Arrow IPC memory exchange.
