# Design Specification: WebAssembly (WASM) Whole-Batch Arrow Transformer

**Date**: 2026-09-20  
**Status**: Approved (Draft Spec)  
**Target Crates**:
- `crates/wasm-transformer` (Host runtime implementing `pipeline_core::pipeline::Transform`)
- `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`, standalone publishable SDK)
- `crates/wasm-cli` (`datalake-wasm-tool`, developer testing and validation CLI)

---

## 1. Overview & Objectives

This specification defines the architecture, ABI, guest SDK, and verification tooling for executing WebAssembly (WASM) transformations on Apache Arrow `RecordBatch` payloads in `opentelemetry-datalake`.

### Key Objectives
* **Whole-Batch Transformations**: Users can inspect, enrich, mask, filter rows, drop columns, or split batches inside a sandboxed WASM environment and emit `0..N` transformed `RecordBatch`es.
* **Explicit Drop/Abort**: WASM transforms can deliberately abort or drop batches (e.g. invalid data, test traffic, compliance filters) with full observability into reasons and counts.
* **Strict Sandboxing & Resilience**: Guaranteed zero-panic host safety. Guest crashes, panics, memory exhaustion, or infinite loops are cleanly contained and recovered without impacting the pipeline.
* **Intuitive Resource Limits**: Configurable wall-clock duration timeouts via epoch interruption and hard memory ceilings.
* **Dual-Layer Testing & Conformity**: Fast, native unit testing (`cargo test`) via an idiomatic Rust trait, paired with a WASM binary conformance harness and standalone CLI tool (`datalake-wasm validate|test|bench`).
* **Clean Language-Agnostic Evolution**: While prioritized for Rust with an ergonomic SDK, the underlying C-ABI exchanges standard Apache Arrow IPC byte streams, allowing future guest languages (TinyGo, C/C++, Zig) or WIT/Component Model adapters without modifying the host.

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
│   │       ├── config.rs              # TOML deserialization (paths, limits, concurrency)
│   │       ├── engine.rs              # Wasmtime Engine & compiled Module cache
│   │       ├── instance.rs            # Store<HostState>, ResourceLimiter, memory bounds
│   │       ├── worker.rs              # Worker task loop & channel orchestration
│   │       ├── host_calls.rs          # Host imports (logging, metrics, capability probe)
│   │       └── error.rs               # WasmTransformError (Timeout, Trap, OOM, IPC)
│   │
│   ├── wasm-sdk/                      # Standalone, publishable guest SDK
│   │   │                              # (opentelemetry-datalake-wasm-sdk)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                 # Public prelude, BatchTransformer trait, TransformResult
│   │       ├── abi.rs                 # FFI export boilerplate & linear memory protocol
│   │       ├── ipc.rs                 # Arrow IPC stream reading & writing in guest
│   │       ├── helpers.rs             # Column dropping, projection, filtering utilities
│   │       ├── logger.rs              # Guest tracing/log forwarder to host
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

The ABI contract is a low-overhead, C-compatible interface operating on WebAssembly linear memory (`wasm32`).

### 3.1 Exported Guest Functions (Required)

```c
// 1. Allocate buffer of `size` bytes in guest linear memory.
// Returns 32-bit pointer (offset in WASM memory), or 0 on failure.
uint32_t datalake_alloc(uint32_t size);

// 2. Deallocate buffer previously allocated by datalake_alloc.
void datalake_dealloc(uint32_t ptr, uint32_t size);

// 3. Optional one-time configuration initialization.
// Returns 0 on success, non-zero on failure.
int32_t datalake_init(uint32_t cfg_ptr, uint32_t cfg_len);

// 4. Transform incoming batch.
// signal_type: 0 = Logs, 1 = Metrics, 2 = Traces
// ipc_ptr / ipc_len: input Arrow IPC stream bytes
// Returns a packed 64-bit value: (response_ptr << 32) | response_len
uint64_t datalake_transform(uint32_t signal_type, uint32_t ipc_ptr, uint32_t ipc_len);
```

### 3.2 Response Header Format

The packed `uint64_t` returned by `datalake_transform` points to a response header in guest memory:

```rust
#[repr(C)]
pub struct TransformResponseHeader {
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

### 3.3 Memory Exchange Lifecycle

1. **Alloc**: Host calls `datalake_alloc(input_bytes.len())` in guest memory.
2. **Copy In**: Host writes Arrow IPC stream bytes into guest memory at the returned pointer.
3. **Invoke**: Host sets epoch deadline and executes `datalake_transform(signal_type, ptr, len)`.
4. **Copy Out**:
   - If `status == 0` (Success): Host reads `batch_count` and each `BatchDescriptor`, deserializing the bytes into Arrow `RecordBatch` instances via `arrow::ipc::reader::StreamReader`.
   - If `status == 1` (Abort/Drop): Host extracts optional `message` reason, drops the payload, and records drop metrics.
   - If `status == 2` (Error): Host extracts error message, handles according to `on_error` policy.
5. **Dealloc**: Host invokes `datalake_dealloc` on input buffer, response header, descriptors, and output batch buffers.

### 3.4 Imported Host Functions (Module: `datalake_env`)

```c
// Emits a structured log message into the host's tracing subsystem.
// The host enriches this call with instance_id and signal_type.
void datalake_host_log(
    uint32_t level,       // 1 = Error, 2 = Warn, 3 = Info, 4 = Debug, 5 = Trace
    uint32_t msg_ptr,
    uint32_t msg_len,
    uint32_t target_ptr,
    uint32_t target_len,
    uint32_t file_ptr,
    uint32_t file_len,
    uint32_t line
);

// Increments a custom metric counter on the host.
void datalake_host_metric_inc(
    uint32_t name_ptr,
    uint32_t name_len,
    uint64_t value
);

// Dynamic capability query for future extensions (cache, geoip, secrets).
// Returns 1 if supported, 0 otherwise.
uint32_t datalake_host_has_capability(
    uint32_t cap_name_ptr,
    uint32_t cap_name_len
);
```

---

## 4. Guest SDK (`opentelemetry-datalake-wasm-sdk`)

Designed for publishability on `crates.io`, this crate allows users to author WASM transformers using safe, idiomatic Rust.

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

### 4.2 Entry Point Registration

A simple macro generates all FFI symbols and handles memory serialization automatically:

```rust
// Macro expanded in user's WASM module:
export_transformer!(MyCustomTransformer);
```

### 4.3 Native Unit Testing Support

Users test their `BatchTransformer` logic directly with `cargo test` on their native host architecture:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_datalake_wasm_sdk::testing::*;

    #[test]
    fn test_custom_enrichment() {
        let mut transformer = MyCustomTransformer::default();
        transformer.init(b"{\"environment\": \"prod\"}").unwrap();

        let input_batch = create_mock_logs_batch(vec![
            ("message", "Database connection reset"),
        ]);

        let result = transformer.transform(SignalType::Logs, input_batch).unwrap();
        match result {
            TransformResult::Success(batches) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
            }
            TransformResult::Drop { .. } => panic!("Unexpected drop"),
        }
    }
}
```

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
on_error = "drop" # "drop" or "passthrough"

# Optional config block passed to datalake_init
[pipeline.transform.wasm.config]
environment = "production"
mask_credit_cards = true
```

### 5.2 Concurrency & Worker Architecture

* **Worker Pool**: Spawns $N$ independent worker tasks (`concurrency`).
* **Instance Per Worker**: Each worker owns a dedicated `wasmtime::Store<HostState>` and `wasmtime::Instance`. There is zero lock contention across worker threads.
* **Epoch-Based Timeout**: A centralized background ticker advances the engine epoch every 10ms. Each execution sets an epoch deadline calculated from `max_execution_duration`.
* **Memory Limiter**: A `wasmtime::ResourceLimiter` implementation enforces `max_memory` (default: 256 MiB).
* **Fault Isolation**:
  - Traps, panics, or timeouts return `Result::Err(WasmTransformError)`.
  - The contaminated `Store` is discarded.
  - A clean `Store` and `Instance` are instantiated from the precompiled `Module` and initialized with the configuration bytes.
  - The host process and Tokio runtime never panic.

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

### Logging

* **Guest Log Forwarding**: Host intercepts `datalake_host_log` and emits a `tracing::event!` with target `"wasm_guest"` including:
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
   - Inspects exports (`datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`).
   - Rejects forbidden WASI imports (e.g. raw filesystem/socket calls).
   - Validates memory limits and initialization behavior.

2. **`datalake-wasm test <path.wasm> [--signal logs|metrics|traces] [--input sample.ipc]`**:
   - Executes module against synthetic or user-provided Arrow IPC streams.
   - Verifies handling of 0-row batches, normal batches, and deliberate drops.
   - Detects memory leaks inside guest memory across successive calls.

3. **`datalake-wasm bench <path.wasm> [--rows 10000] [--concurrency 4]`**:
   - Measures batch transformation latency, throughput (rows/sec), and peak memory.

---

## 8. Language-Agnostic Evolution Path

While Phase 1 targets Rust guests via `opentelemetry-datalake-wasm-sdk`, the architecture guarantees future multi-language compatibility:
1. **Core C ABI**: Because the underlying interface is standard C ABI passing Arrow IPC byte buffers, TinyGo, C/C++, and Zig can immediately produce conformant WASM blobs by implementing the 4 exported functions.
2. **Component Model / WASI 0.2 WIT**: A future `.wit` interface definition (`datalake:transformer/transform@0.1.0`) can wrap this same engine, enabling WIT-based bindings without breaking the underlying Arrow IPC memory exchange.
