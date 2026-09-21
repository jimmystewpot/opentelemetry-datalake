# OpenTelemetry Datalake — WebAssembly (WASM) Guest SDK

`opentelemetry-datalake-wasm-sdk` provides the low-overhead C-ABI v1 data structures, memory allocators, canonical immutability guards, host metrics reporting, and transformation abstractions for writing WebAssembly guest transform plugins for `opentelemetry-datalake`.

Plugins compiled with this SDK operate inside a sandboxed WebAssembly runtime (`wasm-transformer`) embedded within the `opentelemetry-datalake` telemetry ingestion pipeline. Telemetry records (Logs, Metrics, and Traces) are exchanged across the WebAssembly boundary using Apache Arrow IPC streams, enabling zero-copy deserialization and vectorized columnar transformations.

---

## Core Features

- **Apache Arrow Columnar Processing**: Transforms operate directly on Arrow `RecordBatch` instances with full access to Arrow arrays and vector computation.
- **C-ABI v1 Specification**: Full conformance with the standardized `opentelemetry-datalake` C-ABI v1 contract.
- **Zero-Panic Safety**: Automatic WebAssembly panic hook integration forwarding guest panics to host tracing (`datalake_host_log`) to prevent unhandled runtime traps.
- **Controlled Host Observability**: Low-overhead guest metrics reporting (`counter`, `gauge`, and `duration`) using normalized scalars and IEEE-754 bitcast conversions without dynamic string allocations.
- **Canonical Immutability Guards**: Enforces core OpenTelemetry compliance by preventing modifications or nullification of immutable identification columns (`trace_id`, `span_id`, `timestamp`, `observed_timestamp`, `name`, `type`).
- **Native Testability**: Built-in thread-safe mock memory allocator allows writing native unit and integration tests (`cargo test`) without requiring a WebAssembly runtime.

---

## Complete C-ABI v1 Guest Lifecycle

The lifecycle of a WebAssembly guest transform plugin consists of five sequential phases:

```text
 Host Runtime (wasm-transformer)                 Guest WASM Module (wasm-sdk)
 ===============================                 ============================
                |                                             |
                |  1. ABI Negotiation: datalake_abi_version() |
                |-------------------------------------------->| (returns ABI_VERSION = 1)
                |                                             |
                |  2. Memory Provisioning: datalake_alloc()   |
                |-------------------------------------------->| (allocates linear memory buffer)
                |     write config JSON into guest buffer     |
                |                                             |
                |  3. Initialization: datalake_init()         |
                |-------------------------------------------->| init_panic_hook()
                |                                             | parse_signal_from_config()
                |                                             | BatchTransformer::init()
                |                                             | stores instance in Mutex
                |<--------------------------------------------| returns status (0=OK, 1=Err)
                |     datalake_dealloc() config buffer        |
                |                                             |
                |  4. Batch Transformation: datalake_transform()
                |     write Arrow IPC batch into guest buffer |
                |-------------------------------------------->| decode_ipc_stream()
                |                                             | BatchTransformer::transform()
                |                                             | emit metrics / logs to host
                |                                             | encode_batch_to_ipc()
                |                                             | packs TransformResponseHeader
                |<--------------------------------------------| returns packed (ptr << 32 | 20)
                |                                             |
                |  5. Host Memory Reclaim: datalake_dealloc() |
                |     reads header, descriptors, IPC buffers  |
                |-------------------------------------------->| frees header, descriptor array,
                |                                             | IPC batch buffers, message
```

### 1. ABI Negotiation
Upon loading the guest WebAssembly module, the host runtime invokes:
```rust
#[unsafe(no_mangle)]
pub extern "C" fn datalake_abi_version() -> u32
```
The guest returns `ABI_VERSION` (currently `1`). If the version does not match host expectations, loading is aborted.

### 2. Memory Provisioning & Ownership Contract
The host writes payloads into guest linear memory by invoking exported allocator functions:
- `datalake_alloc(size: u32) -> u32`: Allocates an 8-byte aligned memory buffer of `size` bytes. Returns `0` on allocation failure or if `size == 0`.
- `datalake_dealloc(ptr: u32, size: u32)`: Reclaims the allocated memory buffer.

**Memory Ownership Contract:**
Any memory allocated by the guest to return results to the host across the ABI boundary—including the 20-byte `TransformResponseHeader`, the `BatchDescriptor` array, each serialized Arrow IPC buffer, and any rejection/error message string—transfers ownership to the host runtime. The host **MUST** call `datalake_dealloc` for each of these buffers once reading is complete.

### 3. Plugin Initialization
The host runtime invokes:
```rust
#[unsafe(no_mangle)]
pub extern "C" fn datalake_init(config_ptr: u32, config_len: u32) -> u32
```
1. Registers the guest panic hook (`init_panic_hook()`), which captures panic payloads and file locations, forwarding them to the host import `datalake_host_log`.
2. Reads the configuration string from guest memory (if `config_ptr != 0` and `config_len > 0`).
3. Extracts the target `SignalType` (`Logs = 0`, `Metrics = 1`, `Traces = 2`) from configuration.
4. Invokes the plugin's `BatchTransformer::init(signal, config_json)`.
5. Stores the initialized transformer instance in static storage (`TRANSFORMER_STATE`).
6. Returns `0` on success or `1` on error.

### 4. Batch Transformation
For every incoming batch of OpenTelemetry data, the host writes an Arrow IPC stream into guest memory and invokes:
```rust
#[unsafe(no_mangle)]
pub extern "C" fn datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64
```
1. Decodes the input Arrow IPC stream into an Arrow `RecordBatch`.
2. Invokes `BatchTransformer::transform(&mut self, batch)`.
3. Encodes the resulting `TransformResult` into output Arrow IPC buffers and constructs a 20-byte `TransformResponseHeader`:
   - `status`: `0` (Success), `1` (Discard), `2` (Reject), `3` (Error).
   - `batch_count`: Number of output record batches.
   - `batches_ptr`: Pointer to an array of `BatchDescriptor { ptr, len }` entries.
   - `message_ptr`: Pointer to optional rejection or error message string.
   - `message_len`: Length of rejection or error message in bytes.
4. Returns a packed 64-bit integer: `((header_ptr as u64) << 32) | 20u64`.

### 5. Memory Reclamation
The host reads the `TransformResponseHeader`, copies the transformed IPC buffers, and reclaims all guest allocations by calling `datalake_dealloc` on:
- The `header_ptr` (20 bytes).
- The `batches_ptr` (`batch_count * 8` bytes).
- Each batch descriptor's buffer (`desc.ptr`, `desc.len`).
- The `message_ptr` (`message_len` bytes, if present).
- The input `ipc_ptr` (`ipc_len` bytes).

---

## Quickstart: Writing a Transform Plugin

### 1. Configure `Cargo.toml`
Create a new library crate and configure it as a C dynamic library (`cdylib`):

```toml
[package]
name = "my-wasm-transformer"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
opentelemetry-datalake-wasm-sdk = "0.1.0"
arrow = { version = "54", default-features = false, features = ["ipc"] }
```

### 2. Implement `BatchTransformer`
Define your transformer struct, implement the `BatchTransformer` trait, and export it using `export_transformer!`:

```rust
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::export_transformer;
use opentelemetry_datalake_wasm_sdk::metrics::counter;
use opentelemetry_datalake_wasm_sdk::traits::{
    BatchTransformer, SignalType, TransformResult,
};

pub struct AttributeFilterTransformer {
    signal: SignalType,
}

impl BatchTransformer for AttributeFilterTransformer {
    fn init(signal: SignalType, config_json: Option<&str>) -> Result<Self, String> {
        // Parse optional JSON configuration or log initialization
        let _ = config_json;
        Ok(Self { signal })
    }

    fn transform(&mut self, batch: RecordBatch) -> TransformResult {
        // Count records transformed
        counter("guest_records_transformed", batch.num_rows() as u64);

        // Discard empty batches
        if batch.num_rows() == 0 {
            return TransformResult::discard();
        }

        // Return transformed batch
        TransformResult::ok(batch)
    }
}

// Export the C-ABI v1 entry points (datalake_init, datalake_transform)
export_transformer!(AttributeFilterTransformer);
```

---

## Handling Transformation Results

The `TransformResult` enum controls how batches are routed by the host pipeline:

| Variant | Constructor | Host Action |
|---|---|---|
| `Continue(Vec<RecordBatch>)` | `TransformResult::ok(batch)`<br>`TransformResult::ok_multiple(batches)`<br>`TransformResult::ok_empty()` | Passes transformed batches downstream to storage sinks. |
| `Discard` | `TransformResult::discard()` | Drops the batch silently. |
| `Reject { reason }` | `TransformResult::reject("reason")` | Diverts the batch to quarantine / dead-letter storage. |
| `Error { reason }` | `TransformResult::error("reason")` | Marks batch processing as failed and triggers host retry/alert policies. |

---

## Guest Observability & Metrics

Guest modules can emit controlled operational metrics back to the host pipeline across the C-ABI boundary using `opentelemetry_datalake_wasm_sdk::metrics`:

```rust
use opentelemetry_datalake_wasm_sdk::metrics::{counter, gauge, duration};
use std::time::{Duration, Instant};

let start = Instant::now();

// Monotonic counter
counter("transform_records_total", 100);

// Instantaneous gauge (f64 bitcast preservation)
gauge("memory_usage_ratio", 0.42);

// Processing duration
duration("batch_execution_time_ns", start.elapsed());
```

---

## Canonical Immutability Guards

To preserve downstream data lake schema integrity and query consistency, the OpenTelemetry specification requires that core identity fields remain intact.

The SDK defines `IMMUTABLE_COLUMNS`:
- `trace_id`
- `span_id`
- `timestamp`
- `observed_timestamp`
- `name`
- `type`

Use `helpers::nullify_column` to safely nullify non-immutable fields (e.g. for PII masking or schema pruning) while protecting core columns:

```rust
use opentelemetry_datalake_wasm_sdk::helpers::nullify_column;

// Safely nullify a mutable attribute column
let sanitized_batch = nullify_column(&batch, target_schema, "user_email")?;

// Attempting to nullify an immutable column returns SdkError::ImmutableFieldViolation
assert!(nullify_column(&batch, target_schema, "trace_id").is_err());
```

---

## Building and Testing

### Unit Testing Natively
The SDK includes a built-in mock memory allocator on native targets (`x86_64`, `aarch64`), allowing full unit tests to run via standard cargo commands:

```bash
cargo test
```

### Compiling to WebAssembly
To build your plugin for deployment to `opentelemetry-datalake`:

```bash
cargo build --target wasm32-unknown-unknown --release
```

The resulting WebAssembly artifact is located at:
`target/wasm32-unknown-unknown/release/<plugin_name>.wasm`
