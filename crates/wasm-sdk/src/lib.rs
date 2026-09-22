//! WebAssembly (WASM) Guest SDK for `opentelemetry-datalake`.
//!
//! Provides low-overhead C-ABI v1 data structures, safe allocators,
//! canonical immutability guards, metrics reporting, and transformation abstractions
//! for WebAssembly guest transform plugins.
//!
//! # Architecture & C-ABI v1 Guest Lifecycle
//!
//! Guest transform modules run inside a sandboxed WebAssembly runtime (`wasm-transformer`)
//! within the `opentelemetry-datalake` pipeline. Data exchange across the WebAssembly
//! boundary uses Apache Arrow IPC stream buffers to achieve zero-copy deserialization
//! and columnar processing.
//!
//! The complete C-ABI v1 guest lifecycle consists of five distinct phases:
//!
//! ```text
//!  Host Runtime (wasm-transformer)                 Guest WASM Module (wasm-sdk)
//!  ===============================                 ============================
//!                 |                                             |
//!                 |  1. ABI Negotiation: datalake_abi_version() |
//!                 |-------------------------------------------->| (returns ABI_VERSION = 1)
//!                 |                                             |
//!                 |  2. Memory Provisioning: datalake_alloc()   |
//!                 |-------------------------------------------->| (allocates linear memory buffer)
//!                 |     write config JSON into guest buffer     |
//!                 |                                             |
//!                 |  3. Initialization: datalake_init()         |
//!                 |-------------------------------------------->| init_panic_hook()
//!                 |                                             | parse_signal_from_config()
//!                 |                                             | BatchTransformer::init()
//!                 |                                             | stores instance in Mutex
//!                 |<--------------------------------------------| returns status (0=OK, 1=Err)
//!                 |     datalake_dealloc() config buffer        |
//!                 |                                             |
//!                 |  4. Batch Transformation: datalake_transform()
//!                 |     write Arrow IPC batch into guest buffer |
//!                 |-------------------------------------------->| decode_ipc_stream()
//!                 |                                             | BatchTransformer::transform()
//!                 |                                             | emit metrics / logs to host
//!                 |                                             | encode_batch_to_ipc()
//!                 |                                             | packs TransformResponseHeader
//!                 |<--------------------------------------------| returns packed (ptr << 32 | 20)
//!                 |                                             |
//!                 |  5. Host Memory Reclaim: datalake_dealloc() |
//!                 |     reads header, descriptors, IPC buffers  |
//!                 |-------------------------------------------->| frees header, descriptor array,
//!                 |                                             | IPC batch buffers, message
//! ```
//!
//! ## 1. ABI Negotiation
//! The host calls the exported function [`abi::datalake_abi_version`] to verify that the
//! guest implements a compatible C-ABI version (currently `ABI_VERSION = 1`).
//!
//! ## 2. Memory Allocation & Ownership Contract
//! WebAssembly modules manage their own linear memory. The guest exports:
//! - [`abi::datalake_alloc`]: Allocates 8-byte aligned linear memory in the guest.
//! - [`abi::datalake_dealloc`]: Reclaims guest linear memory buffers.
//!
//! **Memory Ownership Contract:**
//! Memory allocated by the guest for payloads returned across the ABI boundary
//! (the [`abi::TransformResponseHeader`], the [`abi::BatchDescriptor`] array,
//! the serialized Arrow IPC record batch buffers, and any error message strings)
//! transfers ownership to the host runtime. The host **MUST** free each of these
//! buffers by calling `datalake_dealloc` once it finishes processing.
//!
//! ## 3. Initialization & Panic Hook
//! The host invokes [`export_transformer!`] generated `datalake_init(config_ptr, config_len)`:
//! 1. Installs a global panic hook via [`panic::init_panic_hook`]. In WebAssembly, panics
//!    are caught and forwarded to the host import `datalake_host_log` at error level,
//!    preventing silent crashes.
//! 2. Reads the optional JSON configuration from guest memory and parses the target signal
//!    type ([`traits::SignalType`]) using [`dispatch::parse_signal_from_config`].
//! 3. Instantiates the user's struct via [`traits::BatchTransformer::init`].
//! 4. Stores the instance in a thread-safe static mutex for subsequent transformation calls.
//!
//! ## 4. Batch Transformation
//! For each telemetry batch:
//! 1. The host serializes the record batch into an Arrow IPC stream, allocates guest memory
//!    via `datalake_alloc`, copies the IPC bytes, and calls `datalake_transform(signal_type, ipc_ptr, ipc_len)`.
//! 2. The guest deserializes the IPC stream into an Arrow [`arrow::record_batch::RecordBatch`].
//! 3. The guest calls [`traits::BatchTransformer::transform`], which returns a
//!    [`traits::TransformResult`]:
//!    - [`traits::TransformResult::ok`]: Passes a single transformed batch downstream.
//!    - [`traits::TransformResult::ok_multiple`]: Produces multiple batches (e.g., fan-out or splitting).
//!    - [`traits::TransformResult::ok_empty`]: Drops all rows without error.
//!    - [`traits::TransformResult::discard`]: Drops the batch explicitly.
//!    - [`traits::TransformResult::reject`]: Rejects the batch due to validation failure with a reason.
//!    - [`traits::TransformResult::error`]: Fails the transformation with an execution error reason.
//! 4. Transformed batches are serialized back to Arrow IPC streams in guest memory.
//! 5. A 20-byte [`abi::TransformResponseHeader`] is written, and its address and length are packed
//!    into a 64-bit return value: `((header_ptr as u64) << 32) | 20u64`.
//!
//! ## 5. Host Reclaim & Observability
//! The host reads the response header, retrieves each batch descriptor and IPC buffer, and
//! invokes `datalake_dealloc` on all guest allocations.
//!
//! Guest transformers can emit controlled metrics back to the host pipeline via:
//! - [`metrics::counter`]: Emits a monotonic counter increment.
//! - [`metrics::gauge`]: Emits an instantaneous floating-point gauge (bitcast via IEEE 754).
//! - [`metrics::duration`]: Emits a processing duration measurement in nanoseconds.
//!
//! # Writing a Guest Plugin
//!
//! To build a guest transform plugin:
//!
//! 1. Configure your plugin's `Cargo.toml`:
//!    ```toml
//!    [lib]
//!    crate-type = ["cdylib"]
//!
//!    [dependencies]
//!    opentelemetry-datalake-wasm-sdk = "0.1.0"
//!    arrow = { version = "59", default-features = false, features = ["ipc"] }
//!    ```
//!
//!    Alternatively, plugins can import Arrow types directly via the SDK's re-exported
//!    [`arrow`] module without adding an explicit `arrow` dependency.
//!
//! 2. Implement [`traits::BatchTransformer`] and export your type with [`export_transformer!`]:
//!
//! ```rust
//! use opentelemetry_datalake_wasm_sdk::traits::{BatchTransformer, SignalType, TransformResult};
//! use opentelemetry_datalake_wasm_sdk::export_transformer;
//! use opentelemetry_datalake_wasm_sdk::arrow::record_batch::RecordBatch;
//!
//! pub struct MyTransformer;
//!
//! impl BatchTransformer for MyTransformer {
//!     fn init(_signal: SignalType, _config_json: Option<&str>) -> Result<Self, String> {
//!         Ok(Self)
//!     }
//!
//!     fn transform(&mut self, batch: RecordBatch) -> TransformResult {
//!         // Perform transformations on the RecordBatch...
//!         TransformResult::ok(batch)
//!     }
//! }
//!
//! export_transformer!(MyTransformer);
//!
//! fn main() {}
//! ```
//!
//! # Safety & Immutability Rules
//!
//! - **Canonical Columns:** Core OpenTelemetry fields ([`helpers::IMMUTABLE_COLUMNS`]) such as
//!   `trace_id`, `span_id`, `timestamp`, `observed_timestamp`, `name`, and `type` must never
//!   be dropped or nullified. Use [`helpers::is_immutable_column`] and [`helpers::nullify_column`]
//!   to safeguard compliance.
//! - **Zero Panics:** Plugins must not use `unwrap()` or `expect()` in processing logic.
//!   Return [`traits::TransformResult::error`] or [`traits::TransformResult::reject`] instead.

pub mod abi;
pub mod dispatch;
pub mod error;
pub mod helpers;
pub mod metrics;
pub mod panic;
pub mod traits;

/// Re-exported Arrow crate matching the workspace Arrow version (Arrow 59).
///
/// Plugin authors can use `opentelemetry_datalake_wasm_sdk::arrow` to guarantee type
/// compatibility with the SDK and avoid version mismatches.
pub use arrow;

/// Exports the C-ABI v1 entry points for a guest transform module.
///
/// This macro generates:
/// - A thread-safe static storage slot holding the optional transformer instance
/// - `datalake_init(config_ptr: u32, config_len: u32) -> u32`: Registers the panic hook and initializes the transformer
/// - `datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64`: Dispatches batch transformation
///
/// # Example
///
/// ```rust
/// use opentelemetry_datalake_wasm_sdk::traits::{BatchTransformer, SignalType, TransformResult};
/// use opentelemetry_datalake_wasm_sdk::export_transformer;
/// use opentelemetry_datalake_wasm_sdk::arrow::record_batch::RecordBatch;
///
/// struct MyTransformer;
///
/// impl BatchTransformer for MyTransformer {
///     fn init(_signal: SignalType, _config_json: Option<&str>) -> Result<Self, String> {
///         Ok(Self)
///     }
///
///     fn transform(&mut self, batch: RecordBatch) -> TransformResult {
///         TransformResult::ok(batch)
///     }
/// }
///
/// export_transformer!(MyTransformer);
///
/// fn main() {}
/// ```
#[macro_export]
macro_rules! export_transformer {
    ($transformer_type:ty) => {
        static TRANSFORMER_STATE: std::sync::Mutex<Option<$transformer_type>> =
            std::sync::Mutex::new(None);

        /// Initializes the guest transform plugin with optional JSON configuration.
        // SAFETY: Exporting initialization entry point with standard C linkage for the host WASM runtime.
        #[unsafe(no_mangle)]
        pub extern "C" fn datalake_init(config_ptr: u32, config_len: u32) -> u32 {
            $crate::panic::init_panic_hook();
            $crate::dispatch::dispatch_init(&TRANSFORMER_STATE, config_ptr, config_len)
        }

        /// Transforms an incoming Arrow IPC stream batch according to the signal type.
        // SAFETY: Exporting batch transform entry point with standard C linkage for the host WASM runtime.
        #[unsafe(no_mangle)]
        pub extern "C" fn datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64 {
            $crate::dispatch::dispatch_transform(&TRANSFORMER_STATE, signal_type, ipc_ptr, ipc_len)
        }
    };
}
