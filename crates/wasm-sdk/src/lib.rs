//! WebAssembly (WASM) Guest SDK for `opentelemetry-datalake`.
//!
//! Provides the low-overhead C-ABI v1 data structures, safe allocators,
//! canonical immutability guards, and transformation abstractions for WASM guest transform modules.

pub mod abi;
pub mod dispatch;
pub mod error;
pub mod helpers;
pub mod metrics;
pub mod panic;
pub mod traits;

/// Exports the C-ABI v1 entry points for a guest transform module.
///
/// This macro generates:
/// - A thread-safe static storage slot holding the optional transformer instance
/// - `datalake_init(config_ptr: u32, config_len: u32) -> u32`: Registers the panic hook and initializes the transformer
/// - `datalake_transform(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64`: Dispatches batch transformation
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
