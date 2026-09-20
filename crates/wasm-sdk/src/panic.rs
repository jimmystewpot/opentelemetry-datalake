//! Panic hook registration forwarding guest panics to host logging.
//!
//! Captures unexpected panic messages in WebAssembly guests and routes them
//! into the host logging subsystem (`datalake_host_log`) to prevent silent runtime crashes.

/// Registers a global panic hook forwarding panics to the host logger.
///
/// On WebAssembly targets, panics are formatted into a message string and forwarded
/// to the host import `datalake_host_log` at error level (level 4).
/// On non-wasm32 targets (e.g. host unit tests), panics are printed to `eprintln!`.
pub fn init_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let msg = info.to_string();
        #[cfg(target_arch = "wasm32")]
        {
            // SAFETY: Declaring host logging import provided by the wasm-transformer host environment.
            unsafe extern "C" {
                fn datalake_host_log(level: u32, msg_ptr: u32, msg_len: u32);
            }
            // SAFETY: Passing valid UTF-8 formatted panic message pointer and length
            // in wasm32 linear memory to the host logging import.
            unsafe {
                #[allow(clippy::cast_possible_truncation)]
                datalake_host_log(4, msg.as_ptr() as usize as u32, msg.len() as u32);
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        eprintln!("[wasm-sdk panic] {msg}");
    }));
}
