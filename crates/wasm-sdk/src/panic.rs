//! Panic hook registration forwarding guest panics to host logging.
//!
//! Captures unexpected panic messages in WebAssembly guests and routes them
//! into the host logging subsystem (`datalake_host_log`) to prevent silent runtime crashes.

#[cfg(target_arch = "wasm32")]
use crate::abi::{HostLogRecord, LOG_LEVEL_ERROR};

#[cfg(target_arch = "wasm32")]
// SAFETY: Declaring host logging import provided by the wasm-transformer host environment.
#[link(wasm_import_module = "datalake_host_v1")]
unsafe extern "C" {
    fn datalake_host_log(record_ptr: u32);
}

/// Registers a global panic hook forwarding panics to the host logger.
///
/// On WebAssembly targets, panics are formatted into a message string and forwarded
/// to the host import `datalake_host_log` at error level (`LOG_LEVEL_ERROR` / level 1)
/// via a structured [`crate::abi::HostLogRecord`].
/// On non-wasm32 targets (e.g. host unit tests), panics are printed to `eprintln!`.
#[allow(clippy::print_stderr)]
pub fn init_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let msg = info.to_string();
        #[cfg(target_arch = "wasm32")]
        {
            let target = "wasm_guest::panic";
            let (file, line) = if let Some(loc) = info.location() {
                (loc.file(), loc.line())
            } else {
                ("unknown", 0)
            };
            let record = HostLogRecord {
                level: LOG_LEVEL_ERROR,
                msg_ptr: msg.as_ptr() as usize as u32,
                msg_len: msg.len() as u32,
                target_ptr: target.as_ptr() as usize as u32,
                target_len: target.len() as u32,
                file_ptr: file.as_ptr() as usize as u32,
                file_len: file.len() as u32,
                line,
            };
            // SAFETY: Passing valid HostLogRecord pointer in wasm32 linear memory
            // to the datalake_host_v1 host logging import.
            unsafe {
                #[allow(clippy::cast_possible_truncation)]
                datalake_host_log(std::ptr::addr_of!(record) as usize as u32);
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        eprintln!("[wasm-sdk panic] {msg}");
    }));
}
