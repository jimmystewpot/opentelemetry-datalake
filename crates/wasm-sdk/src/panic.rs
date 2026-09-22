//! Panic hook registration forwarding guest panics to host logging.
//!
//! Captures unexpected panic messages in WebAssembly guests and routes them
//! into the host logging subsystem (`datalake_host_log`) to prevent silent runtime crashes.

#[cfg(target_arch = "wasm32")]
use crate::abi::LOG_LEVEL_ERROR;

#[cfg(target_arch = "wasm32")]
// SAFETY: Declaring host logging import provided by the wasm-transformer host environment.
unsafe extern "C" {
    fn datalake_host_log(level: u32, msg_ptr: u32, msg_len: u32);
}

/// Logs an error message to the host logger on wasm32, or to stderr on native platforms.
#[allow(clippy::print_stderr)]
pub fn log_error(msg: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Passing valid UTF-8 formatted panic message pointer and length
        // in wasm32 linear memory to the host logging import.
        unsafe {
            #[allow(clippy::cast_possible_truncation)]
            datalake_host_log(
                LOG_LEVEL_ERROR,
                msg.as_ptr() as usize as u32,
                msg.len() as u32,
            );
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("[wasm-sdk panic] {msg}");
}

/// Extracts a string slice from a panic payload without allocating memory.
///
/// Handles `&'static str` (e.g. `panic!("literal")`) and `String` (e.g. `panic!("{}", formatted)`).
/// If the payload is neither (e.g. non-string payload or OOM trap), returns a static fallback string.
#[must_use]
pub fn extract_panic_payload<'a>(payload: &'a (dyn std::any::Any + 'static)) -> &'a str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "Wasm Guest Panic (OOM or unformattable)"
    }
}

/// Panic hook callback forwarding formatted guest panic messages to the host logger.
///
/// Avoids dynamic `info.to_string()` allocation which constructs the entire `PanicHookInfo`
/// debug tree and backtrace, preventing secondary OOM crashes in memory-constrained guests.
pub fn wasm_panic_hook(info: &std::panic::PanicHookInfo<'_>) {
    let msg = extract_panic_payload(info.payload());

    let mut loc_buf = [0u8; 256];
    let loc_str = if let Some(location) = info.location() {
        use std::io::Write;
        let mut slice: &mut [u8] = &mut loc_buf;
        let _ = write!(slice, "Panic at {}:{}: ", location.file(), location.line());
        let len = 256 - slice.len();
        std::str::from_utf8(&loc_buf[..len]).unwrap_or("Panic at unknown location: ")
    } else {
        "Panic at unknown location: "
    };

    log_error(loc_str);
    log_error(msg);
}

/// Registers a global panic hook forwarding panics to the host logger.
///
/// On WebAssembly targets, panics are formatted into a message string and forwarded
/// to the host import `datalake_host_log` at error level (`LOG_LEVEL_ERROR` / level 1).
/// On non-wasm32 targets (e.g. host unit tests), panics are printed to `eprintln!`.
pub fn init_panic_hook() {
    std::panic::set_hook(Box::new(wasm_panic_hook));
}
