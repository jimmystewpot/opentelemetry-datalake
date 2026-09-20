//! Sample PII scrubber WebAssembly guest transform module.
//!
//! Implements the datalake WASM C-ABI v1 specification to inspect and
//! scrub sensitive information from OpenTelemetry telemetry streams.

use opentelemetry_datalake_wasm_sdk::panic::init_panic_hook;

/// Returns the C-ABI protocol version implemented by this guest module.
#[must_use]
// SAFETY: Exporting symbol with standard C linkage for the WASM C-ABI protocol.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_abi_version() -> u32 {
    1
}

/// Allocates a linear memory buffer of `size` bytes in the WebAssembly instance.
#[must_use]
// SAFETY: Exporting allocator entry point with standard C linkage for host buffer provisioning.
#[unsafe(no_mangle)]
#[allow(clippy::cast_possible_truncation)]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    let mut buf = Vec::<u8>::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as usize as u32
}

/// Deallocates a buffer previously allocated by `datalake_alloc`.
// SAFETY: Exporting deallocator entry point with standard C linkage for host buffer reclamation.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    if ptr != 0 && size != 0 {
        // SAFETY: ptr was allocated with datalake_alloc with capacity equal to `size` and forgotten.
        unsafe {
            drop(Vec::<u8>::from_raw_parts(ptr as *mut u8, 0, size as usize));
        }
    }
}

/// Initializes the WebAssembly guest transformer and registers the panic hook.
// SAFETY: Exporting initialization entry point with standard C linkage for host lifecycle invocation.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_init(_config_ptr: u32, _config_len: u32) -> u32 {
    init_panic_hook();
    0
}

/// Transforms an incoming Arrow IPC record batch stream.
///
/// Returns a pointer to a `TransformResponseHeader` in linear memory.
// SAFETY: Exporting transform entry point with standard C linkage for host batch transformation.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_transform(_ipc_ptr: u32, _ipc_len: u32) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abi_version_is_one() {
        assert_eq!(datalake_abi_version(), 1);
    }

    #[test]
    fn test_init_succeeds() {
        assert_eq!(datalake_init(0, 0), 0);
    }

    #[test]
    fn test_transform_entry_point() {
        assert_eq!(datalake_transform(0, 0), 0);
    }
}
