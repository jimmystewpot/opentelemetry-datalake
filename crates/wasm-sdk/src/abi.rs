//! Low-level C-ABI v1 definitions and memory allocation functions.
//!
//! This module defines the memory layouts for communication between
//! the host runtime (`wasm-transformer`) and guest WebAssembly transform modules.

/// Current ABI version supported by this SDK.
pub const ABI_VERSION: u32 = 1;

/// Response header returned by `datalake_transform` export.
///
/// Total size: 20 bytes, alignment: 4 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransformResponseHeader {
    /// Status code of the transformation (e.g. Success = 0, Error = 1, Reject = 2).
    pub status: u32,
    /// Number of transformed record batches returned.
    pub batch_count: u32,
    /// Memory pointer to an array of `BatchDescriptor` structs.
    pub batches_ptr: u32,
    /// Memory pointer to an optional UTF-8 error or rejection message.
    pub message_ptr: u32,
    /// Length of the message in bytes.
    pub message_len: u32,
}

/// Descriptor for a single Arrow IPC stream payload in guest memory.
///
/// Total size: 8 bytes, alignment: 4 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchDescriptor {
    /// Memory pointer to the raw Arrow IPC stream buffer.
    pub ptr: u32,
    /// Byte length of the Arrow IPC stream buffer.
    pub len: u32,
}

/// Memory layout for structured log records forwarded to the host via `datalake_host_log`.
///
/// Total size: 32 bytes, alignment: 4 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostLogRecord {
    /// Tracing log level (1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace).
    pub level: u32,
    /// Memory pointer to UTF-8 log message string.
    pub msg_ptr: u32,
    /// Byte length of log message.
    pub msg_len: u32,
    /// Memory pointer to UTF-8 target string (module/span name).
    pub target_ptr: u32,
    /// Byte length of target.
    pub target_len: u32,
    /// Memory pointer to UTF-8 source file path string.
    pub file_ptr: u32,
    /// Byte length of file path.
    pub file_len: u32,
    /// Line number in source code.
    pub line: u32,
}

/// Returns the C-ABI protocol version implemented by this guest module.
#[must_use]
// SAFETY: Exporting symbol with standard C linkage for the WASM C-ABI protocol.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_abi_version() -> u32 {
    ABI_VERSION
}

#[cfg(target_arch = "wasm32")]
// SAFETY: Exporting allocator entry point with C linkage for host buffer provisioning.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    let mut buf = Vec::<u8>::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as usize as u32
}

#[cfg(target_arch = "wasm32")]
// SAFETY: Exporting deallocator entry point with C linkage for host buffer reclamation.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    if ptr != 0 && size != 0 {
        // SAFETY: ptr was allocated with datalake_alloc on wasm32 (32-bit linear address space)
        // with capacity equal to `size` and forgotten.
        unsafe {
            drop(Vec::<u8>::from_raw_parts(ptr as *mut u8, 0, size as usize));
        }
    }
}

// Safe fallback for native host test harness (prevents 64-bit pointer truncation and segfaults)
#[cfg(not(target_arch = "wasm32"))]
static NATIVE_ALLOCS: std::sync::Mutex<Option<std::collections::HashMap<u32, Vec<u8>>>> =
    std::sync::Mutex::new(None);

#[cfg(not(target_arch = "wasm32"))]
#[must_use]
// SAFETY: Exporting test allocator symbol with C linkage.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT_ID: AtomicU32 = AtomicU32::new(1);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let mut lock = match NATIVE_ALLOCS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    lock.get_or_insert_with(std::collections::HashMap::new)
        .insert(id, Vec::with_capacity(size as usize));
    id
}

#[cfg(not(target_arch = "wasm32"))]
// SAFETY: Exporting test deallocator symbol with C linkage.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_dealloc(ptr: u32, _size: u32) {
    if ptr != 0 {
        let mut lock = match NATIVE_ALLOCS.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(map) = lock.as_mut() {
            map.remove(&ptr);
        }
    }
}
