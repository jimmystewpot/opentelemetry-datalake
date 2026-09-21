//! Low-level C-ABI v1 definitions and memory allocation functions.
//!
//! This module defines the memory layouts for communication between
//! the host runtime (`wasm-transformer`) and guest WebAssembly transform modules.
//!
//! # Memory Ownership Model
//!
//! Memory allocated by the guest for payloads returned across the ABI boundary
//! (such as [`TransformResponseHeader::batches_ptr`], [`BatchDescriptor::ptr`], and
//! [`TransformResponseHeader::message_ptr`]) transfers ownership to the host runtime.
//! The host is required to free these allocations by invoking `datalake_dealloc`.

/// Current ABI version supported by this SDK.
pub const ABI_VERSION: u32 = 1;

/// Status code indicating successful batch transformation.
pub const STATUS_SUCCESS: u32 = 0;
/// Status code indicating that the batch was discarded.
pub const STATUS_DISCARD: u32 = 1;
/// Status code indicating batch was rejected due to data validation failure.
pub const STATUS_REJECT: u32 = 2;
/// Status code indicating an unrecoverable execution error.
pub const STATUS_ERROR: u32 = 3;

/// Response header returned by `datalake_transform` export.
///
/// Total size: 20 bytes, alignment: 4 bytes.
///
/// # Memory Ownership Contract
///
/// Ownership of the descriptor array (`batches_ptr`) and all IPC buffers
/// it points to is transferred across the ABI boundary to the host.
/// The Host MUST call `datalake_dealloc` on the descriptor array pointer
/// AND on each individual `BatchDescriptor.ptr` exactly once to prevent memory leaks.
/// If `message_ptr` is non-zero, ownership of the error or rejection message buffer
/// is also transferred to the host, and the Host MUST call `datalake_dealloc` on `message_ptr`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransformResponseHeader {
    /// Status code of the transformation (e.g. [`STATUS_SUCCESS`], [`STATUS_DISCARD`], [`STATUS_REJECT`], [`STATUS_ERROR`]).
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
///
/// # Memory Ownership Contract
///
/// Ownership of the memory buffer referenced by `ptr` (with byte length `len`) is
/// transferred across the ABI boundary to the host runtime upon return of
/// [`TransformResponseHeader`]. The Host MUST call `datalake_dealloc(ptr, len)`
/// exactly once to reclaim the buffer after reading or copying the Arrow IPC stream payload.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchDescriptor {
    /// Memory pointer to the raw Arrow IPC stream buffer.
    pub ptr: u32,
    /// Byte length of the populated Arrow IPC stream buffer.
    pub len: u32,
}

/// Memory layout for structured log records.
///
/// Reserved for structured record logging rather than the flat 3-argument
/// `datalake_host_log` scalar import used by panic hook.
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

/// Tracing log level for error messages.
pub const LOG_LEVEL_ERROR: u32 = 1;

/// Tracing log level for warning messages.
pub const LOG_LEVEL_WARN: u32 = 2;

/// Tracing log level for informational messages.
pub const LOG_LEVEL_INFO: u32 = 3;

/// Tracing log level for debug messages.
pub const LOG_LEVEL_DEBUG: u32 = 4;

/// Tracing log level for trace messages.
pub const LOG_LEVEL_TRACE: u32 = 5;

/// Returns the C-ABI protocol version implemented by this guest module.
#[must_use]
// SAFETY: Exporting symbol with standard C linkage for the WASM C-ABI protocol.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_abi_version() -> u32 {
    ABI_VERSION
}

#[cfg(target_arch = "wasm32")]
use std::alloc::Layout;

/// Allocates an 8-byte aligned linear memory buffer of `size` bytes in the WebAssembly instance.
///
/// Returns the 32-bit linear address of the allocated buffer, or `0` if `size == 0` or allocation fails.
#[cfg(target_arch = "wasm32")]
#[must_use]
// SAFETY: Exporting allocator entry point with C linkage for host buffer provisioning.
#[unsafe(no_mangle)]
#[allow(clippy::cast_possible_truncation)]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    if size == 0 {
        return 0;
    }
    let Ok(layout) = Layout::from_size_align(size as usize, 8) else {
        return 0;
    };
    // SAFETY: Global allocator invocation with verified non-zero 8-byte aligned layout.
    let ptr = unsafe { std::alloc::alloc(layout) };
    ptr as usize as u32
}

/// Deallocates a buffer previously allocated by `datalake_alloc`.
#[cfg(target_arch = "wasm32")]
// SAFETY: Exporting deallocator entry point with C linkage for host buffer reclamation.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    if ptr == 0 || size == 0 {
        return;
    }
    if let Ok(layout) = Layout::from_size_align(size as usize, 8) {
        // SAFETY: ptr was allocated with datalake_alloc with 8-byte alignment and identical size.
        unsafe {
            std::alloc::dealloc(ptr as *mut u8, layout);
        }
    }
}

// Safe fallback for native host test harness (prevents 64-bit pointer truncation and segfaults)
#[cfg(not(target_arch = "wasm32"))]
const MOCK_MAX_ALLOC: usize = 64 * 1024 * 1024; // 64 MB mock allocation ceiling for deterministic testing

#[cfg(not(target_arch = "wasm32"))]
static NATIVE_ALLOCS: std::sync::Mutex<Option<std::collections::HashMap<u32, Vec<u8>>>> =
    std::sync::Mutex::new(None);

#[cfg(not(target_arch = "wasm32"))]
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Allocates a mock buffer entry in the host test allocator map.
///
/// Returns a unique non-zero 32-bit mock handle, or `0` if `size == 0` or size exceeds 64 MB limit.
#[cfg(not(target_arch = "wasm32"))]
#[must_use]
// SAFETY: Exporting test allocator symbol with C linkage.
#[unsafe(no_mangle)]
#[allow(clippy::cast_possible_truncation)]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    if size == 0 || size as usize > MOCK_MAX_ALLOC {
        return 0;
    }
    let mut buf = Vec::<u8>::new();
    if buf.try_reserve_exact(size as usize).is_err() {
        return 0;
    }
    buf.resize(size as usize, 0);
    let mut id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    while (id as u32) == 0 {
        id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let handle = id as u32;
    let mut lock = match NATIVE_ALLOCS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    lock.get_or_insert_with(std::collections::HashMap::new)
        .insert(handle, buf);
    handle
}

/// Deallocates a mock buffer entry previously allocated by `datalake_alloc`.
#[cfg(not(target_arch = "wasm32"))]
// SAFETY: Exporting test deallocator symbol with C linkage.
#[unsafe(no_mangle)]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    if ptr != 0 && size != 0 {
        let mut lock = match NATIVE_ALLOCS.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(map) = lock.as_mut() {
            map.remove(&ptr);
        }
    }
}

/// Copies bytes into the guest linear memory buffer at `ptr`.
pub fn write_guest_memory(ptr: u32, src: &[u8]) {
    if ptr == 0 || src.is_empty() {
        return;
    }
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: `ptr` was allocated by `datalake_alloc` in wasm32 linear memory
        // with capacity >= `src.len()`.
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), ptr as *mut u8, src.len());
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut lock = match NATIVE_ALLOCS.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(buf) = lock.as_mut().and_then(|map| map.get_mut(&ptr)) {
            if buf.len() < src.len() {
                buf.resize(src.len(), 0);
            }
            buf[..src.len()].copy_from_slice(src);
        }
    }
}

/// Reads `len` bytes from the guest linear memory buffer at `ptr`.
#[must_use]
pub fn read_guest_memory(ptr: u32, len: usize) -> Option<Vec<u8>> {
    if ptr == 0 {
        return None;
    }
    if len == 0 {
        return Some(Vec::new());
    }
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: `ptr` is a valid non-null address in wasm32 linear memory
        // containing at least `len` initialized bytes.
        unsafe {
            let slice = std::slice::from_raw_parts(ptr as *const u8, len);
            Some(slice.to_vec())
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let lock = match NATIVE_ALLOCS.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let map = lock.as_ref()?;
        let buf = map.get(&ptr)?;
        if buf.len() >= len {
            Some(buf[..len].to_vec())
        } else {
            None
        }
    }
}

/// Reads and parses a [`TransformResponseHeader`] from guest memory at `ptr`.
#[must_use]
pub fn read_response_header(ptr: u32) -> Option<TransformResponseHeader> {
    let bytes = read_guest_memory(ptr, std::mem::size_of::<TransformResponseHeader>())?;
    if bytes.len() != std::mem::size_of::<TransformResponseHeader>() {
        return None;
    }
    let status = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let batch_count = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let batches_ptr = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    let message_ptr = u32::from_le_bytes(bytes[12..16].try_into().ok()?);
    let message_len = u32::from_le_bytes(bytes[16..20].try_into().ok()?);
    Some(TransformResponseHeader {
        status,
        batch_count,
        batches_ptr,
        message_ptr,
        message_len,
    })
}

/// Reads an array of [`BatchDescriptor`] structs from guest memory at `ptr`.
#[must_use]
pub fn read_batch_descriptors(ptr: u32, count: usize) -> Option<Vec<BatchDescriptor>> {
    if count == 0 {
        return Some(Vec::new());
    }
    let byte_len = count.checked_mul(std::mem::size_of::<BatchDescriptor>())?;
    let bytes = read_guest_memory(ptr, byte_len)?;
    if bytes.len() != byte_len {
        return None;
    }
    let mut descriptors = Vec::with_capacity(count);
    for i in 0..count {
        let offset = i * std::mem::size_of::<BatchDescriptor>();
        let desc_bytes = &bytes[offset..offset + std::mem::size_of::<BatchDescriptor>()];
        let d_ptr = u32::from_le_bytes(desc_bytes[0..4].try_into().ok()?);
        let d_len = u32::from_le_bytes(desc_bytes[4..8].try_into().ok()?);
        descriptors.push(BatchDescriptor {
            ptr: d_ptr,
            len: d_len,
        });
    }
    Some(descriptors)
}

/// Reads a UTF-8 string from guest memory at `ptr` with length `len`.
#[must_use]
pub fn read_guest_string(ptr: u32, len: usize) -> Option<String> {
    if len == 0 {
        return Some(String::new());
    }
    let bytes = read_guest_memory(ptr, len)?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;

    #[test]
    fn test_native_alloc_mutex_poisoning_recovery() {
        // Intentionally poison the NATIVE_ALLOCS mutex
        let _ = std::thread::spawn(|| {
            let _lock = NATIVE_ALLOCS.lock().unwrap();
            panic!("Intentional poison for coverage");
        })
        .join();

        // Must recover gracefully from the poisoned mutex
        let ptr = datalake_alloc(128);
        assert_ne!(ptr, 0);
        datalake_dealloc(ptr, 128);

        // Clean up the lock state for other tests if possible by replacing the Some value
        if let Ok(mut _lock) = NATIVE_ALLOCS.lock() {
            // Already clean
        } else if let Err(poisoned) = NATIVE_ALLOCS.lock() {
            let mut guard = poisoned.into_inner();
            guard.take(); // Clear it
        }
    }

    #[test]
    fn test_test_allocator_u64_wrap_around_prevention() {
        // Read original value to restore it later to maintain test isolation.
        let original_id = NEXT_ID.load(std::sync::atomic::Ordering::SeqCst);

        // Test that NEXT_ID uses AtomicU64 and that when approaching/crossing the 32-bit boundary (u32::MAX),
        // datalake_alloc never returns 0 (null handle) and allocation succeeds.
        NEXT_ID.store(u64::from(u32::MAX), std::sync::atomic::Ordering::SeqCst);

        // First allocation at u32::MAX
        let ptr1 = datalake_alloc(64);
        assert_ne!(ptr1, 0, "handle must not be 0 at u32::MAX");

        // Next allocation crosses 2^32 boundary; must not return 0 (null)
        let ptr2 = datalake_alloc(64);
        assert_ne!(ptr2, 0, "handle must not wrap to 0 (null)");
        assert_ne!(ptr1, ptr2, "consecutive handles must be distinct");

        datalake_dealloc(ptr1, 64);
        datalake_dealloc(ptr2, 64);

        // Restore original value
        NEXT_ID.store(original_id, std::sync::atomic::Ordering::SeqCst);
    }
}
