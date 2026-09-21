use opentelemetry_datalake_wasm_sdk::abi::{
    ABI_VERSION, BatchDescriptor, HostLogRecord, LOG_LEVEL_DEBUG, LOG_LEVEL_ERROR, LOG_LEVEL_INFO,
    LOG_LEVEL_TRACE, LOG_LEVEL_WARN, STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS,
    TransformResponseHeader, datalake_abi_version, datalake_alloc, datalake_dealloc,
};

#[test]
fn test_status_constants() {
    assert_eq!(STATUS_SUCCESS, 0u32);
    assert_eq!(STATUS_DISCARD, 1u32);
    assert_eq!(STATUS_REJECT, 2u32);
    assert_eq!(STATUS_ERROR, 3u32);
}

#[test]
fn test_log_level_constants() {
    assert_eq!(LOG_LEVEL_ERROR, 1u32);
    assert_eq!(LOG_LEVEL_WARN, 2u32);
    assert_eq!(LOG_LEVEL_INFO, 3u32);
    assert_eq!(LOG_LEVEL_DEBUG, 4u32);
    assert_eq!(LOG_LEVEL_TRACE, 5u32);
}

#[test]
fn test_abi_v1_header_memory_layout() {
    assert_eq!(std::mem::size_of::<TransformResponseHeader>(), 20);
    assert_eq!(std::mem::align_of::<TransformResponseHeader>(), 4);
    assert_eq!(std::mem::offset_of!(TransformResponseHeader, status), 0);
    assert_eq!(
        std::mem::offset_of!(TransformResponseHeader, batch_count),
        4
    );
    assert_eq!(
        std::mem::offset_of!(TransformResponseHeader, batches_ptr),
        8
    );
    assert_eq!(
        std::mem::offset_of!(TransformResponseHeader, message_ptr),
        12
    );
    assert_eq!(
        std::mem::offset_of!(TransformResponseHeader, message_len),
        16
    );

    assert_eq!(std::mem::size_of::<BatchDescriptor>(), 8);
    assert_eq!(std::mem::align_of::<BatchDescriptor>(), 4);
    assert_eq!(std::mem::offset_of!(BatchDescriptor, ptr), 0);
    assert_eq!(std::mem::offset_of!(BatchDescriptor, len), 4);

    assert_eq!(std::mem::size_of::<HostLogRecord>(), 32);
    assert_eq!(std::mem::align_of::<HostLogRecord>(), 4);
    assert_eq!(std::mem::offset_of!(HostLogRecord, level), 0);
    assert_eq!(std::mem::offset_of!(HostLogRecord, msg_ptr), 4);
    assert_eq!(std::mem::offset_of!(HostLogRecord, msg_len), 8);
    assert_eq!(std::mem::offset_of!(HostLogRecord, target_ptr), 12);
    assert_eq!(std::mem::offset_of!(HostLogRecord, target_len), 16);
    assert_eq!(std::mem::offset_of!(HostLogRecord, file_ptr), 20);
    assert_eq!(std::mem::offset_of!(HostLogRecord, file_len), 24);
    assert_eq!(std::mem::offset_of!(HostLogRecord, line), 28);
}

#[test]
fn test_abi_version_constant_is_one() {
    assert_eq!(ABI_VERSION, 1u32);
    assert_eq!(datalake_abi_version(), 1u32);
}

#[test]
fn test_alloc_and_dealloc_native_safety() {
    // Must not panic or segfault on 64-bit host architecture
    let ptr1 = datalake_alloc(1024);
    assert_ne!(ptr1, 0);

    let ptr2 = datalake_alloc(2048);
    assert_ne!(ptr2, 0);
    assert_ne!(ptr1, ptr2);

    // Deallocate both
    datalake_dealloc(ptr1, 1024);
    datalake_dealloc(ptr2, 2048);

    // Deallocating zero pointer or redundant deallocation should not panic
    datalake_dealloc(0, 0);
    datalake_dealloc(ptr1, 1024);
}

#[test]
fn test_alloc_oom_returns_null_pointer() {
    // Attempting to allocate an impossible capacity must return 0 instead of panicking
    let impossible_size = u32::MAX;
    let ptr = datalake_alloc(impossible_size);
    assert_eq!(ptr, 0);
    // Deallocating 0 must be a safe no-op
    datalake_dealloc(0, impossible_size);
}

#[test]
fn test_alloc_zero_returns_null_pointer() {
    let ptr = datalake_alloc(0);
    assert_eq!(ptr, 0);
    datalake_dealloc(0, 0);
}
