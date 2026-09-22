#![allow(clippy::cast_possible_truncation)]

use opentelemetry_datalake_wasm_sdk::abi::{
    ABI_VERSION, BatchDescriptor, HostLogRecord, LOG_LEVEL_DEBUG, LOG_LEVEL_ERROR, LOG_LEVEL_INFO,
    LOG_LEVEL_TRACE, LOG_LEVEL_WARN, STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS,
    TransformResponseHeader, datalake_abi_version, datalake_alloc, datalake_dealloc,
    read_batch_descriptors, read_guest_memory, read_guest_string, read_response_header,
    write_guest_memory,
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

#[test]
fn test_raw_guest_memory_write_and_read() {
    let size = 128u32;
    let ptr = datalake_alloc(size);
    assert_ne!(ptr, 0);

    let test_data = b"Hello OpenTelemetry Data Lake WASM!";
    // SAFETY: `ptr` is allocated with 128 bytes, which is >= `test_data.len()`.
    unsafe {
        write_guest_memory(ptr, test_data);
    }

    // SAFETY: `ptr` points to 128 allocated and initialized bytes.
    let read_back = unsafe { read_guest_memory(ptr, test_data.len()) };
    assert_eq!(read_back.as_deref(), Some(&test_data[..]));

    // Reading with length 0 returns empty vector
    // SAFETY: Reading 0 bytes is always valid for allocated pointer.
    let empty_read = unsafe { read_guest_memory(ptr, 0) };
    assert_eq!(empty_read, Some(Vec::new()));

    // Writing to null pointer is a safe no-op
    // SAFETY: Null pointer write is guarded to safely return without dereference.
    unsafe {
        write_guest_memory(0, test_data);
    }

    // Reading from null pointer safely returns None
    // SAFETY: Null pointer read is guarded to safely return None.
    let null_read = unsafe { read_guest_memory(0, 10) };
    assert_eq!(null_read, None);

    datalake_dealloc(ptr, size);
}

#[test]
fn test_raw_guest_memory_response_header_round_trip() {
    let header_size = std::mem::size_of::<TransformResponseHeader>() as u32;
    let ptr = datalake_alloc(header_size);
    assert_ne!(ptr, 0);

    let mut header_bytes = [0u8; 20];
    header_bytes[0..4].copy_from_slice(&STATUS_SUCCESS.to_le_bytes());
    header_bytes[4..8].copy_from_slice(&3u32.to_le_bytes()); // batch_count
    header_bytes[8..12].copy_from_slice(&0x1000u32.to_le_bytes()); // batches_ptr
    header_bytes[12..16].copy_from_slice(&0x2000u32.to_le_bytes()); // message_ptr
    header_bytes[16..20].copy_from_slice(&42u32.to_le_bytes()); // message_len

    // SAFETY: `ptr` was allocated with capacity `header_size` (20 bytes).
    unsafe {
        write_guest_memory(ptr, &header_bytes);
    }

    // SAFETY: `ptr` points to 20 allocated, initialized bytes matching TransformResponseHeader.
    let parsed_header = unsafe { read_response_header(ptr) }.expect("valid header");
    assert_eq!(parsed_header.status, STATUS_SUCCESS);
    assert_eq!(parsed_header.batch_count, 3);
    assert_eq!(parsed_header.batches_ptr, 0x1000);
    assert_eq!(parsed_header.message_ptr, 0x2000);
    assert_eq!(parsed_header.message_len, 42);

    datalake_dealloc(ptr, header_size);
}

#[test]
fn test_raw_guest_memory_batch_descriptors_round_trip() {
    let count = 2usize;
    let byte_len = (count * std::mem::size_of::<BatchDescriptor>()) as u32;
    let ptr = datalake_alloc(byte_len);
    assert_ne!(ptr, 0);

    let mut desc_bytes = Vec::new();
    desc_bytes.extend_from_slice(&100u32.to_le_bytes()); // ptr 1
    desc_bytes.extend_from_slice(&256u32.to_le_bytes()); // len 1
    desc_bytes.extend_from_slice(&200u32.to_le_bytes()); // ptr 2
    desc_bytes.extend_from_slice(&512u32.to_le_bytes()); // len 2

    // SAFETY: `ptr` was allocated with capacity `byte_len` (16 bytes).
    unsafe {
        write_guest_memory(ptr, &desc_bytes);
    }

    // SAFETY: `ptr` points to 2 initialized BatchDescriptor structs.
    let descriptors = unsafe { read_batch_descriptors(ptr, count) }.expect("valid descriptors");
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].ptr, 100);
    assert_eq!(descriptors[0].len, 256);
    assert_eq!(descriptors[1].ptr, 200);
    assert_eq!(descriptors[1].len, 512);

    // Reading 0 descriptors returns empty vector
    // SAFETY: 0 count does not dereference `ptr`.
    let empty_descriptors = unsafe { read_batch_descriptors(ptr, 0) }.expect("empty list");
    assert!(empty_descriptors.is_empty());

    datalake_dealloc(ptr, byte_len);
}

#[test]
fn test_raw_guest_memory_guest_string_round_trip() {
    let sample = "test error or rejection reason payload";
    let sample_bytes = sample.as_bytes();
    let ptr = datalake_alloc(sample_bytes.len() as u32);
    assert_ne!(ptr, 0);

    // SAFETY: `ptr` was allocated with capacity matching `sample_bytes.len()`.
    unsafe {
        write_guest_memory(ptr, sample_bytes);
    }

    // SAFETY: `ptr` points to initialized UTF-8 bytes.
    let read_str = unsafe { read_guest_string(ptr, sample_bytes.len()) }.expect("valid string");
    assert_eq!(read_str, sample);

    // Reading 0 length string returns empty string
    // SAFETY: 0 length read does not dereference `ptr`.
    let empty_str = unsafe { read_guest_string(ptr, 0) }.expect("empty string");
    assert_eq!(empty_str, "");

    datalake_dealloc(ptr, sample_bytes.len() as u32);
}
