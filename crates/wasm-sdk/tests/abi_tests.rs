use opentelemetry_datalake_wasm_sdk::abi::{
    ABI_VERSION, BatchDescriptor, HostLogRecord, TransformResponseHeader, datalake_abi_version,
    datalake_alloc, datalake_dealloc,
};

#[test]
fn test_abi_v1_header_memory_layout() {
    assert_eq!(std::mem::size_of::<TransformResponseHeader>(), 20);
    assert_eq!(std::mem::align_of::<TransformResponseHeader>(), 4);
    assert_eq!(std::mem::size_of::<BatchDescriptor>(), 8);
    assert_eq!(std::mem::align_of::<BatchDescriptor>(), 4);
    assert_eq!(std::mem::size_of::<HostLogRecord>(), 32);
    assert_eq!(std::mem::align_of::<HostLogRecord>(), 4);
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
