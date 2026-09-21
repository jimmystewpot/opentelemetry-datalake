#![allow(clippy::cast_possible_truncation)]

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::abi::{
    STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS, TransformResponseHeader,
    datalake_dealloc, read_batch_descriptors, read_guest_memory, read_guest_string,
    read_response_header,
};
use opentelemetry_datalake_wasm_sdk::dispatch::{
    create_error_response, decode_ipc_stream, encode_batch_to_ipc, encode_response,
};
use opentelemetry_datalake_wasm_sdk::error::SdkError;
use opentelemetry_datalake_wasm_sdk::traits::TransformResult;
use std::sync::Arc;

fn create_sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));

    RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
}

#[test]
fn test_ipc_round_trip() {
    let batch = create_sample_batch();
    let encoded = encode_batch_to_ipc(&batch).unwrap();
    assert!(!encoded.is_empty(), "encoded IPC stream must not be empty");

    let decoded = decode_ipc_stream(&encoded).unwrap();
    assert_eq!(decoded.schema(), batch.schema());
    assert_eq!(decoded, batch);
}

#[test]
fn test_decode_malformed_ipc_returns_error() {
    let invalid_bytes = b"this is definitely not a valid arrow ipc stream";
    let res = decode_ipc_stream(invalid_bytes);
    assert!(
        matches!(res, Err(SdkError::Ipc(_))),
        "malformed IPC payload must return SdkError::Ipc without panicking"
    );

    let empty_bytes = b"";
    let res_empty = decode_ipc_stream(empty_bytes);
    assert!(
        matches!(res_empty, Err(SdkError::Ipc(_))),
        "empty slice must return SdkError::Ipc without panicking"
    );
}

#[test]
fn test_encode_response_success() {
    let batch = create_sample_batch();
    let packed = encode_response(&TransformResult::ok(batch.clone()));

    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    assert_eq!(
        header_len,
        std::mem::size_of::<TransformResponseHeader>() as u32
    );
    assert_ne!(header_ptr, 0, "header_ptr must be non-zero");

    let header = read_response_header(header_ptr).expect("valid response header");
    assert_eq!(header.status, STATUS_SUCCESS);
    assert_eq!(header.batch_count, 1);
    assert_ne!(header.batches_ptr, 0);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(header.message_len, 0);

    let descriptors = read_batch_descriptors(header.batches_ptr, header.batch_count as usize)
        .expect("descriptors");
    assert_eq!(descriptors.len(), 1);
    assert_ne!(descriptors[0].ptr, 0);
    assert!(descriptors[0].len > 0);

    let ipc_data =
        read_guest_memory(descriptors[0].ptr, descriptors[0].len as usize).expect("ipc data");
    let decoded_batch = decode_ipc_stream(&ipc_data).expect("decoded batch");
    assert_eq!(decoded_batch, batch);

    // Clean up allocated guest memory
    datalake_dealloc(descriptors[0].ptr, descriptors[0].len);
    datalake_dealloc(
        header.batches_ptr,
        (header.batch_count as usize
            * std::mem::size_of::<opentelemetry_datalake_wasm_sdk::abi::BatchDescriptor>())
            as u32,
    );
    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_discard() {
    let packed = encode_response(&TransformResult::discard());

    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    assert_eq!(
        header_len,
        std::mem::size_of::<TransformResponseHeader>() as u32
    );
    assert_ne!(header_ptr, 0);

    let header = read_response_header(header_ptr).expect("valid response header");
    assert_eq!(header.status, STATUS_DISCARD);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(header.message_len, 0);

    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_reject() {
    let reason = "invalid schema format";
    let packed = encode_response(&TransformResult::reject(reason));

    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    assert_eq!(
        header_len,
        std::mem::size_of::<TransformResponseHeader>() as u32
    );
    assert_ne!(header_ptr, 0);

    let header = read_response_header(header_ptr).expect("valid response header");
    assert_eq!(header.status, STATUS_REJECT);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_ne!(header.message_ptr, 0);
    assert_eq!(header.message_len, reason.len() as u32);

    let msg = read_guest_string(header.message_ptr, header.message_len as usize)
        .expect("valid reject string");
    assert_eq!(msg, reason);

    datalake_dealloc(header.message_ptr, header.message_len);
    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_error() {
    let reason = "fatal transformation failure";
    let packed = create_error_response(reason);

    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    assert_eq!(
        header_len,
        std::mem::size_of::<TransformResponseHeader>() as u32
    );
    assert_ne!(header_ptr, 0);

    let header = read_response_header(header_ptr).expect("valid response header");
    assert_eq!(header.status, STATUS_ERROR);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_ne!(header.message_ptr, 0);
    assert_eq!(header.message_len, reason.len() as u32);

    let msg = read_guest_string(header.message_ptr, header.message_len as usize)
        .expect("valid error string");
    assert_eq!(msg, reason);

    datalake_dealloc(header.message_ptr, header.message_len);
    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_success_multiple_batches() {
    let batch1 = create_sample_batch();
    let schema2 = Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, true)]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![Arc::new(StringArray::from(vec![Some("val1"), None]))],
    )
    .unwrap();

    let packed = encode_response(&TransformResult::ok_multiple(vec![
        batch1.clone(),
        batch2.clone(),
    ]));
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_SUCCESS);
    assert_eq!(header.batch_count, 2);
    assert_ne!(header.batches_ptr, 0);

    let descriptors = read_batch_descriptors(header.batches_ptr, header.batch_count as usize)
        .expect("descriptors");
    assert_eq!(descriptors.len(), 2);

    let ipc_data1 = read_guest_memory(descriptors[0].ptr, descriptors[0].len as usize).unwrap();
    let ipc_data2 = read_guest_memory(descriptors[1].ptr, descriptors[1].len as usize).unwrap();

    assert_eq!(decode_ipc_stream(&ipc_data1).unwrap(), batch1);
    assert_eq!(decode_ipc_stream(&ipc_data2).unwrap(), batch2);

    datalake_dealloc(descriptors[0].ptr, descriptors[0].len);
    datalake_dealloc(descriptors[1].ptr, descriptors[1].len);
    datalake_dealloc(
        header.batches_ptr,
        (2 * std::mem::size_of::<opentelemetry_datalake_wasm_sdk::abi::BatchDescriptor>()) as u32,
    );
    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_continue_empty() {
    let packed = encode_response(&TransformResult::ok_empty());
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_SUCCESS);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(header.message_len, 0);

    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_error_delegation() {
    let reason = "error delegation test";
    let packed = encode_response(&TransformResult::error(reason));
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_ERROR);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_ne!(header.message_ptr, 0);
    assert_eq!(header.message_len, reason.len() as u32);

    let msg = read_guest_string(header.message_ptr, header.message_len as usize)
        .expect("valid error string");
    assert_eq!(msg, reason);

    datalake_dealloc(header.message_ptr, header.message_len);
    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_create_error_response_empty_message() {
    let packed = create_error_response("");
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_ERROR);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(header.message_len, 0);

    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_reject_empty_reason() {
    let packed = encode_response(&TransformResult::reject(""));
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_REJECT);
    assert_eq!(header.batch_count, 0);
    assert_eq!(header.batches_ptr, 0);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(header.message_len, 0);

    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_create_error_response_allocation_failure_resets_len() {
    static HUGE_MSG: [u8; 65 * 1024 * 1024] = [b'x'; 65 * 1024 * 1024];
    // SAFETY: HUGE_MSG contains valid ASCII 'x' bytes.
    let huge_str = unsafe { std::str::from_utf8_unchecked(&HUGE_MSG) };
    let packed = create_error_response(huge_str);
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_ERROR);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(
        header.message_len, 0,
        "message_len must be 0 when datalake_alloc returns null"
    );

    datalake_dealloc(header_ptr, header_len);
}

#[test]
fn test_encode_response_reject_allocation_failure_resets_len() {
    static HUGE_MSG: [u8; 65 * 1024 * 1024] = [b'x'; 65 * 1024 * 1024];
    // SAFETY: HUGE_MSG contains valid ASCII 'x' bytes.
    let huge_str = unsafe { std::str::from_utf8_unchecked(&HUGE_MSG) };
    let packed = encode_response(&TransformResult::reject(huge_str));
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_REJECT);
    assert_eq!(header.message_ptr, 0);
    assert_eq!(
        header.message_len, 0,
        "message_len must be 0 when datalake_alloc returns null"
    );

    datalake_dealloc(header_ptr, header_len);
}
