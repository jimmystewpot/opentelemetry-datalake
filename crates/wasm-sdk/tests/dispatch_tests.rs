#![allow(clippy::cast_possible_truncation)]

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::abi::{
    STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS, TransformResponseHeader,
    datalake_alloc, datalake_dealloc, read_batch_descriptors, read_guest_memory, read_guest_string,
    read_response_header, write_guest_memory,
};
use opentelemetry_datalake_wasm_sdk::dispatch::{
    create_error_response, decode_ipc_stream, dispatch_init, dispatch_transform,
    encode_batch_to_ipc, encode_response, parse_signal_from_config,
};
use opentelemetry_datalake_wasm_sdk::error::SdkError;
use opentelemetry_datalake_wasm_sdk::traits::{BatchTransformer, SignalType, TransformResult};
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

#[test]
fn test_parse_signal_from_config() {
    assert_eq!(
        parse_signal_from_config(r#"{"signal":"metrics"}"#),
        Some(SignalType::Metrics)
    );
    assert_eq!(
        parse_signal_from_config(r#"{"signal":"logs"}"#),
        Some(SignalType::Logs)
    );
    assert_eq!(
        parse_signal_from_config(r#"{"signal":"traces"}"#),
        Some(SignalType::Traces)
    );
    assert_eq!(
        parse_signal_from_config(r#"{"other": 1, "signal": "metrics"}"#),
        Some(SignalType::Metrics)
    );
    assert_eq!(
        parse_signal_from_config(r#"{"signal": 1}"#),
        Some(SignalType::Metrics)
    );
    assert_eq!(
        parse_signal_from_config("metrics"),
        Some(SignalType::Metrics)
    );
    assert_eq!(parse_signal_from_config("logs"), Some(SignalType::Logs));
    assert_eq!(parse_signal_from_config("traces"), Some(SignalType::Traces));
    assert_eq!(parse_signal_from_config(r#"{"signal":"invalid"}"#), None);
    assert_eq!(parse_signal_from_config(""), None);
    assert_eq!(parse_signal_from_config(r#"{"no_signal":"here"}"#), None);
}

struct TestInitTransformer {
    signal: SignalType,
    config: Option<String>,
}

impl BatchTransformer for TestInitTransformer {
    fn init(signal: SignalType, config_json: Option<&str>) -> Result<Self, String> {
        if config_json.is_some_and(|cfg| cfg.contains("fail_init")) {
            return Err("Initialization error requested".to_string());
        }
        Ok(Self {
            signal,
            config: config_json.map(ToString::to_string),
        })
    }

    fn transform(&mut self, _batch: RecordBatch) -> TransformResult {
        TransformResult::discard()
    }
}

#[test]
fn test_dispatch_init_with_config() {
    let state = std::sync::Mutex::new(None);
    let config = r#"{"signal":"metrics","setting":"value"}"#;
    let cfg_bytes = config.as_bytes();
    let cfg_len = cfg_bytes.len() as u32;
    let cfg_ptr = datalake_alloc(cfg_len);
    assert_ne!(cfg_ptr, 0);
    write_guest_memory(cfg_ptr, cfg_bytes);

    let status = dispatch_init::<TestInitTransformer>(&state, cfg_ptr, cfg_len);
    assert_eq!(status, 0);

    let guard = state.lock().unwrap();
    let transformer = guard.as_ref().expect("transformer should be present");
    assert_eq!(transformer.signal, SignalType::Metrics);
    assert_eq!(transformer.config.as_deref(), Some(config));
    drop(guard);

    datalake_dealloc(cfg_ptr, cfg_len);

    // Test zero ptr / zero len defaults to Logs and None config
    let state_default = std::sync::Mutex::new(None);
    let status_default = dispatch_init::<TestInitTransformer>(&state_default, 0, 0);
    assert_eq!(status_default, 0);
    let guard_default = state_default.lock().unwrap();
    let transformer_default = guard_default.as_ref().expect("transformer initialized");
    assert_eq!(transformer_default.signal, SignalType::Logs);
    assert_eq!(transformer_default.config, None);
    drop(guard_default);

    // Test initialization failure returns 1
    let fail_config = r#"{"signal":"logs","fail_init":true}"#;
    let fail_ptr = datalake_alloc(fail_config.len() as u32);
    write_guest_memory(fail_ptr, fail_config.as_bytes());
    let state_fail = std::sync::Mutex::new(None);
    let status_fail =
        dispatch_init::<TestInitTransformer>(&state_fail, fail_ptr, fail_config.len() as u32);
    assert_eq!(status_fail, 1);
    assert!(state_fail.lock().unwrap().is_none());
    datalake_dealloc(fail_ptr, fail_config.len() as u32);

    // Test invalid UTF-8 returns 1
    let invalid_utf8 = [0xff, 0xfe, 0xfd];
    let invalid_ptr = datalake_alloc(invalid_utf8.len() as u32);
    write_guest_memory(invalid_ptr, &invalid_utf8);
    let state_invalid = std::sync::Mutex::new(None);
    let status_invalid = dispatch_init::<TestInitTransformer>(
        &state_invalid,
        invalid_ptr,
        invalid_utf8.len() as u32,
    );
    assert_eq!(status_invalid, 1);
    assert!(state_invalid.lock().unwrap().is_none());
    datalake_dealloc(invalid_ptr, invalid_utf8.len() as u32);
}

struct TestTransformMock {
    call_count: u32,
}

impl BatchTransformer for TestTransformMock {
    fn init(_signal: SignalType, _config_json: Option<&str>) -> Result<Self, String> {
        Ok(Self { call_count: 0 })
    }

    fn transform(&mut self, batch: RecordBatch) -> TransformResult {
        self.call_count += 1;
        TransformResult::ok(batch)
    }
}

#[test]
fn test_dispatch_transform_execution() {
    let state = std::sync::Mutex::new(Some(TestTransformMock { call_count: 0 }));
    let batch = create_sample_batch();
    let ipc_bytes = encode_batch_to_ipc(&batch).unwrap();
    let ipc_len = ipc_bytes.len() as u32;
    let ipc_ptr = datalake_alloc(ipc_len);
    assert_ne!(ipc_ptr, 0);
    write_guest_memory(ipc_ptr, &ipc_bytes);

    // Valid call: signal 0 = Logs
    let packed = dispatch_transform(&state, 0, ipc_ptr, ipc_len);
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;
    assert_eq!(
        header_len,
        std::mem::size_of::<TransformResponseHeader>() as u32
    );
    assert_ne!(header_ptr, 0);

    let header = read_response_header(header_ptr).expect("valid response header");
    assert_eq!(header.status, STATUS_SUCCESS);
    assert_eq!(header.batch_count, 1);
    assert_ne!(header.batches_ptr, 0);

    let descriptors =
        read_batch_descriptors(header.batches_ptr, header.batch_count as usize).unwrap();
    assert_eq!(descriptors.len(), 1);

    let out_ipc = read_guest_memory(descriptors[0].ptr, descriptors[0].len as usize).unwrap();
    let out_batch = decode_ipc_stream(&out_ipc).unwrap();
    assert_eq!(out_batch, batch);

    // Verify transformer internal state mutated
    let guard = state.lock().unwrap();
    assert_eq!(guard.as_ref().unwrap().call_count, 1);
    drop(guard);

    // Clean up memory
    datalake_dealloc(descriptors[0].ptr, descriptors[0].len);
    datalake_dealloc(
        header.batches_ptr,
        (header.batch_count as usize
            * std::mem::size_of::<opentelemetry_datalake_wasm_sdk::abi::BatchDescriptor>())
            as u32,
    );
    datalake_dealloc(header_ptr, header_len);
    datalake_dealloc(ipc_ptr, ipc_len);

    // Test invalid signal type (e.g. 99) returns error response
    let invalid_signal_packed = dispatch_transform(&state, 99, 0, 0);
    let err_header_ptr = (invalid_signal_packed >> 32) as u32;
    let err_header = read_response_header(err_header_ptr).expect("error header");
    assert_eq!(err_header.status, STATUS_ERROR);
    let err_msg =
        read_guest_string(err_header.message_ptr, err_header.message_len as usize).unwrap();
    assert!(err_msg.contains("Invalid signal type"));
    datalake_dealloc(err_header.message_ptr, err_header.message_len);
    datalake_dealloc(err_header_ptr, 20);

    // Test lazy initialization when state is None
    let lazy_state = std::sync::Mutex::new(None);
    let batch2 = create_sample_batch();
    let ipc_bytes2 = encode_batch_to_ipc(&batch2).unwrap();
    let ipc_len2 = ipc_bytes2.len() as u32;
    let ipc_ptr2 = datalake_alloc(ipc_len2);
    write_guest_memory(ipc_ptr2, &ipc_bytes2);

    let lazy_packed = dispatch_transform::<TestTransformMock>(&lazy_state, 1, ipc_ptr2, ipc_len2);
    let lazy_header_ptr = (lazy_packed >> 32) as u32;
    let lazy_header = read_response_header(lazy_header_ptr).expect("valid header");
    assert_eq!(lazy_header.status, STATUS_SUCCESS);
    assert_eq!(lazy_header.batch_count, 1);

    let lazy_desc =
        read_batch_descriptors(lazy_header.batches_ptr, lazy_header.batch_count as usize).unwrap();
    datalake_dealloc(lazy_desc[0].ptr, lazy_desc[0].len);
    datalake_dealloc(
        lazy_header.batches_ptr,
        (lazy_header.batch_count as usize
            * std::mem::size_of::<opentelemetry_datalake_wasm_sdk::abi::BatchDescriptor>())
            as u32,
    );
    datalake_dealloc(lazy_header_ptr, 20);
    datalake_dealloc(ipc_ptr2, ipc_len2);

    assert!(lazy_state.lock().unwrap().is_some());
}

struct MacroMockTransformer {
    #[allow(dead_code)]
    signal: SignalType,
}

impl BatchTransformer for MacroMockTransformer {
    fn init(signal: SignalType, _config_json: Option<&str>) -> Result<Self, String> {
        Ok(Self { signal })
    }

    fn transform(&mut self, batch: RecordBatch) -> TransformResult {
        TransformResult::ok(batch)
    }
}

mod mock_plugin {
    use super::*;
    opentelemetry_datalake_wasm_sdk::export_transformer!(MacroMockTransformer);
}

#[test]
fn test_export_transformer_macro_compilation() {
    let config = r#"{"signal":"traces"}"#;
    let cfg_bytes = config.as_bytes();
    let cfg_len = cfg_bytes.len() as u32;
    let cfg_ptr = datalake_alloc(cfg_len);
    write_guest_memory(cfg_ptr, cfg_bytes);

    let init_status = mock_plugin::datalake_init(cfg_ptr, cfg_len);
    assert_eq!(init_status, 0);
    datalake_dealloc(cfg_ptr, cfg_len);

    let batch = create_sample_batch();
    let ipc_bytes = encode_batch_to_ipc(&batch).unwrap();
    let ipc_len = ipc_bytes.len() as u32;
    let ipc_ptr = datalake_alloc(ipc_len);
    write_guest_memory(ipc_ptr, &ipc_bytes);

    // Traces = 2
    let packed = mock_plugin::datalake_transform(2, ipc_ptr, ipc_len);
    let header_ptr = (packed >> 32) as u32;
    let header = read_response_header(header_ptr).expect("valid header");
    assert_eq!(header.status, STATUS_SUCCESS);
    assert_eq!(header.batch_count, 1);

    let descriptors =
        read_batch_descriptors(header.batches_ptr, header.batch_count as usize).unwrap();
    let out_ipc = read_guest_memory(descriptors[0].ptr, descriptors[0].len as usize).unwrap();
    let out_batch = decode_ipc_stream(&out_ipc).unwrap();
    assert_eq!(out_batch, batch);

    datalake_dealloc(descriptors[0].ptr, descriptors[0].len);
    datalake_dealloc(
        header.batches_ptr,
        (header.batch_count as usize
            * std::mem::size_of::<opentelemetry_datalake_wasm_sdk::abi::BatchDescriptor>())
            as u32,
    );
    datalake_dealloc(header_ptr, 20);
    datalake_dealloc(ipc_ptr, ipc_len);
}
