//! Arrow IPC serialization/deserialization and C-ABI response builders.
//!
//! Provides the core dispatch functions for guest transform plugins:
//! decoding incoming Arrow IPC streams into [`RecordBatch`] structures,
//! serializing transformed batches back to IPC streams, and constructing
//! packed C-ABI v1 responses.

use crate::abi::{
    BatchDescriptor, STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS,
    TransformResponseHeader, datalake_alloc, datalake_dealloc, read_guest_memory,
    write_guest_memory,
};
use crate::error::SdkError;
use crate::traits::{BatchTransformer, SignalType, TransformResult};
use arrow::record_batch::RecordBatch;

/// Decodes a single Arrow [`RecordBatch`] from an Arrow IPC stream byte slice.
///
/// # Errors
///
/// Returns [`SdkError::Ipc`] if the byte slice is empty, malformed, or fails to decode.
pub fn decode_ipc_stream(slice: &[u8]) -> Result<RecordBatch, SdkError> {
    if slice.is_empty() {
        return Err(SdkError::Ipc("Empty IPC stream payload".to_string()));
    }
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(slice), None)
        .map_err(|e| SdkError::Ipc(e.to_string()))?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(SdkError::Ipc(e.to_string())),
        None => Ok(RecordBatch::new_empty(reader.schema())),
    }
}

/// Encodes an Arrow [`RecordBatch`] into an Arrow IPC stream byte buffer.
///
/// # Errors
///
/// Returns [`SdkError::Ipc`] if writing the record batch into the IPC stream fails.
pub fn encode_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, SdkError> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| SdkError::Ipc(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| SdkError::Ipc(e.to_string()))?;
    writer.finish().map_err(|e| SdkError::Ipc(e.to_string()))?;
    Ok(buf)
}

/// Encodes a [`TransformResult`] into guest memory buffers and returns a packed 64-bit ABI response.
///
/// The returned `u64` packs the 32-bit linear address of the allocated [`TransformResponseHeader`]
/// in the high 32 bits and the header size (20 bytes) in the low 32 bits:
/// `((header_ptr as u64) << 32) | 20u64`.
#[must_use]
pub fn encode_response(result: &TransformResult) -> u64 {
    match result {
        TransformResult::Continue(batches) => {
            if batches.is_empty() {
                write_and_pack_header(STATUS_SUCCESS, 0, 0, 0, 0)
            } else {
                let mut ipc_buffers = Vec::with_capacity(batches.len());
                for batch in batches {
                    match encode_batch_to_ipc(batch) {
                        Ok(buf) => ipc_buffers.push(buf),
                        Err(e) => {
                            return create_error_response(&format!(
                                "IPC serialization failed: {e}"
                            ));
                        }
                    }
                }

                let mut descriptors: Vec<BatchDescriptor> = Vec::with_capacity(ipc_buffers.len());
                for buf in &ipc_buffers {
                    #[allow(clippy::cast_possible_truncation)]
                    let buf_len = buf.len() as u32;
                    let buf_ptr = datalake_alloc(buf_len);
                    if buf_ptr == 0 && buf_len > 0 {
                        for d in &descriptors {
                            datalake_dealloc(d.ptr, d.len);
                        }
                        return create_error_response("Memory allocation failed for batch buffer");
                    }
                    if buf_ptr != 0 {
                        // SAFETY: `buf_ptr` was allocated via `datalake_alloc` with capacity `buf_len` (buf.len()),
                        // and `buf` is a valid slice of length `buf_len`.
                        unsafe {
                            write_guest_memory(buf_ptr, buf);
                        }
                    }
                    descriptors.push(BatchDescriptor {
                        ptr: buf_ptr,
                        len: buf_len,
                    });
                }

                let desc_byte_len = descriptors.len() * std::mem::size_of::<BatchDescriptor>();
                #[allow(clippy::cast_possible_truncation)]
                let desc_len_u32 = desc_byte_len as u32;
                let desc_ptr = datalake_alloc(desc_len_u32);
                if desc_ptr == 0 && !descriptors.is_empty() {
                    for d in &descriptors {
                        datalake_dealloc(d.ptr, d.len);
                    }
                    return create_error_response("Memory allocation failed for batch descriptors");
                }
                if desc_ptr != 0 {
                    let mut desc_bytes = Vec::with_capacity(desc_byte_len);
                    for d in &descriptors {
                        desc_bytes.extend_from_slice(&d.ptr.to_le_bytes());
                        desc_bytes.extend_from_slice(&d.len.to_le_bytes());
                    }
                    // SAFETY: `desc_ptr` was allocated via `datalake_alloc` with capacity `desc_len_u32` (desc_byte_len),
                    // and `desc_bytes` is a valid slice containing exactly `desc_byte_len` bytes.
                    unsafe {
                        write_guest_memory(desc_ptr, &desc_bytes);
                    }
                }

                #[allow(clippy::cast_possible_truncation)]
                let batch_count = descriptors.len() as u32;
                write_and_pack_header(STATUS_SUCCESS, batch_count, desc_ptr, 0, 0)
            }
        }
        TransformResult::Discard => write_and_pack_header(STATUS_DISCARD, 0, 0, 0, 0),
        TransformResult::Reject { reason } => {
            let reason_bytes = reason.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            let mut msg_len = reason_bytes.len() as u32;
            let msg_ptr = if msg_len == 0 {
                0
            } else {
                let ptr = datalake_alloc(msg_len);
                if ptr == 0 {
                    msg_len = 0;
                    0
                } else {
                    // SAFETY: `ptr` was allocated via `datalake_alloc` with capacity `msg_len` (reason_bytes.len()),
                    // and `reason_bytes` is a valid slice of length `msg_len`.
                    unsafe {
                        write_guest_memory(ptr, reason_bytes);
                    }
                    ptr
                }
            };
            write_and_pack_header(STATUS_REJECT, 0, 0, msg_ptr, msg_len)
        }
        TransformResult::Error { reason } => create_error_response(reason),
    }
}

/// Creates an error response from an error message string and returns a packed 64-bit ABI response.
#[must_use]
pub fn create_error_response(message: &str) -> u64 {
    let msg_bytes = message.as_bytes();
    #[allow(clippy::cast_possible_truncation)]
    let mut msg_len = msg_bytes.len() as u32;
    let msg_ptr = if msg_len == 0 {
        0
    } else {
        let ptr = datalake_alloc(msg_len);
        if ptr == 0 {
            msg_len = 0;
            0
        } else {
            // SAFETY: `ptr` was allocated via `datalake_alloc` with capacity `msg_len` (msg_bytes.len()),
            // and `msg_bytes` is a valid slice of length `msg_len`.
            unsafe {
                write_guest_memory(ptr, msg_bytes);
            }
            ptr
        }
    };
    write_and_pack_header(STATUS_ERROR, 0, 0, msg_ptr, msg_len)
}

fn write_and_pack_header(
    status: u32,
    batch_count: u32,
    batches_ptr: u32,
    message_ptr: u32,
    message_len: u32,
) -> u64 {
    let mut header_bytes = [0u8; 20];
    header_bytes[0..4].copy_from_slice(&status.to_le_bytes());
    header_bytes[4..8].copy_from_slice(&batch_count.to_le_bytes());
    header_bytes[8..12].copy_from_slice(&batches_ptr.to_le_bytes());
    header_bytes[12..16].copy_from_slice(&message_ptr.to_le_bytes());
    header_bytes[16..20].copy_from_slice(&message_len.to_le_bytes());

    #[allow(clippy::cast_possible_truncation)]
    let header_size = std::mem::size_of::<TransformResponseHeader>() as u32;
    let header_ptr = datalake_alloc(header_size);
    if header_ptr != 0 {
        // SAFETY: `header_ptr` was allocated via `datalake_alloc` with capacity `header_size`
        // (std::mem::size_of::<TransformResponseHeader>() == 20), matching `header_bytes.len()`.
        unsafe {
            write_guest_memory(header_ptr, &header_bytes);
        }
    }
    pack_header(header_ptr)
}

#[inline]
fn pack_header(header_ptr: u32) -> u64 {
    (u64::from(header_ptr) << 32) | 0x14_u64
}

/// Extracts the target [`SignalType`] from a configuration string, matching C-ABI conventions.
///
/// Supports JSON configuration containing a `"signal"` key (e.g. `{"signal":"metrics"}` or `{"signal": 1}`)
/// as well as plain signal names (e.g. `"metrics"`, `"logs"`, `"traces"`).
#[must_use]
pub fn parse_signal_from_config(config: &str) -> Option<SignalType> {
    fn match_signal_token(token: &str) -> Option<SignalType> {
        let trimmed = token.trim();
        if trimmed.eq_ignore_ascii_case("logs")
            || trimmed.eq_ignore_ascii_case("log")
            || trimmed == "0"
        {
            Some(SignalType::Logs)
        } else if trimmed.eq_ignore_ascii_case("metrics")
            || trimmed.eq_ignore_ascii_case("metric")
            || trimmed == "1"
        {
            Some(SignalType::Metrics)
        } else if trimmed.eq_ignore_ascii_case("traces")
            || trimmed.eq_ignore_ascii_case("trace")
            || trimmed == "2"
        {
            Some(SignalType::Traces)
        } else {
            None
        }
    }

    let trimmed = config.trim();
    if let Some(signal) = match_signal_token(trimmed) {
        return Some(signal);
    }

    // Look for `"signal"` key in JSON-like configuration
    let mut search_idx = 0;
    while let Some(pos) = config[search_idx..].find("\"signal\"") {
        let key_end = search_idx + pos + 8; // len of `"signal"` is 8
        search_idx = key_end;

        // Skip whitespace to colon
        let after_key = &config[key_end..];
        let Some((colon_offset, ':')) =
            after_key.char_indices().find(|(_, ch)| !ch.is_whitespace())
        else {
            continue;
        };

        // After colon, find start of value
        let after_colon = &after_key[colon_offset + 1..];
        let Some((start_offset, _)) = after_colon
            .char_indices()
            .find(|(_, ch)| !ch.is_whitespace())
        else {
            continue;
        };

        let val_slice = &after_colon[start_offset..];
        if let Some(stripped) = val_slice.strip_prefix('"') {
            // Quoted string value: find closing quote
            if let Some(end_quote) = stripped.find('"') {
                let token = &stripped[..end_quote];
                if let Some(sig) = match_signal_token(token) {
                    return Some(sig);
                }
            }
        } else {
            // Unquoted token (e.g. number 0, 1, 2)
            let end_idx = match val_slice.find(|c: char| c == ',' || c == '}' || c.is_whitespace())
            {
                Some(idx) => idx,
                None => val_slice.len(),
            };
            let token = &val_slice[..end_idx];
            if let Some(sig) = match_signal_token(token) {
                return Some(sig);
            }
        }
    }

    None
}

/// Dispatches the guest module initialization call across the C-ABI boundary.
///
/// Reads optional configuration from guest memory at `config_ptr` and `config_len`,
/// resolves the target signal type from configuration, invokes [`BatchTransformer::init`],
/// and stores the initialized instance into `state`.
///
/// # Returns
///
/// `0` on success, or `1` on error (with error logged via [`crate::panic::log_error`]).
#[must_use]
pub fn dispatch_init<T: BatchTransformer>(
    state: &std::sync::Mutex<Option<T>>,
    config_ptr: u32,
    config_len: u32,
) -> u32 {
    let (signal, config_json_payload) = if config_ptr != 0 && config_len > 0 {
        // SAFETY: `config_ptr` and `config_len` were supplied by the host runtime across the ABI boundary.
        // Both `config_ptr != 0` and `config_len > 0` are verified, and the buffer is allocated and initialized by the host.
        let bytes_opt = unsafe { read_guest_memory(config_ptr, config_len as usize) };
        let Some(bytes) = bytes_opt else {
            crate::panic::log_error("Failed to read configuration payload from guest memory");
            return 1;
        };
        let s = match String::from_utf8(bytes) {
            Ok(valid) => valid,
            Err(e) => {
                crate::panic::log_error(&format!("Invalid UTF-8 in configuration payload: {e}"));
                return 1;
            }
        };
        let sig = match parse_signal_from_config(&s) {
            Some(sig) => sig,
            None => SignalType::Logs,
        };
        (sig, Some(s))
    } else {
        (SignalType::Logs, None)
    };

    match T::init(signal, config_json_payload.as_deref()) {
        Ok(instance) => {
            let mut lock = match state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *lock = Some(instance);
            0
        }
        Err(e) => {
            crate::panic::log_error(&format!("Failed to initialize transformer: {e}"));
            1
        }
    }
}

/// Dispatches a batch transformation call across the C-ABI boundary.
///
/// Decodes the incoming Arrow IPC stream from `ipc_ptr` and `ipc_len`,
/// invokes [`BatchTransformer::transform`], and encodes the resulting [`TransformResult`]
/// into C-ABI v1 response buffers.
///
/// # Returns
///
/// A packed 64-bit value where the upper 32 bits contain the pointer to [`TransformResponseHeader`]
/// and the lower 32 bits contain the header size (20 bytes).
#[must_use]
pub fn dispatch_transform<T: BatchTransformer>(
    state: &std::sync::Mutex<Option<T>>,
    signal_type: u32,
    ipc_ptr: u32,
    ipc_len: u32,
) -> u64 {
    let Some(signal) = SignalType::from_u32(signal_type) else {
        return create_error_response("Invalid signal type");
    };

    let mut guard = match state.lock() {
        Ok(lock) => lock,
        Err(poisoned) => poisoned.into_inner(),
    };

    if guard.is_none() {
        match T::init(signal, None) {
            Ok(instance) => {
                *guard = Some(instance);
            }
            Err(e) => {
                crate::panic::log_error(&format!("Lazy initialization failed: {e}"));
                return create_error_response("Transformer not initialized");
            }
        }
    }

    let Some(transformer) = guard.as_mut() else {
        return create_error_response("Transformer not initialized");
    };

    if ipc_ptr == 0 || ipc_len == 0 {
        return create_error_response("Empty or null IPC stream pointer");
    }

    // SAFETY: `ipc_ptr` and `ipc_len` were supplied by the host runtime across the ABI boundary.
    // Both are verified non-zero above, and the host runtime allocated and populated the buffer with `ipc_len` bytes.
    let ipc_bytes_opt = unsafe { read_guest_memory(ipc_ptr, ipc_len as usize) };
    let Some(ipc_bytes) = ipc_bytes_opt else {
        return create_error_response("Failed to read IPC stream payload from guest memory");
    };

    let batch = match decode_ipc_stream(&ipc_bytes) {
        Ok(b) => b,
        Err(e) => {
            return create_error_response(&format!("IPC decode failed: {e}"));
        }
    };

    let result = transformer.transform(batch);
    encode_response(&result)
}
