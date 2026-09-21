//! Arrow IPC serialization/deserialization and C-ABI response builders.
//!
//! Provides the core dispatch functions for guest transform plugins:
//! decoding incoming Arrow IPC streams into [`RecordBatch`] structures,
//! serializing transformed batches back to IPC streams, and constructing
//! packed C-ABI v1 responses.

use crate::abi::{
    BatchDescriptor, STATUS_DISCARD, STATUS_ERROR, STATUS_REJECT, STATUS_SUCCESS,
    TransformResponseHeader, datalake_alloc, write_guest_memory,
};
use crate::error::SdkError;
use crate::traits::TransformResult;
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

                let mut descriptors = Vec::with_capacity(ipc_buffers.len());
                for buf in &ipc_buffers {
                    #[allow(clippy::cast_possible_truncation)]
                    let buf_len = buf.len() as u32;
                    let buf_ptr = datalake_alloc(buf_len);
                    if buf_ptr != 0 {
                        write_guest_memory(buf_ptr, buf);
                    }
                    descriptors.push(BatchDescriptor {
                        ptr: buf_ptr,
                        len: buf_len,
                    });
                }

                let desc_byte_len = descriptors.len() * std::mem::size_of::<BatchDescriptor>();
                #[allow(clippy::cast_possible_truncation)]
                let desc_ptr = datalake_alloc(desc_byte_len as u32);
                if desc_ptr != 0 {
                    let mut desc_bytes = Vec::with_capacity(desc_byte_len);
                    for d in &descriptors {
                        desc_bytes.extend_from_slice(&d.ptr.to_le_bytes());
                        desc_bytes.extend_from_slice(&d.len.to_le_bytes());
                    }
                    write_guest_memory(desc_ptr, &desc_bytes);
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
            let msg_len = reason_bytes.len() as u32;
            let msg_ptr = if msg_len == 0 {
                0
            } else {
                let ptr = datalake_alloc(msg_len);
                if ptr != 0 {
                    write_guest_memory(ptr, reason_bytes);
                }
                ptr
            };
            write_and_pack_header(STATUS_REJECT, 0, 0, msg_ptr, msg_len)
        }
        TransformResult::Error { reason } => {
            let reason_bytes = reason.as_bytes();
            #[allow(clippy::cast_possible_truncation)]
            let msg_len = reason_bytes.len() as u32;
            let msg_ptr = if msg_len == 0 {
                0
            } else {
                let ptr = datalake_alloc(msg_len);
                if ptr != 0 {
                    write_guest_memory(ptr, reason_bytes);
                }
                ptr
            };
            write_and_pack_header(STATUS_ERROR, 0, 0, msg_ptr, msg_len)
        }
    }
}

/// Creates an error response from an error message string and returns a packed 64-bit ABI response.
#[must_use]
pub fn create_error_response(message: &str) -> u64 {
    let msg_bytes = message.as_bytes();
    #[allow(clippy::cast_possible_truncation)]
    let msg_len = msg_bytes.len() as u32;
    let msg_ptr = if msg_len == 0 {
        0
    } else {
        let ptr = datalake_alloc(msg_len);
        if ptr != 0 {
            write_guest_memory(ptr, msg_bytes);
        }
        ptr
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
        write_guest_memory(header_ptr, &header_bytes);
    }
    pack_header(header_ptr)
}

#[inline]
fn pack_header(header_ptr: u32) -> u64 {
    (u64::from(header_ptr) << 32) | 0x14_u64
}
