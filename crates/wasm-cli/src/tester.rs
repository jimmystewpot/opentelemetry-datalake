//! Guest module conformance and immutability test suite runner.

use anyhow::{Context, Result};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use std::sync::Arc;
use thiserror::Error;
use wasmtime::{Config, Engine, Instance, Module, Store};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TesterError {
    #[error("Value mismatch in immutable column {0}")]
    ValueMismatch(&'static str),
    #[error("Immutable column '{0}' present in input but missing from output")]
    MissingColumn(&'static str),
}

/// Creates a synthetic [`RecordBatch`] containing canonical OpenTelemetry fields.
///
/// Fields include `trace_id`, `span_id`, `timestamp`, `observed_timestamp`, `name`, `type`, and `attributes`.
///
/// # Errors
///
/// Returns an error if record batch construction fails.
pub fn create_synthetic_batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("observed_timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["0123456789abcdef0123456789abcdef"])),
            Arc::new(StringArray::from(vec!["0123456789abcdef"])),
            Arc::new(Int64Array::from(vec![1_700_000_000_000_000_000_i64])),
            Arc::new(Int64Array::from(vec![1_700_000_000_001_000_000_i64])),
            Arc::new(StringArray::from(vec!["HTTP GET /api/v1/resource"])),
            Arc::new(StringArray::from(vec!["span"])),
            Arc::new(StringArray::from(vec![Some(
                "{\"service.name\":\"auth-svc\",\"http.status\":200}",
            )])),
        ],
    )?;

    Ok(batch)
}

/// Serializes an Arrow [`RecordBatch`] into an in-memory Arrow IPC stream.
///
/// # Errors
///
/// Returns an error if Arrow IPC serialization fails.
pub fn serialize_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
        .context("Failed to initialize Arrow IPC stream writer")?;
    writer
        .write(batch)
        .context("Failed to write RecordBatch to Arrow IPC stream")?;
    writer
        .finish()
        .context("Failed to finalize Arrow IPC stream")?;
    Ok(buffer)
}

/// Verifies that immutable OpenTelemetry columns are preserved between input and output batches.
///
/// Ensures that any canonical immutable column (`trace_id`, `span_id`, `timestamp`,
/// `observed_timestamp`, `name`, `type`) present in `input` is also present in `output`,
/// and that its array contents remain unchanged.
///
/// # Errors
///
/// Returns [`TesterError::MissingColumn`] if an immutable column present in `input` is missing from `output`,
/// or [`TesterError::ValueMismatch`] if the values in an immutable column have been altered.
pub fn verify_batch_immutability(
    input: &RecordBatch,
    output: &RecordBatch,
) -> std::result::Result<(), TesterError> {
    let in_schema = input.schema();
    let out_schema = output.schema();
    for &col_name in IMMUTABLE_COLUMNS {
        if let (Ok(i_idx), Ok(o_idx)) =
            (in_schema.index_of(col_name), out_schema.index_of(col_name))
        {
            let in_col = input.column(i_idx);
            let out_col = output.column(o_idx);
            if in_col != out_col {
                return Err(TesterError::ValueMismatch(col_name));
            }
        } else if in_schema.index_of(col_name).is_ok() {
            return Err(TesterError::MissingColumn(col_name));
        }
    }
    Ok(())
}

/// Executes the full immutability and conformance test suite against guest WASM bytecode.
///
/// Validates ABI exports, sends synthetic Arrow IPC batches with fuel metering enabled,
/// and verifies that immutable OpenTelemetry columns are preserved.
///
/// # Errors
///
/// Returns an error if:
/// - Engine configuration, compilation, or instantiation fails.
/// - Required ABI exports are missing.
/// - Execution traps (such as fuel exhaustion from infinite loops) or returns an error status.
/// - Output batches cannot be parsed from Arrow IPC.
/// - Any canonical immutable column is missing or altered.
#[allow(clippy::too_many_lines)]
pub fn run_immutability_suite(bytes: &[u8], signal: &str, config_json: &str) -> Result<()> {
    crate::validator::validate_wasm_bytes(bytes)
        .map_err(|e| anyhow::anyhow!("Module validation failed: {e}"))?;

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize wasmtime engine: {e}"))?;
    let module = Module::new(&engine, bytes)
        .map_err(|e| anyhow::anyhow!("Failed to compile WebAssembly module: {e}"))?;

    let mut store = Store::new(&engine, ());
    store
        .set_fuel(1_000_000)
        .map_err(|e| anyhow::anyhow!("Failed to configure fuel limit: {e}"))?;

    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| anyhow::anyhow!("Failed to instantiate WebAssembly module: {e}"))?;

    let alloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
        .map_err(|e| anyhow::anyhow!("Missing 'datalake_alloc' export: {e}"))?;
    let dealloc_fn = instance
        .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
        .map_err(|e| anyhow::anyhow!("Missing 'datalake_dealloc' export: {e}"))?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("Missing 'memory' export"))?;

    if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), i32>(&mut store, "datalake_init") {
        let parsed_config = serde_json::from_str::<serde_json::Value>(config_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let init_payload = serde_json::json!({
            "signal": signal,
            "env": {},
            "config": parsed_config,
        });
        let init_bytes =
            serde_json::to_vec(&init_payload).context("Failed to serialize init payload")?;
        let init_len =
            u32::try_from(init_bytes.len()).context("Init payload size exceeds u32 limit")?;

        let init_ptr = alloc_fn
            .call(&mut store, init_len)
            .map_err(|e| anyhow::anyhow!("datalake_alloc failed for init payload: {e}"))?;

        if init_ptr == 0 {
            anyhow::bail!("datalake_alloc returned null pointer during initialization");
        }

        memory
            .write(&mut store, init_ptr as usize, &init_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to write init payload to guest memory: {e}"))?;

        let status = init_fn
            .call(&mut store, (init_ptr, init_len))
            .map_err(|e| anyhow::anyhow!("datalake_init trapped during initialization: {e}"))?;

        let _ = dealloc_fn.call(&mut store, (init_ptr, init_len));

        if status != 0 {
            anyhow::bail!("datalake_init returned non-zero status code: {status}");
        }
    }

    let input_batch = create_synthetic_batch()?;
    let ipc_bytes = serialize_batch_to_ipc(&input_batch)?;
    let ipc_len =
        u32::try_from(ipc_bytes.len()).context("Serialized IPC payload size exceeds u32 limit")?;

    let ipc_ptr = alloc_fn
        .call(&mut store, ipc_len)
        .map_err(|e| anyhow::anyhow!("datalake_alloc failed for IPC buffer: {e}"))?;
    if ipc_ptr == 0 {
        anyhow::bail!("datalake_alloc returned null pointer for IPC buffer");
    }

    memory
        .write(&mut store, ipc_ptr as usize, &ipc_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to write IPC payload into guest memory: {e}"))?;

    let desc_ptr = alloc_fn.call(&mut store, 8).unwrap_or(0);
    if desc_ptr != 0 {
        let mut desc_bytes = [0u8; 8];
        desc_bytes[0..4].copy_from_slice(&ipc_ptr.to_le_bytes());
        desc_bytes[4..8].copy_from_slice(&ipc_len.to_le_bytes());
        let _ = memory.write(&mut store, desc_ptr as usize, &desc_bytes);
    }

    // Call datalake_transform supporting v1 (3 args), legacy (2 args), or descriptor (1 arg)
    let (header_ptr, _header_len) = if let Ok(f) =
        instance.get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
    {
        let packed = f
            .call(&mut store, (0, ipc_ptr, ipc_len))
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        let ptr = u32::try_from(packed >> 32).unwrap_or(0);
        let len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
        if ptr == 0 {
            anyhow::bail!("datalake_transform returned a null pointer for response header");
        }
        if len < 20 {
            anyhow::bail!("datalake_transform returned header length < 20: {len}");
        }
        (ptr, len)
    } else if let Ok(f) =
        instance.get_typed_func::<(u32, u32), u64>(&mut store, "datalake_transform")
    {
        let packed = f
            .call(&mut store, (ipc_ptr, ipc_len))
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        let ptr = u32::try_from(packed >> 32).unwrap_or(0);
        let len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
        (
            if ptr != 0 {
                ptr
            } else {
                u32::try_from(packed).unwrap_or(0)
            },
            if len >= 20 { len } else { 20 },
        )
    } else if let Ok(f) =
        instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")
    {
        let ptr = f
            .call(&mut store, (ipc_ptr, ipc_len))
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        (ptr, 20)
    } else if let Ok(f) = instance.get_typed_func::<u32, u64>(&mut store, "datalake_transform") {
        let packed = f
            .call(&mut store, desc_ptr)
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        let ptr = u32::try_from(packed >> 32).unwrap_or(0);
        let len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
        (
            if ptr != 0 {
                ptr
            } else {
                u32::try_from(packed).unwrap_or(0)
            },
            if len >= 20 { len } else { 20 },
        )
    } else if let Ok(f) = instance.get_typed_func::<u32, u32>(&mut store, "datalake_transform") {
        let ptr = f
            .call(&mut store, desc_ptr)
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        (ptr, 20)
    } else {
        anyhow::bail!("Unsupported datalake_transform signature");
    };

    let mem_size = memory.data_size(&store);
    if (header_ptr as usize).saturating_add(20) > mem_size {
        anyhow::bail!("Transform response header out of guest memory bounds");
    }

    let mut header_buf = [0u8; 20];
    memory
        .read(&store, header_ptr as usize, &mut header_buf)
        .context("Failed to read TransformResponseHeader from guest memory")?;

    let status = u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    let batch_count =
        u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
    let batches_ptr =
        u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
    let message_ptr = u32::from_le_bytes([
        header_buf[12],
        header_buf[13],
        header_buf[14],
        header_buf[15],
    ]);
    let message_len = u32::from_le_bytes([
        header_buf[16],
        header_buf[17],
        header_buf[18],
        header_buf[19],
    ]);

    if status != 0 {
        let msg = if message_ptr != 0
            && message_len > 0
            && (message_ptr as usize).saturating_add(message_len as usize) <= mem_size
        {
            let mut msg_buf = vec![0u8; message_len as usize];
            if memory
                .read(&store, message_ptr as usize, &mut msg_buf)
                .is_ok()
            {
                String::from_utf8_lossy(&msg_buf).to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        anyhow::bail!("Transform failed with status {status}: {msg}");
    }

    if batch_count == 0 {
        anyhow::bail!("Transform returned 0 output batches; unable to verify immutability");
    }

    let desc_table_len = (batch_count as usize).saturating_mul(8);
    if (batches_ptr as usize).saturating_add(desc_table_len) > mem_size {
        anyhow::bail!("Batch descriptor array exceeds guest memory bounds");
    }

    let mut verified_count = 0usize;
    for i in 0..batch_count {
        let offset = (batches_ptr as usize).saturating_add((i as usize) * 8);
        let mut desc_buf = [0u8; 8];
        memory
            .read(&store, offset, &mut desc_buf)
            .context("Failed to read BatchDescriptor")?;
        let b_ptr = u32::from_le_bytes([desc_buf[0], desc_buf[1], desc_buf[2], desc_buf[3]]);
        let b_len = u32::from_le_bytes([desc_buf[4], desc_buf[5], desc_buf[6], desc_buf[7]]);

        if b_ptr == 0 || b_len == 0 {
            anyhow::bail!("BatchDescriptor contains null or empty pointer");
        }

        let start = b_ptr as usize;
        let end = start.saturating_add(b_len as usize);
        if end > mem_size {
            anyhow::bail!("Batch IPC buffer exceeds guest memory bounds");
        }

        let mut out_ipc_bytes = vec![0u8; b_len as usize];
        memory
            .read(&store, start, &mut out_ipc_bytes)
            .context("Failed to read batch IPC bytes")?;

        let cursor = std::io::Cursor::new(out_ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            anyhow::anyhow!("Failed to initialize Arrow StreamReader on output batch: {e}")
        })?;

        for maybe_batch in reader {
            let out_batch = maybe_batch.map_err(|e| {
                anyhow::anyhow!("Failed to read output RecordBatch from IPC stream: {e}")
            })?;
            verify_batch_immutability(&input_batch, &out_batch)?;
            verified_count = verified_count.saturating_add(1);
        }
    }

    // Cleanup host allocations
    let _ = dealloc_fn.call(&mut store, (ipc_ptr, ipc_len));
    if desc_ptr != 0 {
        let _ = dealloc_fn.call(&mut store, (desc_ptr, 8));
    }

    println!(
        "✓ Passed immutability & conformance test suite ({verified_count} batch(es) verified)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_verify_batch_immutability_value_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )]));
        let in_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["trace_1"]))],
        )
        .expect("in batch");
        let out_batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["trace_2"]))])
                .expect("out batch");

        let err = verify_batch_immutability(&in_batch, &out_batch).expect_err("should fail");
        assert_eq!(err, TesterError::ValueMismatch("trace_id"));
    }

    #[test]
    fn test_verify_batch_immutability_missing_column() {
        let in_schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
        ]));
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "span_id",
            DataType::Utf8,
            false,
        )]));
        let in_batch = RecordBatch::try_new(
            in_schema,
            vec![
                Arc::new(StringArray::from(vec!["trace_1"])),
                Arc::new(StringArray::from(vec!["span_1"])),
            ],
        )
        .expect("in batch");
        let out_batch = RecordBatch::try_new(
            out_schema,
            vec![Arc::new(StringArray::from(vec!["span_1"]))],
        )
        .expect("out batch");

        let err = verify_batch_immutability(&in_batch, &out_batch).expect_err("should fail");
        assert_eq!(err, TesterError::MissingColumn("trace_id"));
    }
}
