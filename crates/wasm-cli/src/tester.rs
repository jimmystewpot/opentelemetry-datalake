use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, Int32Array, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::abi::{STATUS_DISCARD, STATUS_SUCCESS};
use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
use thiserror::Error;
use wasmtime::{Config, Engine, Module, Store};

use crate::validator::validate_wasm_bytes;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TesterError {
    #[error("Value mismatch in immutable column {0}")]
    ValueMismatch(&'static str),
    #[error("Immutable column '{0}' present in input but missing from output")]
    MissingColumn(&'static str),
}

/// Verifies that immutable OpenTelemetry columns are preserved between input and output batches.
///
/// Ensures that any canonical immutable column (`trace_id`, `span_id`, `timestamp`,
/// `observed_timestamp`, `name`, `type`) present in `input` is also present in `output.schema()`,
/// and that emitted rows preserve their immutable values without alteration.
/// Filtered batches (with zero or fewer rows) and split batches (`ok_multiple`) are supported
/// by verifying that every row present in `output` matches a corresponding row in `input`.
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`). It operates solely on borrowed
/// [`RecordBatch`] references without internal or global mutable state.
///
/// # Errors
///
/// Returns [`TesterError::MissingColumn`] if an immutable column present in `input` is missing from `output`,
/// or [`TesterError::ValueMismatch`] if the values in an immutable column have been altered.
pub fn verify_batch_immutability(
    input: &RecordBatch,
    output: &RecordBatch,
) -> std::result::Result<(), TesterError> {
    let mut used_inputs = vec![false; input.num_rows()];
    verify_batch_immutability_with_used(input, output, &mut used_inputs)
}

/// Verifies that immutable OpenTelemetry columns are preserved between input and output batches,
/// tracking used input rows across multiple batches using the provided `used_inputs` mask.
///
/// # Concurrency Characteristics
///
/// This function is thread-safe (`Send + Sync`) and operates on caller-provided references.
///
/// # Errors
///
/// Returns [`TesterError::MissingColumn`] if an immutable column present in `input` is missing from `output`,
/// or [`TesterError::ValueMismatch`] if the values in an immutable column have been altered or
/// if an input row is consumed more than once (multiplicity violation).
pub fn verify_batch_immutability_with_used(
    input: &RecordBatch,
    output: &RecordBatch,
    used_inputs: &mut [bool],
) -> std::result::Result<(), TesterError> {
    if used_inputs.len() < input.num_rows() {
        return Err(TesterError::ValueMismatch("input_row_multiplicity"));
    }
    let in_schema = input.schema();
    let out_schema = output.schema();

    // 1. Verify schema preservation: every immutable column in input must be present in output schema.
    for &col_name in IMMUTABLE_COLUMNS {
        if in_schema.index_of(col_name).is_ok() && out_schema.index_of(col_name).is_err() {
            return Err(TesterError::MissingColumn(col_name));
        }
    }

    // 2. If output has no rows (e.g. filtered batch), schema check passed and no emitted values were mutated.
    if output.num_rows() == 0 {
        return Ok(());
    }

    // 3. Collect all immutable column array pairs present in both input and output.
    let col_pairs: Vec<(
        &'static str,
        &arrow::array::ArrayRef,
        &arrow::array::ArrayRef,
    )> = IMMUTABLE_COLUMNS
        .iter()
        .filter_map(|&col_name| {
            if let (Ok(i_idx), Ok(o_idx)) =
                (in_schema.index_of(col_name), out_schema.index_of(col_name))
            {
                Some((col_name, input.column(i_idx), output.column(o_idx)))
            } else {
                None
            }
        })
        .collect();

    if col_pairs.is_empty() {
        return Ok(());
    }

    // 4. Fast path: if row counts match, no rows have been used yet,
    // and all common immutable columns are positionally identical, mark all as used and succeed immediately.
    if input.num_rows() == output.num_rows() && used_inputs[..input.num_rows()].iter().all(|&u| !u)
    {
        let all_positionally_identical = col_pairs
            .iter()
            .all(|(_, in_col, out_col)| in_col == out_col);
        if all_positionally_identical {
            for u in &mut used_inputs[..input.num_rows()] {
                *u = true;
            }
            return Ok(());
        }
    }

    // 5. Multiplicity-preserving match: every output row must correspond to a distinct input row
    // with identical immutable column values (supports reordered, filtered subsets, and split batches).
    for o in 0..output.num_rows() {
        let mut matched_idx = None;
        let mut candidate_mismatched_col: Option<&'static str> = None;

        for (i, &is_used) in used_inputs[..input.num_rows()].iter().enumerate() {
            if is_used {
                continue;
            }
            let mut all_cols_match = true;
            for &(col_name, in_col, out_col) in &col_pairs {
                if in_col.slice(i, 1) != out_col.slice(o, 1) {
                    all_cols_match = false;
                    if candidate_mismatched_col.is_none() {
                        candidate_mismatched_col = Some(col_name);
                    }
                    break;
                }
            }
            if all_cols_match {
                matched_idx = Some(i);
                break;
            }
        }

        if let Some(i) = matched_idx {
            used_inputs[i] = true;
        } else {
            let col = candidate_mismatched_col.unwrap_or(col_pairs[0].0);
            return Err(TesterError::ValueMismatch(col));
        }
    }

    Ok(())
}

/// Builds a canonical test Arrow [`RecordBatch`] matching the production schema
/// for the specified signal type:
/// - `0` (Logs): Canonical logs schema matching `arrow-codec::logs`.
/// - `1` (Metrics): Canonical metrics schema matching `arrow-codec::metrics`.
/// - `2` (Traces): Canonical traces schema matching `arrow-codec::traces`.
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`).
///
/// # Errors
///
/// Returns an error if the record batch cannot be created.
pub fn build_canonical_test_batch_for_signal(signal: u32) -> Result<RecordBatch> {
    match signal {
        1 => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("unit", DataType::Utf8, false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, false),
                Field::new("attributes", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, false),
                Field::new("resource_attributes", DataType::Utf8, false),
                Field::new("scope_name", DataType::Utf8, false),
                Field::new("scope_version", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["http.server.request.duration"])),
                    Arc::new(StringArray::from(vec!["Duration of HTTP server requests"])),
                    Arc::new(StringArray::from(vec!["ms"])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_700_000_000_000_000_000_i64,
                    ])),
                    Arc::new(Float64Array::from(vec![42.5_f64])),
                    Arc::new(StringArray::from(vec![r#"{"http.method":"GET"}"#])),
                    Arc::new(StringArray::from(vec!["auth-service"])),
                    Arc::new(StringArray::from(vec![r#"{"host.name":"node-1"}"#])),
                    Arc::new(StringArray::from(vec!["my.scope"])),
                    Arc::new(StringArray::from(vec!["1.0.0"])),
                ],
            )?;
            Ok(batch)
        }
        2 => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("trace_id", DataType::Utf8, false),
                Field::new("span_id", DataType::Utf8, false),
                Field::new("trace_state", DataType::Utf8, false),
                Field::new("parent_span_id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("kind", DataType::Int32, false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new(
                    "end_time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("attributes", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, false),
                Field::new("resource_attributes", DataType::Utf8, false),
                Field::new("scope_name", DataType::Utf8, false),
                Field::new("scope_version", DataType::Utf8, false),
                Field::new("status_code", DataType::Int32, false),
                Field::new("status_message", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["4bf92f3577b34da6a3ce929d0e0e4736"])),
                    Arc::new(StringArray::from(vec!["00f067aa0ba902b7"])),
                    Arc::new(StringArray::from(vec!["congo=t61rcWkgMzE"])),
                    Arc::new(StringArray::from(vec!["0000000000000000"])),
                    Arc::new(StringArray::from(vec!["HTTP GET /api/v1/resource"])),
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_700_000_000_000_000_000_i64,
                    ])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_700_000_000_050_000_000_i64,
                    ])),
                    Arc::new(StringArray::from(vec![
                        r#"{"http.route":"/api/v1/resource"}"#,
                    ])),
                    Arc::new(StringArray::from(vec!["auth-service"])),
                    Arc::new(StringArray::from(vec![r#"{"host.name":"node-1"}"#])),
                    Arc::new(StringArray::from(vec!["my.scope"])),
                    Arc::new(StringArray::from(vec!["1.0.0"])),
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["OK"])),
                ],
            )?;
            Ok(batch)
        }
        _ => {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new(
                    "observed_timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("severity_number", DataType::Int32, false),
                Field::new("severity_text", DataType::Utf8, false),
                Field::new("body", DataType::Utf8, false),
                Field::new("trace_id", DataType::Utf8, false),
                Field::new("span_id", DataType::Utf8, false),
                Field::new("flags", DataType::UInt32, false),
                Field::new("attributes", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, false),
                Field::new("resource_attributes", DataType::Utf8, false),
                Field::new("scope_name", DataType::Utf8, false),
                Field::new("scope_version", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_700_000_000_000_000_000_i64,
                    ])),
                    Arc::new(TimestampNanosecondArray::from(vec![
                        1_700_000_000_100_000_000_i64,
                    ])),
                    Arc::new(Int32Array::from(vec![9])),
                    Arc::new(StringArray::from(vec!["INFO"])),
                    Arc::new(StringArray::from(vec![
                        "HTTP request processed successfully",
                    ])),
                    Arc::new(StringArray::from(vec!["4bf92f3577b34da6a3ce929d0e0e4736"])),
                    Arc::new(StringArray::from(vec!["00f067aa0ba902b7"])),
                    Arc::new(UInt32Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![r#"{"http.status_code":200}"#])),
                    Arc::new(StringArray::from(vec!["auth-service"])),
                    Arc::new(StringArray::from(vec![r#"{"host.name":"node-1"}"#])),
                    Arc::new(StringArray::from(vec!["my.scope"])),
                    Arc::new(StringArray::from(vec!["1.0.0"])),
                ],
            )?;
            Ok(batch)
        }
    }
}

/// Builds a canonical test Arrow [`RecordBatch`] for default logs signal.
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`).
///
/// # Errors
///
/// Returns an error if the record batch cannot be created.
pub fn build_canonical_test_batch() -> Result<RecordBatch> {
    build_canonical_test_batch_for_signal(0)
}

/// Serializes an Arrow [`RecordBatch`] to IPC stream bytes.
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`).
///
/// # Errors
///
/// Returns an error if the IPC writer fails.
pub fn serialize_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

fn wasm_err<E: std::fmt::Display>(err: E) -> anyhow::Error {
    anyhow::anyhow!("{err}")
}

/// Executes the full immutability and conformance test suite against guest WASM bytecode
/// with an explicit signal type and optional initialization configuration payload.
///
/// # Concurrency Characteristics
///
/// This function creates an isolated Wasmtime [`Engine`] and [`Store`], executing entirely
/// within the calling thread. Multiple threads can call this function concurrently with
/// independent guest modules.
///
/// # Errors
///
/// Returns an error if the module fails validation, instantiation, initialization,
/// execution, or immutability checks.
pub fn run_immutability_suite_with_options(
    bytes: &[u8],
    signal: u32,
    config_payload: Option<&str>,
) -> Result<()> {
    validate_wasm_bytes(bytes)?;

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config).map_err(wasm_err)?;
    let module = Module::new(&engine, bytes).map_err(wasm_err)?;
    let mut store: Store<()> = Store::new(&engine, ());
    store.set_fuel(10_000_000_000).map_err(wasm_err)?;

    let linker = crate::create_default_linker(&engine, &module).map_err(wasm_err)?;
    let instance = linker.instantiate(&mut store, &module).map_err(wasm_err)?;

    let alloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
        .map_err(wasm_err)?;
    let dealloc_fn = instance
        .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
        .map_err(wasm_err)?;
    let transform_fn = instance
        .get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        .map_err(wasm_err)?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("Module missing memory export"))?;

    if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_init") {
        let (config_ptr, config_len) = if let Some(cfg) = config_payload {
            let cfg_bytes = cfg.as_bytes();
            let len = u32::try_from(cfg_bytes.len())?;
            if len > 0 {
                let ptr = alloc_fn.call(&mut store, len).map_err(wasm_err)?;
                if ptr == 0 {
                    anyhow::bail!(
                        "datalake_alloc returned null pointer when allocating config buffer of length {len}"
                    );
                }
                let mem_len = memory.data(&store).len();
                let start = ptr as usize;
                let end = start
                    .checked_add(len as usize)
                    .ok_or_else(|| anyhow::anyhow!("Overflow computing config buffer end"))?;
                if end > mem_len {
                    anyhow::bail!("Allocated config buffer out of guest memory bounds");
                }
                memory.data_mut(&mut store)[start..end].copy_from_slice(cfg_bytes);
                (ptr, len)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        let init_res = init_fn
            .call(&mut store, (config_ptr, config_len))
            .map_err(wasm_err)?;

        if config_ptr != 0 && config_len > 0 {
            dealloc_fn
                .call(&mut store, (config_ptr, config_len))
                .map_err(wasm_err)?;
        }

        if init_res != 0 {
            anyhow::bail!("datalake_init returned non-zero code: {init_res}");
        }
    }

    let input_batch = build_canonical_test_batch_for_signal(signal)?;
    let buffer = serialize_batch_to_ipc(&input_batch)?;

    let input_len = u32::try_from(buffer.len())?;
    let input_ptr = alloc_fn.call(&mut store, input_len).map_err(wasm_err)?;
    if input_ptr == 0 && input_len > 0 {
        anyhow::bail!(
            "datalake_alloc returned null pointer when allocating input buffer of length {input_len}"
        );
    }

    let mem_len = memory.data(&store).len();
    let start = input_ptr as usize;
    let end = start
        .checked_add(input_len as usize)
        .ok_or_else(|| anyhow::anyhow!("Overflow computing buffer end"))?;
    if end > mem_len {
        anyhow::bail!("Allocated input buffer out of guest memory bounds");
    }
    memory.data_mut(&mut store)[start..end].copy_from_slice(&buffer);

    let packed = transform_fn
        .call(&mut store, (signal, input_ptr, input_len))
        .map_err(wasm_err)?;
    let header_ptr = (packed >> 32) as u32;
    let header_len = (packed & 0xffff_ffff) as u32;

    verify_transform_response(&memory, &store, header_ptr, header_len, &input_batch)?;

    reclaim_transform_response(&memory, &mut store, &dealloc_fn, header_ptr, header_len)?;

    dealloc_fn
        .call(&mut store, (input_ptr, input_len))
        .map_err(wasm_err)?;
    println!("✓ Immutability conformance suite passed");
    Ok(())
}

/// Executes the full immutability and conformance test suite against guest WASM bytecode
/// using default signal (`Logs` / `0`) and no configuration.
///
/// # Concurrency Characteristics
///
/// This function creates an isolated Wasmtime [`Engine`] and [`Store`], executing entirely
/// within the calling thread. Multiple threads can call this function concurrently with
/// independent guest modules.
///
/// # Errors
///
/// Returns an error if the module fails validation, instantiation, execution, or immutability checks.
pub fn run_immutability_suite(bytes: &[u8]) -> Result<()> {
    run_immutability_suite_with_options(bytes, 0, None)
}

/// Reads and verifies the response status in the [`opentelemetry_datalake_wasm_sdk::abi::TransformResponseHeader`].
///
/// # Concurrency Characteristics
///
/// This function is thread-safe (`Send + Sync`) and operates on caller-provided references.
///
/// # Errors
///
/// Returns an error if the header pointer is null or out of memory bounds, if the header length
/// is not exactly 20 bytes as required by ABI v1, or if the guest reported an error or rejection status.
pub fn verify_transform_status(
    memory: &wasmtime::Memory,
    store: &Store<()>,
    header_ptr: u32,
    header_len: u32,
) -> Result<()> {
    if header_ptr == 0 {
        anyhow::bail!("datalake_transform returned a null response header pointer");
    }
    if header_len != 20 {
        anyhow::bail!(
            "datalake_transform returned invalid header length: expected exactly 20 bytes, got {header_len}"
        );
    }

    let h_start = header_ptr as usize;
    let h_end = h_start
        .checked_add(header_len as usize)
        .ok_or_else(|| anyhow::anyhow!("Overflow in header range"))?;
    let mem = memory.data(store);
    if h_end > mem.len() {
        anyhow::bail!("Header pointer out of memory bounds");
    }

    let status = u32::from_le_bytes(
        mem[h_start..h_start + 4]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read status: {e}"))?,
    );
    let batch_count = u32::from_le_bytes(
        mem[h_start + 4..h_start + 8]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read batch_count: {e}"))?,
    );
    let batches_ptr = u32::from_le_bytes(
        mem[h_start + 8..h_start + 12]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read batches_ptr: {e}"))?,
    );
    let message_ptr = u32::from_le_bytes(
        mem[h_start + 12..h_start + 16]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read message_ptr: {e}"))?,
    );
    let message_len = u32::from_le_bytes(
        mem[h_start + 16..h_start + 20]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read message_len: {e}"))?,
    );

    if status == STATUS_DISCARD {
        if batch_count != 0 || batches_ptr != 0 || message_ptr != 0 || message_len != 0 {
            anyhow::bail!(
                "Discard response header must have zero batches and no message payload: batch_count={batch_count}, batches_ptr={batches_ptr}, message_ptr={message_ptr}, message_len={message_len}"
            );
        }
        return Ok(());
    }

    if status == STATUS_SUCCESS {
        if (batch_count == 0 && batches_ptr != 0) || (batch_count > 0 && batches_ptr == 0) {
            anyhow::bail!(
                "Success response header has inconsistent batch fields: batch_count={batch_count}, batches_ptr={batches_ptr}"
            );
        }
        if message_ptr != 0 || message_len != 0 {
            anyhow::bail!(
                "Success response header must have no message payload: message_ptr={message_ptr}, message_len={message_len}"
            );
        }
        return Ok(());
    }

    let msg = if message_len > 0 {
        let m_start = message_ptr as usize;
        let m_end = m_start
            .checked_add(message_len as usize)
            .ok_or_else(|| anyhow::anyhow!("Overflow in message bounds"))?;
        if m_end <= mem.len() {
            String::from_utf8_lossy(&mem[m_start..m_end]).to_string()
        } else {
            "out-of-bounds error message".to_string()
        }
    } else {
        format!("transform failed with status {status}")
    };
    anyhow::bail!("Guest transform failed: {msg}");
}

/// Extracts and deserializes all Arrow [`RecordBatch`]es from a guest transform response.
///
/// If the response status is [`STATUS_DISCARD`], an empty vector is returned.
///
/// # Concurrency Characteristics
///
/// This function is thread-safe (`Send + Sync`) and operates on caller-provided references.
///
/// # Errors
///
/// Returns an error if the status indicates guest failure, if descriptor or batch pointers
/// are out of memory bounds, or if the Arrow IPC streams cannot be deserialized.
pub fn extract_transform_payloads(
    memory: &wasmtime::Memory,
    store: &Store<()>,
    header_ptr: u32,
    header_len: u32,
) -> Result<Vec<RecordBatch>> {
    verify_transform_status(memory, store, header_ptr, header_len)?;

    let h_start = header_ptr as usize;
    let mem = memory.data(store);

    let status = u32::from_le_bytes(
        mem[h_start..h_start + 4]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read status: {e}"))?,
    );
    if status == STATUS_DISCARD {
        return Ok(Vec::new());
    }

    let batch_count = u32::from_le_bytes(
        mem[h_start + 4..h_start + 8]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read batch_count: {e}"))?,
    );
    let batches_ptr = u32::from_le_bytes(
        mem[h_start + 8..h_start + 12]
            .try_into()
            .map_err(|e| anyhow::anyhow!("Failed to read batches_ptr: {e}"))?,
    );

    let mut batches = Vec::new();
    for i in 0..batch_count {
        let desc_offset = (batches_ptr as usize)
            .checked_add(
                (i as usize)
                    .checked_mul(8)
                    .ok_or_else(|| anyhow::anyhow!("Overflow in descriptor offset"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("Overflow in descriptor offset"))?;
        let desc_end = desc_offset
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("Overflow in descriptor end"))?;
        let mem = memory.data(store);
        if desc_end > mem.len() {
            anyhow::bail!("BatchDescriptor offset out of guest memory bounds");
        }
        let b_ptr = u32::from_le_bytes(
            mem[desc_offset..desc_offset + 4]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read descriptor ptr: {e}"))?,
        ) as usize;
        let b_len = u32::from_le_bytes(
            mem[desc_offset + 4..desc_offset + 8]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read descriptor len: {e}"))?,
        ) as usize;
        let b_end = b_ptr
            .checked_add(b_len)
            .ok_or_else(|| anyhow::anyhow!("Overflow in batch buffer end"))?;
        if b_end > mem.len() {
            anyhow::bail!("Batch buffer out of guest memory bounds");
        }
        let batch_bytes = &mem[b_ptr..b_end];
        let cursor = Cursor::new(batch_bytes);
        let reader = StreamReader::try_new(cursor, None)?;
        for out_batch_res in reader {
            let out_batch = out_batch_res?;
            batches.push(out_batch);
        }
    }
    Ok(batches)
}

fn verify_transform_response(
    memory: &wasmtime::Memory,
    store: &Store<()>,
    header_ptr: u32,
    header_len: u32,
    input_batch: &RecordBatch,
) -> Result<()> {
    let output_batches = extract_transform_payloads(memory, store, header_ptr, header_len)?;
    let mut used_inputs = vec![false; input_batch.num_rows()];
    for out_batch in &output_batches {
        verify_batch_immutability_with_used(input_batch, out_batch, &mut used_inputs)?;
    }
    Ok(())
}

/// Reclaims guest-allocated response memory (header, descriptor array, IPC output buffers, error message).
///
/// # Concurrency Characteristics
///
/// Requires exclusive mutable access to [`Store<()>`], ensuring single-threaded synchronization
/// during memory reclamation.
///
/// # Errors
///
/// Returns an error if memory offsets are invalid or if guest dealloc traps.
pub fn reclaim_transform_response(
    memory: &wasmtime::Memory,
    store: &mut Store<()>,
    dealloc_fn: &wasmtime::TypedFunc<(u32, u32), ()>,
    header_ptr: u32,
    header_len: u32,
) -> Result<()> {
    if header_ptr == 0 {
        return Ok(());
    }

    let h_start = header_ptr as usize;
    let h_end = h_start
        .checked_add(header_len as usize)
        .ok_or_else(|| anyhow::anyhow!("Overflow in header range"))?;
    if header_len < 20 {
        anyhow::bail!("Header length {header_len} is less than required 20 bytes for reclamation");
    }

    let (batch_count, batches_ptr, message_ptr, message_len) = {
        let mem = memory.data(&*store);
        if h_end > mem.len() {
            anyhow::bail!("Header out of memory bounds during response reclamation");
        }

        let batch_count = u32::from_le_bytes(
            mem[h_start + 4..h_start + 8]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read batch_count: {e}"))?,
        );
        let batches_ptr = u32::from_le_bytes(
            mem[h_start + 8..h_start + 12]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read batches_ptr: {e}"))?,
        );
        let message_ptr = u32::from_le_bytes(
            mem[h_start + 12..h_start + 16]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read message_ptr: {e}"))?,
        );
        let message_len = u32::from_le_bytes(
            mem[h_start + 16..h_start + 20]
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to read message_len: {e}"))?,
        );
        (batch_count, batches_ptr, message_ptr, message_len)
    };

    // Free message string if present
    if message_ptr != 0 && message_len > 0 {
        dealloc_fn
            .call(&mut *store, (message_ptr, message_len))
            .map_err(wasm_err)?;
    }

    // Free batch buffers and descriptor array
    if batches_ptr != 0 && batch_count > 0 {
        let desc_size = 8_u32;
        let total_desc_bytes = batch_count
            .checked_mul(desc_size)
            .ok_or_else(|| anyhow::anyhow!("Overflow in descriptor array size"))?;
        let desc_start = batches_ptr as usize;
        let desc_end = desc_start
            .checked_add(total_desc_bytes as usize)
            .ok_or_else(|| anyhow::anyhow!("Overflow in descriptor range"))?;

        let batch_buffers = {
            let mem = memory.data(&*store);
            if desc_end > mem.len() {
                anyhow::bail!("Descriptor array out of bounds during response reclamation");
            }
            let mut bufs = Vec::with_capacity(batch_count as usize);
            for i in 0..batch_count {
                let offset = desc_start + (i as usize * 8);
                let b_ptr = u32::from_le_bytes(
                    mem[offset..offset + 4]
                        .try_into()
                        .map_err(|e| anyhow::anyhow!("Failed to read b_ptr: {e}"))?,
                );
                let b_len = u32::from_le_bytes(
                    mem[offset + 4..offset + 8]
                        .try_into()
                        .map_err(|e| anyhow::anyhow!("Failed to read b_len: {e}"))?,
                );
                if b_ptr != 0 && b_len > 0 {
                    bufs.push((b_ptr, b_len));
                }
            }
            bufs
        };

        for (b_ptr, b_len) in batch_buffers {
            dealloc_fn
                .call(&mut *store, (b_ptr, b_len))
                .map_err(wasm_err)?;
        }
        dealloc_fn
            .call(&mut *store, (batches_ptr, total_desc_bytes))
            .map_err(wasm_err)?;
    }

    // Free header
    if header_len > 0 {
        dealloc_fn
            .call(&mut *store, (header_ptr, header_len))
            .map_err(wasm_err)?;
    }

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
