//! Host worker execution loop for WebAssembly transformation modules.
//!
//! Provides the [`WasmWorker`] execution engine which manages an isolated Wasmtime
//! instance, serializes Arrow batches to Arrow IPC streams, invokes guest transforms
//! over the C-ABI v1 boundary, and enforces soft rejuvenation hygiene.

use crate::engine::EngineCache;
use crate::error::WasmTransformError;
use crate::guard::{backfill_missing_columns, verify_structural_immutability};
use crate::host_calls::{HostPhase, HostState, MetricRegistry, build_host_linker};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pipeline_core::config::{SchemaGuardMode, WasmTransformerConfig};
use pipeline_core::pipeline::SignalBatch;
use std::sync::Arc;
use wasmtime::{Instance, Memory, Module, Store, TypedFunc};

/// Size of the C-ABI `TransformResponseHeader` in bytes.
pub const RESPONSE_HEADER_SIZE: usize = 20;

/// The outcome of processing a batch of telemetry records through the WASM worker.
#[derive(Debug)]
pub enum WorkerOutcome {
    /// Processing succeeded and produced transformed batches (or passed through).
    Emitted(Vec<SignalBatch>),
    /// Guest transformer explicitly discarded the batch.
    Discarded,
    /// Guest transformer rejected the batch with a specified reason.
    Rejected {
        /// Reason explaining why the batch was rejected.
        reason: String,
        /// Original input batch retained for DLQ routing.
        original: SignalBatch,
    },
    /// Guest execution failed or returned an error status code.
    Errored {
        /// Reason explaining the execution error.
        reason: String,
        /// Original input batch retained for DLQ routing.
        original: SignalBatch,
    },
}

/// Typed handle to the guest's `datalake_transform` export.
///
/// Supports both the canonical C-ABI v1 3-argument signature
/// `(signal_type: u32, ipc_ptr: u32, ipc_len: u32) -> u64`
/// and the legacy 2-argument signature `(ipc_ptr: u32, ipc_len: u32) -> u32`.
#[derive(Clone)]
pub enum TransformFunc {
    /// Canonical C-ABI v1: `datalake_transform(signal_type, ipc_ptr, ipc_len) -> u64`.
    V1(TypedFunc<(u32, u32, u32), u64>),
    /// 2-argument variant: `datalake_transform(ipc_ptr, ipc_len) -> u32`.
    V0(TypedFunc<(u32, u32), u32>),
}

impl std::fmt::Debug for TransformFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1(_) => write!(f, "TransformFunc::V1"),
            Self::V0(_) => write!(f, "TransformFunc::V0"),
        }
    }
}

/// Internal container for guest Wasmtime execution state and exported entry points.
struct GuestComponents {
    store: Store<HostState>,
    instance: Instance,
    alloc_fn: TypedFunc<u32, u32>,
    dealloc_fn: TypedFunc<(u32, u32), ()>,
    transform_fn: TransformFunc,
    memory: Memory,
}

/// Decoded response header fields from `TransformResponseHeader` in guest memory.
struct ParsedHeader {
    status: u32,
    batch_count: u32,
    batches_ptr: u32,
    message_ptr: u32,
    message_len: u32,
}

/// WebAssembly transformation worker executing Whole-Batch transformations.
///
/// Encapsulates a Wasmtime [`Store`], [`Instance`], exported C-ABI v1 entry points,
/// and instance-local memory. Automatically rejuvenates the guest instance upon
/// reaching configured batch count or memory limits.
pub struct WasmWorker {
    /// Worker identifier within the worker pool.
    pub id: usize,
    engine: Arc<EngineCache>,
    module: Arc<Module>,
    config: WasmTransformerConfig,
    filtered_env: std::collections::HashMap<String, String>,
    registry: Arc<MetricRegistry>,
    guest: Option<GuestComponents>,
    batches_processed: u64,
    local_generation: u64,
    rejuvenate_threshold_bytes: usize,
    epoch_deadline_ticks: u64,
}

impl std::fmt::Debug for WasmWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmWorker")
            .field("id", &self.id)
            .field("batches_processed", &self.batches_processed)
            .field("local_generation", &self.local_generation)
            .field(
                "rejuvenate_threshold_bytes",
                &self.rejuvenate_threshold_bytes,
            )
            .finish_non_exhaustive()
    }
}

impl WasmWorker {
    /// Creates a new `WasmWorker` with its own isolated Wasmtime store and instance.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if instance creation fails, required
    /// C-ABI v1 exports (`datalake_alloc`, `datalake_dealloc`, `datalake_transform`,
    /// `memory`) are missing, guest initialization fails, or `rejuvenate_threshold` is invalid.
    pub fn new(
        id: usize,
        engine: Arc<EngineCache>,
        module: Arc<Module>,
        config: WasmTransformerConfig,
    ) -> Result<Self, WasmTransformError> {
        let trimmed_threshold = config.rejuvenate_threshold.trim();
        let rejuvenate_threshold_bytes = if trimmed_threshold.is_empty() {
            0
        } else {
            parse_byte_size(trimmed_threshold).ok_or_else(|| {
                WasmTransformError::Pipeline(format!(
                    "Invalid memory threshold: {}",
                    config.rejuvenate_threshold
                ))
            })?
        };

        let init_duration_ms = parse_duration_ms(&config.init_timeout).unwrap_or(2000);
        let init_deadline_ticks = init_duration_ms.max(10) / 10;

        let filtered_env =
            crate::wasi_env::filter_environment_variables(&config.env_whitelist, &config.env);
        let registry = Arc::new(MetricRegistry::new(&config.id));
        let mut guest = Self::instantiate_guest(
            engine.engine(),
            &module,
            init_deadline_ticks,
            Arc::clone(&registry),
            &filtered_env,
        )?;
        Self::initialize_guest(&mut guest, &config)?;
        let local_generation = engine.module_generation();

        let duration_ms = parse_duration_ms(&config.max_execution_duration).unwrap_or(500);
        let epoch_deadline_ticks = duration_ms.max(10) / 10;

        Ok(Self {
            id,
            engine,
            module,
            config,
            filtered_env,
            registry,
            guest: Some(guest),
            batches_processed: 0,
            local_generation,
            rejuvenate_threshold_bytes,
            epoch_deadline_ticks,
        })
    }

    /// Returns a reference to the worker's metric registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<MetricRegistry> {
        &self.registry
    }

    /// Executes a transformation over a [`SignalBatch`].
    ///
    /// The incoming batch is serialized into an Arrow IPC stream, transferred into guest
    /// memory, and processed by calling `datalake_transform`. The returned response header
    /// is decoded to produce the corresponding [`WorkerOutcome`].
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if IPC serialization fails, guest execution traps,
    /// or guest memory bounds are violated.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn execute_batch(
        &mut self,
        batch: SignalBatch,
    ) -> Result<WorkerOutcome, WasmTransformError> {
        self.check_hot_reload()?;

        let mut guest = self.guest.take().ok_or_else(|| {
            WasmTransformError::Pipeline("WASM worker guest components missing".into())
        })?;

        let epoch_deadline_ticks = self.epoch_deadline_ticks;
        let schema_guard = self.config.schema_guard;
        let module_path = self.config.module_path.clone();
        let worker_id = self.id;

        let run_transform =
            move || -> (GuestComponents, Result<WorkerOutcome, WasmTransformError>) {
                let res = execute_batch_in_guest(
                    &mut guest,
                    batch,
                    epoch_deadline_ticks,
                    schema_guard,
                    &module_path,
                    worker_id,
                );
                (guest, res)
            };

        let (guest, outcome) = if tokio::runtime::Handle::try_current().is_ok() {
            let handle = tokio::task::spawn_blocking(run_transform);
            handle.await.map_err(|e| {
                WasmTransformError::Pipeline(format!("Blocking task join error: {e}"))
            })?
        } else {
            run_transform()
        };

        self.guest = Some(guest);

        match outcome {
            Ok(outcome) => {
                self.batches_processed = self.batches_processed.saturating_add(1);
                self.check_rejuvenation()?;
                Ok(outcome)
            }
            Err(e) => {
                let _ = self.rejuvenate();
                Err(e)
            }
        }
    }

    /// Rejuvenates the worker by discarding its store and creating a fresh instance.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if re-instantiation fails, required exports are missing,
    /// or guest initialization fails.
    pub fn rejuvenate(&mut self) -> Result<(), WasmTransformError> {
        let mut guest = Self::instantiate_guest(
            self.engine.engine(),
            &self.module,
            self.epoch_deadline_ticks,
            Arc::clone(&self.registry),
            &self.filtered_env,
        )?;
        Self::initialize_guest(&mut guest, &self.config)?;
        self.guest = Some(guest);
        self.batches_processed = 0;
        Ok(())
    }

    /// Returns the number of batches processed by this instance since its last rejuvenation.
    #[must_use]
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed
    }

    /// Returns the local module generation counter observed by this worker.
    #[must_use]
    pub fn local_generation(&self) -> u64 {
        self.local_generation
    }

    /// Returns the parsed memory rejuvenation threshold in bytes.
    #[must_use]
    pub fn rejuvenate_threshold_bytes(&self) -> usize {
        self.rejuvenate_threshold_bytes
    }

    /// Returns an optional reference to the active Wasmtime [`Instance`].
    #[must_use]
    pub fn instance(&self) -> Option<&Instance> {
        self.guest.as_ref().map(|g| &g.instance)
    }

    /// Returns a reference to the underlying [`Module`].
    #[must_use]
    pub fn module(&self) -> &Arc<Module> {
        &self.module
    }

    /// Returns a reference to the [`WasmTransformerConfig`].
    #[must_use]
    pub fn config(&self) -> &WasmTransformerConfig {
        &self.config
    }

    /// Returns a reference to the zero-trust filtered environment variables for this worker.
    #[must_use]
    pub fn filtered_env(&self) -> &std::collections::HashMap<String, String> {
        &self.filtered_env
    }

    /// Checks if the engine cache has compiled a newer module generation and reloads.
    fn check_hot_reload(&mut self) -> Result<(), WasmTransformError> {
        let (current_module, current_gen) = self.engine.current_module();
        if self.local_generation != current_gen
            && let Some(new_mod) = current_module
        {
            self.module = new_mod;
            self.rejuvenate()?;
            self.local_generation = current_gen;
        }
        Ok(())
    }

    /// Rejuvenates the guest instance if batch count or memory limits are exceeded.
    fn check_rejuvenation(&mut self) -> Result<(), WasmTransformError> {
        let memory_exceeded = if self.rejuvenate_threshold_bytes > 0 {
            if let Some(guest) = &self.guest {
                guest.memory.data_size(&guest.store) >= self.rejuvenate_threshold_bytes
            } else {
                false
            }
        } else {
            false
        };

        if (self.config.rejuvenate_batches > 0
            && self.batches_processed >= self.config.rejuvenate_batches)
            || memory_exceeded
        {
            self.rejuvenate()?;
        }
        Ok(())
    }

    /// Helper to instantiate a guest module and extract required ABI exports.
    fn instantiate_guest(
        engine: &wasmtime::Engine,
        module: &Module,
        init_deadline_ticks: u64,
        registry: Arc<MetricRegistry>,
        env: &std::collections::HashMap<String, String>,
    ) -> Result<GuestComponents, WasmTransformError> {
        let mut wasi_builder = wasmtime_wasi::WasiCtxBuilder::new();
        let env_pairs: Vec<(&str, &str)> =
            env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        wasi_builder.envs(&env_pairs);
        let wasi = wasi_builder.build_p1();

        let mut store = Store::new(engine, HostState::new(HostPhase::Init, registry, wasi));
        store.set_epoch_deadline(init_deadline_ticks);
        let linker = build_host_linker(engine)?;
        let instance = linker.instantiate(&mut store, module)?;

        let abi_fn = instance
            .get_typed_func::<(), u32>(&mut store, "datalake_abi_version")
            .map_err(|_| WasmTransformError::MissingExport("datalake_abi_version".into()))?;

        let version = abi_fn.call(&mut store, ())?;
        if version != opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION {
            return Err(WasmTransformError::AbiVersionMismatch(version));
        }

        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
            .map_err(|_| WasmTransformError::MissingExport("datalake_alloc".into()))?;
        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
            .map_err(|_| WasmTransformError::MissingExport("datalake_dealloc".into()))?;
        let transform_fn = if let Ok(func) =
            instance.get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        {
            TransformFunc::V1(func)
        } else if let Ok(func) =
            instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")
        {
            TransformFunc::V0(func)
        } else {
            return Err(WasmTransformError::MissingExport(
                "datalake_transform".into(),
            ));
        };
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmTransformError::MissingExport("memory".into()))?;

        Ok(GuestComponents {
            store,
            instance,
            alloc_fn,
            dealloc_fn,
            transform_fn,
            memory,
        })
    }

    /// Invokes the optional `datalake_init` lifecycle hook exported by the guest.
    fn initialize_guest(
        guest: &mut GuestComponents,
        config: &WasmTransformerConfig,
    ) -> Result<(), WasmTransformError> {
        if let Ok(init_fn) = guest
            .instance
            .get_typed_func::<(u32, u32), i32>(&mut guest.store, "datalake_init")
        {
            let (conf_ptr, conf_len) = if let Some(ref conf_val) = config.config {
                let serialized_config = serde_json::to_string(conf_val).map_err(|e| {
                    WasmTransformError::Pipeline(format!("Failed to serialize config JSON: {e}"))
                })?;
                let conf_bytes = serialized_config.as_bytes();
                let conf_len = u32::try_from(conf_bytes.len()).map_err(|_| {
                    WasmTransformError::Pipeline("Config JSON exceeds u32::MAX".to_string())
                })?;
                let conf_ptr = match guest.alloc_fn.call(&mut guest.store, conf_len) {
                    Ok(ptr) => ptr,
                    Err(e) => return Err(WasmTransformError::InitFailed(e.to_string())),
                };
                if conf_ptr == 0 && conf_len > 0 {
                    return Err(WasmTransformError::InitFailed(
                        "Guest allocation failed for config payload".to_string(),
                    ));
                }
                if let Err(e) = guest
                    .memory
                    .write(&mut guest.store, conf_ptr as usize, conf_bytes)
                {
                    let _ = guest
                        .dealloc_fn
                        .call(&mut guest.store, (conf_ptr, conf_len));
                    return Err(WasmTransformError::InitFailed(e.to_string()));
                }
                (conf_ptr, conf_len)
            } else {
                (0, 0)
            };

            let init_duration_ms = parse_duration_ms(&config.init_timeout).unwrap_or(2000);
            let init_deadline_ticks = init_duration_ms.max(10) / 10;
            guest.store.set_epoch_deadline(init_deadline_ticks);

            let init_res = init_fn.call(&mut guest.store, (conf_ptr, conf_len));
            if conf_ptr > 0 && conf_len > 0 {
                let _ = guest
                    .dealloc_fn
                    .call(&mut guest.store, (conf_ptr, conf_len));
            }

            match init_res {
                Ok(0) => {}
                Ok(code) => {
                    return Err(WasmTransformError::InitFailed(format!(
                        "Guest init returned error code {code}"
                    )));
                }
                Err(e) => return Err(WasmTransformError::InitFailed(e.to_string())),
            }
        }
        Ok(())
    }
}

impl GuestComponents {
    /// Reads and parses the 20-byte `TransformResponseHeader` from guest memory.
    fn read_response_header(&self, header_ptr: u32) -> Result<ParsedHeader, WasmTransformError> {
        let mut header_bytes = [0u8; 20];
        self.memory
            .read(&self.store, header_ptr as usize, &mut header_bytes)
            .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

        let status = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);
        let batch_count = u32::from_le_bytes([
            header_bytes[4],
            header_bytes[5],
            header_bytes[6],
            header_bytes[7],
        ]);
        let batches_ptr = u32::from_le_bytes([
            header_bytes[8],
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
        ]);
        let message_ptr = u32::from_le_bytes([
            header_bytes[12],
            header_bytes[13],
            header_bytes[14],
            header_bytes[15],
        ]);
        let message_len = u32::from_le_bytes([
            header_bytes[16],
            header_bytes[17],
            header_bytes[18],
            header_bytes[19],
        ]);

        Ok(ParsedHeader {
            status,
            batch_count,
            batches_ptr,
            message_ptr,
            message_len,
        })
    }

    /// Dispatches the response status code into a [`WorkerOutcome`].
    fn dispatch_outcome(
        &self,
        header: &ParsedHeader,
        message: &str,
        batch: SignalBatch,
        schema_guard: pipeline_core::config::SchemaGuardMode,
        allocs_to_free: &mut Vec<(u32, u32)>,
    ) -> Result<WorkerOutcome, WasmTransformError> {
        match header.status {
            0 => {
                if header.batch_count == 0 {
                    Ok(WorkerOutcome::Emitted(vec![]))
                } else if header.batches_ptr == 0 {
                    Err(WasmTransformError::Pipeline(
                        "Malformed response: positive batch_count with null descriptor pointer"
                            .into(),
                    ))
                } else {
                    let out_batches = extract_output_batches(
                        &self.memory,
                        &self.store,
                        header.batches_ptr,
                        header.batch_count,
                        &batch,
                        schema_guard,
                        allocs_to_free,
                    )?;
                    Ok(WorkerOutcome::Emitted(out_batches))
                }
            }
            1 => Ok(WorkerOutcome::Discarded),
            2 => {
                let reason = if message.is_empty() {
                    "Guest rejected batch".to_string()
                } else {
                    message.to_string()
                };
                Ok(WorkerOutcome::Rejected {
                    reason,
                    original: batch,
                })
            }
            _ => {
                let reason = if message.is_empty() {
                    format!("Guest returned error status {}", header.status)
                } else {
                    message.to_string()
                };
                Ok(WorkerOutcome::Errored {
                    reason,
                    original: batch,
                })
            }
        }
    }
}

fn execute_batch_in_guest(
    guest: &mut GuestComponents,
    batch: SignalBatch,
    epoch_deadline_ticks: u64,
    schema_guard: pipeline_core::config::SchemaGuardMode,
    module_path: &str,
    worker_id: usize,
) -> Result<WorkerOutcome, WasmTransformError> {
    guest.store.set_epoch_deadline(epoch_deadline_ticks);

    let record_batch = match &batch {
        SignalBatch::Logs(rb) | SignalBatch::Metrics(rb) | SignalBatch::Traces(rb) => rb,
    };

    // 1. Serialize input RecordBatch to Arrow IPC Stream
    let ipc_buf = serialize_batch_to_ipc(record_batch)?;
    let ipc_len = u32::try_from(ipc_buf.len())
        .map_err(|_| WasmTransformError::Pipeline("IPC payload exceeds u32::MAX".to_string()))?;

    // 2. Allocate buffer in guest linear memory and copy payload
    let ipc_ptr = guest
        .alloc_fn
        .call(&mut guest.store, ipc_len)
        .map_err(WasmTransformError::Wasmtime)?;
    if ipc_ptr == 0 && ipc_len > 0 {
        return Err(WasmTransformError::Oom {
            module: module_path.to_string(),
            instance: worker_id,
        });
    }
    guest
        .memory
        .write(&mut guest.store, ipc_ptr as usize, &ipc_buf)
        .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

    // 3. Invoke datalake_transform and free input buffer
    let transform_res: Result<u32, WasmTransformError> = match &guest.transform_fn {
        TransformFunc::V1(func) => {
            let signal_type = match &batch {
                SignalBatch::Logs(_) => 0u32,
                SignalBatch::Metrics(_) => 1u32,
                SignalBatch::Traces(_) => 2u32,
            };
            match func.call(&mut guest.store, (signal_type, ipc_ptr, ipc_len)) {
                Ok(packed) => {
                    let ptr = (packed >> 32) as u32;
                    let len = (packed & 0xFFFF_FFFF) as u32;
                    if ptr == 0 || (len as usize) < RESPONSE_HEADER_SIZE {
                        Err(WasmTransformError::Pipeline(format!(
                            "Malformed C-ABI v1 response header: ptr={ptr}, len={len} (minimum required: {RESPONSE_HEADER_SIZE} bytes)"
                        )))
                    } else {
                        Ok(ptr)
                    }
                }
                Err(e) => Err(WasmTransformError::Wasmtime(e)),
            }
        }
        TransformFunc::V0(func) => func
            .call(&mut guest.store, (ipc_ptr, ipc_len))
            .map_err(WasmTransformError::Wasmtime),
    };
    if transform_res.is_ok() {
        guest
            .dealloc_fn
            .call(&mut guest.store, (ipc_ptr, ipc_len))
            .map_err(WasmTransformError::Wasmtime)?;
    }
    let header_ptr = transform_res?;

    // 4. Read TransformResponseHeader (20 bytes) safely without unwrap
    let header = guest.read_response_header(header_ptr)?;
    let message = read_guest_message(
        &guest.memory,
        &guest.store,
        header.message_ptr,
        header.message_len,
    );

    let mut allocs_to_free = vec![(header_ptr, 20)];
    if header.message_ptr > 0 && header.message_len > 0 {
        allocs_to_free.push((header.message_ptr, header.message_len));
    }
    if header.batch_count > 0 && header.batches_ptr > 0 {
        allocs_to_free.push((header.batches_ptr, header.batch_count.saturating_mul(8)));
    }

    // 5. Dispatch outcome and extract transformed batches while guest memory is intact
    let outcome =
        guest.dispatch_outcome(&header, &message, batch, schema_guard, &mut allocs_to_free)?;

    // 6. Free guest allocations on the happy path to prevent memory growth
    for (ptr, len) in allocs_to_free {
        guest
            .dealloc_fn
            .call(&mut guest.store, (ptr, len))
            .map_err(WasmTransformError::Wasmtime)?;
    }

    Ok(outcome)
}

/// Serializes an Arrow [`RecordBatch`] to an Arrow IPC stream buffer.
fn serialize_batch_to_ipc(record_batch: &RecordBatch) -> Result<Vec<u8>, WasmTransformError> {
    let mut ipc_buf = Vec::with_capacity(64 * 1024);
    let mut writer = StreamWriter::try_new(&mut ipc_buf, &record_batch.schema())
        .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
    writer
        .write(record_batch)
        .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
    Ok(ipc_buf)
}

const MAX_GUEST_MESSAGE_LEN: usize = 64 * 1024;

/// Reads an optional UTF-8 message string from guest memory.
fn read_guest_message<T>(
    memory: &Memory,
    store: &Store<T>,
    message_ptr: u32,
    message_len: u32,
) -> String {
    let m_ptr = message_ptr as usize;
    let m_len = message_len as usize;
    let mem_size = memory.data_size(store);

    if message_len > 0 && message_ptr > 0 && m_ptr.saturating_add(m_len) <= mem_size {
        let alloc_len = m_len.min(MAX_GUEST_MESSAGE_LEN);
        let mut msg_bytes = vec![0u8; alloc_len];
        if memory.read(store, m_ptr, &mut msg_bytes).is_ok() {
            String::from_utf8_lossy(&msg_bytes).into_owned()
        } else {
            String::new()
        }
    } else {
        String::new()
    }
}

const MAX_GUEST_BATCH_COUNT: u32 = 1024;

/// Extracts transformed output batches from guest memory via a `BatchDescriptor` array.
fn extract_output_batches<T>(
    memory: &Memory,
    store: &Store<T>,
    batches_ptr: u32,
    batch_count: u32,
    input_batch: &SignalBatch,
    schema_guard: SchemaGuardMode,
    allocs_to_free: &mut Vec<(u32, u32)>,
) -> Result<Vec<SignalBatch>, WasmTransformError> {
    if batch_count > MAX_GUEST_BATCH_COUNT {
        return Err(WasmTransformError::Pipeline(format!(
            "Guest batch count {batch_count} exceeds maximum allowed limit of {MAX_GUEST_BATCH_COUNT}"
        )));
    }

    let descriptor_size = 8usize;
    let mem_size = memory.data_size(store);

    if (batches_ptr as usize).saturating_add((batch_count as usize).saturating_mul(descriptor_size))
        > mem_size
    {
        return Err(WasmTransformError::Pipeline(format!(
            "Batch descriptors array bounds exceed guest memory size {mem_size}"
        )));
    }

    let input_rb = match input_batch {
        SignalBatch::Logs(rb) | SignalBatch::Metrics(rb) | SignalBatch::Traces(rb) => rb,
    };

    let mut out_batches = Vec::with_capacity((batch_count as usize).min(64));
    let mut total_bytes = 0usize;

    for i in 0..batch_count {
        let offset =
            (batches_ptr as usize).saturating_add((i as usize).saturating_mul(descriptor_size));
        let mut desc_bytes = [0u8; 8];
        memory
            .read(store, offset, &mut desc_bytes)
            .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;
        let b_ptr =
            u32::from_le_bytes([desc_bytes[0], desc_bytes[1], desc_bytes[2], desc_bytes[3]]);
        let b_len =
            u32::from_le_bytes([desc_bytes[4], desc_bytes[5], desc_bytes[6], desc_bytes[7]]);

        if b_ptr > 0 && b_len > 0 {
            allocs_to_free.push((b_ptr, b_len));
        }

        let b_len_usize = b_len as usize;
        total_bytes = total_bytes.saturating_add(b_len_usize);
        if total_bytes > 64 * 1024 * 1024 {
            return Err(WasmTransformError::Pipeline(
                "Cumulative batch output size exceeds maximum allowed 64MiB limit".into(),
            ));
        }

        if (b_ptr as usize).saturating_add(b_len_usize) > mem_size {
            return Err(WasmTransformError::Pipeline(format!(
                "Batch IPC buffer bounds exceed guest memory size {mem_size}"
            )));
        }

        let mut out_ipc_bytes = vec![0u8; b_len_usize];
        memory
            .read(store, b_ptr as usize, &mut out_ipc_bytes)
            .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

        let cursor = std::io::Cursor::new(out_ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
        for maybe_rb in reader {
            let mut rb = maybe_rb.map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
            verify_structural_immutability(input_rb, &rb)?;
            match schema_guard {
                SchemaGuardMode::Defensive => {
                    rb = backfill_missing_columns(input_rb.schema().as_ref(), rb)?;
                }
                SchemaGuardMode::Strict => {
                    if rb.schema() != input_rb.schema() {
                        return Err(WasmTransformError::Pipeline(
                            "Schema guard strict violation: guest output schema does not match input schema"
                                .into(),
                        ));
                    }
                }
            }
            let signal = match input_batch {
                SignalBatch::Logs(_) => SignalBatch::Logs(rb),
                SignalBatch::Metrics(_) => SignalBatch::Metrics(rb),
                SignalBatch::Traces(_) => SignalBatch::Traces(rb),
            };
            out_batches.push(signal);
        }
    }

    Ok(out_batches)
}

/// Parses a byte size string with standard unit suffixes into a byte count.
fn parse_byte_size(s: &str) -> Option<usize> {
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix("GiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024 * 1024))
    } else if let Some(num) = trimmed.strip_suffix("MiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024))
    } else if let Some(num) = trimmed.strip_suffix("KiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024))
    } else if let Some(num) = trimmed.strip_suffix('B') {
        num.trim().parse::<usize>().ok()
    } else {
        trimmed.parse::<usize>().ok()
    }
}

/// Parses a duration string (e.g., "500ms", "1s") into milliseconds.
fn parse_duration_ms(s: &str) -> Option<u64> {
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix("ms") {
        num.trim().parse::<u64>().ok()
    } else if let Some(num) = trimmed.strip_suffix('s') {
        num.trim().parse::<u64>().ok().map(|s| s * 1000)
    } else {
        trimmed.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size(""), None);
        assert_eq!(parse_byte_size("   "), None);
        assert_eq!(parse_byte_size("100XYZ"), None);
        assert_eq!(parse_byte_size("1024B"), Some(1024));
        assert_eq!(parse_byte_size("16KiB"), Some(16 * 1024));
        assert_eq!(parse_byte_size("128MiB"), Some(128 * 1024 * 1024));
        assert_eq!(parse_byte_size("1GiB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("500"), Some(500));
    }
}
