//! Host worker execution loop for WebAssembly transformation modules.
//!
//! Provides the [`WasmWorker`] execution engine which manages an isolated Wasmtime
//! instance, serializes Arrow batches to Arrow IPC streams, invokes guest transforms
//! over the C-ABI v1 boundary, and enforces soft rejuvenation hygiene.

use crate::engine::EngineCache;
use crate::error::WasmTransformError;
use crate::host_calls::{HostPhase, HostState, MetricRegistry};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use pipeline_core::config::WasmTransformerConfig;
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

/// Typed handle to the guest `datalake_transform` export.
///
/// Supports both documented C-ABI v1 `(u32, u32, u32) -> u64` and backwards-compatible legacy `(u32, u32) -> u32`.
#[derive(Clone)]
pub enum TransformFn {
    /// Documented C-ABI v1: `datalake_transform(signal_type, ipc_ptr, ipc_len) -> (response_ptr << 32) | response_len`.
    V1(TypedFunc<(u32, u32, u32), u64>),
    /// Legacy/test mock signature: `datalake_transform(ipc_ptr, ipc_len) -> response_ptr`.
    Legacy(TypedFunc<(u32, u32), u32>),
}

impl std::fmt::Debug for TransformFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1(_) => write!(f, "TransformFn::V1"),
            Self::Legacy(_) => write!(f, "TransformFn::Legacy"),
        }
    }
}

/// Internal container for guest Wasmtime execution state and exported entry points.
struct GuestComponents {
    store: Store<HostState>,
    instance: Instance,
    alloc_fn: TypedFunc<u32, u32>,
    dealloc_fn: TypedFunc<(u32, u32), ()>,
    transform_fn: TransformFn,
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
    registry: Arc<MetricRegistry>,
    guest: Option<GuestComponents>,
    batches_processed: u64,
    local_generation: u64,
    rejuvenate_threshold_bytes: usize,
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
        registry: Arc<MetricRegistry>,
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

        let snapshot = engine.current_snapshot();
        let (target_module, initial_generation) = if let Some(snap) = snapshot {
            (snap.module, snap.generation)
        } else {
            let current_generation = engine.module_generation();
            (module, current_generation)
        };

        let guest = Self::instantiate_guest(engine.engine(), &target_module, &registry, &config)?;

        let mut worker = Self {
            id,
            engine,
            module: target_module,
            config,
            registry,
            guest: Some(guest),
            batches_processed: 0,
            local_generation: initial_generation,
            rejuvenate_threshold_bytes,
        };

        // If a reload occurred while instantiating the module, adopt the newly published snapshot immediately
        worker.check_hot_reload()?;

        Ok(worker)
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
    /// Returns `Err((batch, err))` with the preserved input batch if IPC serialization fails,
    /// guest execution traps, or guest memory bounds are violated.
    #[allow(clippy::too_many_lines)]
    pub fn execute_batch(
        &mut self,
        batch: SignalBatch,
    ) -> Result<WorkerOutcome, (SignalBatch, WasmTransformError)> {
        if let Err(e) = self.check_hot_reload() {
            return Err((batch, e));
        }

        let num_rows = match &batch {
            SignalBatch::Logs(rb) | SignalBatch::Metrics(rb) | SignalBatch::Traces(rb) => {
                rb.num_rows()
            }
        };

        if self.config.max_batch_rows > 0 && num_rows > self.config.max_batch_rows {
            return Ok(WorkerOutcome::Rejected {
                reason: format!(
                    "Batch row count {num_rows} exceeds configured maximum {}",
                    self.config.max_batch_rows
                ),
                original: batch,
            });
        }

        let record_batch = match &batch {
            SignalBatch::Logs(rb) | SignalBatch::Metrics(rb) | SignalBatch::Traces(rb) => rb,
        };

        // 1. Serialize input RecordBatch to Arrow IPC Stream
        let ipc_buf = match serialize_batch_to_ipc(record_batch) {
            Ok(buf) => buf,
            Err(e) => return Err((batch, e)),
        };
        let Ok(ipc_len) = u32::try_from(ipc_buf.len()) else {
            return Err((
                batch,
                WasmTransformError::Pipeline("IPC payload exceeds u32::MAX".to_string()),
            ));
        };

        // Parse execution deadline from config
        let timeout_dur = parse_duration(&self.config.max_execution_duration)
            .unwrap_or_else(|| std::time::Duration::from_millis(500));
        let timeout_ms = u64::try_from(timeout_dur.as_millis()).unwrap_or(500);
        let ticks = u64::try_from((timeout_dur.as_millis().saturating_add(9)) / 10)
            .unwrap_or(u64::MAX)
            .max(1);

        let Some(guest) = self.guest.as_mut() else {
            return Err((
                batch,
                WasmTransformError::Pipeline("WASM worker guest components missing".into()),
            ));
        };
        guest.store.set_epoch_deadline(ticks);

        // 2. Allocate buffer in guest linear memory and copy payload
        let ipc_ptr = match guest.alloc_fn.call(&mut guest.store, ipc_len) {
            Ok(ptr) => ptr,
            Err(e) => {
                let err_chain = format!("{e:#}");
                let is_timeout = err_chain.contains("interrupt")
                    || err_chain.contains("epoch deadline")
                    || err_chain.contains("deadline")
                    || e.downcast_ref::<wasmtime::Trap>()
                        .is_some_and(|t| matches!(t, wasmtime::Trap::Interrupt));
                let err = if is_timeout {
                    WasmTransformError::ExecutionTimeout(timeout_ms)
                } else {
                    e.into()
                };
                let _ = self.rejuvenate();
                return Err((batch, err));
            }
        };
        if ipc_ptr == 0 && ipc_len > 0 {
            return Err((
                batch,
                WasmTransformError::Oom {
                    module: self.config.module_path.clone(),
                    instance: self.id,
                },
            ));
        }

        if let Err(e) = guest
            .memory
            .write(&mut guest.store, ipc_ptr as usize, &ipc_buf)
        {
            let _ = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len));
            return Err((batch, WasmTransformError::Wasmtime(e.into())));
        }

        let signal_type = match &batch {
            SignalBatch::Logs(_) => 0u32,
            SignalBatch::Metrics(_) => 1u32,
            SignalBatch::Traces(_) => 2u32,
        };

        // 3. Reset deadline for transform invocation, invoke datalake_transform and free input buffer
        guest.store.set_epoch_deadline(ticks);
        let (header_ptr, header_len) = match &guest.transform_fn {
            TransformFn::V1(f) => match f.call(&mut guest.store, (signal_type, ipc_ptr, ipc_len)) {
                Ok(packed) => {
                    let ptr = u32::try_from(packed >> 32).unwrap_or(0);
                    let len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
                    if ptr == 0 {
                        let _ = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len));
                        return Err((
                            batch,
                            WasmTransformError::Pipeline(
                                "Malformed C-ABI v1 response header: ptr=0, returned null response header pointer".to_string(),
                            ),
                        ));
                    }
                    if len < 20 {
                        let _ = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len));
                        return Err((
                            batch,
                            WasmTransformError::Pipeline(format!(
                                "Malformed C-ABI v1 response header: ptr={ptr}, len={len}: minimum header size is 20 bytes (minimum required: 20 bytes)"
                            )),
                        ));
                    }
                    let mem_size = guest.memory.data_size(&guest.store);
                    if (ptr as usize).saturating_add(len as usize) > mem_size {
                        let _ = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len));
                        return Err((
                            batch,
                            WasmTransformError::Pipeline(format!(
                                "Response header at offset {ptr} with length {len} exceeds guest memory bounds {mem_size}"
                            )),
                        ));
                    }
                    if let Err(e) = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len)) {
                        let _ = self.rejuvenate();
                        return Err((batch, WasmTransformError::Wasmtime(e)));
                    }
                    (ptr, len)
                }
                Err(e) => {
                    let err_chain = format!("{e:#}");
                    let is_timeout = err_chain.contains("interrupt")
                        || err_chain.contains("epoch deadline")
                        || err_chain.contains("deadline")
                        || e.downcast_ref::<wasmtime::Trap>()
                            .is_some_and(|t| matches!(t, wasmtime::Trap::Interrupt));
                    let err = if is_timeout {
                        WasmTransformError::ExecutionTimeout(timeout_ms)
                    } else {
                        e.into()
                    };
                    let _ = self.rejuvenate();
                    return Err((batch, err));
                }
            },
            TransformFn::Legacy(f) => match f.call(&mut guest.store, (ipc_ptr, ipc_len)) {
                Ok(ptr) => {
                    let mem_size = guest.memory.data_size(&guest.store);
                    if (ptr as usize).saturating_add(20) > mem_size {
                        let _ = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len));
                        return Err((
                            batch,
                            WasmTransformError::Pipeline(format!(
                                "Response header at offset {ptr} with length 20 exceeds guest memory bounds {mem_size}"
                            )),
                        ));
                    }
                    if let Err(e) = guest.dealloc_fn.call(&mut guest.store, (ipc_ptr, ipc_len)) {
                        let _ = self.rejuvenate();
                        return Err((batch, WasmTransformError::Wasmtime(e)));
                    }
                    (ptr, 20)
                }
                Err(e) => {
                    let err_chain = format!("{e:#}");
                    let is_timeout = err_chain.contains("interrupt")
                        || err_chain.contains("epoch deadline")
                        || err_chain.contains("deadline")
                        || e.downcast_ref::<wasmtime::Trap>()
                            .is_some_and(|t| matches!(t, wasmtime::Trap::Interrupt));
                    let err = if is_timeout {
                        WasmTransformError::ExecutionTimeout(timeout_ms)
                    } else {
                        e.into()
                    };
                    let _ = self.rejuvenate();
                    return Err((batch, err));
                }
            },
        };

        // 4. Read response header and message
        let header = match guest.read_response_header(header_ptr) {
            Ok(h) => h,
            Err(e) => return Err((batch, e)),
        };
        let message = read_guest_message(
            &guest.memory,
            &guest.store,
            header.message_ptr,
            header.message_len,
        );

        let mut allocs_to_free = vec![(header_ptr, header_len)];
        if header.message_ptr > 0 && header.message_len > 0 {
            allocs_to_free.push((header.message_ptr, header.message_len));
        }
        if header.batch_count > 0 && header.batches_ptr > 0 {
            allocs_to_free.push((header.batches_ptr, header.batch_count.saturating_mul(8)));
        }

        // 5. Dispatch outcome and extract output batches
        let outcome = guest.dispatch_outcome(
            &header,
            &message,
            batch,
            self.config.schema_guard,
            &mut allocs_to_free,
        );

        // Best effort: free guest allocations on all paths
        for (ptr, len) in allocs_to_free {
            if let Err(e) = guest.dealloc_fn.call(&mut guest.store, (ptr, len)) {
                tracing::warn!(
                    ptr,
                    len,
                    error = %e,
                    "Guest deallocation failed; instance memory may be leaked"
                );
            }
        }

        match outcome {
            Ok(outcome) => {
                self.batches_processed = self.batches_processed.saturating_add(1);
                if let Err(e) = self.check_rejuvenation() {
                    tracing::warn!(
                        worker_id = self.id,
                        error = %e,
                        "Post-batch rejuvenation failed; continuing execution with current instance"
                    );
                }
                Ok(outcome)
            }
            Err((orig_batch, err)) => Err((orig_batch, err)),
        }
    }

    /// Rejuvenates the guest instance, dropping the old instance before re-instantiating.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if re-instantiation fails or required exports are missing.
    pub fn rejuvenate(&mut self) -> Result<(), WasmTransformError> {
        drop(self.guest.take());
        let guest = Self::instantiate_guest(
            self.engine.engine(),
            &self.module,
            &self.registry,
            &self.config,
        )?;
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

    /// Checks if a newer module snapshot is available in [`EngineCache`] and rejuvenates if so.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if guest re-instantiation fails.
    pub fn check_hot_reload(&mut self) -> Result<(), WasmTransformError> {
        while self.local_generation != self.engine.module_generation() {
            let Some(snapshot) = self.engine.current_snapshot() else {
                break;
            };
            if self.local_generation == snapshot.generation {
                break;
            }
            let target_generation = snapshot.generation;
            let target_module = snapshot.module;

            // Probe and instantiate candidate
            match Self::instantiate_guest(
                self.engine.engine(),
                &target_module,
                &self.registry,
                &self.config,
            ) {
                Ok(new_guest) => {
                    drop(self.guest.take());
                    self.guest = Some(new_guest);
                    self.module = target_module;
                    self.local_generation = target_generation;
                    self.batches_processed = 0;
                }
                Err(e) => {
                    tracing::warn!(
                        worker_id = self.id,
                        error = %e,
                        "Hot-reload candidate failed verification; keeping existing active module"
                    );
                    self.local_generation = target_generation;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Rejuvenates the guest instance if batch count or memory limits are exceeded.
    fn check_rejuvenation(&mut self) -> Result<(), WasmTransformError> {
        let memory_exceeded = if let Some(ref guest) = self.guest {
            self.rejuvenate_threshold_bytes > 0
                && guest.memory.data_size(&guest.store) >= self.rejuvenate_threshold_bytes
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

    /// Probes a candidate WebAssembly module to ensure it can be instantiated,
    /// exports required C-ABI v1 symbols, and initializes successfully.
    ///
    /// Unlike [`WasmWorker::new`], this method instantiates the supplied candidate
    /// module directly without consulting or adopting any cached module snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if instantiation fails, required exports are
    /// missing, or guest initialization fails.
    pub fn probe_candidate(
        engine: &EngineCache,
        module: &Module,
        config: &WasmTransformerConfig,
        registry: &Arc<MetricRegistry>,
    ) -> Result<(), WasmTransformError> {
        Self::instantiate_guest(engine.engine(), module, registry, config).map(|_| ())
    }

    /// Helper to instantiate a guest module, invoke `datalake_init` if exported, and extract required ABI exports.
    fn instantiate_guest(
        engine: &wasmtime::Engine,
        module: &Module,
        registry: &Arc<MetricRegistry>,
        config: &WasmTransformerConfig,
    ) -> Result<GuestComponents, WasmTransformError> {
        let host_state = HostState::with_default_wasi(HostPhase::Init, Arc::clone(registry));
        let mut store = Store::new(engine, host_state);
        let init_timeout_dur = parse_duration(&config.init_timeout)
            .unwrap_or_else(|| std::time::Duration::from_secs(2));
        let init_ticks = u64::try_from((init_timeout_dur.as_millis().saturating_add(9)) / 10)
            .unwrap_or(u64::MAX)
            .max(1);
        store.set_epoch_deadline(init_ticks);

        let linker = crate::host_calls::build_host_linker(engine)?;
        let instance = linker.instantiate(&mut store, module)?;

        let version_fn = instance
            .get_typed_func::<(), u32>(&mut store, "datalake_abi_version")
            .map_err(|_| WasmTransformError::MissingExport("datalake_abi_version".into()))?;
        let version = version_fn.call(&mut store, ()).map_err(|e| {
            WasmTransformError::InitFailed(format!("Failed to call datalake_abi_version: {e}"))
        })?;
        if version != opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION {
            return Err(WasmTransformError::AbiVersionMismatch(version));
        }

        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
            .map_err(|_| WasmTransformError::MissingExport("datalake_alloc".into()))?;
        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
            .map_err(|_| WasmTransformError::MissingExport("datalake_dealloc".into()))?;
        let transform_fn = if let Ok(f) =
            instance.get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        {
            TransformFn::V1(f)
        } else if let Ok(f) =
            instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")
        {
            TransformFn::Legacy(f)
        } else {
            return Err(WasmTransformError::MissingExport(
                "datalake_transform".into(),
            ));
        };
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| WasmTransformError::MissingExport("memory".into()))?;

        if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), i32>(&mut store, "datalake_init")
        {
            let init_payload = serde_json::json!({
                "signal": config.env.get("signal").map_or("unknown", |s| s.as_str()),
                "env": crate::wasi_env::filter_environment_variables(&config.env_whitelist, &config.env),
                "config": config.config,
            });
            let init_bytes = serde_json::to_vec(&init_payload)
                .map_err(|e| WasmTransformError::InitFailed(e.to_string()))?;
            let len = u32::try_from(init_bytes.len()).map_err(|_| {
                WasmTransformError::InitFailed("Config JSON exceeds u32 limit".into())
            })?;

            let ptr = alloc_fn
                .call(&mut store, len)
                .map_err(|e| WasmTransformError::InitFailed(e.to_string()))?;

            if ptr == 0 {
                return Err(WasmTransformError::InitFailed(
                    "datalake_alloc returned null pointer during initialization".into(),
                ));
            }

            memory
                .write(&mut store, ptr as usize, &init_bytes)
                .map_err(|e| WasmTransformError::InitFailed(e.to_string()))?;

            let init_status = init_fn
                .call(&mut store, (ptr, len))
                .map_err(|e| WasmTransformError::InitFailed(e.to_string()))?;

            let _ = dealloc_fn.call(&mut store, (ptr, len));

            if init_status != 0 {
                return Err(WasmTransformError::InitFailed(format!(
                    "datalake_init returned non-zero status: {init_status}"
                )));
            }
        }

        store.data_mut().phase = HostPhase::Execution;

        Ok(GuestComponents {
            store,
            instance,
            alloc_fn,
            dealloc_fn,
            transform_fn,
            memory,
        })
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
    ) -> Result<WorkerOutcome, (SignalBatch, WasmTransformError)> {
        match header.status {
            0 => {
                if header.batch_count == 0 {
                    Ok(WorkerOutcome::Emitted(vec![]))
                } else if header.batches_ptr == 0 {
                    Err((
                        batch,
                        WasmTransformError::Pipeline(
                            "Protocol error: guest returned status 0 with batch_count > 0 but null batches_ptr".to_string(),
                        ),
                    ))
                } else {
                    let mut out_batches = match extract_output_batches(
                        &self.memory,
                        &self.store,
                        header.batches_ptr,
                        header.batch_count,
                        &batch,
                        allocs_to_free,
                    ) {
                        Ok(b) => b,
                        Err(e) => return Err((batch, e)),
                    };

                    let in_rb = match &batch {
                        SignalBatch::Logs(rb)
                        | SignalBatch::Metrics(rb)
                        | SignalBatch::Traces(rb) => rb,
                    };

                    for out_batch in &mut out_batches {
                        let out_rb = match &*out_batch {
                            SignalBatch::Logs(rb)
                            | SignalBatch::Metrics(rb)
                            | SignalBatch::Traces(rb) => rb,
                        };

                        if let Err(e) = crate::guard::verify_structural_immutability(in_rb, out_rb)
                        {
                            return Err((batch, e));
                        }

                        match schema_guard {
                            pipeline_core::config::SchemaGuardMode::Strict => {
                                if let Err(e) =
                                    crate::guard::verify_strict_schema_equality(in_rb, out_rb)
                                {
                                    return Err((batch, e));
                                }
                            }
                            pipeline_core::config::SchemaGuardMode::Defensive => {
                                let backfilled = match crate::guard::backfill_missing_columns(
                                    &in_rb.schema(),
                                    out_rb.clone(),
                                ) {
                                    Ok(b) => b,
                                    Err(e) => return Err((batch, e)),
                                };
                                *out_batch = match out_batch {
                                    SignalBatch::Logs(_) => SignalBatch::Logs(backfilled),
                                    SignalBatch::Metrics(_) => SignalBatch::Metrics(backfilled),
                                    SignalBatch::Traces(_) => SignalBatch::Traces(backfilled),
                                };
                            }
                        }
                    }

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

/// Serializes an Arrow [`RecordBatch`] to an Arrow IPC stream buffer.
fn serialize_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, WasmTransformError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
        writer
            .write(batch)
            .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
    }
    Ok(buffer)
}

/// Maximum permitted length for an optional guest error or status message (64 KiB).
const MAX_GUEST_MESSAGE_LEN: usize = 64 * 1024;

/// Reads an optional UTF-8 message string from guest memory.
fn read_guest_message<T>(
    memory: &Memory,
    store: &Store<T>,
    message_ptr: u32,
    message_len: u32,
) -> String {
    if message_ptr == 0 || message_len == 0 {
        return String::new();
    }

    let capped_len = (message_len as usize).min(MAX_GUEST_MESSAGE_LEN);
    let offset = message_ptr as usize;

    let mut buf = vec![0u8; capped_len];
    if memory.read(store, offset, &mut buf).is_err() {
        return String::new();
    }

    String::from_utf8_lossy(&buf).into_owned()
}

/// Maximum number of output batches decoded from a single response to prevent unbounded memory amplification.
const MAX_GUEST_BATCH_COUNT: u32 = 1024;

/// Extracts transformed output batches from guest memory via a `BatchDescriptor` array.
fn extract_output_batches<T>(
    memory: &Memory,
    store: &Store<T>,
    batches_ptr: u32,
    batch_count: u32,
    input_batch: &SignalBatch,
    allocs_to_free: &mut Vec<(u32, u32)>,
) -> Result<Vec<SignalBatch>, WasmTransformError> {
    if batch_count > MAX_GUEST_BATCH_COUNT {
        return Err(WasmTransformError::Pipeline(format!(
            "Guest batch count {batch_count} exceeds maximum allowed limit of {MAX_GUEST_BATCH_COUNT}"
        )));
    }

    let mem_size = memory.data_size(store);
    let descriptor_table_len = (batch_count as usize).saturating_mul(8);
    let descriptor_table_end = (batches_ptr as usize).saturating_add(descriptor_table_len);

    if descriptor_table_end > mem_size {
        return Err(WasmTransformError::Pipeline(format!(
            "Batch descriptors array bounds exceed guest memory size {mem_size}"
        )));
    }

    let mut out_batches = Vec::with_capacity((batch_count as usize).min(64));
    let max_decoded = MAX_GUEST_BATCH_COUNT as usize;
    let mut total_bytes = 0usize;

    for i in 0..batch_count {
        let offset = (batches_ptr as usize)
            .checked_add((i as usize) * 8)
            .ok_or_else(|| {
                WasmTransformError::Pipeline("Batch descriptor offset overflow".to_string())
            })?;

        let mut desc_bytes = [0u8; 8];
        memory
            .read(store, offset, &mut desc_bytes)
            .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

        let b_ptr =
            u32::from_le_bytes([desc_bytes[0], desc_bytes[1], desc_bytes[2], desc_bytes[3]]);
        let b_len =
            u32::from_le_bytes([desc_bytes[4], desc_bytes[5], desc_bytes[6], desc_bytes[7]]);

        if b_ptr == 0 {
            return Err(WasmTransformError::Pipeline(
                "Protocol error: batch descriptor contained null IPC buffer pointer".to_string(),
            ));
        }

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

        let start = b_ptr as usize;
        let end = start.saturating_add(b_len_usize);

        if end > mem_size {
            return Err(WasmTransformError::Pipeline(format!(
                "Batch IPC buffer bounds exceed guest memory size {mem_size}"
            )));
        }

        let slice = memory.data(store).get(start..end).ok_or_else(|| {
            WasmTransformError::Pipeline(format!(
                "Batch IPC slice bounds exceed guest memory size {mem_size}"
            ))
        })?;

        let cursor = std::io::Cursor::new(slice);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
        for maybe_rb in reader {
            if out_batches.len() >= max_decoded {
                return Err(WasmTransformError::Pipeline(format!(
                    "Aggregate decoded batch count exceeds maximum allowed limit of {MAX_GUEST_BATCH_COUNT}"
                )));
            }
            let rb = maybe_rb.map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
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

/// Parses human-readable byte sizes (e.g. "64MiB", "100MB", "1024").
#[must_use]
pub fn parse_byte_size(s: &str) -> Option<usize> {
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix("GiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024 * 1024))
    } else if let Some(num) = trimmed.strip_suffix("GB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1000 * 1000 * 1000))
    } else if let Some(num) = trimmed.strip_suffix("MiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024 * 1024))
    } else if let Some(num) = trimmed.strip_suffix("MB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1000 * 1000))
    } else if let Some(num) = trimmed.strip_suffix("KiB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1024))
    } else if let Some(num) = trimmed.strip_suffix("KB") {
        num.trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(1000))
    } else {
        trimmed.parse::<usize>().ok()
    }
}

/// Parses a duration string (e.g. "500ms", "5s", "1m") into a [`std::time::Duration`].
#[must_use]
pub fn parse_duration(s: &str) -> Option<std::time::Duration> {
    let trimmed = s.trim();
    if let Some(num) = trimmed.strip_suffix("ms") {
        num.trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_millis)
    } else if let Some(num) = trimmed.strip_suffix('s') {
        num.trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_secs)
    } else if let Some(num) = trimmed.strip_suffix('m') {
        num.trim()
            .parse::<u64>()
            .ok()
            .map(|m| std::time::Duration::from_secs(m.saturating_mul(60)))
    } else {
        trimmed
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_secs)
    }
}
