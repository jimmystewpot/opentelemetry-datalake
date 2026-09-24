//! Concrete Wasmtime host linker and concurrent metrics registry for WASM transformers.
//!
//! Provides host function imports exposed to guest WebAssembly modules (`datalake_host_metric_emit`
//! and `datalake_host_log`), safe memory reads with bounds validation and allocation capping,
//! and a concurrent [`MetricRegistry`] powered by [`DashMap`].

use crate::error::WasmTransformError;
use dashmap::DashMap;
use opentelemetry_datalake_wasm_sdk::abi::{
    LOG_LEVEL_DEBUG, LOG_LEVEL_ERROR, LOG_LEVEL_INFO, LOG_LEVEL_WARN,
};
use opentelemetry_datalake_wasm_sdk::metrics::{
    METRIC_TYPE_COUNTER, METRIC_TYPE_DURATION, METRIC_TYPE_GAUGE,
};
use std::sync::Arc;
use wasmtime::{Caller, Engine, Linker};

/// Maximum allowed length in bytes for metric names read from guest memory.
pub const MAX_METRIC_NAME_LEN: usize = 256;

/// Maximum allowed length in bytes for log messages read from guest memory (64 KiB).
pub const MAX_LOG_MESSAGE_LEN: usize = 65_536;

/// Maximum distinct metric entries permitted in a [`MetricRegistry`] to prevent unbounded memory growth.
pub const MAX_METRIC_ENTRIES: usize = 50;

/// Validates whether a metric name conforms to the OpenTelemetry custom metric naming rules.
///
/// Metric names must be non-empty, at most [`MAX_METRIC_NAME_LEN`] characters, and consist
/// solely of ASCII alphanumeric characters and underscores (`_`).
#[must_use]
pub fn is_valid_metric_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_METRIC_NAME_LEN
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Lifecycle execution phase of the host worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostPhase {
    /// Worker is initializing or loading configuration.
    Init,
    /// Worker is actively processing telemetry batches.
    Execution,
}

/// A summary of duration observations capturing sample count, cumulative duration in nanoseconds,
/// minimum, maximum, and the latest duration observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationSummary {
    /// Total number of duration samples recorded.
    pub count: u64,
    /// Cumulative sum of observed duration in nanoseconds.
    pub sum_nanos: u64,
    /// Minimum observed duration in nanoseconds.
    pub min_nanos: u64,
    /// Maximum observed duration in nanoseconds.
    pub max_nanos: u64,
    /// Most recent observed duration in nanoseconds.
    pub last_nanos: u64,
}

/// A stored metric value in the [`MetricRegistry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricValue {
    /// A monotonic counter value.
    Counter(u64),
    /// A gauge value represented as raw IEEE 754 64-bit binary bits.
    Gauge(u64),
    /// A duration metric tracking observation count and distribution metrics in nanoseconds.
    Duration(DurationSummary),
}

/// Concrete OpenTelemetry instrument handle cached in [`MetricRegistry`].
#[derive(Debug, Clone)]
pub enum MetricHandle {
    /// Synchronous monotonic counter instrument.
    Counter(opentelemetry::metrics::Counter<u64>),
    /// Synchronous gauge instrument.
    Gauge(opentelemetry::metrics::Gauge<f64>),
    /// Synchronous duration histogram instrument.
    Histogram(opentelemetry::metrics::Histogram<f64>),
}

/// Thread-safe concurrent registry for metrics emitted by guest WebAssembly transformers.
///
/// Metric keys are internally prefixed with `datalake_transformers_{component_id}_{name}`.
#[derive(Debug)]
pub struct MetricRegistry {
    component_id: String,
    prefix: String,
    signal: std::sync::RwLock<String>,
    /// Pre-computed OpenTelemetry attribute set for metric emissions.
    /// Avoids acquiring `signal` `RwLock` and cloning strings on every emission.
    cached_attributes: std::sync::RwLock<[opentelemetry::KeyValue; 2]>,
    registration_lock: std::sync::Mutex<()>,
    metrics: DashMap<String, MetricValue>,
    handles: DashMap<String, MetricHandle>,
}

impl MetricRegistry {
    /// Creates a new `MetricRegistry` for the specified component identifier.
    #[must_use]
    pub fn new(component_id: &str) -> Self {
        Self::with_signal(component_id, "")
    }

    /// Creates a new `MetricRegistry` for the specified component identifier and signal context.
    #[must_use]
    pub fn with_signal(component_id: &str, signal: &str) -> Self {
        let attrs = [
            opentelemetry::KeyValue::new("component_id", component_id.to_string()),
            opentelemetry::KeyValue::new("signal", signal.to_string()),
        ];
        Self {
            component_id: component_id.to_string(),
            prefix: format!("datalake_transformers_{component_id}_"),
            signal: std::sync::RwLock::new(signal.to_string()),
            cached_attributes: std::sync::RwLock::new(attrs),
            registration_lock: std::sync::Mutex::new(()),
            metrics: DashMap::new(),
            handles: DashMap::new(),
        }
    }

    /// Updates the pipeline signal context for exported metrics.
    pub fn set_signal(&self, signal: &str) {
        if let Ok(mut sig) = self.signal.write() {
            *sig = signal.to_string();
        }
        if let Ok(mut attrs) = self.cached_attributes.write() {
            *attrs = [
                opentelemetry::KeyValue::new("component_id", self.component_id.clone()),
                opentelemetry::KeyValue::new("signal", signal.to_string()),
            ];
        }
    }

    /// Returns the active pipeline signal context.
    #[must_use]
    pub fn signal(&self) -> String {
        self.signal
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Formats a metric name into its full scoped key with pre-allocated capacity.
    #[must_use]
    pub fn format_key(&self, name: &str) -> String {
        let mut key = String::with_capacity(self.prefix.len() + name.len());
        key.push_str(&self.prefix);
        key.push_str(name);
        key
    }

    /// Invokes a closure with a formatted metric key string without heap allocation when possible.
    fn with_key<R>(&self, name: &str, f: impl FnOnce(&str) -> R) -> R {
        let required_len = self.prefix.len() + name.len();
        if required_len <= 512 {
            let mut buf = [0u8; 512];
            buf[..self.prefix.len()].copy_from_slice(self.prefix.as_bytes());
            buf[self.prefix.len()..required_len].copy_from_slice(name.as_bytes());
            if let Ok(s) = std::str::from_utf8(&buf[..required_len]) {
                return f(s);
            }
        }
        let mut key = String::with_capacity(required_len);
        key.push_str(&self.prefix);
        key.push_str(name);
        f(&key)
    }

    /// Returns the pre-computed OpenTelemetry attributes for metric emissions.
    fn emit_attributes(&self) -> [opentelemetry::KeyValue; 2] {
        self.cached_attributes
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Retrieves an existing cached instrument handle, or registers a new instrument under [`MetricRegistry::registration_lock`].
    fn get_or_register_handle<F>(&self, name: &str, create_fn: F) -> Option<MetricHandle>
    where
        F: FnOnce() -> MetricHandle,
    {
        if let Some(handle) = self.handles.get(name) {
            return Some(handle.clone());
        }

        let _guard = self
            .registration_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(handle) = self.handles.get(name) {
            return Some(handle.clone());
        }

        if self.handles.len() >= MAX_METRIC_ENTRIES {
            tracing::warn!(
                component = %self.component_id,
                max_entries = MAX_METRIC_ENTRIES,
                "Metric registry capacity exceeded; dropping new metric '{name}'"
            );
            return None;
        }

        let handle = create_fn();
        self.handles.insert(name.to_string(), handle.clone());
        Some(handle)
    }

    /// Updates an existing metric entry or registers a new entry under [`MetricRegistry::registration_lock`]
    /// ensuring capacity bounds are strictly enforced under high concurrency.
    fn update_or_register_metric<U, C>(
        &self,
        name: &str,
        metric_type: &str,
        update_fn: U,
        create_fn: C,
    ) where
        U: Fn(&mut MetricValue),
        C: FnOnce() -> MetricValue,
    {
        self.with_key(name, |key| {
            if let Some(mut val) = self.metrics.get_mut(key) {
                update_fn(&mut val);
                return;
            }

            let _guard = self
                .registration_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            if let Some(mut val) = self.metrics.get_mut(key) {
                update_fn(&mut val);
                return;
            }

            if self.metrics.len() >= MAX_METRIC_ENTRIES {
                tracing::warn!(
                    component = %self.component_id,
                    max_entries = MAX_METRIC_ENTRIES,
                    "Metric registry capacity exceeded; dropping new {metric_type} metric registration"
                );
                return;
            }

            self.metrics.insert(key.to_string(), create_fn());
        });
    }

    /// Records a counter increment with saturating addition and bounded entry capacity.
    pub fn record_counter(&self, name: &str, delta: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted counter with invalid name");
            return;
        }

        self.update_or_register_metric(
            name,
            "counter",
            |val| {
                if let MetricValue::Counter(c) = val {
                    *c = (*c).saturating_add(delta);
                } else {
                    *val = MetricValue::Counter(delta);
                }
            },
            || MetricValue::Counter(delta),
        );

        if let Some(MetricHandle::Counter(counter)) = self.get_or_register_handle(name, || {
            let meter = opentelemetry::global::meter("opentelemetry-datalake");
            let instrument_name = format!("{}{name}", self.prefix);
            let c = meter
                .u64_counter(instrument_name)
                .with_description("Guest emitted counter from WASM transformer")
                .build();
            MetricHandle::Counter(c)
        }) {
            let attrs = self.emit_attributes();
            counter.add(delta, &attrs);
        }
    }

    /// Records an instantaneous gauge bitcast value with bounded entry capacity.
    pub fn record_gauge(&self, name: &str, bits: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted gauge with invalid name");
            return;
        }

        self.update_or_register_metric(
            name,
            "gauge",
            |val| *val = MetricValue::Gauge(bits),
            || MetricValue::Gauge(bits),
        );

        if let Some(MetricHandle::Gauge(gauge)) = self.get_or_register_handle(name, || {
            let meter = opentelemetry::global::meter("opentelemetry-datalake");
            let instrument_name = format!("{}{name}", self.prefix);
            let g = meter
                .f64_gauge(instrument_name)
                .with_description("Guest emitted gauge from WASM transformer")
                .build();
            MetricHandle::Gauge(g)
        }) {
            let float_val = f64::from_bits(bits);
            let attrs = self.emit_attributes();
            gauge.record(float_val, &attrs);
        }
    }

    /// Records a duration observation in nanoseconds with bounded entry capacity and histogram export.
    pub fn record_duration(&self, name: &str, nanos: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted duration with invalid name");
            return;
        }

        self.update_or_register_metric(
            name,
            "duration",
            |val| {
                if let MetricValue::Duration(d) = val {
                    d.count = d.count.saturating_add(1);
                    d.sum_nanos = d.sum_nanos.saturating_add(nanos);
                    d.min_nanos = d.min_nanos.min(nanos);
                    d.max_nanos = d.max_nanos.max(nanos);
                    d.last_nanos = nanos;
                } else {
                    *val = MetricValue::Duration(DurationSummary {
                        count: 1,
                        sum_nanos: nanos,
                        min_nanos: nanos,
                        max_nanos: nanos,
                        last_nanos: nanos,
                    });
                }
            },
            || {
                MetricValue::Duration(DurationSummary {
                    count: 1,
                    sum_nanos: nanos,
                    min_nanos: nanos,
                    max_nanos: nanos,
                    last_nanos: nanos,
                })
            },
        );

        if let Some(MetricHandle::Histogram(histogram)) = self.get_or_register_handle(name, || {
            let meter = opentelemetry::global::meter("opentelemetry-datalake");
            let instrument_name = if name.ends_with("_duration_seconds") {
                format!("{}{name}", self.prefix)
            } else {
                format!("{}{name}_duration_seconds", self.prefix)
            };
            let h = meter
                .f64_histogram(instrument_name)
                .with_unit("s")
                .with_description("Guest emitted duration from WASM transformer")
                .build();
            MetricHandle::Histogram(h)
        }) {
            #[allow(clippy::cast_precision_loss)]
            let seconds = (nanos as f64) / 1_000_000_000.0;
            let attrs = self.emit_attributes();
            histogram.record(seconds, &attrs);
        }
    }

    /// Reads the current value of a counter, returning `0` if not found.
    #[must_use]
    pub fn read_counter(&self, name: &str) -> u64 {
        self.with_key(name, |key| match self.metrics.get(key).as_deref() {
            Some(&MetricValue::Counter(c)) => c,
            _ => 0,
        })
    }

    /// Reads the current raw bitcast value of a gauge, returning `None` if not found.
    #[must_use]
    pub fn read_gauge(&self, name: &str) -> Option<u64> {
        self.with_key(name, |key| match self.metrics.get(key).as_deref() {
            Some(&MetricValue::Gauge(bits)) => Some(bits),
            _ => None,
        })
    }

    /// Reads the current duration metric summary, returning `None` if not found.
    #[must_use]
    pub fn read_duration(&self, name: &str) -> Option<DurationSummary> {
        self.with_key(name, |key| match self.metrics.get(key).as_deref() {
            Some(&MetricValue::Duration(d)) => Some(d),
            _ => None,
        })
    }

    /// Returns the component identifier configured for this registry.
    #[must_use]
    pub fn component_id(&self) -> &str {
        &self.component_id
    }

    /// Returns a reference to the underlying concurrent metrics map.
    #[must_use]
    pub fn metrics(&self) -> &DashMap<String, MetricValue> {
        &self.metrics
    }

    /// Returns a reference to the underlying cached OpenTelemetry metric handles.
    #[must_use]
    pub fn handles(&self) -> &DashMap<String, MetricHandle> {
        &self.handles
    }
}

/// Host execution context stored within the Wasmtime [`wasmtime::Store`].
pub struct HostState {
    /// Current execution phase of the host worker.
    pub phase: HostPhase,
    /// Shared metric registry for collecting guest metrics.
    pub registry: Arc<MetricRegistry>,
    /// WASI Preview 1 execution context.
    pub wasi: wasmtime_wasi::p1::WasiP1Ctx,
}

impl std::fmt::Debug for HostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostState")
            .field("phase", &self.phase)
            .field("registry", &self.registry)
            .field("wasi", &"<WasiP1Ctx>")
            .finish()
    }
}

impl HostState {
    /// Creates a new `HostState` with a provided WASI preview 1 context.
    #[must_use]
    pub fn new(
        phase: HostPhase,
        registry: Arc<MetricRegistry>,
        wasi: wasmtime_wasi::p1::WasiP1Ctx,
    ) -> Self {
        Self {
            phase,
            registry,
            wasi,
        }
    }

    /// Creates a new `HostState` with default zero-trust WASI preview 1 configuration.
    #[must_use]
    pub fn with_default_wasi(phase: HostPhase, registry: Arc<MetricRegistry>) -> Self {
        Self {
            phase,
            registry,
            wasi: wasmtime_wasi::WasiCtxBuilder::new().build_p1(),
        }
    }
}

/// Invokes a closure with a borrowed string slice from guest linear memory, checking bounds and UTF-8.
fn with_guest_str<R>(
    caller: &mut Caller<'_, HostState>,
    ptr: u32,
    len: u32,
    max_len: usize,
    f: impl FnOnce(&Caller<'_, HostState>, &str) -> R,
) -> Option<R> {
    let Some(export) = caller.get_export("memory") else {
        tracing::warn!("WASM guest invoked host function without exporting 'memory'");
        return None;
    };
    let Some(memory) = export.into_memory() else {
        tracing::warn!("WASM guest export 'memory' is not a linear memory");
        return None;
    };

    let Some(end_u32) = ptr.checked_add(len) else {
        tracing::warn!(ptr, len, "WASM guest memory address addition overflow");
        return None;
    };

    let caller_ref = &*caller;
    let mem_data = memory.data(caller_ref);
    let offset = ptr as usize;
    let total_end = end_u32 as usize;

    if total_end > mem_data.len() {
        tracing::warn!(
            offset,
            raw_len = len as usize,
            memory_len = mem_data.len(),
            "WASM guest memory read out of bounds"
        );
        return None;
    }

    let read_len = (len as usize).min(max_len);
    let end = offset.saturating_add(read_len);
    let slice = &mem_data[offset..end];

    if let Ok(s) = std::str::from_utf8(slice) {
        Some(f(caller_ref, s))
    } else {
        let owned = String::from_utf8_lossy(slice).into_owned();
        Some(f(caller_ref, &owned))
    }
}

/// Builds and configures a Wasmtime [`Linker`] with standard host functions.
///
/// Links the following imports:
/// - `"wasi_snapshot_preview1"`: WASI Preview 1 host imports from [`wasmtime_wasi::p1`].
/// - `"env:datalake_host_metric_emit"`: Safe metric emission from guest to [`MetricRegistry`].
/// - `"env:datalake_host_log"`: Safe logging forwarding from guest to host [`tracing`].
/// - `"env:datalake_host_has_capability"`: Host capability negotiation during initialization.
/// - `"env:datalake_host_now_nanos"`: Fast monotonic timestamp query in nanoseconds.
///
/// # Errors
///
/// Returns [`WasmTransformError`] if function definition in the linker fails.
#[allow(clippy::too_many_lines)]
pub fn build_host_linker(engine: &Engine) -> Result<Linker<HostState>, WasmTransformError> {
    let mut linker = Linker::new(engine);

    wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut HostState| &mut state.wasi)?;

    linker.func_wrap(
        "env",
        "datalake_host_metric_emit",
        |mut caller: Caller<'_, HostState>,
         metric_type: u32,
         name_ptr: u32,
         name_len: u32,
         value: u64| {
            let _ = with_guest_str(
                &mut caller,
                name_ptr,
                name_len,
                MAX_METRIC_NAME_LEN,
                |caller_ref, name| {
                    if name.is_empty() {
                        tracing::warn!("WASM guest emitted metric with empty name");
                        return;
                    }

                    match metric_type {
                        METRIC_TYPE_COUNTER => {
                            caller_ref.data().registry.record_counter(name, value);
                        }
                        METRIC_TYPE_GAUGE => {
                            caller_ref.data().registry.record_gauge(name, value);
                        }
                        METRIC_TYPE_DURATION => {
                            caller_ref.data().registry.record_duration(name, value);
                        }
                        _ => {
                            tracing::warn!(
                                metric_type,
                                name = %name,
                                "Received unknown metric type from guest WebAssembly module"
                            );
                        }
                    }
                },
            );
        },
    )?;

    linker.func_wrap(
        "env",
        "datalake_host_log",
        |mut caller: Caller<'_, HostState>, level: u32, msg_ptr: u32, msg_len: u32| {
            let _ = with_guest_str(
                &mut caller,
                msg_ptr,
                msg_len,
                MAX_LOG_MESSAGE_LEN,
                |caller_ref, msg| {
                    let comp_id = caller_ref.data().registry.component_id();
                    match level {
                        LOG_LEVEL_ERROR => {
                            tracing::error!(target: "wasm_guest", component = %comp_id, "{msg}");
                        }
                        LOG_LEVEL_WARN => {
                            tracing::warn!(target: "wasm_guest", component = %comp_id, "{msg}");
                        }
                        LOG_LEVEL_INFO => {
                            tracing::info!(target: "wasm_guest", component = %comp_id, "{msg}");
                        }
                        LOG_LEVEL_DEBUG => {
                            tracing::debug!(target: "wasm_guest", component = %comp_id, "{msg}");
                        }
                        _ => tracing::trace!(target: "wasm_guest", component = %comp_id, "{msg}"),
                    }
                },
            );
        },
    )?;

    linker.func_wrap(
        "env",
        "datalake_host_has_capability",
        |mut caller: Caller<'_, HostState>, cap_name_ptr: u32, cap_name_len: u32| -> u32 {
            if caller.data().phase != HostPhase::Init {
                tracing::warn!(
                    "Guest module queried datalake_host_has_capability outside of init phase; returning 0"
                );
                return 0;
            }

            with_guest_str(
                &mut caller,
                cap_name_ptr,
                cap_name_len,
                MAX_METRIC_NAME_LEN,
                |_caller_ref, cap_name| {
                    tracing::warn!(
                        capability = %cap_name,
                        "Guest module queried unrecognized capability in datalake_host_has_capability; returning 0"
                    );
                    0
                },
            )
            .unwrap_or(0)
        },
    )?;

    linker.func_wrap("env", "datalake_host_now_nanos", || -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
    })?;

    Ok(linker)
}

/// Handle maintaining active OpenTelemetry metric bridge for a [`MetricRegistry`].
#[derive(Debug)]
pub struct MetricBridgeHandle {
    _registry: Arc<MetricRegistry>,
}

/// Bridges custom guest metrics from the provided [`MetricRegistry`] to OpenTelemetry.
///
/// Sets the active pipeline signal context on the registry so that subsequent guest metric
/// recordings are properly tagged with `component_id` and `signal`.
#[must_use]
pub fn bridge_metrics_to_opentelemetry(
    registry: &Arc<MetricRegistry>,
    signal: &str,
) -> MetricBridgeHandle {
    registry.set_signal(signal);
    MetricBridgeHandle {
        _registry: Arc::clone(registry),
    }
}
