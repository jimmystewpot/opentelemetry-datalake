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
pub const MAX_METRIC_NAME_LEN: usize = 64;

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

/// A stored metric value in the [`MetricRegistry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricValue {
    /// A monotonic counter value.
    Counter(u64),
    /// A gauge value represented as raw IEEE 754 64-bit binary bits.
    Gauge(u64),
    /// A duration measurement represented in nanoseconds.
    Duration(u64),
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
        Self {
            component_id: component_id.to_string(),
            prefix: format!("datalake_transformers_{component_id}_"),
            signal: std::sync::RwLock::new(signal.to_string()),
            metrics: DashMap::new(),
            handles: DashMap::new(),
        }
    }

    /// Updates the pipeline signal context for exported metrics.
    pub fn set_signal(&self, signal: &str) {
        if let Ok(mut sig) = self.signal.write() {
            *sig = signal.to_string();
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
    fn format_key(&self, name: &str) -> String {
        let mut key = String::with_capacity(self.prefix.len() + name.len());
        key.push_str(&self.prefix);
        key.push_str(name);
        key
    }

    /// Records a counter increment with saturating addition, caching and calling an OpenTelemetry [`Counter`].
    pub fn record_counter(&self, name: &str, delta: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted counter with invalid name");
            return;
        }

        let key = self.format_key(name);
        if self.metrics.len() < MAX_METRIC_ENTRIES || self.metrics.contains_key(&key) {
            self.metrics
                .entry(key)
                .and_modify(|val| {
                    if let MetricValue::Counter(c) = val {
                        *c = c.saturating_add(delta);
                    } else {
                        *val = MetricValue::Counter(delta);
                    }
                })
                .or_insert(MetricValue::Counter(delta));
        }

        let counter = if let Some(handle) = self.handles.get(name) {
            if let MetricHandle::Counter(ref c) = *handle {
                c.clone()
            } else {
                tracing::warn!(
                    name,
                    "Metric type mismatch in registry cache; expected counter"
                );
                return;
            }
        } else {
            if self.handles.len() >= MAX_METRIC_ENTRIES {
                tracing::warn!(
                    component = %self.component_id,
                    max_entries = MAX_METRIC_ENTRIES,
                    "Metric registry capacity exceeded; dropping new counter metric '{name}'"
                );
                return;
            }
            let meter = opentelemetry::global::meter("opentelemetry-datalake");
            let instrument_name = format!("{}{name}", self.prefix);
            let c = meter
                .u64_counter(instrument_name)
                .with_description("Guest emitted counter from WASM transformer")
                .build();
            self.handles
                .insert(name.to_string(), MetricHandle::Counter(c.clone()));
            c
        };

        let sig = self.signal();
        counter.add(
            delta,
            &[
                opentelemetry::KeyValue::new("component_id", self.component_id.clone()),
                opentelemetry::KeyValue::new("signal", sig),
            ],
        );
    }

    /// Records an instantaneous gauge bitcast value, caching and calling an OpenTelemetry [`Gauge`].
    pub fn record_gauge(&self, name: &str, bits: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted gauge with invalid name");
            return;
        }

        let key = self.format_key(name);
        if self.metrics.len() < MAX_METRIC_ENTRIES || self.metrics.contains_key(&key) {
            self.metrics.insert(key, MetricValue::Gauge(bits));
        }

        let gauge = if let Some(handle) = self.handles.get(name) {
            if let MetricHandle::Gauge(ref g) = *handle {
                g.clone()
            } else {
                tracing::warn!(
                    name,
                    "Metric type mismatch in registry cache; expected gauge"
                );
                return;
            }
        } else {
            if self.handles.len() >= MAX_METRIC_ENTRIES {
                tracing::warn!(
                    component = %self.component_id,
                    max_entries = MAX_METRIC_ENTRIES,
                    "Metric registry capacity exceeded; dropping new gauge metric '{name}'"
                );
                return;
            }
            let meter = opentelemetry::global::meter("opentelemetry-datalake");
            let instrument_name = format!("{}{name}", self.prefix);
            let g = meter
                .f64_gauge(instrument_name)
                .with_description("Guest emitted gauge from WASM transformer")
                .build();
            self.handles
                .insert(name.to_string(), MetricHandle::Gauge(g.clone()));
            g
        };

        let float_val = f64::from_bits(bits);
        let sig = self.signal();
        gauge.record(
            float_val,
            &[
                opentelemetry::KeyValue::new("component_id", self.component_id.clone()),
                opentelemetry::KeyValue::new("signal", sig),
            ],
        );
    }

    /// Records a duration observation in nanoseconds, converting to seconds and recording to an OpenTelemetry [`Histogram`].
    pub fn record_duration(&self, name: &str, nanos: u64) {
        if !is_valid_metric_name(name) {
            tracing::warn!(name, "WASM guest emitted duration with invalid name");
            return;
        }

        let key = self.format_key(name);
        if self.metrics.len() < MAX_METRIC_ENTRIES || self.metrics.contains_key(&key) {
            self.metrics.insert(key, MetricValue::Duration(nanos));
        }

        let histogram = if let Some(handle) = self.handles.get(name) {
            if let MetricHandle::Histogram(ref h) = *handle {
                h.clone()
            } else {
                tracing::warn!(
                    name,
                    "Metric type mismatch in registry cache; expected histogram"
                );
                return;
            }
        } else {
            if self.handles.len() >= MAX_METRIC_ENTRIES {
                tracing::warn!(
                    component = %self.component_id,
                    max_entries = MAX_METRIC_ENTRIES,
                    "Metric registry capacity exceeded; dropping new duration metric '{name}'"
                );
                return;
            }
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
            self.handles
                .insert(name.to_string(), MetricHandle::Histogram(h.clone()));
            h
        };

        #[allow(clippy::cast_precision_loss)]
        let seconds = (nanos as f64) / 1_000_000_000.0;
        let sig = self.signal();
        histogram.record(
            seconds,
            &[
                opentelemetry::KeyValue::new("component_id", self.component_id.clone()),
                opentelemetry::KeyValue::new("signal", sig),
            ],
        );
    }

    /// Reads the current value of a counter, returning `0` if not found.
    #[must_use]
    pub fn read_counter(&self, name: &str) -> u64 {
        let key = self.format_key(name);
        match self.metrics.get(&key).as_deref() {
            Some(MetricValue::Counter(c)) => *c,
            _ => 0,
        }
    }

    /// Reads the current raw bitcast value of a gauge, returning `None` if not found.
    #[must_use]
    pub fn read_gauge(&self, name: &str) -> Option<u64> {
        let key = self.format_key(name);
        match self.metrics.get(&key).as_deref() {
            Some(MetricValue::Gauge(bits)) => Some(*bits),
            _ => None,
        }
    }

    /// Reads the current duration in nanoseconds, returning `None` if not found.
    #[must_use]
    pub fn read_duration(&self, name: &str) -> Option<u64> {
        let key = self.format_key(name);
        match self.metrics.get(&key).as_deref() {
            Some(MetricValue::Duration(nanos)) => Some(*nanos),
            _ => None,
        }
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
#[derive(Debug, Clone)]
pub struct HostState {
    /// Current execution phase of the host worker.
    pub phase: HostPhase,
    /// Shared metric registry for collecting guest metrics.
    pub registry: Arc<MetricRegistry>,
}

/// Safely reads a string from guest linear memory, checking bounds and capping allocation size.
fn read_guest_string(
    caller: &mut Caller<'_, HostState>,
    ptr: u32,
    len: u32,
    max_len: usize,
) -> Option<String> {
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

    let mem_data = memory.data(caller);
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

    Some(String::from_utf8_lossy(&mem_data[offset..end]).into_owned())
}

/// Builds and configures a Wasmtime [`Linker`] with standard host functions.
///
/// Links the following imports into the `"env"` module namespace:
/// - `"datalake_host_metric_emit"`: Safe metric emission from guest to [`MetricRegistry`].
/// - `"datalake_host_log"`: Safe logging forwarding from guest to host [`tracing`].
///
/// # Errors
///
/// Returns [`WasmTransformError`] if function definition in the linker fails.
pub fn build_host_linker(engine: &Engine) -> Result<Linker<HostState>, WasmTransformError> {
    let mut linker = Linker::new(engine);

    linker.func_wrap(
        "env",
        "datalake_host_metric_emit",
        |mut caller: Caller<'_, HostState>,
         metric_type: u32,
         name_ptr: u32,
         name_len: u32,
         value: u64| {
            let Some(name) =
                read_guest_string(&mut caller, name_ptr, name_len, MAX_METRIC_NAME_LEN)
            else {
                return;
            };

            if name.is_empty() {
                tracing::warn!("WASM guest emitted metric with empty name");
                return;
            }

            match metric_type {
                METRIC_TYPE_COUNTER => {
                    caller.data().registry.record_counter(&name, value);
                }
                METRIC_TYPE_GAUGE => {
                    caller.data().registry.record_gauge(&name, value);
                }
                METRIC_TYPE_DURATION => {
                    caller.data().registry.record_duration(&name, value);
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
    )?;

    linker.func_wrap(
        "env",
        "datalake_host_log",
        |mut caller: Caller<'_, HostState>, level: u32, msg_ptr: u32, msg_len: u32| {
            let Some(msg) = read_guest_string(&mut caller, msg_ptr, msg_len, MAX_LOG_MESSAGE_LEN)
            else {
                return;
            };

            let comp_id = caller.data().registry.component_id();
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
    )?;

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
