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
}

/// Thread-safe concurrent registry for metrics emitted by guest WebAssembly transformers.
///
/// Metric keys are internally prefixed with `datalake_transformers_{component_id}_{name}`.
#[derive(Debug)]
pub struct MetricRegistry {
    component_id: String,
    metrics: DashMap<String, MetricValue>,
}

impl MetricRegistry {
    /// Creates a new `MetricRegistry` for the specified component identifier.
    #[must_use]
    pub fn new(component_id: &str) -> Self {
        Self {
            component_id: component_id.to_string(),
            metrics: DashMap::new(),
        }
    }

    /// Formats a metric name into its full scoped key.
    fn format_key(&self, name: &str) -> String {
        format!("datalake_transformers_{}_{name}", self.component_id)
    }

    /// Records a counter increment with saturating addition.
    pub fn record_counter(&self, name: &str, delta: u64) {
        let key = self.format_key(name);
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

    /// Records an instantaneous gauge bitcast value.
    pub fn record_gauge(&self, name: &str, bits: u64) {
        let key = self.format_key(name);
        self.metrics.insert(key, MetricValue::Gauge(bits));
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

    let Ok(offset) = usize::try_from(ptr) else {
        tracing::warn!(ptr, "WASM guest memory pointer out of usize range");
        return None;
    };
    let Ok(raw_len) = usize::try_from(len) else {
        tracing::warn!(len, "WASM guest memory length out of usize range");
        return None;
    };

    let mem_data = memory.data(caller);

    let Some(total_end) = offset.checked_add(raw_len) else {
        tracing::warn!(
            offset,
            raw_len,
            "WASM guest memory address addition overflow"
        );
        return None;
    };

    if total_end > mem_data.len() {
        tracing::warn!(
            offset,
            raw_len,
            memory_len = mem_data.len(),
            "WASM guest memory read out of bounds"
        );
        return None;
    }

    let read_len = raw_len.min(max_len);
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
pub fn build_host_linker(
    engine: &Engine,
    _registry: Arc<MetricRegistry>,
) -> Result<Linker<HostState>, WasmTransformError> {
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
                METRIC_TYPE_COUNTER | METRIC_TYPE_DURATION => {
                    caller.data().registry.record_counter(&name, value);
                }
                METRIC_TYPE_GAUGE => {
                    caller.data().registry.record_gauge(&name, value);
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

            match level {
                LOG_LEVEL_ERROR => tracing::error!(target: "wasm_guest", "{msg}"),
                LOG_LEVEL_WARN => tracing::warn!(target: "wasm_guest", "{msg}"),
                LOG_LEVEL_INFO => tracing::info!(target: "wasm_guest", "{msg}"),
                LOG_LEVEL_DEBUG => tracing::debug!(target: "wasm_guest", "{msg}"),
                _ => tracing::trace!(target: "wasm_guest", "{msg}"),
            }
        },
    )?;

    Ok(linker)
}
