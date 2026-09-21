//! Controlled guest metric emission functions and bitcast helpers.
//!
//! Provides functions to emit counters, gauges, and durations to the host
//! runtime across the C-ABI boundary using normalized scalar types.

use std::time::Duration;

/// Metric type identifier for monotonic counter increments.
pub const METRIC_TYPE_COUNTER: u32 = 0;

/// Metric type identifier for gauge bitcast values.
pub const METRIC_TYPE_GAUGE: u32 = 1;

/// Metric type identifier for duration nanosecond values.
pub const METRIC_TYPE_DURATION: u32 = 2;

/// Converts an `f64` gauge value into its raw IEEE 754 64-bit binary representation.
///
/// This bitcast preserves subnormals, negative zeros, infinities, and NaNs when transmitting
/// floating-point numbers across the C-ABI boundary using a 64-bit unsigned integer.
#[must_use]
pub fn gauge_to_bits(value: f64) -> u64 {
    value.to_bits()
}

/// Converts a [`Duration`] into nanoseconds represented as a `u64`.
///
/// Saturates to `u64::MAX` (~584 years) if the duration exceeds the representation limit,
/// preventing overflow panics and truncation under pedantic clippy rules.
#[must_use]
pub fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Strongly-typed handle for a pre-registered monotonic counter metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct CounterHandle(pub u32);

/// Strongly-typed handle for a pre-registered instantaneous gauge metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct GaugeHandle(pub u32);

/// Strongly-typed handle for a pre-registered duration metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DurationHandle(pub u32);

#[cfg(target_arch = "wasm32")]
// SAFETY: Declaring host runtime imports provided by the wasm-transformer host environment.
unsafe extern "C" {
    fn datalake_host_metric_register(metric_type: u32, name_ptr: u32, name_len: u32) -> u32;
    fn datalake_host_metric_emit(handle: u32, value: u64);
}

/// Registers a metric with the host runtime during module initialization and returns a handle integer.
#[must_use]
pub fn register_metric(metric_type: u32, name: &str) -> u32 {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Passing valid UTF-8 string pointer and length within wasm32 linear memory.
        unsafe {
            #[allow(clippy::cast_possible_truncation)]
            datalake_host_metric_register(
                metric_type,
                name.as_ptr() as usize as u32,
                name.len() as u32,
            )
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT_HANDLE: AtomicU32 = AtomicU32::new(1);
        let _ = (metric_type, name);
        NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
    }
}

/// Registers a monotonic counter metric and returns a strongly-typed [`CounterHandle`].
#[must_use]
pub fn register_counter(name: &str) -> CounterHandle {
    CounterHandle(register_metric(METRIC_TYPE_COUNTER, name))
}

/// Registers an instantaneous gauge metric and returns a strongly-typed [`GaugeHandle`].
#[must_use]
pub fn register_gauge(name: &str) -> GaugeHandle {
    GaugeHandle(register_metric(METRIC_TYPE_GAUGE, name))
}

/// Registers a duration nanosecond metric and returns a strongly-typed [`DurationHandle`].
#[must_use]
pub fn register_duration(name: &str) -> DurationHandle {
    DurationHandle(register_metric(METRIC_TYPE_DURATION, name))
}

/// Emits a counter increment metric using a pre-registered [`CounterHandle`].
pub fn counter(handle: CounterHandle, value: u64) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Invoking host metric emit with a valid pre-registered handle.
        unsafe {
            datalake_host_metric_emit(handle.0, value);
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (handle, value);
    }
}

/// Emits an instantaneous gauge metric using a pre-registered [`GaugeHandle`].
pub fn gauge(handle: GaugeHandle, value: f64) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Invoking host metric emit with a valid pre-registered handle and bitcasted f64.
        unsafe {
            datalake_host_metric_emit(handle.0, gauge_to_bits(value));
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (handle, value);
    }
}

/// Emits a duration measurement metric using a pre-registered [`DurationHandle`].
pub fn duration(handle: DurationHandle, dur: Duration) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Invoking host metric emit with a valid pre-registered handle and duration in nanos.
        unsafe {
            datalake_host_metric_emit(handle.0, duration_to_nanos(dur));
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (handle, dur);
    }
}
