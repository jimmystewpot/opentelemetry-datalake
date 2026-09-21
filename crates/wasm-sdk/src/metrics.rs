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

#[cfg(target_arch = "wasm32")]
// SAFETY: Declaring host runtime imports provided by the wasm-transformer host environment.
unsafe extern "C" {
    fn datalake_host_metric_emit(metric_type: u32, name_ptr: u32, name_len: u32, value: u64);
}

/// Emits a counter increment metric to the host runtime.
pub fn counter(name: &str, value: u64) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Passing valid UTF-8 string pointer and length within wasm32 linear memory.
        unsafe {
            #[allow(clippy::cast_possible_truncation)]
            datalake_host_metric_emit(
                METRIC_TYPE_COUNTER,
                name.as_ptr() as usize as u32,
                name.len() as u32,
                value,
            );
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (name, value);
    }
}

/// Emits an instantaneous gauge metric to the host runtime.
pub fn gauge(name: &str, value: f64) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Passing valid UTF-8 string pointer and length within wasm32 linear memory.
        unsafe {
            #[allow(clippy::cast_possible_truncation)]
            datalake_host_metric_emit(
                METRIC_TYPE_GAUGE,
                name.as_ptr() as usize as u32,
                name.len() as u32,
                gauge_to_bits(value),
            );
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (name, value);
    }
}

/// Emits a duration measurement metric in nanoseconds to the host runtime.
pub fn duration(name: &str, dur: Duration) {
    #[cfg(target_arch = "wasm32")]
    {
        // SAFETY: Passing valid UTF-8 string pointer and length within wasm32 linear memory.
        unsafe {
            #[allow(clippy::cast_possible_truncation)]
            datalake_host_metric_emit(
                METRIC_TYPE_DURATION,
                name.as_ptr() as usize as u32,
                name.len() as u32,
                duration_to_nanos(dur),
            );
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (name, dur);
    }
}
