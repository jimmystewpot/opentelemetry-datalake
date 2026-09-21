use opentelemetry_datalake_wasm_sdk::metrics::{
    CounterHandle, DurationHandle, GaugeHandle, counter, duration, duration_to_nanos, gauge,
    gauge_to_bits, register_counter, register_duration, register_gauge,
};
use opentelemetry_datalake_wasm_sdk::panic::init_panic_hook;
use std::time::Duration;

#[test]
#[allow(clippy::float_cmp)]
fn test_gauge_f64_bitcast_preserves_negative_and_fractions() {
    let original = -12.375_f64;
    assert_eq!(f64::from_bits(gauge_to_bits(original)), original);
}

#[test]
#[allow(clippy::float_cmp)]
fn test_gauge_f64_special_values() {
    // Zero and negative zero
    assert_eq!(f64::from_bits(gauge_to_bits(0.0)), 0.0);
    assert_eq!(f64::from_bits(gauge_to_bits(-0.0)), -0.0);
    assert!(f64::from_bits(gauge_to_bits(-0.0)).is_sign_negative());

    // Infinities
    assert_eq!(f64::from_bits(gauge_to_bits(f64::INFINITY)), f64::INFINITY);
    assert_eq!(
        f64::from_bits(gauge_to_bits(f64::NEG_INFINITY)),
        f64::NEG_INFINITY
    );

    // NaN
    assert!(f64::from_bits(gauge_to_bits(f64::NAN)).is_nan());

    // Min and Max
    assert_eq!(f64::from_bits(gauge_to_bits(f64::MIN)), f64::MIN);
    assert_eq!(f64::from_bits(gauge_to_bits(f64::MAX)), f64::MAX);
}

#[test]
fn test_duration_standardized_on_nanos() {
    assert_eq!(
        duration_to_nanos(Duration::from_millis(1500)),
        1_500_000_000_u64
    );
    assert_eq!(duration_to_nanos(Duration::ZERO), 0_u64);
    assert_eq!(duration_to_nanos(Duration::from_nanos(42)), 42_u64);
    assert_eq!(
        duration_to_nanos(Duration::from_secs(10)),
        10_000_000_000_u64
    );
}

#[test]
fn test_duration_overflow_saturates_u64_max() {
    // Duration::MAX in nanos is way larger than u64::MAX (which is ~584 years).
    // duration_to_nanos must not panic or truncate; it saturates at u64::MAX.
    let max_dur = Duration::MAX;
    assert_eq!(duration_to_nanos(max_dur), u64::MAX);
}

#[test]
fn test_metric_emission_helpers_callable_native() {
    let c: CounterHandle = register_counter("guest_records_transformed_total");
    let g: GaugeHandle = register_gauge("guest_memory_usage_ratio");
    let d: DurationHandle = register_duration("guest_batch_processing_duration_ns");

    // Verify distinct handles are generated on native targets
    assert_ne!(c.0, g.0);
    assert_ne!(g.0, d.0);
    assert_ne!(c.0, d.0);

    // Emitting metrics via registered typed handles on native host is a safe no-op
    counter(c, 42);
    gauge(g, 0.75);
    duration(d, Duration::from_millis(15));
}

#[test]
fn test_init_panic_hook_handles_panics() {
    let prev_hook = std::panic::take_hook();
    init_panic_hook();

    let caught = std::panic::catch_unwind(|| {
        panic!("simulated test panic to verify hook integration");
    });
    std::panic::set_hook(prev_hook);
    assert!(caught.is_err());
}
