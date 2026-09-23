use std::sync::Arc;
use wasm_transformer::host_calls::{
    HostPhase, HostState, MAX_METRIC_ENTRIES, MAX_METRIC_NAME_LEN, MetricRegistry,
    build_host_linker,
};
use wasmtime::{Engine, Store};

#[test]
fn test_host_linker_defines_required_guest_imports() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("test_comp"));
    let linker = build_host_linker(&engine).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (memory 1)
        (func (export "test_call")
            (call $metric (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 42))
            (call $log (i32.const 1) (i32.const 0) (i32.const 0))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let test_fn = instance
        .get_typed_func::<(), ()>(&mut store, "test_call")
        .unwrap();
    assert!(test_fn.call(&mut store, ()).is_ok());
}

#[test]
#[allow(clippy::float_cmp)]
fn test_metric_registry_counter_and_gauge_bitcast() {
    let registry = MetricRegistry::new("test_comp");
    registry.record_counter("test_cnt", 10);
    assert_eq!(registry.read_counter("test_cnt"), 10);

    let val = -4.25_f64;
    registry.record_gauge("test_gauge", val.to_bits());
    assert_eq!(
        f64::from_bits(registry.read_gauge("test_gauge").unwrap()),
        val
    );

    // Verify raw key format in DashMap
    assert!(
        registry
            .metrics()
            .contains_key("datalake_transformers_test_comp_test_cnt")
    );
    assert!(
        registry
            .metrics()
            .contains_key("datalake_transformers_test_comp_test_gauge")
    );
    assert_eq!(registry.component_id(), "test_comp");
}

#[test]
fn test_edge_case_missing_memory_export() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("missing_mem"));
    let linker = build_host_linker(&engine).unwrap();

    // Module with NO exported "memory"
    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (func (export "call_no_mem")
            (call $metric (i32.const 0) (i32.const 0) (i32.const 5) (i64.const 10))
            (call $log (i32.const 1) (i32.const 0) (i32.const 5))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Init,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "call_no_mem")
        .unwrap();

    // Call should safely complete without crashing or panicking
    assert!(func.call(&mut store, ()).is_ok());
    // Registry should have recorded nothing because memory was missing
    assert_eq!(registry.read_counter("test"), 0);
}

#[test]
fn test_edge_case_out_of_bounds_memory_reads() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("oob_comp"));
    let linker = build_host_linker(&engine).unwrap();

    // 1 page = 65,536 bytes
    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "call_oob")
            ;; Pointer beyond memory page (offset 70000)
            (call $metric (i32.const 0) (i32.const 70000) (i32.const 10) (i64.const 99))
            ;; Offset + len overflows memory (offset 65530 + len 20 = 65550 > 65536)
            (call $metric (i32.const 0) (i32.const 65530) (i32.const 20) (i64.const 88))
            ;; Pointer arithmetic overflow (u32::MAX)
            (call $metric (i32.const 0) (i32.const 4294967295) (i32.const 10) (i64.const 77))
            ;; Log with OOB pointer
            (call $log (i32.const 1) (i32.const 80000) (i32.const 100))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "call_oob")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    assert_eq!(registry.metrics().len(), 0);
}

#[test]
#[allow(clippy::float_cmp)]
fn test_edge_case_gauge_negative_float_preservation() {
    let registry = MetricRegistry::new("gauge_test");

    let special_values = [
        -123.456_f64,
        -0.0_f64,
        f64::NEG_INFINITY,
        f64::INFINITY,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
    ];

    for (i, &val) in special_values.iter().enumerate() {
        let name = format!("gauge_{i}");
        registry.record_gauge(&name, val.to_bits());
        let read_bits = registry.read_gauge(&name).unwrap();
        assert_eq!(f64::from_bits(read_bits), val);
        assert_eq!(read_bits, val.to_bits());
    }

    // NaN bit preservation
    let nan_val = f64::NAN;
    registry.record_gauge("nan_gauge", nan_val.to_bits());
    let read_nan = f64::from_bits(registry.read_gauge("nan_gauge").unwrap());
    assert!(read_nan.is_nan());

    // Non-existent gauge returns None
    assert_eq!(registry.read_gauge("unknown"), None);
}

#[test]
fn test_edge_case_duration_nanos_counter_recording() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("duration_comp"));
    let linker = build_host_linker(&engine).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (memory (export "memory") 1)
        (data (i32.const 10) "latency_nanos")
        (func (export "emit_duration")
            ;; metric_type = 2 (DURATION), name_ptr = 10, name_len = 13, value = 5000000
            (call $metric (i32.const 2) (i32.const 10) (i32.const 13) (i64.const 5000000))
            ;; Second emission accumulating 2500000
            (call $metric (i32.const 2) (i32.const 10) (i32.const 13) (i64.const 2500000))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_duration")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    // Duration is exported as a histogram, not a cumulative counter
    assert_eq!(registry.read_counter("latency_nanos"), 0);
    assert_eq!(registry.read_duration("latency_nanos"), Some(2_500_000));
    assert!(registry.handles().contains_key("latency_nanos"));
}

#[test]
fn test_edge_case_log_levels_1_through_4_and_default_trace() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("log_comp"));
    let linker = build_host_linker(&engine).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "test log payload")
        (func (export "emit_logs")
            ;; Level 1 (Error)
            (call $log (i32.const 1) (i32.const 0) (i32.const 16))
            ;; Level 2 (Warn)
            (call $log (i32.const 2) (i32.const 0) (i32.const 16))
            ;; Level 3 (Info)
            (call $log (i32.const 3) (i32.const 0) (i32.const 16))
            ;; Level 4 (Debug)
            (call $log (i32.const 4) (i32.const 0) (i32.const 16))
            ;; Level 5 (Trace)
            (call $log (i32.const 5) (i32.const 0) (i32.const 16))
            ;; Level 99 (Default fallback -> Trace)
            (call $log (i32.const 99) (i32.const 0) (i32.const 16))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_logs")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
}

#[test]
fn test_edge_case_concurrent_counter_increments() {
    let registry = Arc::new(MetricRegistry::new("concurrent_comp"));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let reg = Arc::clone(&registry);
        handles.push(std::thread::spawn(move || {
            for _ in 0..1000 {
                reg.record_counter("concurrent_hits", 1);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(registry.read_counter("concurrent_hits"), 10_000);
}

#[test]
fn test_edge_case_counter_saturating_add_and_type_override() {
    let registry = MetricRegistry::new("sat_comp");
    registry.record_counter("sat_cnt", u64::MAX - 5);
    registry.record_counter("sat_cnt", 10);
    assert_eq!(registry.read_counter("sat_cnt"), u64::MAX);

    // If a gauge existed with same key, record_counter overwrites with counter
    registry.record_gauge("shared_key", 1234);
    assert_eq!(registry.read_gauge("shared_key"), Some(1234));
    registry.record_counter("shared_key", 50);
    assert_eq!(registry.read_counter("shared_key"), 50);
    assert_eq!(registry.read_gauge("shared_key"), None);
}

#[test]
fn test_edge_case_metric_name_allocation_capping() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("cap_comp"));
    let linker = build_host_linker(&engine).unwrap();

    // Module with 300-byte string in memory.
    // Host should cap to MAX_METRIC_NAME_LEN (256 bytes) and record the clamped name.
    let mut wat = String::from(
        r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (memory (export "memory") 1)
        (data (i32.const 0) ""#,
    );
    for _ in 0..300 {
        wat.push('a');
    }
    wat.push_str(
        r#"")
        (func (export "emit_long_name")
            (call $metric (i32.const 0) (i32.const 0) (i32.const 300) (i64.const 77))
        )
    )"#,
    );

    let wasm_bytes = wat::parse_str(&wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_long_name")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());

    let capped_name = "a".repeat(MAX_METRIC_NAME_LEN);
    assert_eq!(registry.read_counter(&capped_name), 77);
}

#[test]
fn test_edge_case_unknown_metric_type_and_empty_name() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("empty_unknown"));
    let linker = build_host_linker(&engine).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (memory (export "memory") 1)
        (data (i32.const 0) "valid_metric")
        (func (export "test_edge")
            ;; Empty name (len = 0)
            (call $metric (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 10))
            ;; Unknown metric type (type = 99)
            (call $metric (i32.const 99) (i32.const 0) (i32.const 12) (i64.const 20))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "test_edge")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    // Empty name should not be recorded
    assert_eq!(registry.read_counter(""), 0);
    // Unknown metric type should not be recorded as counter or gauge
    assert_eq!(registry.read_counter("valid_metric"), 0);
    assert_eq!(registry.read_gauge("valid_metric"), None);
}

#[test]
fn test_edge_case_gauge_metric_emission_from_wasm() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("gauge_wasm"));
    let linker = build_host_linker(&engine).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (memory (export "memory") 1)
        (data (i32.const 0) "cpu_usage")
        (func (export "emit_gauge")
            ;; metric_type = 1 (GAUGE), name_ptr = 0, name_len = 9, value = 4607182418800017408 (1.0 f64 bits)
            (call $metric (i32.const 1) (i32.const 0) (i32.const 9) (i64.const 4607182418800017408))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_gauge")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    assert_eq!(registry.read_gauge("cpu_usage"), Some(1.0_f64.to_bits()));
}

#[test]
fn test_edge_case_memory_export_not_a_memory() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("not_mem"));
    let linker = build_host_linker(&engine).unwrap();

    // Module with export named "memory", but it is a global, not a linear memory
    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (global (export "memory") i32 (i32.const 42))
        (func (export "call_not_mem")
            (call $metric (i32.const 0) (i32.const 0) (i32.const 5) (i64.const 10))
            (call $log (i32.const 1) (i32.const 0) (i32.const 5))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Init,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "call_not_mem")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    assert_eq!(registry.read_counter("test"), 0);
}

#[test]
fn test_edge_case_log_message_allocation_capping() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("log_cap"));
    let linker = build_host_linker(&engine).unwrap();

    // Module with 2 memory pages (131,072 bytes) and 70,000-byte log message (exceeding MAX_LOG_MESSAGE_LEN)
    let wat = r#"(module
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (memory (export "memory") 2)
        (func (export "emit_long_log")
            (call $log (i32.const 3) (i32.const 0) (i32.const 70000))
        )
    )"#;

    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_long_log")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
}

#[test]
fn test_metric_registry_capacity_limit_prevents_unbounded_growth() {
    let registry = MetricRegistry::new("cap_test");

    // Populate up to capacity
    for i in 0..MAX_METRIC_ENTRIES {
        registry.record_counter(&format!("counter_{i}"), 1);
    }
    assert_eq!(registry.metrics().len(), MAX_METRIC_ENTRIES);

    // Attempting to add a new distinct entry must be dropped
    registry.record_counter("overflow_counter", 100);
    assert_eq!(registry.read_counter("overflow_counter"), 0);
    assert_eq!(registry.metrics().len(), MAX_METRIC_ENTRIES);

    registry.record_gauge("overflow_gauge", 42);
    assert_eq!(registry.read_gauge("overflow_gauge"), None);
    assert_eq!(registry.metrics().len(), MAX_METRIC_ENTRIES);

    // Updating existing entries still succeeds
    registry.record_counter("counter_0", 5);
    assert_eq!(registry.read_counter("counter_0"), 6);
}

#[test]
#[allow(clippy::float_cmp)]
fn test_bridge_metrics_to_opentelemetry_multi_signals() {
    let registry = Arc::new(MetricRegistry::new("test_signals"));
    registry.record_counter("processed_count", 100);
    let gauge_val = 1.5_f64;
    registry.record_gauge("heap_size", gauge_val.to_bits());

    let handle_logs =
        wasm_transformer::host_calls::bridge_metrics_to_opentelemetry(&registry, "logs");
    let handle_traces =
        wasm_transformer::host_calls::bridge_metrics_to_opentelemetry(&registry, "traces");
    let handle_metrics =
        wasm_transformer::host_calls::bridge_metrics_to_opentelemetry(&registry, "metrics");

    assert!(format!("{handle_logs:?}").contains("MetricBridgeHandle"));
    assert!(format!("{handle_traces:?}").contains("MetricBridgeHandle"));
    assert!(format!("{handle_metrics:?}").contains("MetricBridgeHandle"));

    assert_eq!(registry.read_counter("processed_count"), 100);
    assert_eq!(registry.read_gauge("heap_size"), Some(gauge_val.to_bits()));
    assert_eq!(
        f64::from_bits(registry.read_gauge("heap_size").unwrap()),
        gauge_val
    );
}

#[test]
fn test_metric_name_validation_rules() {
    let registry = Arc::new(MetricRegistry::new("valid_test"));
    // Valid names
    registry.record_counter("valid_counter_1", 10);
    assert_eq!(registry.read_counter("valid_counter_1"), 10);
    assert!(registry.handles().contains_key("valid_counter_1"));

    registry.record_gauge("valid_gauge_2", 100);
    assert_eq!(registry.read_gauge("valid_gauge_2"), Some(100));
    assert!(registry.handles().contains_key("valid_gauge_2"));

    registry.record_duration("valid_duration_3", 500);
    assert_eq!(registry.read_duration("valid_duration_3"), Some(500));
    assert!(registry.handles().contains_key("valid_duration_3"));

    // Invalid names: empty, special characters, too long
    registry.record_counter("", 1);
    assert_eq!(registry.read_counter(""), 0);

    registry.record_counter("invalid-name-with-dash", 1);
    assert_eq!(registry.read_counter("invalid-name-with-dash"), 0);
    assert!(!registry.handles().contains_key("invalid-name-with-dash"));

    registry.record_gauge("invalid.name.with.dots", 1);
    assert_eq!(registry.read_gauge("invalid.name.with.dots"), None);
    assert!(!registry.handles().contains_key("invalid.name.with.dots"));

    registry.record_duration("invalid name with spaces", 1);
    assert_eq!(registry.read_duration("invalid name with spaces"), None);
    assert!(!registry.handles().contains_key("invalid name with spaces"));

    let too_long = "a".repeat(65);
    registry.record_counter(&too_long, 1);
    assert_eq!(registry.read_counter(&too_long), 0);
    assert!(!registry.handles().contains_key(&too_long));
}

#[test]
fn test_custom_metric_prefixed_instruments_and_duration_histogram() {
    use wasm_transformer::host_calls::MetricHandle;

    let registry = Arc::new(MetricRegistry::with_signal("my_comp", "logs"));
    registry.record_counter("requests", 42);
    registry.record_gauge("queue_depth", 10.0_f64.to_bits());
    registry.record_duration("process_latency", 1_500_000_000);
    registry.record_duration("request_duration_seconds", 2_000_000_000);

    let handles = registry.handles();
    assert!(matches!(
        handles.get("requests").as_deref(),
        Some(MetricHandle::Counter(_))
    ));
    assert!(matches!(
        handles.get("queue_depth").as_deref(),
        Some(MetricHandle::Gauge(_))
    ));
    assert!(matches!(
        handles.get("process_latency").as_deref(),
        Some(MetricHandle::Histogram(_))
    ));
    assert!(matches!(
        handles.get("request_duration_seconds").as_deref(),
        Some(MetricHandle::Histogram(_))
    ));

    assert_eq!(registry.read_counter("requests"), 42);
    assert_eq!(registry.read_gauge("queue_depth"), Some(10.0_f64.to_bits()));
    assert_eq!(
        registry.read_duration("process_latency"),
        Some(1_500_000_000)
    );
    assert_eq!(
        registry.read_duration("request_duration_seconds"),
        Some(2_000_000_000)
    );
}

#[test]
fn test_cardinality_ceiling_50_metrics() {
    let registry = MetricRegistry::new("cap_50");
    for i in 0..50 {
        registry.record_counter(&format!("metric_{i}"), 1);
    }
    assert_eq!(registry.handles().len(), 50);

    // 51st metric must be rejected
    registry.record_counter("metric_50", 1);
    assert_eq!(registry.read_counter("metric_50"), 0);
    assert_eq!(registry.handles().len(), 50);
}

#[test]
fn test_concurrent_metric_registration_respects_capacity_limit() {
    let registry = Arc::new(MetricRegistry::new("concurrent_cap"));
    let mut handles = Vec::new();

    // Spawn 50 threads, each attempting to register 20 distinct metric names (1000 total unique names)
    for thread_id in 0..50 {
        let reg = Arc::clone(&registry);
        handles.push(std::thread::spawn(move || {
            for i in 0..20 {
                let name = format!("c_metric_{thread_id}_{i}");
                reg.record_counter(&name, 1);
                reg.record_gauge(&name, 100);
                reg.record_duration(&name, 500);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Must never exceed the 50-metric cardinality limit under high concurrency
    assert_eq!(
        registry.handles().len(),
        50,
        "Concurrent metric registration must strictly cap handles at 50"
    );
    assert_eq!(
        registry.metrics().len(),
        50,
        "Concurrent metric values must strictly cap at 50"
    );
}
