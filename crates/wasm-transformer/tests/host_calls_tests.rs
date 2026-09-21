use std::sync::Arc;
use wasm_transformer::host_calls::{
    HostPhase, HostState, MAX_METRIC_NAME_LEN, MetricRegistry, build_host_linker,
};
use wasmtime::{Engine, Store};

#[test]
fn test_host_linker_defines_required_guest_imports() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("test_comp"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    assert_eq!(registry.read_counter("latency_nanos"), 7_500_000);
}

#[test]
fn test_edge_case_log_levels_1_through_4_and_default_trace() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("log_comp"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    assert_eq!(registry.read_gauge("cpu_usage"), Some(4607182418800017408));
}

#[test]
fn test_edge_case_memory_export_not_a_memory() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("not_mem"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

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
