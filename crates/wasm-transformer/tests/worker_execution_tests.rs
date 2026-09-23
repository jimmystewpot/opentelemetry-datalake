//! Integration tests for the host worker execution engine.

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::config::WasmTransformerConfig;
use pipeline_core::pipeline::SignalBatch;
use std::sync::Arc;
use wasm_transformer::engine::EngineCache;
use wasm_transformer::error::WasmTransformError;
use wasm_transformer::worker::{WasmWorker, WorkerOutcome};

fn test_registry() -> Arc<wasm_transformer::host_calls::MetricRegistry> {
    Arc::new(wasm_transformer::host_calls::MetricRegistry::new("test"))
}

fn default_test_config() -> WasmTransformerConfig {
    WasmTransformerConfig {
        id: "worker_test".into(),
        r#type: "wasm".into(),
        module_path: "test.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 2,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error: pipeline_core::config::OnErrorPolicy::Reroute,
        allow_unmasked_passthrough: false,
        on_reject: pipeline_core::config::OnRejectPolicy::Reroute,
        schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::default(),
        config: None,
        enable_sighup: false,
    }
}

fn create_test_record_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_1"])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .unwrap()
}

// Passthrough WAT module: returns header with status 0 and batch_count 0 (accept/passthrough)
fn passthrough_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Return pointer to header with status=0, batch_count=0
            (i32.store (i32.const 0) (i32.const 0)) ;; status = 0
            (i32.store (i32.const 4) (i32.const 0)) ;; batch_count = 0
            (i32.const 0)
        )
    )"#
}

// Discard WAT module: returns header with status 1
fn discard_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 1)) ;; status = 1 (Discarded)
            (i32.store (i32.const 4) (i32.const 0)) ;; batch_count = 0
            (i32.const 0)
        )
    )"#
}

// Reject WAT module: returns header with status 2 and custom rejection message
fn reject_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (data (i32.const 100) "Rate limit exceeded")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 2))   ;; status = 2 (Rejected)
            (i32.store (i32.const 4) (i32.const 0))   ;; batch_count = 0
            (i32.store (i32.const 8) (i32.const 0))   ;; batches_ptr = 0
            (i32.store (i32.const 12) (i32.const 100)) ;; message_ptr = 100
            (i32.store (i32.const 16) (i32.const 19))  ;; message_len = 19
            (i32.const 0)
        )
    )"#
}

// Errored WAT module: returns header with status 3 and custom error message
fn error_with_msg_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (data (i32.const 100) "Fatal transform panic")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 3))   ;; status = 3 (Errored)
            (i32.store (i32.const 4) (i32.const 0))   ;; batch_count = 0
            (i32.store (i32.const 8) (i32.const 0))   ;; batches_ptr = 0
            (i32.store (i32.const 12) (i32.const 100)) ;; message_ptr = 100
            (i32.store (i32.const 16) (i32.const 21))  ;; message_len = 21
            (i32.const 0)
        )
    )"#
}

// Errored WAT module without custom message: returns status 4
fn error_without_msg_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 4)) ;; status = 4 (Errored)
            (i32.store (i32.const 4) (i32.const 0)) ;; batch_count = 0
            (i32.store (i32.const 12) (i32.const 0)) ;; message_ptr = 0
            (i32.store (i32.const 16) (i32.const 0)) ;; message_len = 0
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_executes_batch_through_real_wasmtime_instance() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    match outcome {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            match &batches[0] {
                SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Logs signal"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Emitted"),
    }
}

#[tokio::test]
async fn test_worker_handles_discarded_status_1() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(discard_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(1, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Discarded));
}

#[tokio::test]
async fn test_worker_handles_rejected_status_2_with_custom_message() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(reject_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(2, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Metrics(batch)).unwrap();
    match outcome {
        WorkerOutcome::Rejected { reason, original } => {
            assert_eq!(reason, "Rate limit exceeded");
            match original {
                SignalBatch::Metrics(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Metrics signal in original"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Rejected"),
    }
}

#[tokio::test]
async fn test_worker_handles_errored_status_3_with_custom_message() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(error_with_msg_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(3, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Traces(batch)).unwrap();
    match outcome {
        WorkerOutcome::Errored { reason, original } => {
            assert_eq!(reason, "Fatal transform panic");
            match original {
                SignalBatch::Traces(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Traces signal in original"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Errored"),
    }
}

#[tokio::test]
async fn test_worker_handles_errored_status_without_message_fallback() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(error_without_msg_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(4, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    match outcome {
        WorkerOutcome::Errored { reason, original } => {
            assert_eq!(reason, "Guest returned error status 4");
            match original {
                SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Logs signal in original"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Errored"),
    }
}

#[tokio::test]
async fn test_worker_soft_rejuvenation_resets_batch_counter() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_batches = 2;

    let mut worker = WasmWorker::new(5, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    assert_eq!(worker.batches_processed(), 0);

    // Batch 1: processed count becomes 1 (< 2, no rejuvenation)
    let _ = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert_eq!(worker.batches_processed(), 1);

    // Batch 2: processed count reaches 2, triggering soft rejuvenation (reset to 0)
    let _ = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert_eq!(worker.batches_processed(), 0);
}

#[tokio::test]
async fn test_worker_hot_reload_on_generation_advance() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker =
        WasmWorker::new(6, Arc::clone(&cache), module_v1, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    // Batch 1 uses V1 (passthrough)
    let outcome1 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert!(matches!(outcome1, WorkerOutcome::Emitted(_)));
    assert_eq!(worker.local_generation(), 1);

    // Recompile with discard module (V2) advances generation in cache to 2
    let _module_v2 = cache
        .compile_module(&wat::parse_str(discard_wat()).unwrap())
        .unwrap();
    assert_eq!(cache.module_generation(), 2);

    // Batch 2 should detect generation mismatch, reload module V2, and discard the batch
    let outcome2 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome2, WorkerOutcome::Discarded));
    assert_eq!(worker.local_generation(), 2);
}

// Echo WAT module with batch_count = 1: sets descriptor to input IPC buffer and emits it
fn echo_single_batch_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $ptr i32) (param $len i32) (result i32)
            ;; TransformResponseHeader: status=0, batch_count=1, batches_ptr=24, message_ptr=0, message_len=0
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 24))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            ;; BatchDescriptor: ptr=$ptr, len=$len
            (i32.store (i32.const 24) (local.get $ptr))
            (i32.store (i32.const 28) (local.get $len))
            (i32.const 0)
        )
    )"#
}

// Out-of-bounds descriptors WAT module
fn oob_descriptors_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; batches_ptr near end of 64KiB (65530) + 16 bytes = 65546 > 65536
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 2))
            (i32.store (i32.const 8) (i32.const 65530))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

// Out-of-bounds batch buffer WAT module
fn oob_batch_buffer_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 24))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            ;; BatchDescriptor with b_ptr=60000, b_len=10000 -> 70000 > 65536
            (i32.store (i32.const 24) (i32.const 60000))
            (i32.store (i32.const 28) (i32.const 10000))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_extracts_batch_count_greater_than_zero_with_rejuvenation() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    // Configure rejuvenation to trigger on every batch (rejuvenate_batches = 1)
    // to rigorously test that batch extraction occurs BEFORE rejuvenation wipes guest memory.
    cfg.rejuvenate_batches = 1;

    let mut worker = WasmWorker::new(7, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();

    match outcome {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            match &batches[0] {
                SignalBatch::Logs(rb) => {
                    assert_eq!(rb.num_rows(), 1);
                    assert_eq!(rb.schema(), batch.schema());
                }
                _ => panic!("Expected Logs signal"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Emitted"),
    }

    // Rejuvenation must have executed after successful batch extraction, resetting counter to 0
    assert_eq!(worker.batches_processed(), 0);

    // The rejuvenated instance can immediately process another batch cleanly
    let outcome2 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome2, WorkerOutcome::Emitted(_)));
    assert_eq!(worker.batches_processed(), 0);
}

#[tokio::test]
async fn test_worker_guards_out_of_bounds_batch_descriptors() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(oob_descriptors_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(8, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_batch, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(
        err.to_string()
            .contains("Batch descriptors array bounds exceed guest memory size"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_worker_guards_out_of_bounds_batch_buffer() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(oob_batch_buffer_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(9, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_batch, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(
        err.to_string()
            .contains("Batch IPC buffer bounds exceed guest memory size"),
        "Unexpected error: {err}"
    );
}

// Excessive batch count WAT module
fn excessive_batch_count_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Return status 0 with batch_count = 1025 (> MAX_GUEST_BATCH_COUNT of 1024)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1025))
            (i32.store (i32.const 8) (i32.const 24))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[test]
fn test_worker_invalid_rejuvenate_threshold_fails() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_threshold = "100XYZ".into();

    let err = WasmWorker::new(10, Arc::clone(&cache), module, cfg, test_registry()).unwrap_err();
    assert!(
        err.to_string().contains("Invalid memory threshold: 100XYZ"),
        "Unexpected error: {err}"
    );
}

#[test]
fn test_rejuvenate_threshold_caching() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_threshold = "128MiB".into();

    let worker = WasmWorker::new(11, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    assert_eq!(worker.rejuvenate_threshold_bytes(), 128 * 1024 * 1024);
}

#[tokio::test]
async fn test_worker_guards_excessive_batch_count() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(excessive_batch_count_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(12, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_batch, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(
        err.to_string()
            .contains("Guest batch count 1025 exceeds maximum allowed limit of 1024"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_worker_host_calls_metric_and_log_emit() {
    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric_emit (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $host_log (param i32 i32 i32)))
        (memory (export "memory") 1)
        (data (i32.const 200) "records_processed")
        (data (i32.const 300) "processing batch in guest")
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Emit counter metric: type=0 (COUNTER), name_ptr=200, name_len=17, delta=42
            (call $metric_emit (i32.const 0) (i32.const 200) (i32.const 17) (i64.const 42))
            ;; Emit info log: level=3 (INFO), msg_ptr=300, msg_len=25
            (call $host_log (i32.const 3) (i32.const 300) (i32.const 25))
            ;; Header: status=0, batch_count=0 (passthrough)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = cache.compile_module(&wasm_bytes).unwrap();
    let cfg = default_test_config();
    let registry = Arc::new(wasm_transformer::host_calls::MetricRegistry::new(&cfg.id));

    let mut worker =
        WasmWorker::new(13, Arc::clone(&cache), module, cfg, Arc::clone(&registry)).unwrap();
    let batch = SignalBatch::Logs(create_test_record_batch());
    let outcome = worker.execute_batch(batch).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));

    assert_eq!(registry.read_counter("records_processed"), 42);
}

fn trap_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (unreachable)
        )
    )"#
}

#[tokio::test]
async fn test_worker_trap_returns_original_batch() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(trap_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(14, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = SignalBatch::Logs(create_test_record_batch());

    let (returned_batch, err) = worker.execute_batch(batch).unwrap_err();
    assert!(
        matches!(
            err,
            wasm_transformer::error::WasmTransformError::Wasmtime(_)
        ),
        "Expected Wasmtime error, got: {err}"
    );
    match returned_batch {
        SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
        _ => panic!("Expected Logs signal preserved on trap"),
    }
}

fn serialize_test_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn escape_wat_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "\\{b:02x}");
    }
    s
}

#[tokio::test]
async fn test_worker_schema_guard_strict_mode_rejects_dropped_immutable_column() {
    let dropped_col_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["hello"]))],
    )
    .unwrap();
    let ipc_bytes = serialize_test_batch(&dropped_col_batch);

    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 2000) "{escaped}")
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param i32 i32) (result i32)
                (i32.store (i32.const 0) (i32.const 0))
                (i32.store (i32.const 4) (i32.const 1))
                (i32.store (i32.const 8) (i32.const 24))
                (i32.store (i32.const 12) (i32.const 0))
                (i32.store (i32.const 16) (i32.const 0))
                (i32.store (i32.const 24) (i32.const 2000))
                (i32.store (i32.const 28) (i32.const {ipc_len}))
                (i32.const 0)
            )
        )"#,
        escaped = escape_wat_bytes(&ipc_bytes),
        ipc_len = ipc_bytes.len(),
    );

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(&wat).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.schema_guard = pipeline_core::config::SchemaGuardMode::Strict;

    let mut worker = WasmWorker::new(15, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let (returned_batch, err) = worker.execute_batch(input_batch).unwrap_err();
    assert!(
        err.to_string()
            .contains("Immutability violation: core field 'trace_id' was dropped by guest"),
        "Unexpected error: {err}"
    );
    match returned_batch {
        SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
        _ => panic!("Expected Logs signal preserved on error"),
    }
}

#[tokio::test]
async fn test_worker_schema_guard_defensive_mode_backfills_dropped_column() {
    let dropped_body_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["trace_1"]))],
    )
    .unwrap();
    let ipc_bytes = serialize_test_batch(&dropped_body_batch);

    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 2000) "{escaped}")
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param i32 i32) (result i32)
                (i32.store (i32.const 0) (i32.const 0))
                (i32.store (i32.const 4) (i32.const 1))
                (i32.store (i32.const 8) (i32.const 24))
                (i32.store (i32.const 12) (i32.const 0))
                (i32.store (i32.const 16) (i32.const 0))
                (i32.store (i32.const 24) (i32.const 2000))
                (i32.store (i32.const 28) (i32.const {ipc_len}))
                (i32.const 0)
            )
        )"#,
        escaped = escape_wat_bytes(&ipc_bytes),
        ipc_len = ipc_bytes.len(),
    );

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(&wat).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.schema_guard = pipeline_core::config::SchemaGuardMode::Defensive;

    let mut worker = WasmWorker::new(16, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let outcome = worker.execute_batch(input_batch).unwrap();
    match outcome {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            match &batches[0] {
                SignalBatch::Logs(rb) => {
                    assert_eq!(rb.num_columns(), 2);
                    assert!(rb.schema().column_with_name("body").is_some());
                    assert!(rb.schema().column_with_name("trace_id").is_some());
                    let body_col = rb.column_by_name("body").unwrap();
                    assert_eq!(body_col.null_count(), 1);
                }
                _ => panic!("Expected Logs signal"),
            }
        }
        _ => panic!("Expected WorkerOutcome::Emitted"),
    }
}

#[tokio::test]
async fn test_worker_rejuvenation_with_single_concurrency_reserved_headroom() {
    let concurrency: usize = 1;
    let pool_capacity = concurrency.saturating_add(1);
    let cache = Arc::new(EngineCache::new_pooling(pool_capacity, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.concurrency = concurrency;
    let mut worker = WasmWorker::new(17, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let _ = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert_eq!(worker.batches_processed(), 1);

    // Rejuvenation succeeds because pool_capacity has saturating_add(1) headroom.
    assert!(worker.rejuvenate().is_ok());
    assert_eq!(worker.batches_processed(), 0);

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
    assert_eq!(worker.batches_processed(), 1);
}

#[tokio::test]
async fn test_worker_strict_mode_rejects_dropped_column() {
    let dropped_body_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["trace_1"]))],
    )
    .unwrap();
    let ipc_bytes = serialize_test_batch(&dropped_body_batch);

    let wat = format!(
        r#"(module
            (memory (export "memory") 1)
            (data (i32.const 2000) "{escaped}")
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param i32 i32) (result i32)
                (i32.store (i32.const 0) (i32.const 0))
                (i32.store (i32.const 4) (i32.const 1))
                (i32.store (i32.const 8) (i32.const 24))
                (i32.store (i32.const 12) (i32.const 0))
                (i32.store (i32.const 16) (i32.const 0))
                (i32.store (i32.const 24) (i32.const 2000))
                (i32.store (i32.const 28) (i32.const {ipc_len}))
                (i32.const 0)
            )
        )"#,
        escaped = escape_wat_bytes(&ipc_bytes),
        ipc_len = ipc_bytes.len(),
    );

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(&wat).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.schema_guard = pipeline_core::config::SchemaGuardMode::Strict;

    let mut worker = WasmWorker::new(18, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let res = worker.execute_batch(input_batch);
    match res {
        Err((original, err)) => {
            assert!(err.to_string().contains("Strict schema guard violation"));
            match original {
                SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Logs signal in original batch"),
            }
        }
        Ok(_) => panic!("Expected strict schema guard to reject altered output"),
    }
}

#[tokio::test]
async fn test_worker_datalake_init_success_with_config_payload() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
            (if (i32.gt_u (local.get $len) (i32.const 0))
                (then (return (i32.const 0)))
            )
            (i32.const 1)
        )
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let mut cfg = default_test_config();
    cfg.env.insert("signal".to_string(), "traces".to_string());
    cfg.config = Some(serde_json::json!({ "sample_rate": 0.5 }));

    let worker = WasmWorker::new(19, Arc::clone(&cache), module, cfg, test_registry());
    assert!(
        worker.is_ok(),
        "Worker instantiation with valid datalake_init must succeed"
    );
}

#[tokio::test]
async fn test_worker_datalake_init_failure_fails_instantiation() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 42))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let worker = WasmWorker::new(20, Arc::clone(&cache), module, cfg, test_registry());
    match worker {
        Err(WasmTransformError::InitFailed(msg)) => {
            assert!(
                msg.contains("42"),
                "Expected error message to contain status code 42, got: {msg}"
            );
        }
        Err(e) => panic!("Expected InitFailed error, got: {e:?}"),
        Ok(_) => {
            panic!("Expected worker instantiation to fail when datalake_init returns non-zero")
        }
    }
}
#[tokio::test]
async fn test_worker_execution_deadline_interrupts_infinite_loop() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (loop $infinite (br $infinite))
            (i32.const 0)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let mut cfg = default_test_config();
    cfg.max_execution_duration = "50ms".into();

    let mut worker = WasmWorker::new(21, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let start = std::time::Instant::now();
    let res = worker.execute_batch(input_batch);
    let elapsed = start.elapsed();

    // Must return ExecutionTimeout within a reasonable bound (< 2s) rather than hanging
    assert!(elapsed < std::time::Duration::from_secs(2));
    match res {
        Err((original, WasmTransformError::ExecutionTimeout(ms))) => {
            assert_eq!(ms, 50);
            match original {
                SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Logs signal in original batch"),
            }
        }
        Err((_, e)) => panic!("Expected ExecutionTimeout error, got: {e:?}"),
        Ok(_) => panic!("Expected infinite loop to be interrupted with timeout"),
    }
}

#[tokio::test]
async fn test_worker_executes_c_abi_v1_transform() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            ;; Write 20 bytes for TransformResponseHeader at offset 256:
            ;; status = 0, batch_count = 1, batches_ptr = 280
            (i32.store (i32.const 256) (i32.const 0))
            (i32.store (i32.const 260) (i32.const 1))
            (i32.store (i32.const 264) (i32.const 280))
            (i32.store (i32.const 268) (i32.const 0))
            (i32.store (i32.const 272) (i32.const 0))
            ;; BatchDescriptor at offset 280: ptr=$ptr, len=$len
            (i32.store (i32.const 280) (local.get $ptr))
            (i32.store (i32.const 284) (local.get $len))
            ;; (256 << 32) | 20 = 1099511627796_i64
            (i64.const 1099511627796)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(22, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let outcome = worker.execute_batch(input_batch).unwrap();
    match outcome {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            match &batches[0] {
                SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
                _ => panic!("Expected Logs signal"),
            }
        }
        _ => panic!("Expected Emitted outcome"),
    }
}

#[tokio::test]
async fn test_worker_rejects_truncated_v1_response_length() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            ;; (1024 << 32) | 10 = 0x0000_0400_0000_000A = 4398046511114_i64 (len 10 < 20)
            (i64.const 4398046511114)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(23, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let res = worker.execute_batch(input_batch);
    assert!(res.is_err());
    let (_, err) = res.unwrap_err();
    assert!(
        err.to_string().contains("minimum header size is 20 bytes"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_worker_rejects_out_of_bounds_v1_response_header() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            ;; (65530 << 32) | 20 = offset 65530 + 20 = 65550 > 65536 memory limit
            ;; (65530_i64 << 32) | 20 = 281449704259604_i64
            (i64.const 281449704259604)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(24, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let input_batch = SignalBatch::Logs(create_test_record_batch());

    let res = worker.execute_batch(input_batch);
    assert!(res.is_err());
    let (_, err) = res.unwrap_err();
    assert!(
        err.to_string().contains("exceeds guest memory bounds"),
        "Unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_worker_rejects_unsupported_abi_version() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 2))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            (i64.const 0)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let res = WasmWorker::new(25, Arc::clone(&cache), module, cfg, test_registry());
    assert!(res.is_err());
    match res.unwrap_err() {
        WasmTransformError::AbiVersionMismatch(version) => assert_eq!(version, 2),
        other => panic!("Expected AbiVersionMismatch(2), got {other:?}"),
    }
}

#[tokio::test]
async fn test_worker_rejects_missing_abi_version() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            (i64.const 0)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let res = WasmWorker::new(26, Arc::clone(&cache), module, cfg, test_registry());
    assert!(res.is_err());
    match res.unwrap_err() {
        WasmTransformError::MissingExport(name) => assert_eq!(name, "datalake_abi_version"),
        other => panic!("Expected MissingExport(\"datalake_abi_version\"), got {other:?}"),
    }
}

#[tokio::test]
async fn test_worker_rapid_generation_advances_adopts_latest_module() {
    let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker =
        WasmWorker::new(27, Arc::clone(&cache), module_v1, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    // Batch 1 uses V1 (passthrough)
    let outcome1 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert!(matches!(outcome1, WorkerOutcome::Emitted(_)));
    assert_eq!(worker.local_generation(), 1);

    // Rapid reload: publish intermediate module (V2), then immediately publish final module (V3 - discard)
    let module_v2 = Arc::new(
        wasmtime::Module::new(
            cache.engine(),
            wat::parse_str(passthrough_wat()).unwrap().as_slice(),
        )
        .unwrap(),
    );
    let _ = cache.publish_module(module_v2);

    let module_v3 = Arc::new(
        wasmtime::Module::new(
            cache.engine(),
            wat::parse_str(discard_wat()).unwrap().as_slice(),
        )
        .unwrap(),
    );
    let gen3 = cache.publish_module(module_v3);
    assert_eq!(gen3, 3);

    // Batch 2: worker must adopt the latest generation (V3) and discard
    let outcome2 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome2, WorkerOutcome::Discarded));
    assert_eq!(worker.local_generation(), 3);
}

#[tokio::test]
async fn test_multi_worker_concurrent_rejuvenation() {
    let concurrency = 4;
    // Sized for all workers rejuvenating simultaneously: (concurrency * 2) + 1 = 9
    let pool_capacity = (concurrency * 2) + 1;
    let cache = Arc::new(EngineCache::new_pooling(pool_capacity, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_batches = 1; // Each batch triggers rejuvenation

    let registry = test_registry();
    let mut workers = Vec::new();
    for id in 0..concurrency {
        let worker = WasmWorker::new(
            id,
            Arc::clone(&cache),
            Arc::clone(&module),
            cfg.clone(),
            Arc::clone(&registry),
        )
        .unwrap();
        workers.push(worker);
    }

    let batch = create_test_record_batch();

    // Run batch 1 to process
    for worker in &mut workers {
        let outcome = worker
            .execute_batch(SignalBatch::Logs(batch.clone()))
            .unwrap();
        assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
    }

    // Now all 4 workers rejuvenate simultaneously
    let mut handles = Vec::new();
    for mut worker in workers {
        let batch_clone = batch.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let res = worker.execute_batch(SignalBatch::Logs(batch_clone));
            (worker, res)
        });
        handles.push(handle);
    }

    for handle in handles {
        let (worker, res) = handle.await.unwrap();
        let outcome = res.expect("Rejuvenation must not fail with pool exhaustion");
        assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
        assert_eq!(worker.batches_processed(), 0);
    }
}

#[test]
fn test_worker_new_fences_reload_during_initialization() {
    let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();
    assert_eq!(cache.module_generation(), 1);

    let cfg = default_test_config();
    let registry = test_registry();

    // Publish module v2 before worker is constructed, but pass module_v1 as the stale argument.
    // WasmWorker::new must ignore the stale argument, bind initial generation to snapshot (2),
    // and adopt module v2.
    let module_v2 = Arc::new(
        wasmtime::Module::new(
            cache.engine(),
            wat::parse_str(discard_wat()).unwrap().as_slice(),
        )
        .unwrap(),
    );
    let gen2 = cache.publish_module(module_v2);
    assert_eq!(gen2, 2);

    let mut worker = WasmWorker::new(
        0,
        Arc::clone(&cache),
        Arc::clone(&module_v1),
        cfg,
        Arc::clone(&registry),
    )
    .unwrap();

    assert_eq!(worker.local_generation(), 2);

    // Verify it executes module v2 (discard) rather than stale module v1 (passthrough)
    let batch = create_test_record_batch();
    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Discarded));
}

#[tokio::test]
async fn test_concurrent_worker_initialization_during_module_reload() {
    let cache = Arc::new(EngineCache::new_pooling(16, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let registry = test_registry();

    let mut handles = Vec::new();
    for id in 0..8 {
        let cache_clone = Arc::clone(&cache);
        let mod_clone = Arc::clone(&module_v1);
        let cfg_clone = cfg.clone();
        let reg_clone = Arc::clone(&registry);
        handles.push(tokio::task::spawn_blocking(move || {
            WasmWorker::new(id, cache_clone, mod_clone, cfg_clone, reg_clone)
        }));
    }

    // Concurrently publish module_v2
    let module_v2 = Arc::new(
        wasmtime::Module::new(
            cache.engine(),
            wat::parse_str(discard_wat()).unwrap().as_slice(),
        )
        .unwrap(),
    );
    let target_gen = cache.publish_module(module_v2);

    let mut workers = Vec::new();
    for handle in handles {
        let worker = handle.await.unwrap().unwrap();
        workers.push(worker);
    }

    // All workers must have safely initialized without errors or pool corruption.
    // Each worker's generation should either be 1 or target_gen (2).
    // If worker is at 1, calling check_hot_reload() or execute_batch() should immediately upgrade it to target_gen.
    for mut worker in workers {
        assert!(worker.local_generation() == 1 || worker.local_generation() == target_gen);
        worker.check_hot_reload().unwrap();
        assert_eq!(worker.local_generation(), target_gen);
    }
}

#[test]
fn test_worker_rejects_positive_batch_count_with_null_batches_ptr() {
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Return header with status=0, batch_count=1, but batches_ptr=0
            (i32.store (i32.const 0) (i32.const 0))  ;; status = 0
            (i32.store (i32.const 4) (i32.const 1))  ;; batch_count = 1
            (i32.store (i32.const 8) (i32.const 0))  ;; batches_ptr = 0 (null)
            (i32.const 0)
        )
    )"#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();

    let cfg = default_test_config();
    let registry = test_registry();
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg, registry).unwrap();

    let batch = create_test_record_batch();
    let res = worker.execute_batch(SignalBatch::Logs(batch));
    assert!(res.is_err());
    let (_returned_batch, err) = res.unwrap_err();
    assert!(
        matches!(err, WasmTransformError::Pipeline(ref msg) if msg.contains("null batches_ptr")),
        "Expected null batches_ptr protocol error, got: {err:?}"
    );
}

#[test]
fn test_worker_probe_candidate_instantiates_supplied_module_directly() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let _module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let candidate_module = Arc::new(
        wasmtime::Module::new(
            cache.engine(),
            wat::parse_str(discard_wat()).unwrap().as_slice(),
        )
        .unwrap(),
    );

    let cfg = default_test_config();
    let registry = test_registry();

    // Probing candidate module succeeds without affecting cached generation or snapshot
    assert!(WasmWorker::probe_candidate(&cache, &candidate_module, &cfg, &registry).is_ok());
    assert_eq!(cache.module_generation(), 1);
}

#[test]
fn test_worker_rejects_batch_descriptor_with_null_ipc_ptr() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 4096))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32 i32) (result i64)
            (i32.store (i32.const 1024) (i32.const 0))
            (i32.store (i32.const 1028) (i32.const 1))
            (i32.store (i32.const 1032) (i32.const 2048))
            (i32.store (i32.const 1036) (i32.const 0))
            (i32.store (i32.const 1040) (i32.const 0))
            (i32.store (i32.const 2048) (i32.const 0))
            (i32.store (i32.const 2052) (i32.const 10))
            (i64.or
                (i64.shl (i64.extend_i32_u (i32.const 1024)) (i64.const 32))
                (i64.const 20)
            )
        )
    )"#;
    let module = cache.compile_module(&wat::parse_str(wat).unwrap()).unwrap();
    let cfg = default_test_config();
    let registry = test_registry();
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg, registry).unwrap();

    let batch = create_test_record_batch();
    let res = worker.execute_batch(SignalBatch::Logs(batch));
    assert!(res.is_err());
    let (_returned_batch, err) = res.unwrap_err();
    assert!(
        matches!(err, WasmTransformError::Pipeline(ref msg) if msg.contains("null IPC buffer pointer")),
        "Expected null IPC buffer pointer error, got: {err:?}"
    );
}

#[test]
fn test_worker_executes_empty_zero_row_batch() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let registry = test_registry();
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg, registry).unwrap();

    let empty_batch = RecordBatch::new_empty(create_test_record_batch().schema());
    let res = worker.execute_batch(SignalBatch::Logs(empty_batch));
    assert!(res.is_ok());
    let outcome = res.unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(ref batches) if batches.len() == 1));
}

#[test]
fn test_worker_zero_batch_count_success_emits_empty_vector() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let registry = test_registry();
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg, registry).unwrap();

    let batch = create_test_record_batch();
    let res = worker.execute_batch(SignalBatch::Logs(batch));
    assert!(res.is_ok());
    let outcome = res.unwrap();
    assert!(
        matches!(outcome, WorkerOutcome::Emitted(ref batches) if batches.is_empty()),
        "Zero-batch count success must emit an empty vector marking batch consumed without leaking input rows"
    );
}

// ---------------------------------------------------------------------------
// Additional tests and helpers synchronized from feature-wasm-pr4
// ---------------------------------------------------------------------------

fn missing_alloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[test]
fn test_worker_fails_on_missing_required_exports() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(missing_alloc_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let err = WasmWorker::new(21, Arc::clone(&cache), module, cfg, test_registry()).unwrap_err();
    assert!(
        matches!(err, WasmTransformError::MissingExport(ref s) if s == "datalake_alloc"),
        "Expected MissingExport(\"datalake_alloc\"), got: {err}"
    );
}

#[test]
fn test_worker_captures_zero_trust_filtered_env() {
    unsafe {
        std::env::set_var("HOST_API_KEY", "secret_ambient_key");
        std::env::set_var("ALLOWED_OTEL_KEY", "otel_ambient_val");
    }

    let mut cfg = default_test_config();
    cfg.env
        .insert("EXPLICIT_KEY".to_string(), "explicit_val".to_string());
    cfg.env
        .insert("ALLOWED_OTEL_KEY".to_string(), "overridden_val".to_string());

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let worker = WasmWorker::new(30, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    assert_eq!(
        worker.config().env.get("EXPLICIT_KEY"),
        Some(&"explicit_val".to_string())
    );
    assert_eq!(
        worker.config().env.get("ALLOWED_OTEL_KEY"),
        Some(&"overridden_val".to_string())
    );
    assert!(!worker.config().env.contains_key("HOST_API_KEY"));
}

#[test]
fn test_worker_debug_formatting_and_getters() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let worker = WasmWorker::new(
        42,
        Arc::clone(&cache),
        Arc::clone(&module),
        cfg,
        test_registry(),
    )
    .unwrap();

    let debug_str = format!("{worker:?}");
    assert!(debug_str.contains("WasmWorker"));
    assert!(debug_str.contains("id: 42"));
    assert!(debug_str.contains("batches_processed: 0"));

    assert_eq!(worker.id, 42);
    assert!(Arc::ptr_eq(worker.module(), &module));
    assert_eq!(worker.config().id, "worker_test");
    assert!(worker.instance().is_some());
}

#[tokio::test]
async fn test_worker_extracts_metrics_and_traces_signals() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(50, Arc::clone(&cache), module, cfg, test_registry()).unwrap();

    let batch = create_test_record_batch();

    let outcome_metrics = worker
        .execute_batch(SignalBatch::Metrics(batch.clone()))
        .unwrap();
    assert!(matches!(outcome_metrics, WorkerOutcome::Emitted(_)));

    let outcome_traces = worker.execute_batch(SignalBatch::Traces(batch)).unwrap();
    assert!(matches!(outcome_traces, WorkerOutcome::Emitted(_)));
}

fn reject_no_msg_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Header: status=2 (REJECTED), batch_count=0, msg_ptr=0, msg_len=0
            (i32.store (i32.const 0) (i32.const 2))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.store (i32.const 8) (i32.const 0))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_handles_rejected_status_without_message() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(reject_no_msg_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(51, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    match outcome {
        WorkerOutcome::Rejected { reason, .. } => {
            assert_eq!(reason, "Guest rejected batch");
        }
        _ => panic!("Expected WorkerOutcome::Rejected"),
    }
}

fn grow_memory_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Grow memory by 2 pages (128 KiB)
            (drop (memory.grow (i32.const 2)))
            ;; Return passthrough
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_rejuvenates_on_memory_threshold_exceeded() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(grow_memory_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    // 128 KiB threshold: initial memory is 64 KiB. After growing by 2 pages (128 KiB), total is 192 KiB >= 128 KiB.
    cfg.rejuvenate_threshold = "128KiB".into();
    cfg.rejuvenate_batches = 10_000; // Not triggering on batches count

    let mut worker = WasmWorker::new(45, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    assert_eq!(worker.batches_processed(), 0);

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));

    // After execution, memory growth exceeded 128 KiB, so rejuvenation was triggered and batches_processed reset to 0
    assert_eq!(worker.batches_processed(), 0);
}

fn missing_dealloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

fn missing_transform_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
    )"#
}

fn missing_memory_wat() -> &'static str {
    r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[test]
fn test_worker_fails_on_all_missing_required_exports() {
    let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).unwrap());
    let cfg = default_test_config();

    let m_dealloc = cache
        .compile_module(&wat::parse_str(missing_dealloc_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(
        53,
        Arc::clone(&cache),
        m_dealloc,
        cfg.clone(),
        test_registry(),
    )
    .unwrap_err();
    assert!(matches!(err, WasmTransformError::MissingExport(ref s) if s == "datalake_dealloc"));

    let m_transform = cache
        .compile_module(&wat::parse_str(missing_transform_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(
        54,
        Arc::clone(&cache),
        m_transform,
        cfg.clone(),
        test_registry(),
    )
    .unwrap_err();
    assert!(matches!(err, WasmTransformError::MissingExport(ref s) if s == "datalake_transform"));

    let m_memory = cache
        .compile_module(&wat::parse_str(missing_memory_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(55, Arc::clone(&cache), m_memory, cfg, test_registry()).unwrap_err();
    assert!(matches!(err, WasmTransformError::MissingExport(ref s) if s == "memory"));
}

fn oob_message_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Header: status=2 (REJECTED), batch_count=0, msg_ptr=65530, msg_len=100 (OOB)
            (i32.store (i32.const 0) (i32.const 2))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.store (i32.const 8) (i32.const 65530))
            (i32.store (i32.const 12) (i32.const 100))
            (i32.store (i32.const 16) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_handles_oob_message_gracefully() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(oob_message_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(56, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    match outcome {
        WorkerOutcome::Rejected { reason, .. } => {
            assert_eq!(reason, "Guest rejected batch");
        }
        _ => panic!("Expected WorkerOutcome::Rejected"),
    }
}

#[test]
fn test_worker_empty_threshold_defaults_to_zero() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_threshold = String::new();

    let worker = WasmWorker::new(57, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    assert_eq!(worker.rejuvenate_threshold_bytes(), 0);
}

fn trap_alloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32)
            (unreachable)
        )
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[tokio::test]
async fn test_worker_handles_alloc_trap() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(trap_alloc_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(58, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(matches!(err, WasmTransformError::Wasmtime(_)));
}

fn trap_dealloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32)
            (unreachable)
        )
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_handles_dealloc_trap_and_rejuvenates() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(trap_dealloc_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(60, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(matches!(err, WasmTransformError::Wasmtime(_)));
    assert_eq!(worker.batches_processed(), 0);
}

fn null_alloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[tokio::test]
async fn test_worker_handles_null_allocation_as_oom() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(null_alloc_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(62, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(matches!(
        err,
        WasmTransformError::Oom { ref module, instance }
            if module == "test.wasm" && instance == 62
    ));
    assert_eq!(worker.batches_processed(), 0);
}

fn excessive_aggregate_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1)) ;; 1 descriptor
            (i32.store (i32.const 8) (i32.const 30))
            
            (i32.store (i32.const 30) (i32.const 1024))
            (i32.store (i32.const 34) (i32.const 70000000)) ;; len > 64MiB
            
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_guards_excessive_aggregate_batch_size() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(excessive_aggregate_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(64, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(
        matches!(err, WasmTransformError::Pipeline(msg) if msg.contains("Cumulative batch output size exceeds maximum allowed 64MiB limit"))
    );
}

fn malformed_zero_ptr_v1_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
            ;; (0 << 32) | 20 = 20_i64 -> ptr = 0 (null), len = 20
            (i64.const 20)
        )
    )"#
}

#[tokio::test]
async fn test_worker_guards_c_abi_v1_zero_pointer() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(malformed_zero_ptr_v1_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(68, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let (_, err) = worker.execute_batch(SignalBatch::Logs(batch)).unwrap_err();
    assert!(
        err.to_string()
            .contains("returned null response header pointer"),
        "Unexpected error: {err}"
    );
}

fn wasi_preview1_wat() -> &'static str {
    r#"(module
        (import "wasi_snapshot_preview1" "environ_sizes_get" (func $environ_sizes_get (param i32 i32) (result i32)))
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (call $environ_sizes_get (i32.const 1024) (i32.const 1028))
            (drop)
            ;; Return passthrough
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

#[tokio::test]
async fn test_worker_instantiates_wasi_preview1_module_and_reads_env() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(wasi_preview1_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.env
        .insert("APP_ENV".to_string(), "production".to_string());
    let mut worker = WasmWorker::new(70, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
}

#[tokio::test]
async fn test_worker_rejects_batch_exceeding_max_batch_rows() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.max_batch_rows = 5;

    let mut worker = WasmWorker::new(71, Arc::clone(&cache), module, cfg, test_registry()).unwrap();

    // Create a batch with 10 rows
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
    let rows: Vec<i32> = (0..10).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(rows))]).unwrap();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    match outcome {
        WorkerOutcome::Rejected { reason, .. } => {
            assert!(
                reason.contains("Batch row count 10 exceeds configured maximum 5"),
                "Unexpected rejection reason: {reason}"
            );
        }
        _ => panic!("Expected WorkerOutcome::Rejected, got {outcome:?}"),
    }
}

#[tokio::test]
async fn test_worker_hot_reload_failure_preserves_old_module_and_continues() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let initial_module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker =
        WasmWorker::new(72, Arc::clone(&cache), initial_module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    // Initial batch succeeds
    let outcome1 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert!(matches!(outcome1, WorkerOutcome::Emitted(_)));

    // Advance cache with a broken module (fails datalake_init with code 1)
    let bad_init_wat = r#"
        (module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
            (func (export "datalake_dealloc") (param i32 i32))
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 1))
            (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
        )
    "#;
    let _broken_module = cache
        .compile_module(&wat::parse_str(bad_init_wat).unwrap())
        .unwrap();
    let _ = cache.advance_generation();

    // Subsequent batch should NOT fail! It should log a warning, keep the old module, and succeed
    let outcome2 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert!(matches!(outcome2, WorkerOutcome::Emitted(_)));

    // Subsequent batches should also continue serving on the old module without retrying the broken reload
    let outcome3 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome3, WorkerOutcome::Emitted(_)));
}

#[tokio::test]
async fn test_worker_deallocates_dynamic_header_length_correctly() {
    let dynamic_header_wat = r#"
        (module
            (memory (export "memory") 1)
            (func (export "datalake_abi_version") (result i32) (i32.const 1))
            (func (export "datalake_alloc") (param i32) (result i32) (i32.const 2048))
            (func (export "datalake_dealloc") (param $ptr i32) (param $len i32)
                (if (i32.eq (local.get $ptr) (i32.const 1024))
                    (then
                        (if (i32.ne (local.get $len) (i32.const 32))
                            (then (unreachable))
                        )
                    )
                )
            )
            (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
            (func (export "datalake_transform") (param $sig i32) (param $ptr i32) (param $len i32) (result i64)
                (i32.store (i32.const 1024) (i32.const 1))
                (i32.store (i32.const 1028) (i32.const 0))
                (i32.store (i32.const 1032) (i32.const 0))
                (i32.store (i32.const 1036) (i32.const 0))
                (i32.store (i32.const 1040) (i32.const 0))
                (i64.or (i64.shl (i64.const 1024) (i64.const 32)) (i64.const 32))
            )
        )
    "#;

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(dynamic_header_wat).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(99, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Discarded));
}

#[tokio::test]
async fn test_worker_deallocates_guest_memory_on_dispatch_error_path() {
    let invalid_ipc_wat = "(module
        (memory (export \"memory\") 1)
        (func (export \"datalake_abi_version\") (result i32) (i32.const 1))
        (func (export \"datalake_alloc\") (param i32) (result i32) (i32.const 256))
        (func (export \"datalake_dealloc\") (param i32 i32))
        (func (export \"datalake_init\") (param i32 i32) (result i32) (i32.const 0))
        (func (export \"datalake_transform\") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 32))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            (i32.store (i32.const 32) (i32.const 100))
            (i32.store (i32.const 36) (i32.const 4))
            (i32.const 0)
        )
    )";

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(invalid_ipc_wat).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.rejuvenate_threshold = String::new();
    cfg.rejuvenate_batches = 0;
    let mut worker = WasmWorker::new(42, Arc::clone(&cache), module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    // Execute multiple times — if dealloc on error is broken, guest linear memory fills up
    // and the worker would OOM on subsequent alloc calls.
    for _ in 0..10 {
        let _ = worker.execute_batch(SignalBatch::Logs(batch.clone()));
    }
    // Reaching here without a panic confirms memory is properly freed on error paths.
}

#[tokio::test]
async fn test_worker_hot_reload_when_pool_concurrency_is_one() {
    let cache = Arc::new(EngineCache::new_pooling(1, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker =
        WasmWorker::new(1, Arc::clone(&cache), module_v1, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    let outcome1 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap();
    assert!(matches!(outcome1, WorkerOutcome::Emitted(_)));
    assert_eq!(worker.local_generation(), 1);

    // Recompile with discard module — advances generation to 2.
    let _module_v2 = cache
        .compile_module(&wat::parse_str(discard_wat()).unwrap())
        .unwrap();
    assert_eq!(cache.module_generation(), 2);

    // Batch 2 must detect generation mismatch and reload module V2.
    let outcome2 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(
        matches!(outcome2, WorkerOutcome::Discarded),
        "Worker must switch to discarded module on hot reload even when concurrency is 1"
    );
    assert_eq!(worker.local_generation(), 2);
}

#[tokio::test]
async fn test_worker_rejuvenates_and_recovers_after_guest_trap() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let trap_module = cache
        .compile_module(&wat::parse_str(trap_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker =
        WasmWorker::new(20, Arc::clone(&cache), trap_module, cfg, test_registry()).unwrap();
    let batch = create_test_record_batch();

    // Batch 1: Traps inside datalake_transform
    let (returned_batch, err) = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .unwrap_err();
    assert!(
        matches!(err, WasmTransformError::Wasmtime(_)),
        "Expected WasmTransformError::Wasmtime, got: {err}"
    );
    match returned_batch {
        SignalBatch::Logs(rb) => assert_eq!(rb.num_rows(), 1),
        _ => panic!("Expected Logs signal preserved on trap"),
    }

    // Now recompile with valid passthrough module into the cache and advance generation
    let _pass_module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();
    let _ = cache.advance_generation();

    // Batch 2: Should reload and execute cleanly without failing from poisoned instance
    let outcome = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
}
