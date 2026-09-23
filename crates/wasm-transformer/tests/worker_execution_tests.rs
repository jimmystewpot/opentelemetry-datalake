//! Integration tests for the host worker execution engine.

use arrow::array::StringArray;
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
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
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
    assert_eq!(worker.local_generation(), 0);

    // Recompile with discard module (V2) and advance generation in cache
    let _module_v2 = cache
        .compile_module(&wat::parse_str(discard_wat()).unwrap())
        .unwrap();
    let new_gen = cache.advance_generation();
    assert_eq!(new_gen, 1);

    // Batch 2 should detect generation mismatch, reload module V2, and discard the batch
    let outcome2 = worker.execute_batch(SignalBatch::Logs(batch)).unwrap();
    assert!(matches!(outcome2, WorkerOutcome::Discarded));
    assert_eq!(worker.local_generation(), 1);
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
            ;; Write 20 bytes of zeros for TransformResponseHeader at offset 1024
            (i32.store (i32.const 1024) (i32.const 0))
            (i32.store (i32.const 1028) (i32.const 0))
            (i32.store (i32.const 1032) (i32.const 0))
            (i32.store (i32.const 1036) (i32.const 0))
            (i32.store (i32.const 1040) (i32.const 0))
            ;; (1024 << 32) | 20 = 0x0000_0400_0000_0014 = 4398046511124_i64
            (i64.const 4398046511124)
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
