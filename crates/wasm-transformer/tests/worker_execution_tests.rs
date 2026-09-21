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
    let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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
    let mut worker = WasmWorker::new(1, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
    assert!(matches!(outcome, WorkerOutcome::Discarded));
}

#[tokio::test]
async fn test_worker_handles_rejected_status_2_with_custom_message() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(reject_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(2, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Metrics(batch))
        .await
        .unwrap();
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
    let mut worker = WasmWorker::new(3, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Traces(batch))
        .await
        .unwrap();
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
    let mut worker = WasmWorker::new(4, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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

    let mut worker = WasmWorker::new(5, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    assert_eq!(worker.batches_processed(), 0);

    // Batch 1: processed count becomes 1 (< 2, no rejuvenation)
    let _ = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .await
        .unwrap();
    assert_eq!(worker.batches_processed(), 1);

    // Batch 2: processed count reaches 2, triggering soft rejuvenation (reset to 0)
    let _ = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
    assert_eq!(worker.batches_processed(), 0);
}

#[tokio::test]
async fn test_worker_hot_reload_on_generation_advance() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module_v1 = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(6, Arc::clone(&cache), module_v1, cfg).unwrap();
    let batch = create_test_record_batch();

    // Batch 1 uses V1 (passthrough)
    let outcome1 = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .await
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
    let outcome2 = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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

    let mut worker = WasmWorker::new(7, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .await
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
    let outcome2 = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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
    let mut worker = WasmWorker::new(8, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
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
    let mut worker = WasmWorker::new(9, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
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

    let err = WasmWorker::new(10, Arc::clone(&cache), module, cfg).unwrap_err();
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

    let worker = WasmWorker::new(11, Arc::clone(&cache), module, cfg).unwrap();
    assert_eq!(worker.rejuvenate_threshold_bytes(), 128 * 1024 * 1024);
}

#[tokio::test]
async fn test_worker_guards_excessive_batch_count() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(excessive_batch_count_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(12, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Guest batch count 1025 exceeds maximum allowed limit of 1024"),
        "Unexpected error: {err}"
    );
}

// Trap WAT module: executes unreachable instruction inside datalake_transform
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
async fn test_worker_rejuvenates_and_recovers_after_guest_trap() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let trap_module = cache
        .compile_module(&wat::parse_str(trap_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(20, Arc::clone(&cache), trap_module, cfg).unwrap();
    let batch = create_test_record_batch();

    // Batch 1: Traps inside datalake_transform
    let err = worker
        .execute_batch(SignalBatch::Logs(batch.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, WasmTransformError::Wasmtime(_)),
        "Expected WasmTransformError::Wasmtime, got: {err}"
    );

    // Now recompile with valid passthrough module into the cache and advance generation
    let _pass_module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();
    let _ = cache.advance_generation();

    // Batch 2: Should reload and execute cleanly without failing from poisoned instance
    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
}

// Module missing datalake_alloc export
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
    let err = WasmWorker::new(21, Arc::clone(&cache), module, cfg).unwrap_err();
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

    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let mut cfg = default_test_config();
    cfg.env_whitelist = vec!["ALLOWED_OTEL_KEY".to_string()];
    cfg.env
        .insert("STATIC_OVERRIDE".to_string(), "static_val".to_string());

    let worker = WasmWorker::new(30, Arc::clone(&cache), module, cfg).unwrap();
    let env = worker.filtered_env();

    assert!(!env.contains_key("HOST_API_KEY"));
    assert_eq!(
        env.get("ALLOWED_OTEL_KEY").map(String::as_str),
        Some("otel_ambient_val")
    );
    assert_eq!(
        env.get("STATIC_OVERRIDE").map(String::as_str),
        Some("static_val")
    );
}

#[test]
fn test_worker_debug_formatting_and_getters() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let worker = WasmWorker::new(42, Arc::clone(&cache), Arc::clone(&module), cfg).unwrap();

    let debug_str = format!("{worker:?}");
    assert!(debug_str.contains("WasmWorker"));
    assert!(debug_str.contains("id: 42"));
    assert!(debug_str.contains("batches_processed: 0"));

    assert_eq!(worker.id, 42);
    assert!(Arc::ptr_eq(worker.module(), &module));
    assert_eq!(worker.config().id, "worker_test");
    let _instance = worker.instance();
}

#[tokio::test]
async fn test_worker_extracts_metrics_and_traces_signals() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(echo_single_batch_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(43, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    // Test Metrics signal extraction
    let outcome_metrics = worker
        .execute_batch(SignalBatch::Metrics(batch.clone()))
        .await
        .unwrap();
    match outcome_metrics {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            assert!(matches!(&batches[0], SignalBatch::Metrics(_)));
        }
        _ => panic!("Expected WorkerOutcome::Emitted for Metrics"),
    }

    // Test Traces signal extraction
    let outcome_traces = worker
        .execute_batch(SignalBatch::Traces(batch))
        .await
        .unwrap();
    match outcome_traces {
        WorkerOutcome::Emitted(batches) => {
            assert_eq!(batches.len(), 1);
            assert!(matches!(&batches[0], SignalBatch::Traces(_)));
        }
        _ => panic!("Expected WorkerOutcome::Emitted for Traces"),
    }
}

fn reject_no_msg_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
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
    let mut worker = WasmWorker::new(44, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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
            (drop (memory.grow (i32.const 2)))
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

    let mut worker = WasmWorker::new(45, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    assert_eq!(worker.batches_processed(), 0);

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
    assert!(matches!(outcome, WorkerOutcome::Emitted(_)));

    // After execution, memory growth exceeded 128 KiB, so rejuvenation was triggered and batches_processed reset to 0
    assert_eq!(worker.batches_processed(), 0);
}

fn missing_dealloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

fn missing_transform_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

fn missing_memory_wat() -> &'static str {
    r#"(module
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[test]
fn test_worker_fails_on_all_missing_required_exports() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let cfg = default_test_config();

    let mod_dealloc = cache
        .compile_module(&wat::parse_str(missing_dealloc_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(46, Arc::clone(&cache), mod_dealloc, cfg.clone()).unwrap_err();
    assert!(matches!(err, WasmTransformError::MissingExport(ref s) if s == "datalake_dealloc"));

    let mod_transform = cache
        .compile_module(&wat::parse_str(missing_transform_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(47, Arc::clone(&cache), mod_transform, cfg.clone()).unwrap_err();
    assert!(matches!(err, WasmTransformError::MissingExport(ref s) if s == "datalake_transform"));

    let mod_mem = cache
        .compile_module(&wat::parse_str(missing_memory_wat()).unwrap())
        .unwrap();
    let err = WasmWorker::new(48, Arc::clone(&cache), mod_mem, cfg).unwrap_err();
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
            (i32.store (i32.const 0) (i32.const 2))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.store (i32.const 8) (i32.const 0))
            (i32.store (i32.const 12) (i32.const 65530))
            (i32.store (i32.const 16) (i32.const 100))
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
    let mut worker = WasmWorker::new(49, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let outcome = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap();
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
    cfg.rejuvenate_threshold = "   ".into();

    let worker = WasmWorker::new(50, Arc::clone(&cache), module, cfg).unwrap();
    assert_eq!(worker.rejuvenate_threshold_bytes(), 0);
}

fn trap_alloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (unreachable))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
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
    let mut worker = WasmWorker::new(51, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
    assert!(matches!(err, WasmTransformError::Wasmtime(_)));
}

fn oob_header_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            ;; Return pointer near end of 64KiB (65530) where 20-byte header exceeds memory size
            (i32.const 65530)
        )
    )"#
}

#[tokio::test]
async fn test_worker_handles_oob_response_header() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
    let module = cache
        .compile_module(&wat::parse_str(oob_header_wat()).unwrap())
        .unwrap();

    let cfg = default_test_config();
    let mut worker = WasmWorker::new(52, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
    assert!(matches!(err, WasmTransformError::Pipeline(_)));
}

fn trap_dealloc_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32)
            (unreachable)
        )
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
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
    let mut worker = WasmWorker::new(60, Arc::clone(&cache), module, cfg).unwrap();
    let batch = create_test_record_batch();

    let err = worker
        .execute_batch(SignalBatch::Logs(batch))
        .await
        .unwrap_err();
    assert!(matches!(err, WasmTransformError::Wasmtime(_)));
    assert_eq!(worker.batches_processed(), 0);
}
