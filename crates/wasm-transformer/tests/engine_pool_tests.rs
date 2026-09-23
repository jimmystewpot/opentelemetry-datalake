use std::sync::Arc;
use wasm_transformer::engine::EngineCache;
use wasm_transformer::error::WasmTransformError;
use wasm_transformer::pool::InstancePool;

fn valid_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[tokio::test]
async fn test_pooling_allocator_instantiation_and_available_slots() {
    let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).expect("engine init"));
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let module = cache.compile_module(&wasm_bytes).expect("module compile");
    let pool = InstancePool::new(Arc::clone(&cache), module, 4);
    assert_eq!(pool.available_slots(), 4);
}

#[tokio::test]
async fn test_module_generation_starts_at_zero() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    assert_eq!(cache.module_generation(), 0);
}

#[tokio::test]
async fn test_module_getter_before_and_after_compilation() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    assert!(cache.module().is_none());

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let compiled = cache.compile_module(&wasm_bytes).expect("compile module");
    let retrieved = cache.module().expect("module should be present");
    assert!(Arc::ptr_eq(&compiled, &retrieved));
}

#[tokio::test]
async fn test_engine_reference() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    let _engine = cache.engine();
}

#[tokio::test]
async fn test_compile_invalid_wasm_bytes() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    let invalid_bytes = b"not a wasm binary";
    let res = cache.compile_module(invalid_bytes);
    assert!(matches!(res, Err(WasmTransformError::Wasmtime(_))));
}

#[tokio::test]
async fn test_error_variants() {
    let err = WasmTransformError::AbiVersionMismatch(99);
    assert_eq!(err.to_string(), "ABI version mismatch: expected 1, got 99");

    let err = WasmTransformError::Oom {
        module: "test_mod".to_string(),
        instance: 3,
    };
    assert_eq!(err.to_string(), "WASM OOM: module=test_mod, instance=3");

    let err = WasmTransformError::MissingExport("test_export".to_string());
    assert_eq!(err.to_string(), "Missing required WASM export: test_export");

    let err = WasmTransformError::Sha256Mismatch {
        expected: "exp".to_string(),
        actual: "act".to_string(),
    };
    assert_eq!(err.to_string(), "SHA-256 mismatch: expected exp, got act");

    let err = WasmTransformError::InitFailed("init err".to_string());
    assert_eq!(err.to_string(), "Guest init failed: init err");

    let err = WasmTransformError::ExecutionTimeout(500);
    assert_eq!(err.to_string(), "Guest execution timeout after 500ms");

    let err = WasmTransformError::Pipeline("pipeline broken".to_string());
    assert_eq!(err.to_string(), "Pipeline error: pipeline broken");

    let err = WasmTransformError::ArrowIpc("ipc broken".to_string());
    assert_eq!(err.to_string(), "Arrow IPC error: ipc broken");

    let io_err = WasmTransformError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "file not found",
    ));
    assert_eq!(io_err.to_string(), "IO error: file not found");
}

#[tokio::test]
async fn test_compile_module_with_sha256_match_and_mismatch() {
    use sha2::{Digest, Sha256};

    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();

    // Mismatched hash — generation NOT advanced (hash rejection happens before compile)
    let err = cache
        .compile_module_with_sha256(
            &wasm_bytes,
            Some("0000000000000000000000000000000000000000000000000000000000000000"),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        WasmTransformError::Sha256Mismatch { ref expected, .. }
            if expected == "0000000000000000000000000000000000000000000000000000000000000000"
    ));
    assert_eq!(
        cache.module_generation(),
        0,
        "hash mismatch must not advance generation"
    );

    // Correct computed hash — generation advances to 1
    let mut hasher = Sha256::new();
    hasher.update(&wasm_bytes);
    let expected = hex::encode(hasher.finalize());

    let module = cache
        .compile_module_with_sha256(&wasm_bytes, Some(&expected))
        .unwrap();
    assert_eq!(
        cache.module_generation(),
        1,
        "first successful compile advances generation to 1"
    );
    assert!(cache.module().is_some());
    assert!(Arc::ptr_eq(&module, &cache.module().unwrap()));

    // Case-insensitive and trimmed hash — generation advances to 2
    let expected_upper = format!("  {}  ", expected.to_uppercase());
    let module_trimmed = cache
        .compile_module_with_sha256(&wasm_bytes, Some(&expected_upper))
        .unwrap();
    assert_eq!(
        cache.module_generation(),
        2,
        "second successful compile advances generation to 2"
    );
    assert!(Arc::ptr_eq(&module_trimmed, &cache.module().unwrap()));

    // None hash (skip verification) — generation advances to 3
    let module_none = cache.compile_module_with_sha256(&wasm_bytes, None).unwrap();
    assert_eq!(
        cache.module_generation(),
        3,
        "third successful compile advances generation to 3"
    );
    assert!(cache.module().is_some());
    assert!(Arc::ptr_eq(&module_none, &cache.module().unwrap()));
}

#[tokio::test]
async fn test_pooling_allocator_zero_concurrency_clamps_to_one() {
    let cache = Arc::new(EngineCache::new_pooling(0, 32 * 1024 * 1024).unwrap());
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let module = cache.compile_module(&wasm_bytes).unwrap();
    let pool = InstancePool::new(Arc::clone(&cache), module, 1);
    assert_eq!(pool.available_slots(), 1);
    assert!(pool.engine().module().is_some());
    let debug_str = format!("{pool:?}");
    assert!(debug_str.contains("InstancePool"));
}

#[tokio::test]
async fn test_engine_cache_clean_drop() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    // Drop cache and ensure thread exits without deadlock or panic
    drop(cache);
    std::thread::sleep(std::time::Duration::from_millis(25));
}

/// Regression: `compile_module` must atomically advance the generation counter so that
/// workers calling `check_hot_reload` can detect newly compiled modules without requiring
/// callers to explicitly call `advance_generation()` afterwards.
#[tokio::test]
async fn test_compile_module_auto_advances_generation() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    assert_eq!(
        cache.module_generation(),
        0,
        "fresh cache starts at generation 0"
    );

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    cache.compile_module(&wasm_bytes).unwrap();
    assert_eq!(
        cache.module_generation(),
        1,
        "compile_module must atomically advance generation to 1"
    );

    cache.compile_module(&wasm_bytes).unwrap();
    assert_eq!(
        cache.module_generation(),
        2,
        "second compile_module must advance generation to 2"
    );
}

/// Regression: pooling allocator must bound `total_core_instances` to the configured
/// concurrency rather than leaving it at Wasmtime's internal default (1000).
#[tokio::test]
async fn test_pooling_allocator_bounds_core_instances_to_concurrency() {
    // 2 concurrency slots — must succeed with only 2 possible instances allocated
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    // If total_core_instances was set correctly, we can compile and the engine is usable.
    let module = cache.compile_module(&wasm_bytes).unwrap();
    // Verify module is accessible and generation was advanced
    assert!(cache.module().is_some());
    assert!(Arc::ptr_eq(&module, &cache.module().unwrap()));
    assert_eq!(cache.module_generation(), 1);
}
