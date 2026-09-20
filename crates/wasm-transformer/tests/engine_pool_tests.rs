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

#[test]
fn test_pooling_allocator_instantiation_and_available_slots() {
    let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).expect("engine init"));
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let module = cache.compile_module(&wasm_bytes).expect("module compile");
    let pool = InstancePool::new(Arc::clone(&cache), module, 4);
    assert_eq!(pool.available_slots(), 4);
}

#[test]
fn test_module_generation_starts_at_zero() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    assert_eq!(cache.module_generation(), 0);
}

#[test]
fn test_module_getter_before_and_after_compilation() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    assert!(cache.module().is_none());

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let compiled = cache.compile_module(&wasm_bytes).expect("compile module");
    let retrieved = cache.module().expect("module should be present");
    assert!(Arc::ptr_eq(&compiled, &retrieved));
}

#[test]
fn test_engine_reference() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    let _engine = cache.engine();
}

#[test]
fn test_compile_invalid_wasm_bytes() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).expect("engine init");
    let invalid_bytes = b"not a wasm binary";
    let res = cache.compile_module(invalid_bytes);
    assert!(matches!(res, Err(WasmTransformError::Wasmtime(_))));
}

#[test]
fn test_error_variants() {
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
