use std::{path::PathBuf, sync::Arc};
use wasm_transformer::{
    engine::EngineCache,
    error::WasmTransformError,
    reload::{compute_sha256, spawn_sighup_listener},
};

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

fn valid_wat_v2() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 2))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[test]
fn test_reload_increments_generation_counter() {
    let cache = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(cache.module_generation(), 0);
    let bytes = wat::parse_str(valid_wat()).unwrap();
    let gen1 = cache.reload_from_bytes(&bytes, None).unwrap();
    assert_eq!(gen1, 1);
    let gen2 = cache.reload_from_bytes(&bytes, None).unwrap();
    assert_eq!(gen2, 2);
}

#[test]
fn test_reload_accepts_correct_sha256() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let bytes = wat::parse_str(valid_wat()).unwrap();
    let sha = compute_sha256(&bytes);
    assert!(cache.reload_from_bytes(&bytes, Some(&sha)).is_ok());
}

#[test]
fn test_reload_rejects_wrong_sha256() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let bytes = wat::parse_str(valid_wat()).unwrap();
    let res = cache.reload_from_bytes(&bytes, Some("deadbeefdeadbeef"));
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("SHA-256 mismatch"));
}

#[test]
fn test_monotonic_generation_increments_across_multiple_reloads() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let bytes1 = wat::parse_str(valid_wat()).unwrap();
    let bytes2 = wat::parse_str(valid_wat_v2()).unwrap();

    assert_eq!(cache.module_generation(), 0);

    for i in 1..=10 {
        let bytes = if i % 2 == 0 { &bytes2 } else { &bytes1 };
        let current_gen = cache.reload_from_bytes(bytes, None).unwrap();
        assert_eq!(current_gen, i);
        assert_eq!(cache.module_generation(), i);
    }
}

#[test]
fn test_case_insensitive_sha256_validation() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let bytes = wat::parse_str(valid_wat()).unwrap();
    let sha_lower = compute_sha256(&bytes);
    let sha_upper = sha_lower.to_ascii_uppercase();

    // Uppercase sha must pass
    let gen1 = cache.reload_from_bytes(&bytes, Some(&sha_upper)).unwrap();
    assert_eq!(gen1, 1);

    // Lowercase sha must pass
    let gen2 = cache.reload_from_bytes(&bytes, Some(&sha_lower)).unwrap();
    assert_eq!(gen2, 2);

    // Mixed case sha must pass
    let mut chars: Vec<char> = sha_lower.chars().collect();
    for (idx, ch) in chars.iter_mut().enumerate() {
        if idx % 2 == 0 {
            *ch = ch.to_ascii_uppercase();
        }
    }
    let sha_mixed: String = chars.into_iter().collect();
    let gen3 = cache.reload_from_bytes(&bytes, Some(&sha_mixed)).unwrap();
    assert_eq!(gen3, 3);
}

#[test]
fn test_compilation_failure_preserves_existing_module_and_generation() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let valid_bytes = wat::parse_str(valid_wat()).unwrap();

    let gen1 = cache.reload_from_bytes(&valid_bytes, None).unwrap();
    assert_eq!(gen1, 1);
    let orig_module = cache.module().unwrap();

    // 1. Invalid WASM bytes compilation error
    let invalid_bytes = b"not a valid wasm binary header";
    let res = cache.reload_from_bytes(invalid_bytes, None);
    assert!(matches!(res, Err(WasmTransformError::Wasmtime(_))));

    // Generation must remain unchanged
    assert_eq!(cache.module_generation(), 1);
    // Module must still point to original module
    let current_module = cache.module().unwrap();
    assert!(Arc::ptr_eq(&orig_module, &current_module));

    // 2. SHA mismatch error
    let res_sha = cache.reload_from_bytes(&valid_bytes, Some("badhash"));
    assert!(matches!(
        res_sha,
        Err(WasmTransformError::Sha256Mismatch { .. })
    ));

    // Generation and module must still be intact
    assert_eq!(cache.module_generation(), 1);
    assert!(Arc::ptr_eq(&orig_module, &cache.module().unwrap()));
}

#[test]
fn test_atomic_module_swap_replaces_active_module() {
    let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
    let bytes1 = wat::parse_str(valid_wat()).unwrap();
    let bytes2 = wat::parse_str(valid_wat_v2()).unwrap();

    let gen1 = cache.reload_from_bytes(&bytes1, None).unwrap();
    assert_eq!(gen1, 1);
    let mod1 = cache.module().unwrap();

    let gen2 = cache.reload_from_bytes(&bytes2, None).unwrap();
    assert_eq!(gen2, 2);
    let mod2 = cache.module().unwrap();

    // The two modules should be distinct Arc pointers
    assert!(!Arc::ptr_eq(&mod1, &mod2));
}

#[tokio::test]
async fn test_thread_safe_concurrent_reloads() {
    let cache = Arc::new(EngineCache::new_pooling(8, 32 * 1024 * 1024).unwrap());
    let bytes1 = Arc::new(wat::parse_str(valid_wat()).unwrap());
    let bytes2 = Arc::new(wat::parse_str(valid_wat_v2()).unwrap());

    let mut handles = Vec::new();
    let concurrent_tasks: u64 = 8;

    for i in 0..concurrent_tasks {
        let cache_clone = Arc::clone(&cache);
        let b1 = Arc::clone(&bytes1);
        let b2 = Arc::clone(&bytes2);
        handles.push(tokio::spawn(async move {
            let bytes = if i % 2 == 0 { &b1 } else { &b2 };
            cache_clone.reload_from_bytes(bytes, None)
        }));
    }

    let mut success_count: u64 = 0;
    for handle in handles {
        let res = handle.await.unwrap();
        assert!(res.is_ok());
        success_count += 1;
    }

    assert_eq!(success_count, concurrent_tasks);
    assert_eq!(cache.module_generation(), concurrent_tasks);
    assert!(cache.module().is_some());
}

#[test]
fn test_compute_sha256_known_vectors() {
    // Known SHA-256 for empty byte string
    let empty_sha = compute_sha256(b"");
    assert_eq!(
        empty_sha,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );

    // Known SHA-256 for "hello world"
    let hello_sha = compute_sha256(b"hello world");
    assert_eq!(
        hello_sha,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

#[test]
fn test_spawn_sighup_listener_disabled_returns_none() {
    let cache = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(cache, PathBuf::from("nonexistent.wasm"), None, false);
    assert!(handle.is_none());
}

#[tokio::test]
async fn test_spawn_sighup_listener_enabled() {
    let cache = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(cache, PathBuf::from("dummy.wasm"), None, true);

    #[cfg(unix)]
    {
        assert!(handle.is_some());
        if let Some(h) = handle {
            h.abort();
        }
    }

    #[cfg(not(unix))]
    {
        assert!(handle.is_none());
    }
}
