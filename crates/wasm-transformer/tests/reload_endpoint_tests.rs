use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::{path::PathBuf, sync::Arc};
use tower::ServiceExt;
use wasm_transformer::{
    engine::EngineCache,
    reload::{build_admin_router, build_admin_router_with_sha, spawn_sighup_listener},
};

#[tokio::test]
async fn test_sighup_listener_returns_none_when_disabled() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(engine, PathBuf::from("/tmp/test.wasm"), None, false);
    assert!(
        handle.is_none(),
        "disabled SIGHUP listener must return None"
    );
}

#[tokio::test]
async fn test_sighup_listener_returns_some_when_enabled() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(engine, PathBuf::from("/tmp/test.wasm"), None, true);
    #[cfg(unix)]
    {
        assert!(
            handle.is_some(),
            "enabled SIGHUP listener must return Some on unix"
        );
        if let Some(h) = handle {
            h.abort();
        }
    }
    #[cfg(not(unix))]
    {
        assert!(handle.is_none());
    }
}

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
async fn test_admin_router_reload_endpoint() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"module_path": "/tmp/non_existent.wasm"}"#))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    // Expecting 500 because file doesn't exist
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_success() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "test_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    let router = build_admin_router(Arc::clone(&engine));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}"}}"#,
            module_path.to_str().unwrap()
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(json["status"], "reload successful");
    assert_eq!(json["path"], module_path.to_str().unwrap());

    // Verify module generation was incremented
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_invalid_wasm() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "invalid_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    tokio::fs::write(&module_path, b"not a valid wasm file")
        .await
        .unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    let router = build_admin_router(Arc::clone(&engine));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}"}}"#,
            module_path.to_str().unwrap()
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    // Expecting 400 Bad Request because the wasm module compilation fails
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Verify module generation remained 0
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_matching_expected_sha() {
    use wasm_transformer::reload::compute_sha256;

    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "matching_sha_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let sha = compute_sha256(&wasm_bytes);
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    let router = build_admin_router(Arc::clone(&engine));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}", "expected_sha": "{}"}}"#,
            module_path.to_str().unwrap(),
            sha
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_mismatching_expected_sha() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "mismatching_sha_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    let router = build_admin_router(Arc::clone(&engine));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}", "expected_sha": "0000000000000000000000000000000000000000000000000000000000000000"}}"#,
            module_path.to_str().unwrap()
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    // Expecting 400 Bad Request because the sha verification fails
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_with_configured_sha_fallback_success() {
    use wasm_transformer::reload::compute_sha256;

    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "configured_sha_success_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let sha = compute_sha256(&wasm_bytes);
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    // Initialize router with configured SHA
    let router = build_admin_router_with_sha(Arc::clone(&engine), Some(sha));
    // Omit expected_sha in payload to verify fallback to configured SHA
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}"}}"#,
            module_path.to_str().unwrap()
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_with_configured_sha_fallback_mismatch() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "configured_sha_mismatch_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine.module_generation(), 0);

    // Initialize router with mismatching configured SHA
    let router = build_admin_router_with_sha(
        Arc::clone(&engine),
        Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
    );
    // Omit expected_sha in payload; configured SHA check must fail
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"module_path": "{}"}}"#,
            module_path.to_str().unwrap()
        )))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}
