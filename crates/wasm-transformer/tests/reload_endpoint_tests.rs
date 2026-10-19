use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::{path::PathBuf, sync::Arc};
use tower::ServiceExt;
use wasm_transformer::{
    engine::EngineCache,
    reload::{
        build_admin_router, build_admin_router_multi, build_admin_router_with_sha,
        spawn_sighup_listener, spawn_sighup_listener_multi,
    },
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
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[tokio::test]
async fn test_admin_router_reload_endpoint() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine, PathBuf::from("/tmp/non_existent.wasm"));
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
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

    let router = build_admin_router(Arc::clone(&engine), module_path.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
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

    let router = build_admin_router(Arc::clone(&engine), module_path.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
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

    let router = build_admin_router(Arc::clone(&engine), module_path.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"expected_sha": "{sha}"}}"#)))
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

    let router = build_admin_router(Arc::clone(&engine), module_path.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"expected_sha": "0000000000000000000000000000000000000000000000000000000000000000"}"#,
        ))
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
    let router = build_admin_router_with_sha(Arc::clone(&engine), module_path.clone(), Some(sha));
    // Omit expected_sha in payload to verify fallback to configured SHA
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
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
        module_path.clone(),
        Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
    );
    // Omit expected_sha in payload; configured SHA check must fail
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_multi_engine_reload_success() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "multi_engine_success_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine1 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let engine2 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert_eq!(engine1.module_generation(), 0);
    assert_eq!(engine2.module_generation(), 0);

    let router = build_admin_router_multi(
        vec![Arc::clone(&engine1), Arc::clone(&engine2)],
        module_path.clone(),
        None,
    );
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine1.module_generation(), 1);
    assert_eq!(engine2.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_sighup_listener_multi_returns_none_when_empty_or_disabled() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    assert!(
        spawn_sighup_listener_multi(vec![], PathBuf::from("/tmp/test.wasm"), None, true).is_none()
    );
    assert!(
        spawn_sighup_listener_multi(vec![engine], PathBuf::from("/tmp/test.wasm"), None, false)
            .is_none()
    );
}

#[tokio::test]
async fn test_admin_router_multi_engine_empty_fails() {
    let router = build_admin_router_multi(vec![], PathBuf::from("/tmp/any.wasm"), None);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_admin_router_multi_engine_matching_expected_sha() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "multi_engine_matching_sha_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();
    let sha = wasm_transformer::reload::compute_sha256(&wasm_bytes);

    let engine1 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let engine2 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

    let router = build_admin_router_multi(
        vec![Arc::clone(&engine1), Arc::clone(&engine2)],
        module_path.clone(),
        None,
    );
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"expected_sha": "{sha}"}}"#)))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine1.module_generation(), 1);
    assert_eq!(engine2.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_multi_engine_mismatching_expected_sha() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "multi_engine_mismatching_sha_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine1 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let engine2 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

    let router = build_admin_router_multi(
        vec![Arc::clone(&engine1), Arc::clone(&engine2)],
        module_path.clone(),
        None,
    );
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"expected_sha": "0000000000000000000000000000000000000000000000000000000000000000"}"#,
        ))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine1.module_generation(), 0);
    assert_eq!(engine2.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_typed_response() {
    use wasm_transformer::reload::WasmReloadResponse;

    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "typed_resp_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(Arc::clone(&engine), module_path.clone());
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let typed: WasmReloadResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(typed.status, "reload successful");
    assert_eq!(typed.path, module_path.to_str().unwrap());
    assert_eq!(typed.generation, 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_blank_module_path_rejected() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

    // Empty string path
    let router1 = build_admin_router(Arc::clone(&engine), PathBuf::from(""));
    let req1 = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let resp1 = router1.oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::BAD_REQUEST);

    // Whitespace string path
    let router2 = build_admin_router(engine, PathBuf::from("   \t\n  "));
    let req2 = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let resp2 = router2.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_admin_router_with_configured_sha_fallback_when_expected_sha_is_blank() {
    use wasm_transformer::reload::compute_sha256;

    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "blank_expected_sha_fallback_{}_{}.wasm",
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
    let router = build_admin_router_with_sha(Arc::clone(&engine), module_path.clone(), Some(sha));

    // Send whitespace expected_sha, should fall back to configured_sha and succeed
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"expected_sha": "   "}"#))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_with_whitespace_configured_sha_normalized_to_none() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "whitespace_configured_sha_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    // Pinned SHA is whitespace-only, which must be normalized to None
    let router = build_admin_router_with_sha(
        Arc::clone(&engine),
        module_path.clone(),
        Some("   \t \n  ".to_string()),
    );

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_trims_module_path_whitespace() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "trimmed_path_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let padded_path = PathBuf::from(format!("  \t {} \n  ", module_path.to_str().unwrap()));
    let router = build_admin_router(Arc::clone(&engine), padded_path);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(json["status"], "reload successful");
    assert_eq!(json["path"], module_path.to_str().unwrap());
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_configured_sha_cannot_be_bypassed_by_request_sha() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "pinned_sha_test_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    let actual_sha = wasm_transformer::reload::compute_sha256(&wasm_bytes);
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    // Pinned to an arbitrary hash that does not match this module
    let pinned_sha = "0000000000000000000000000000000000000000000000000000000000000000".to_string();
    let router =
        build_admin_router_with_sha(Arc::clone(&engine), module_path.clone(), Some(pinned_sha));

    // Request attempts to bypass pinned_sha by passing actual_sha in request body
    let payload = serde_json::json!({
        "expected_sha": actual_sha
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&payload).unwrap()))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    // Must be rejected with 400 Bad Request because pinned sha takes precedence!
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_rejects_path_traversal() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine, PathBuf::from("../../../../etc/passwd"));

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_admin_router_rejects_directory_path() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let temp_dir = std::env::temp_dir();
    let router = build_admin_router(engine, temp_dir);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_admin_router_rejects_empty_file() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "empty_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    tokio::fs::write(&module_path, b"").await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine, module_path.clone());

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_rejects_oversized_file() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "oversized_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Create a sparse file of 65 MiB (exceeding MAX_MODULE_SIZE of 64 MiB)
    let file = std::fs::File::create(&module_path).unwrap();
    file.set_len(65 * 1024 * 1024).unwrap();
    drop(file);

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine, module_path.clone());

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_rejects_client_supplied_module_path_field() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "pinned_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(Arc::clone(&engine), module_path.clone());

    let payload = serde_json::json!({
        "module_path": "/etc/passwd"
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&payload).unwrap()))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(engine.module_generation(), 0);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_reload_endpoint_accepts_empty_body() {
    let temp_dir = std::env::temp_dir();
    let module_path = temp_dir.join(format!(
        "empty_body_module_{}_{}.wasm",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
    tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(Arc::clone(&engine), module_path.clone());

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.module_generation(), 1);

    let _ = tokio::fs::remove_file(&module_path).await;
}

#[tokio::test]
async fn test_admin_router_rejects_malformed_json_body() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let router = build_admin_router(engine, PathBuf::from("/tmp/unused.wasm"));

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from("{malformed_json"))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_security_guard_wasm_reload_contract_and_review_docs() {
    use wasm_transformer::reload::WasmReloadRequest;

    for forbidden_key in ["module_path", "path", "file", "allowed_directory"] {
        let payload = serde_json::json!({ forbidden_key: "/tmp/evil.wasm" });
        assert!(
            serde_json::from_value::<WasmReloadRequest>(payload).is_err(),
            "WasmReloadRequest must reject forbidden field '{forbidden_key}'"
        );
    }

    let empty_req = serde_json::from_value::<WasmReloadRequest>(serde_json::json!({})).unwrap();
    assert_eq!(empty_req.expected_sha, None);

    let sha_req = serde_json::from_value::<WasmReloadRequest>(serde_json::json!({
        "expected_sha": "abc"
    }))
    .unwrap();
    assert_eq!(sha_req.expected_sha.as_deref(), Some("abc"));

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let agents_md = std::fs::read_to_string(workspace_root.join("AGENTS.md")).unwrap();
    assert!(
        agents_md.contains("rust/path-injection") && agents_md.contains("deny_unknown_fields"),
        "AGENTS.md must codify rust/path-injection and deny_unknown_fields safeguards"
    );

    let review_md =
        std::fs::read_to_string(workspace_root.join("CODE_REVIEW_INSTRUCTIONS.md")).unwrap();
    assert!(
        review_md.contains("rust/path-injection") && review_md.contains("deny_unknown_fields"),
        "CODE_REVIEW_INSTRUCTIONS.md must enforce rust/path-injection and deny_unknown_fields checks"
    );

    let admin_doc = std::fs::read_to_string(workspace_root.join("docs/administration.md")).unwrap();
    assert!(
        admin_doc.contains("/api/v1/transforms/wasm/reload")
            && admin_doc.contains("deny_unknown_fields"),
        "docs/administration.md must specify the pinned-path contract for /api/v1/transforms/wasm/reload"
    );
}
