use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::{path::PathBuf, sync::Arc};
use tower::ServiceExt;
use wasm_transformer::{
    engine::EngineCache,
    reload::{build_admin_router, spawn_sighup_listener},
};

#[tokio::test]
async fn test_sighup_listener_returns_none_when_disabled() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(engine, PathBuf::from("/tmp/test.wasm"), false);
    assert!(
        handle.is_none(),
        "disabled SIGHUP listener must return None"
    );
}

#[tokio::test]
async fn test_sighup_listener_returns_some_when_enabled() {
    let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
    let handle = spawn_sighup_listener(engine, PathBuf::from("/tmp/test.wasm"), true);
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

#[tokio::test]
async fn test_admin_router_reload_endpoint() {
    let router = build_admin_router();
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/transforms/wasm/reload")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"module_path": "/path/to/module.wasm"}"#))
        .unwrap();

    let response = router.oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(json["status"], "reload accepted");
    assert_eq!(json["path"], "/path/to/module.wasm");
}
