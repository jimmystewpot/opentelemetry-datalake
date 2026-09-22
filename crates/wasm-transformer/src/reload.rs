//! Hot-reload utilities and SIGHUP signal listener for WebAssembly modules.

use hex::encode;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::task::JoinHandle;

use crate::engine::EngineCache;

/// Computes the lowercase hex-encoded SHA-256 digest of the provided bytes.
#[must_use]
pub fn compute_sha256(bytes: &[u8]) -> String {
    encode(Sha256::digest(bytes))
}

/// Spawns an asynchronous background task listening for `SIGHUP` signals to trigger module reload.
///
/// When receiving `SIGHUP`, the listener reads the module from `module_path` and offloads
/// module compilation and validation (including optional verification against `expected_sha`)
/// to a dedicated blocking task via [`tokio::task::spawn_blocking`].
///
/// If `enabled` is `false` or when compiled on non-Unix platforms, returns `None`.
#[must_use]
pub fn spawn_sighup_listener(
    engine: Arc<EngineCache>,
    module_path: PathBuf,
    expected_sha: Option<String>,
    enabled: bool,
) -> Option<JoinHandle<()>> {
    if !enabled {
        return None;
    }

    #[cfg(unix)]
    {
        Some(tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut stream = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Failed to install SIGHUP handler: {e}");
                    return;
                }
            };
            while stream.recv().await.is_some() {
                tracing::warn!(
                    path = %module_path.display(),
                    "SECURITY AUDIT: SIGHUP hot-reload triggered"
                );
                match tokio::fs::read(&module_path).await {
                    Ok(bytes) => {
                        let engine = Arc::clone(&engine);
                        let sha = expected_sha.clone();
                        let compile_res = tokio::task::spawn_blocking(move || {
                            engine.reload_from_bytes(&bytes, sha.as_deref())
                        })
                        .await;

                        match compile_res {
                            Ok(Ok(new_gen)) => {
                                tracing::info!(generation = new_gen, "Hot-reload successful");
                            }
                            Ok(Err(e)) => tracing::warn!("Hot-reload failed: {e}"),
                            Err(e) => tracing::warn!("Hot-reload blocking task panicked: {e}"),
                        }
                    }
                    Err(e) => tracing::warn!("Hot-reload: failed to read module: {e}"),
                }
            }
        }))
    }

    #[cfg(not(unix))]
    {
        let _ = (engine, module_path, expected_sha);
        None
    }
}

/// Request body for the WASM hot-reload REST endpoint.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WasmReloadRequest {
    /// Filesystem path to the new WebAssembly module.
    pub module_path: String,
    /// Optional expected hex-encoded SHA-256 digest of the new WebAssembly module.
    #[serde(default)]
    pub expected_sha: Option<String>,
}

/// State provided to the WASM hot-reload REST endpoint handler.
#[derive(Clone)]
pub struct WasmReloadState {
    /// Reference to the shared [`EngineCache`].
    pub engine: Arc<EngineCache>,
    /// Optional configured fallback SHA-256 digest pinned at initialization.
    pub configured_sha: Option<String>,
}

impl WasmReloadState {
    /// Creates a new [`WasmReloadState`] with the given engine and optional configured digest.
    #[must_use]
    pub fn new(engine: Arc<EngineCache>, configured_sha: Option<String>) -> Self {
        Self {
            engine,
            configured_sha,
        }
    }
}

impl From<Arc<EngineCache>> for WasmReloadState {
    fn from(engine: Arc<EngineCache>) -> Self {
        Self {
            engine,
            configured_sha: None,
        }
    }
}

/// Handler for the WASM hot-reload REST endpoint.
///
/// Extracts the JSON payload containing `module_path`, emits a security audit warning,
/// offloads compilation and atomic module swapping to [`tokio::task::spawn_blocking`],
/// and returns an acceptance response.
///
/// # Errors
///
/// Returns [`axum::http::StatusCode::INTERNAL_SERVER_ERROR`] if reading the module file fails
/// or if the background compilation task panics.
/// Returns [`axum::http::StatusCode::BAD_REQUEST`] if the module compilation or SHA-256 verification fails.
pub async fn wasm_reload_handler(
    axum::extract::State(state): axum::extract::State<WasmReloadState>,
    axum::Json(payload): axum::Json<WasmReloadRequest>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    tracing::warn!(
        path = %payload.module_path,
        "SECURITY AUDIT: REST hot-reload endpoint invoked"
    );

    let bytes = tokio::fs::read(&payload.module_path).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read module: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let engine = Arc::clone(&state.engine);
    let expected_sha = payload.expected_sha.or(state.configured_sha);

    tokio::task::spawn_blocking(move || engine.reload_from_bytes(&bytes, expected_sha.as_deref()))
        .await
        .map_err(|e| {
            tracing::warn!("Hot-reload REST blocking task panicked: {e}");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?
        .map_err(|e| {
            tracing::warn!("Hot-reload REST: reload failed: {e}");
            axum::http::StatusCode::BAD_REQUEST
        })?;

    Ok(axum::Json(serde_json::json!({
        "status": "reload successful",
        "path": payload.module_path,
    })))
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`.
pub fn build_admin_router(engine: Arc<EngineCache>) -> axum::Router {
    build_admin_router_with_sha(engine, None)
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`
/// with a fallback configured expected SHA-256 digest.
pub fn build_admin_router_with_sha(
    engine: Arc<EngineCache>,
    configured_sha: Option<String>,
) -> axum::Router {
    let state = WasmReloadState::new(engine, configured_sha);
    axum::Router::new()
        .route(
            "/api/v1/transforms/wasm/reload",
            axum::routing::post(wasm_reload_handler),
        )
        .with_state(state)
}
