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
/// When receiving `SIGHUP`, the listener reads the module from `module_path` and attempts
/// an atomic hot reload in the provided [`EngineCache`].
///
/// If `enabled` is `false` or when compiled on non-Unix platforms, returns `None`.
#[must_use]
pub fn spawn_sighup_listener(
    engine: Arc<EngineCache>,
    module_path: PathBuf,
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
            loop {
                stream.recv().await;
                tracing::warn!(
                    path = %module_path.display(),
                    "SECURITY AUDIT: SIGHUP hot-reload triggered"
                );
                match tokio::fs::read(&module_path).await {
                    Ok(bytes) => match engine.reload_from_bytes(&bytes, None) {
                        Ok(new_gen) => {
                            tracing::info!(generation = new_gen, "Hot-reload successful");
                        }
                        Err(e) => tracing::warn!("Hot-reload failed: {e}"),
                    },
                    Err(e) => tracing::warn!("Hot-reload: failed to read module: {e}"),
                }
            }
        }))
    }

    #[cfg(not(unix))]
    {
        let _ = (engine, module_path);
        None
    }
}

/// Request body for the WASM hot-reload REST endpoint.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WasmReloadRequest {
    /// Filesystem path to the new WebAssembly module.
    pub module_path: String,
}

/// Handler for the WASM hot-reload REST endpoint.
///
/// Extracts the JSON payload containing `module_path`, emits a security audit warning,
/// attempts the reload, and returns an acceptance response.
pub async fn wasm_reload_handler(
    axum::extract::State(engine): axum::extract::State<Arc<EngineCache>>,
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

    engine.reload_from_bytes(&bytes, None).map_err(|e| {
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
    axum::Router::new()
        .route(
            "/api/v1/transforms/wasm/reload",
            axum::routing::post(wasm_reload_handler),
        )
        .with_state(engine)
}
