//! Hot-reload utilities and SIGHUP signal listener for WebAssembly modules.

use hex::encode;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::task::JoinHandle;

use crate::engine::EngineCache;
use crate::error::WasmTransformError;

/// Computes the lowercase hex-encoded SHA-256 digest of the provided bytes.
#[must_use]
pub fn compute_sha256(bytes: &[u8]) -> String {
    encode(Sha256::digest(bytes))
}

/// Compiles and probes the candidate WebAssembly module on every engine, and if all succeed,
/// publishes the candidate module atomically to each engine.
fn stage_and_publish_module(
    engines: &[Arc<EngineCache>],
    bytes: &[u8],
    expected_sha: Option<&str>,
) -> Result<u64, WasmTransformError> {
    if let Some(expected) = expected_sha {
        let actual = compute_sha256(bytes);
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(WasmTransformError::Sha256Mismatch {
                expected: expected.to_string(),
                actual,
            });
        }
    }
    // Stage 1: compile & probe candidate module across every engine
    let mut candidates = Vec::with_capacity(engines.len());
    for engine in engines {
        let module = engine.compile_and_probe_candidate(bytes, None)?;
        candidates.push(module);
    }
    // Stage 2: atomically publish candidate module to each engine
    let mut last_gen = 0;
    for (engine, module) in engines.iter().zip(candidates) {
        last_gen = engine.publish_module(module);
    }
    Ok(last_gen)
}

/// Spawns an asynchronous background task listening for `SIGHUP` signals to trigger module reload
/// across multiple [`EngineCache`] instances simultaneously.
///
/// When receiving `SIGHUP`, the listener reads the module from `module_path` and offloads
/// module compilation and atomic swapping for each engine to [`tokio::task::spawn_blocking`].
///
/// If `enabled` is `false`, `engines` is empty, or when compiled on non-Unix platforms, returns `None`.
#[must_use]
pub fn spawn_sighup_listener_multi(
    engines: Vec<Arc<EngineCache>>,
    module_path: PathBuf,
    expected_sha: Option<String>,
    enabled: bool,
) -> Option<JoinHandle<()>> {
    if !enabled || engines.is_empty() {
        return None;
    }

    let expected_sha = expected_sha.filter(|s| !s.trim().is_empty());

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
                        let engines_clone = engines.clone();
                        let sha = expected_sha.clone();
                        let compile_res = tokio::task::spawn_blocking(move || {
                            stage_and_publish_module(&engines_clone, &bytes, sha.as_deref())
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
        let _ = (engines, module_path, expected_sha);
        None
    }
}

/// Spawns an asynchronous background task listening for `SIGHUP` signals to trigger module reload.
///
/// Convenience wrapper around [`spawn_sighup_listener_multi`] for a single [`EngineCache`].
///
/// If `enabled` is `false` or when compiled on non-Unix platforms, returns `None`.
#[must_use]
pub fn spawn_sighup_listener(
    engine: Arc<EngineCache>,
    module_path: PathBuf,
    expected_sha: Option<String>,
    enabled: bool,
) -> Option<JoinHandle<()>> {
    spawn_sighup_listener_multi(vec![engine], module_path, expected_sha, enabled)
}

/// Request body for the WASM hot-reload REST endpoint.
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct WasmReloadRequest {
    /// Optional expected hex-encoded SHA-256 digest of the new WebAssembly module.
    #[serde(default)]
    pub expected_sha: Option<String>,
}

/// Response body returned by the WASM hot-reload REST endpoint upon successful reload.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct WasmReloadResponse {
    /// Human-readable status message confirming successful reload.
    pub status: String,
    /// Filesystem path of the reloaded WebAssembly module.
    pub path: String,
    /// New active module generation counter across the reloaded engines.
    pub generation: u64,
}

/// Maximum allowable WebAssembly module file size (64 MiB).
pub const MAX_MODULE_SIZE: u64 = 64 * 1024 * 1024;

/// State provided to the WASM hot-reload REST endpoint handler.
#[derive(Clone)]
pub struct WasmReloadState {
    /// References to the shared [`EngineCache`] instances to update upon reload.
    pub engines: Vec<Arc<EngineCache>>,
    /// Server-configured filesystem path to the WebAssembly module.
    pub module_path: PathBuf,
    /// Optional configured fallback SHA-256 digest pinned at initialization.
    pub configured_sha: Option<String>,
}

impl WasmReloadState {
    /// Creates a new [`WasmReloadState`] with a single engine, pinned module path, and optional configured digest.
    #[must_use]
    pub fn new(
        engine: Arc<EngineCache>,
        module_path: PathBuf,
        configured_sha: Option<String>,
    ) -> Self {
        Self::new_multi(vec![engine], module_path, configured_sha)
    }

    /// Creates a new [`WasmReloadState`] with multiple engines, pinned module path, and optional configured digest.
    #[must_use]
    pub fn new_multi(
        engines: Vec<Arc<EngineCache>>,
        module_path: PathBuf,
        configured_sha: Option<String>,
    ) -> Self {
        Self {
            engines,
            module_path,
            configured_sha: configured_sha.filter(|s| !s.trim().is_empty()),
        }
    }
}

/// Sanitizes, bounds, and reads the WebAssembly module file at `module_path`.
async fn validate_and_read_module(
    module_path: &std::path::Path,
) -> Result<Vec<u8>, axum::http::StatusCode> {
    if module_path.as_os_str().is_empty() || module_path.to_string_lossy().trim().is_empty() {
        tracing::warn!("Hot-reload REST: empty or blank module_path configured");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    // Reject path traversal components (e.g. "../")
    if module_path
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        tracing::warn!(
            path = %module_path.display(),
            "Hot-reload REST: path traversal attempted"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let metadata = tokio::fs::metadata(module_path).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read module metadata: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if !metadata.is_file() {
        tracing::warn!(
            path = %module_path.display(),
            "Hot-reload REST: path is not a regular file"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let canonical_path = tokio::fs::canonicalize(module_path).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to canonicalize module path: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let file = tokio::fs::File::open(&canonical_path).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to open module file: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let file_meta = file.metadata().await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read open file metadata: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if !file_meta.is_file() {
        tracing::warn!(
            path = %canonical_path.display(),
            "Hot-reload REST: opened path is not a regular file"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if file_meta.len() == 0 {
        tracing::warn!(
            path = %canonical_path.display(),
            "Hot-reload REST: module file is empty"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if file_meta.len() > MAX_MODULE_SIZE {
        tracing::warn!(
            path = %canonical_path.display(),
            size = file_meta.len(),
            max_size = MAX_MODULE_SIZE,
            "Hot-reload REST: module file exceeds maximum allowed size"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let mut bytes = Vec::with_capacity(usize::try_from(file_meta.len()).unwrap_or(0));
    tokio::io::AsyncReadExt::read_to_end(
        &mut tokio::io::AsyncReadExt::take(file, MAX_MODULE_SIZE + 1),
        &mut bytes,
    )
    .await
    .map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read module: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if bytes.is_empty() || u64::try_from(bytes.len()).map_or(true, |len| len > MAX_MODULE_SIZE) {
        tracing::warn!(
            path = %canonical_path.display(),
            size = bytes.len(),
            max_size = MAX_MODULE_SIZE,
            "Hot-reload REST: read module bytes invalid or exceeded maximum allowed size"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    Ok(bytes)
}

/// Handler for the WASM hot-reload REST endpoint.
///
/// Reloads the WebAssembly module from the server-configured `state.module_path`, emits a
/// security audit warning, offloads compilation and atomic module swapping across all configured
/// engines to [`tokio::task::spawn_blocking`], and returns an acceptance response.
///
/// # Errors
///
/// Returns [`axum::http::StatusCode::INTERNAL_SERVER_ERROR`] if reading the module file fails
/// or if the background compilation task panics.
/// Returns [`axum::http::StatusCode::BAD_REQUEST`] if the request body is malformed JSON or
/// contains unknown fields, if the configured `module_path` is empty or whitespace, if path
/// traversal is detected, if the path is a directory or empty file or exceeds [`MAX_MODULE_SIZE`],
/// if no engines are configured, or if the module compilation or SHA-256 verification fails.
pub async fn wasm_reload_handler(
    axum::extract::State(state): axum::extract::State<WasmReloadState>,
    body: axum::body::Bytes,
) -> Result<axum::Json<WasmReloadResponse>, axum::http::StatusCode> {
    let payload = if body.is_empty() {
        WasmReloadRequest::default()
    } else {
        serde_json::from_slice::<WasmReloadRequest>(&body).map_err(|e| {
            tracing::warn!("Hot-reload REST: invalid request payload: {e}");
            axum::http::StatusCode::BAD_REQUEST
        })?
    };

    let raw_path = state.module_path.to_string_lossy();
    let trimmed_path_str = raw_path.trim();
    let trimmed_path = std::path::Path::new(trimmed_path_str);
    tracing::warn!(
        path = %trimmed_path.display(),
        "SECURITY AUDIT: REST hot-reload endpoint invoked"
    );

    if state.engines.is_empty() {
        tracing::warn!("Hot-reload REST: no engines configured in state");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let bytes = validate_and_read_module(trimmed_path).await?;

    let engines = state.engines.clone();
    let expected_sha = if let Some(ref configured) = state.configured_sha {
        if let Some(ref req_sha) = payload.expected_sha.filter(|s| !s.trim().is_empty())
            && !req_sha.eq_ignore_ascii_case(configured)
        {
            tracing::warn!(
                req_sha = %req_sha,
                configured_sha = %configured,
                "Hot-reload REST: request-supplied digest conflicts with pinned configured SHA-256"
            );
            return Err(axum::http::StatusCode::BAD_REQUEST);
        }
        Some(configured.clone())
    } else {
        payload.expected_sha.filter(|s| !s.trim().is_empty())
    };

    let generation = tokio::task::spawn_blocking(move || {
        stage_and_publish_module(&engines, &bytes, expected_sha.as_deref())
    })
    .await
    .map_err(|e| {
        tracing::warn!("Hot-reload REST blocking task panicked: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        tracing::warn!("Hot-reload REST: reload failed: {e}");
        axum::http::StatusCode::BAD_REQUEST
    })?;

    Ok(axum::Json(WasmReloadResponse {
        status: "reload successful".to_string(),
        path: trimmed_path_str.to_string(),
        generation,
    }))
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`.
pub fn build_admin_router(engine: Arc<EngineCache>, module_path: PathBuf) -> axum::Router {
    build_admin_router_with_sha(engine, module_path, None)
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`
/// with a fallback configured expected SHA-256 digest.
pub fn build_admin_router_with_sha(
    engine: Arc<EngineCache>,
    module_path: PathBuf,
    configured_sha: Option<String>,
) -> axum::Router {
    build_admin_router_multi(vec![engine], module_path, configured_sha)
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`
/// across multiple [`EngineCache`] instances with an optional configured fallback SHA-256 digest.
pub fn build_admin_router_multi(
    engines: Vec<Arc<EngineCache>>,
    module_path: PathBuf,
    configured_sha: Option<String>,
) -> axum::Router {
    let state = WasmReloadState::new_multi(engines, module_path, configured_sha);
    axum::Router::new()
        .route(
            "/api/v1/transforms/wasm/reload",
            axum::routing::post(wasm_reload_handler),
        )
        .with_state(state)
}
