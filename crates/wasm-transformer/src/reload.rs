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
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WasmReloadRequest {
    /// Filesystem path to the new WebAssembly module.
    pub module_path: String,
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
    /// Optional configured fallback SHA-256 digest pinned at initialization.
    pub configured_sha: Option<String>,
    /// Optional boundary directory restricting which WASM files may be reloaded.
    pub allowed_directory: Option<PathBuf>,
}

impl WasmReloadState {
    /// Creates a new [`WasmReloadState`] with a single engine and optional configured digest.
    #[must_use]
    pub fn new(engine: Arc<EngineCache>, configured_sha: Option<String>) -> Self {
        Self {
            engines: vec![engine],
            configured_sha: configured_sha.filter(|s| !s.trim().is_empty()),
            allowed_directory: None,
        }
    }

    /// Creates a new [`WasmReloadState`] with multiple engines and optional configured digest.
    #[must_use]
    pub fn new_multi(engines: Vec<Arc<EngineCache>>, configured_sha: Option<String>) -> Self {
        Self {
            engines,
            configured_sha: configured_sha.filter(|s| !s.trim().is_empty()),
            allowed_directory: None,
        }
    }

    /// Sets an allowed directory boundary for module reloading.
    ///
    /// Paths requested for reload will be verified to reside within this directory.
    #[must_use]
    pub fn with_allowed_directory(mut self, dir: PathBuf) -> Self {
        self.allowed_directory = Some(std::fs::canonicalize(&dir).unwrap_or(dir));
        self
    }
}

impl From<Arc<EngineCache>> for WasmReloadState {
    fn from(engine: Arc<EngineCache>) -> Self {
        Self::new(engine, None)
    }
}

/// Sanitizes, bounds, and reads the WebAssembly module file at `module_path`.
async fn validate_and_read_module(
    module_path: &str,
    allowed_directory: Option<&std::path::Path>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
    let path_obj = std::path::Path::new(module_path);

    // Reject path traversal components (e.g. "../")
    if path_obj
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        tracing::warn!(path = %module_path, "Hot-reload REST: path traversal attempted");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let metadata = tokio::fs::metadata(path_obj).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read module metadata: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if !metadata.is_file() {
        tracing::warn!(path = %module_path, "Hot-reload REST: path is not a regular file");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if metadata.len() == 0 {
        tracing::warn!(path = %module_path, "Hot-reload REST: module file is empty");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if metadata.len() > MAX_MODULE_SIZE {
        tracing::warn!(
            path = %module_path,
            size = metadata.len(),
            max_size = MAX_MODULE_SIZE,
            "Hot-reload REST: module file exceeds maximum allowed size"
        );
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let canonical_path = tokio::fs::canonicalize(path_obj).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to canonicalize module path: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(allowed_dir) = allowed_directory {
        let canonical_allowed = tokio::fs::canonicalize(allowed_dir)
            .await
            .unwrap_or_else(|_| allowed_dir.to_path_buf());
        if !canonical_path.starts_with(&canonical_allowed) {
            tracing::warn!(
                path = %canonical_path.display(),
                allowed = %canonical_allowed.display(),
                "Hot-reload REST: path outside allowed directory"
            );
            return Err(axum::http::StatusCode::BAD_REQUEST);
        }
    }

    tokio::fs::read(&canonical_path).await.map_err(|e| {
        tracing::warn!("Hot-reload REST: failed to read module: {e}");
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })
}

/// Handler for the WASM hot-reload REST endpoint.
///
/// Extracts the JSON payload containing `module_path`, emits a security audit warning,
/// offloads compilation and atomic module swapping across all configured engines to [`tokio::task::spawn_blocking`],
/// and returns an acceptance response.
///
/// # Errors
///
/// Returns [`axum::http::StatusCode::INTERNAL_SERVER_ERROR`] if reading the module file fails
/// or if the background compilation task panics.
/// Returns [`axum::http::StatusCode::BAD_REQUEST`] if `module_path` is empty or whitespace, if path traversal
/// is detected, if the path is a directory or empty file or exceeds [`MAX_MODULE_SIZE`], if outside the allowed directory,
/// if no engines are configured, or if the module compilation or SHA-256 verification fails.
pub async fn wasm_reload_handler(
    axum::extract::State(state): axum::extract::State<WasmReloadState>,
    axum::Json(payload): axum::Json<WasmReloadRequest>,
) -> Result<axum::Json<WasmReloadResponse>, axum::http::StatusCode> {
    let module_path = payload.module_path.trim();
    tracing::warn!(
        path = %module_path,
        "SECURITY AUDIT: REST hot-reload endpoint invoked"
    );

    if module_path.is_empty() {
        tracing::warn!("Hot-reload REST: empty or blank module_path provided");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    if state.engines.is_empty() {
        tracing::warn!("Hot-reload REST: no engines configured in state");
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }

    let bytes = validate_and_read_module(module_path, state.allowed_directory.as_deref()).await?;

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
        path: module_path.to_string(),
        generation,
    }))
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
    build_admin_router_multi(vec![engine], configured_sha)
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`
/// across multiple [`EngineCache`] instances with an optional configured fallback SHA-256 digest
/// and an optional boundary directory restricting which WASM files may be reloaded.
pub fn build_admin_router_multi_with_dir(
    engines: Vec<Arc<EngineCache>>,
    configured_sha: Option<String>,
    allowed_directory: Option<PathBuf>,
) -> axum::Router {
    let mut state = WasmReloadState::new_multi(engines, configured_sha);
    if let Some(dir) = allowed_directory {
        state = state.with_allowed_directory(dir);
    }
    axum::Router::new()
        .route(
            "/api/v1/transforms/wasm/reload",
            axum::routing::post(wasm_reload_handler),
        )
        .with_state(state)
}

/// Builds the admin axum [`axum::Router`] registering `POST /api/v1/transforms/wasm/reload`
/// across multiple [`EngineCache`] instances with an optional configured fallback SHA-256 digest.
pub fn build_admin_router_multi(
    engines: Vec<Arc<EngineCache>>,
    configured_sha: Option<String>,
) -> axum::Router {
    build_admin_router_multi_with_dir(engines, configured_sha, None)
}
