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
                match std::fs::read(&module_path) {
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
