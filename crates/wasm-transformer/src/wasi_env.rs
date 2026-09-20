//! Zero-trust WASI environment variable filtering and configuration.

use std::collections::HashMap;
use tracing::warn;

/// Filters ambient environment variables against a whitelist and applies static configuration overrides.
///
/// Static variables take precedence over ambient environment variables matching the whitelist.
/// Ambient variables matching sensitive tokens (`SECRET`, `TOKEN`, `KEY`, `PASSWORD`, `CREDENTIAL`) that
/// are not included in the whitelist emit a masked security warning.
#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn filter_environment_variables(
    whitelist: &[String],
    static_env: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for key in whitelist {
        if let Ok(value) = std::env::var(key) {
            result.insert(key.clone(), value);
        }
    }
    for (key, value) in static_env {
        result.insert(key.clone(), value.clone());
    }
    let sensitive = ["SECRET", "TOKEN", "KEY", "PASSWORD", "CREDENTIAL"];
    for (key, _) in std::env::vars() {
        if !result.contains_key(&key) {
            let upper = key.to_uppercase();
            if sensitive.iter().any(|p| upper.contains(*p)) {
                warn!(
                    "WASI zero-trust: denied ambient env var matching sensitive pattern (key masked)"
                );
            }
        }
    }
    result
}
