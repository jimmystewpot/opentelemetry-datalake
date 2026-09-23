//! Helper utilities for CLI parsing, config resolution, and WASM error conversions.

use anyhow::Result;
use std::path::Path;

/// Converts any [`std::fmt::Display`] error into an [`anyhow::Error`].
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`).
pub fn wasm_err<E: std::fmt::Display>(err: E) -> anyhow::Error {
    anyhow::anyhow!("{err}")
}

/// Parses a telemetry signal name or code into its C-ABI v1 `u32` representation.
///
/// Supported inputs (case-insensitive):
/// - `"logs"`, `"log"`, `"0"` -> `0` (Logs)
/// - `"metrics"`, `"metric"`, `"1"` -> `1` (Metrics)
/// - `"traces"`, `"trace"`, `"2"` -> `2` (Traces)
///
/// # Concurrency Characteristics
///
/// This function is pure and thread-safe (`Send + Sync`).
///
/// # Errors
///
/// Returns an error if the input is not a recognized signal name or code.
pub fn parse_signal(s: &str) -> Result<u32> {
    match s.trim().to_lowercase().as_str() {
        "logs" | "log" | "0" => Ok(0),
        "metrics" | "metric" | "1" => Ok(1),
        "traces" | "trace" | "2" => Ok(2),
        other => {
            anyhow::bail!("Invalid signal type '{other}': expected 'logs', 'metrics', or 'traces'")
        }
    }
}

/// Resolves an optional CLI configuration argument into an initialization payload string.
///
/// - If `config_arg` is a valid file path, reads and returns the file content.
/// - If `config_arg` is provided as an inline string, uses it.
/// - If configuration is provided, it is parsed as a JSON object, and the CLI-selected
///   `signal` name is injected/merged into the top-level object so SDK-generated modules
///   and signal-specific transformers always receive the correct signal.
/// - If `config_arg` is `None` but `signal_code != 0`, synthesizes a minimal JSON config
///   `{"signal":"..."}` so that guest modules can detect the target signal type on initialization.
/// - If `config_arg` is `None` and `signal_code == 0`, returns `None`.
///
/// # Concurrency Characteristics
///
/// This function is thread-safe (`Send + Sync`). When reading from a file path, it performs
/// standard thread-safe read-only filesystem I/O.
///
/// # Errors
///
/// Returns an error if reading the specified configuration file fails, or if
/// the provided configuration payload is not a valid JSON object.
pub fn resolve_config_payload(
    config_arg: Option<&str>,
    signal_code: u32,
) -> Result<Option<String>> {
    let signal_name = match signal_code {
        1 => "metrics",
        2 => "traces",
        _ => "logs",
    };

    if let Some(arg) = config_arg {
        let raw_content = {
            let p = Path::new(arg);
            if p.exists() {
                std::fs::read_to_string(p)?
            } else {
                arg.to_string()
            }
        };

        let mut val: serde_json::Value = serde_json::from_str(&raw_content)
            .map_err(|e| anyhow::anyhow!("Configuration payload must be valid JSON: {e}"))?;

        match val {
            serde_json::Value::Object(ref mut map) => {
                map.insert(
                    "signal".to_string(),
                    serde_json::Value::String(signal_name.to_string()),
                );
                Ok(Some(serde_json::to_string(&val)?))
            }
            _ => anyhow::bail!("Configuration payload must be a JSON object"),
        }
    } else if signal_code != 0 {
        Ok(Some(format!(r#"{{"signal":"{signal_name}"}}"#)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_signal_valid_inputs() {
        assert_eq!(parse_signal("logs").unwrap(), 0);
        assert_eq!(parse_signal("Logs").unwrap(), 0);
        assert_eq!(parse_signal("log").unwrap(), 0);
        assert_eq!(parse_signal("0").unwrap(), 0);

        assert_eq!(parse_signal("metrics").unwrap(), 1);
        assert_eq!(parse_signal("Metrics").unwrap(), 1);
        assert_eq!(parse_signal("metric").unwrap(), 1);
        assert_eq!(parse_signal("1").unwrap(), 1);

        assert_eq!(parse_signal("traces").unwrap(), 2);
        assert_eq!(parse_signal("Traces").unwrap(), 2);
        assert_eq!(parse_signal("trace").unwrap(), 2);
        assert_eq!(parse_signal("2").unwrap(), 2);
    }

    #[test]
    fn test_parse_signal_invalid_inputs() {
        assert!(parse_signal("").is_err());
        assert!(parse_signal("unknown").is_err());
        assert!(parse_signal("3").is_err());
    }

    #[test]
    fn test_resolve_config_payload_inline_and_none() {
        assert_eq!(resolve_config_payload(None, 0).unwrap(), None);
        assert_eq!(
            resolve_config_payload(None, 1).unwrap(),
            Some(r#"{"signal":"metrics"}"#.to_string())
        );
        assert_eq!(
            resolve_config_payload(None, 2).unwrap(),
            Some(r#"{"signal":"traces"}"#.to_string())
        );

        let custom_res = resolve_config_payload(Some(r#"{"custom":"val"}"#), 0)
            .unwrap()
            .unwrap();
        let val: serde_json::Value = serde_json::from_str(&custom_res).unwrap();
        assert_eq!(val["custom"], "val");
        assert_eq!(val["signal"], "logs");

        // Overrides conflicting signal with CLI-selected signal
        let override_res = resolve_config_payload(Some(r#"{"signal":"logs","threshold":5}"#), 1)
            .unwrap()
            .unwrap();
        let val: serde_json::Value = serde_json::from_str(&override_res).unwrap();
        assert_eq!(val["threshold"], 5);
        assert_eq!(val["signal"], "metrics");
    }

    #[test]
    fn test_resolve_config_payload_invalid_json_or_non_object() {
        assert!(resolve_config_payload(Some("not-json"), 0).is_err());
        assert!(resolve_config_payload(Some("[1, 2, 3]"), 0).is_err());
        assert!(resolve_config_payload(Some("\"string\""), 0).is_err());
    }

    #[test]
    fn test_resolve_config_payload_from_file() {
        let temp_file =
            std::env::temp_dir().join(format!("test_config_{}.json", std::process::id()));
        std::fs::write(&temp_file, r#"{"test_file":true}"#).unwrap();

        let res = resolve_config_payload(Some(temp_file.to_str().unwrap()), 2)
            .unwrap()
            .unwrap();
        let _ = std::fs::remove_file(&temp_file);
        let val: serde_json::Value = serde_json::from_str(&res).unwrap();
        assert_eq!(val["test_file"], true);
        assert_eq!(val["signal"], "traces");
    }
}
