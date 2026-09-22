//! Helper utilities for CLI parsing, config resolution, and WASM error conversions.

use anyhow::Result;
use std::path::Path;

/// Converts any [`std::fmt::Display`] error into an [`anyhow::Error`].
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
/// - If `config_arg` is provided as an inline string, returns it directly.
/// - If `config_arg` is `None` but `signal_code != 0`, synthesizes a minimal JSON config
///   `{"signal":"..."}` so that guest modules can detect the target signal type on initialization.
/// - If `config_arg` is `None` and `signal_code == 0`, returns `None`.
///
/// # Errors
///
/// Returns an error if reading the specified configuration file fails.
pub fn resolve_config_payload(
    config_arg: Option<&str>,
    signal_code: u32,
) -> Result<Option<String>> {
    if let Some(arg) = config_arg {
        let p = Path::new(arg);
        if p.exists() {
            let content = std::fs::read_to_string(p)?;
            Ok(Some(content))
        } else {
            Ok(Some(arg.to_string()))
        }
    } else if signal_code != 0 {
        let signal_name = match signal_code {
            1 => "metrics",
            2 => "traces",
            _ => "logs",
        };
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
        assert_eq!(
            resolve_config_payload(Some(r#"{"custom":"val"}"#), 0).unwrap(),
            Some(r#"{"custom":"val"}"#.to_string())
        );
    }

    #[test]
    fn test_resolve_config_payload_from_file() {
        let temp_file =
            std::env::temp_dir().join(format!("test_config_{}.json", std::process::id()));
        std::fs::write(&temp_file, r#"{"test_file":true}"#).unwrap();

        let res = resolve_config_payload(Some(temp_file.to_str().unwrap()), 0).unwrap();
        let _ = std::fs::remove_file(&temp_file);
        assert_eq!(res, Some(r#"{"test_file":true}"#.to_string()));
    }
}
