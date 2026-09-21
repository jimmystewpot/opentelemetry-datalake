//! Standardized TLS configuration for pipeline sinks and endpoints.

use crate::error::PipelineError;
use serde::{Deserialize, Serialize};

/// Certificate validation behavior for outbound TLS connections.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TlsVerificationMode {
    /// Full verification: validates both certificate authority chain and hostname (Default).
    #[default]
    Full,
    /// Disabled verification: accepts any certificate without validation (insecure; test/dev only).
    Disabled,
}

/// Standard TLS configuration shared across all pipeline sinks.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to a PEM-encoded custom CA certificate file.
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// Certificate verification mode: "full" (default) or "disabled".
    #[serde(default)]
    pub verification: TlsVerificationMode,

    /// Backward-compatibility alias for legacy `insecure_skip_verify = true/false`.
    #[serde(default)]
    pub insecure_skip_verify: Option<bool>,
}

impl TlsConfig {
    /// Returns true if certificate validation is explicitly disabled.
    #[must_use]
    pub fn is_insecure(&self) -> bool {
        if let Some(insecure) = self.insecure_skip_verify {
            insecure || self.verification == TlsVerificationMode::Disabled
        } else {
            self.verification == TlsVerificationMode::Disabled
        }
    }

    /// Validates CA certificate file accessibility at configuration load time.
    ///
    /// # Errors
    /// Returns [`PipelineError::Configuration`] if `ca_cert_path` is specified but the file does not exist.
    pub fn validate(&self) -> Result<(), PipelineError> {
        if let Some(ref path) = self.ca_cert_path {
            if !std::path::Path::new(path).exists() {
                return Err(PipelineError::Configuration(Box::new(
                    figment::Error::from(format!("CA certificate file not found: '{path}'")),
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default_is_secure() {
        let tls = TlsConfig::default();
        assert_eq!(tls.verification, TlsVerificationMode::Full);
        assert!(tls.ca_cert_path.is_none());
        assert!(!tls.is_insecure());
    }

    #[test]
    fn test_tls_config_deserializes_verification_disabled() {
        let toml_str = r#"verification = "disabled""#;
        let tls: TlsConfig = toml::from_str(toml_str).expect("should parse");
        assert_eq!(tls.verification, TlsVerificationMode::Disabled);
        assert!(tls.is_insecure());
    }

    #[test]
    fn test_tls_config_backward_compatibility_insecure_skip_verify() {
        let toml_str = r#"insecure_skip_verify = true"#;
        let tls: TlsConfig = toml::from_str(toml_str).expect("should parse");
        assert!(tls.is_insecure());

        let toml_false = r#"insecure_skip_verify = false"#;
        let tls_false: TlsConfig = toml::from_str(toml_false).expect("should parse");
        assert!(!tls_false.is_insecure());
    }

    #[test]
    fn test_tls_config_rejects_unknown_fields() {
        let toml_str = r#"unknown_field = "unexpected""#;
        let res: Result<TlsConfig, _> = toml::from_str(toml_str);
        assert!(res.is_err(), "TlsConfig must reject unknown fields");
    }

    #[test]
    fn test_tls_config_validate_ca_path() {
        let mut tls = TlsConfig::default();
        assert!(tls.validate().is_ok());

        tls.ca_cert_path = Some("/nonexistent/ca/path/cert.pem".to_string());
        assert!(tls.validate().is_err());
    }
}
