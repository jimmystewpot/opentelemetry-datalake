//! TLS configuration for the Elasticsearch sink.

use serde::{Deserialize, Serialize};

/// TLS configuration for the Elasticsearch/`OpenSearch` HTTP client.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// Path to a PEM-encoded CA certificate file for custom certificate authorities.
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// Skip TLS certificate verification (development only).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let tls = TlsConfig::default();
        assert!(tls.ca_cert_path.is_none());
        assert!(!tls.insecure_skip_verify);
    }

    #[test]
    fn test_tls_config_deserialization() {
        let toml_str = r#"
            ca_cert_path = "/etc/ssl/certs/custom-ca.pem"
            insecure_skip_verify = true
        "#;
        let tls: TlsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            tls.ca_cert_path.as_deref(),
            Some("/etc/ssl/certs/custom-ca.pem")
        );
        assert!(tls.insecure_skip_verify);
    }

    #[test]
    fn test_tls_config_empty_deserialization() {
        let toml_str = "";
        let tls: TlsConfig = toml::from_str(toml_str).unwrap();
        assert!(tls.ca_cert_path.is_none());
        assert!(!tls.insecure_skip_verify);
    }
}
