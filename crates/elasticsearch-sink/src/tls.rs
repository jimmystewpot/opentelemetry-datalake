//! TLS configuration for the Elasticsearch sink.

pub use pipeline_core::tls::{TlsConfig, TlsVerificationMode};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let tls = TlsConfig::default();
        assert!(tls.ca_cert_path.is_none());
        assert_eq!(tls.verification, TlsVerificationMode::Full);
        assert!(!tls.is_insecure());
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
        assert!(tls.is_insecure());
    }

    #[test]
    fn test_tls_config_verification_disabled() {
        let toml_str = r#"
            verification = "disabled"
        "#;
        let tls: TlsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(tls.verification, TlsVerificationMode::Disabled);
        assert!(tls.is_insecure());
    }

    #[test]
    fn test_tls_config_empty_deserialization() {
        let toml_str = "";
        let tls: TlsConfig = toml::from_str(toml_str).unwrap();
        assert!(tls.ca_cert_path.is_none());
        assert!(!tls.is_insecure());
    }

    #[test]
    fn test_tls_config_rejects_unknown_fields() {
        let toml_str = r#"
            ca_cert_path = "/etc/ssl/certs/custom-ca.pem"
            unpack_attributes = true
        "#;
        let result: Result<TlsConfig, _> = toml::from_str(toml_str);
        assert!(
            result.is_err(),
            "TlsConfig must reject unknown fields like unpack_attributes"
        );
    }
}
