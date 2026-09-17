//! Configuration structures and defaults for the Elasticsearch sink.

use crate::tls::TlsConfig;
use serde::{Deserialize, Serialize};

const fn default_max_concurrent_requests() -> usize {
    8
}

const fn default_max_payload_bytes() -> usize {
    20_971_520 // 20 MiB
}

const fn default_connect_timeout_secs() -> u64 {
    10
}

const fn default_request_timeout_secs() -> u64 {
    30
}

const fn default_max_retries() -> usize {
    3
}

const fn default_retry_interval_secs() -> u64 {
    1
}

const fn default_true() -> bool {
    true
}

const fn default_max_batch_size_bytes() -> usize {
    10_485_760 // 10 MiB
}

const fn default_max_batch_interval_sec() -> u64 {
    10
}

const fn default_max_batch_records() -> usize {
    50_000
}

/// Data stream target names for each OTLP signal type.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DataStreamMapping {
    /// Target data stream for log records.
    pub logs: String,
    /// Target data stream for metric records.
    pub metrics: String,
    /// Target data stream for trace/span records.
    pub traces: String,
}

/// Micro-batch accumulation thresholds.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ElasticsearchBatchingConfig {
    /// Maximum accumulated Arrow byte size before flushing.
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    /// Maximum interval in seconds between flushes.
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    /// Maximum accumulated record count before flushing.
    #[serde(default = "default_max_batch_records")]
    pub max_batch_records: usize,
}

impl Default for ElasticsearchBatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_batch_interval_sec: default_max_batch_interval_sec(),
            max_batch_records: default_max_batch_records(),
        }
    }
}

/// Authentication configuration for the Elasticsearch/`OpenSearch` cluster.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ElasticsearchAuthConfig {
    /// No authentication (development / private VPC).
    #[default]
    None,
    /// HTTP Basic Authentication.
    Basic {
        /// Username for Basic Auth.
        username: String,
        /// Password for Basic Auth (prefer env var override).
        #[serde(default)]
        password: Option<String>,
    },
    /// Elasticsearch/`OpenSearch` API Key authentication.
    ApiKey {
        /// The API key value.
        api_key: String,
    },
    /// Bearer token authentication.
    Bearer {
        /// The bearer token value.
        token: String,
    },
    /// AWS `SigV4` request signing (requires `aws` feature).
    #[cfg(feature = "aws")]
    AwsSigv4 {
        /// AWS region for signing.
        region: String,
        /// AWS service name (`es` for managed, `aoss` for serverless).
        #[serde(default = "default_aws_service")]
        service: String,
    },
}

#[cfg(feature = "aws")]
fn default_aws_service() -> String {
    "es".to_string()
}

/// Top-level configuration for the Elasticsearch/`OpenSearch` sink.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ElasticsearchSinkConfig {
    /// One or more cluster node HTTP URLs for round-robin load distribution.
    pub endpoints: Vec<String>,

    /// Authentication configuration.
    #[serde(default)]
    pub auth: ElasticsearchAuthConfig,

    /// Data stream targets per signal type.
    pub data_streams: DataStreamMapping,

    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Whether to parse stringified JSON attributes into native JSON objects.
    #[serde(default = "default_true")]
    pub unpack_attributes: bool,

    /// Enable gzip Content-Encoding for bulk HTTP requests.
    #[serde(default = "default_true")]
    pub gzip_compression: bool,

    /// Maximum concurrent in-flight bulk requests.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Maximum serialized payload size in bytes.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,

    /// Connection timeout in seconds.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Request timeout in seconds.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum retries for transient errors.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Delay between retries in seconds.
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,

    /// Perform startup health check and index template validation.
    #[serde(default = "default_true")]
    pub validate_on_startup: bool,

    /// Optional micro-batch accumulation thresholds.
    #[serde(default)]
    pub batching: Option<ElasticsearchBatchingConfig>,

    /// Optional pre-sorting configuration.
    #[serde(default)]
    pub order_by: Option<pipeline_core::sort::SortConfig>,
}

impl ElasticsearchSinkConfig {
    /// Validates static configuration constraints without initiating network transport.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ElasticsearchError::StartupValidation`] if any configuration
    /// constraint is violated.
    pub fn validate(&self) -> Result<(), crate::error::ElasticsearchError> {
        if self.endpoints.is_empty() {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "at least one endpoint must be configured".to_string(),
            ));
        }

        for ep in &self.endpoints {
            let trimmed = ep.trim().trim_end_matches('/');
            if trimmed.is_empty() {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "endpoint URL cannot be empty".to_string(),
                ));
            }
            if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    format!("endpoint '{ep}' must start with http:// or https://"),
                ));
            }
            if reqwest::Url::parse(trimmed).is_err() {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    format!("endpoint '{ep}' is not a valid URL"),
                ));
            }
        }

        if self.max_concurrent_requests == 0 {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "max_concurrent_requests must be greater than 0".to_string(),
            ));
        }

        if self.max_payload_bytes == 0 {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "max_payload_bytes must be greater than 0".to_string(),
            ));
        }

        if self.data_streams.logs.trim().is_empty()
            || self.data_streams.metrics.trim().is_empty()
            || self.data_streams.traces.trim().is_empty()
        {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                "data stream names for logs, metrics, and traces must not be empty".to_string(),
            ));
        }

        if let Some(ref batching) = self.batching {
            if batching.max_batch_size_bytes == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_size_bytes must be greater than 0".to_string(),
                ));
            }
            if batching.max_batch_interval_sec == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_interval_sec must be greater than 0".to_string(),
                ));
            }
            if batching.max_batch_records == 0 {
                return Err(crate::error::ElasticsearchError::StartupValidation(
                    "batching.max_batch_records must be greater than 0".to_string(),
                ));
            }
        }

        if let Some(ref ca_path) = self.tls.ca_cert_path
            && !std::path::Path::new(ca_path).is_file()
        {
            return Err(crate::error::ElasticsearchError::StartupValidation(
                format!("CA certificate file does not exist: {ca_path}"),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserializes_minimal() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.endpoints.len(), 1);
        assert_eq!(cfg.endpoints[0], "http://localhost:9200");
        assert_eq!(cfg.data_streams.logs, "logs-otel-default");
        assert_eq!(cfg.data_streams.metrics, "metrics-otel-default");
        assert_eq!(cfg.data_streams.traces, "traces-otel-default");
        assert!(matches!(cfg.auth, ElasticsearchAuthConfig::None));
        assert!(cfg.unpack_attributes);
        assert!(cfg.gzip_compression);
        assert_eq!(cfg.max_concurrent_requests, 8);
        assert_eq!(cfg.max_payload_bytes, 20_971_520);
        assert_eq!(cfg.connect_timeout_secs, 10);
        assert_eq!(cfg.request_timeout_secs, 30);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.retry_interval_secs, 1);
        assert!(cfg.validate_on_startup);
        assert!(cfg.batching.is_none());
        assert!(cfg.order_by.is_none());
    }

    #[test]
    fn test_config_deserializes_basic_auth() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [auth]
            type = "basic"
            username = "admin"
            password = "secret"
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        match &cfg.auth {
            ElasticsearchAuthConfig::Basic { username, password } => {
                assert_eq!(username, "admin");
                assert_eq!(password.as_deref(), Some("secret"));
            }
            other => panic!("expected Basic auth, got {other:?}"),
        }
    }

    #[test]
    fn test_config_deserializes_api_key_auth() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [auth]
            type = "api_key"
            api_key = "my-secret-api-key"
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        match &cfg.auth {
            ElasticsearchAuthConfig::ApiKey { api_key } => {
                assert_eq!(api_key, "my-secret-api-key");
            }
            other => panic!("expected ApiKey auth, got {other:?}"),
        }
    }

    #[test]
    fn test_config_deserializes_bearer_auth() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [auth]
            type = "bearer"
            token = "jwt-token-value"
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        match &cfg.auth {
            ElasticsearchAuthConfig::Bearer { token } => {
                assert_eq!(token, "jwt-token-value");
            }
            other => panic!("expected Bearer auth, got {other:?}"),
        }
    }

    #[test]
    fn test_config_deserializes_with_batching() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
            [batching]
            max_batch_size_bytes = 5242880
            max_batch_interval_sec = 5
            max_batch_records = 10000
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        let batching = cfg.batching.unwrap();
        assert_eq!(batching.max_batch_size_bytes, 5_242_880);
        assert_eq!(batching.max_batch_interval_sec, 5);
        assert_eq!(batching.max_batch_records, 10_000);
    }

    #[test]
    fn test_config_deserializes_batching_defaults() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
            [batching]
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        let batching = cfg.batching.unwrap();
        assert_eq!(batching.max_batch_size_bytes, 10_485_760);
        assert_eq!(batching.max_batch_interval_sec, 10);
        assert_eq!(batching.max_batch_records, 50_000);
    }

    #[test]
    fn test_config_deserializes_with_tls() {
        let toml_str = r#"
            endpoints = ["https://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
            [tls]
            ca_cert_path = "/path/to/ca.pem"
            insecure_skip_verify = true
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.tls.ca_cert_path.as_deref(), Some("/path/to/ca.pem"));
        assert!(cfg.tls.insecure_skip_verify);
    }

    #[test]
    fn test_config_deserializes_with_order_by() {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
            [order_by]
            logs = ["service_name ASC", "timestamp DESC"]
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.order_by.is_some());
        let order_by = cfg.order_by.unwrap();
        assert_eq!(order_by.logs.len(), 2);
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_config_deserializes_aws_sigv4_auth() {
        let toml_str = r#"
            endpoints = ["https://search-domain.us-east-1.es.amazonaws.com"]
            [auth]
            type = "aws_sigv4"
            region = "us-east-1"
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        let cfg: ElasticsearchSinkConfig = toml::from_str(toml_str).unwrap();
        match &cfg.auth {
            ElasticsearchAuthConfig::AwsSigv4 { region, service } => {
                assert_eq!(region, "us-east-1");
                assert_eq!(service, "es");
            }
            other => panic!("expected AwsSigv4 auth, got {other:?}"),
        }
    }

    fn make_minimal_config() -> ElasticsearchSinkConfig {
        let toml_str = r#"
            endpoints = ["http://localhost:9200"]
            [data_streams]
            logs = "logs-otel-default"
            metrics = "metrics-otel-default"
            traces = "traces-otel-default"
        "#;
        toml::from_str(toml_str).unwrap()
    }

    #[test]
    fn test_validate_rejects_empty_endpoints() {
        let mut cfg = make_minimal_config();
        cfg.endpoints.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_blank_endpoint() {
        let mut cfg = make_minimal_config();
        cfg.endpoints = vec!["   ".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_url() {
        let mut cfg = make_minimal_config();
        cfg.endpoints = vec!["not a url".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_zero_concurrency() {
        let mut cfg = make_minimal_config();
        cfg.max_concurrent_requests = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_empty_data_stream_names() {
        let mut cfg = make_minimal_config();
        cfg.data_streams.logs = "   ".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_succeeds_for_minimal_config() {
        let cfg = make_minimal_config();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_non_http_endpoint() {
        let mut cfg = make_minimal_config();
        cfg.endpoints = vec!["ftp://localhost:9200".to_string()];
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_zero_payload_bytes() {
        let mut cfg = make_minimal_config();
        cfg.max_payload_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_batching_settings() {
        let mut cfg = make_minimal_config();
        cfg.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 0,
            max_batch_interval_sec: 10,
            max_batch_records: 1000,
        });
        assert!(cfg.validate().is_err());

        cfg.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 1000,
            max_batch_interval_sec: 0,
            max_batch_records: 1000,
        });
        assert!(cfg.validate().is_err());

        cfg.batching = Some(ElasticsearchBatchingConfig {
            max_batch_size_bytes: 1000,
            max_batch_interval_sec: 10,
            max_batch_records: 0,
        });
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_nonexistent_ca_file() {
        let mut cfg = make_minimal_config();
        cfg.tls.ca_cert_path = Some("/nonexistent/path/to/ca.pem".to_string());
        assert!(cfg.validate().is_err());
    }
}
