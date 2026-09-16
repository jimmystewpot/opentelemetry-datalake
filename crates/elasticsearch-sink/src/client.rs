//! HTTP client implementation for Elasticsearch and `OpenSearch` clusters.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::config::{ElasticsearchAuthConfig, ElasticsearchSinkConfig};
use crate::error::ElasticsearchError;

/// Individual error details for a rejected bulk item action.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BulkItemError {
    /// Elasticsearch error type (e.g. `es_rejected_execution_exception`).
    #[serde(rename = "type", default)]
    pub error_type: String,
    /// Human-readable explanation of why the action was rejected.
    #[serde(default)]
    pub reason: String,
    /// Optional underlying cause details.
    #[serde(default)]
    pub caused_by: Option<serde_json::Value>,
}

/// Action item result details within a bulk response.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BulkItem {
    /// Target index or data stream backing index name.
    #[serde(rename = "_index", default)]
    pub index: Option<String>,
    /// Document identifier.
    #[serde(rename = "_id", default)]
    pub id: Option<String>,
    /// HTTP status code for this specific document operation.
    pub status: u16,
    /// Error details if the document operation failed.
    #[serde(default)]
    pub error: Option<BulkItemError>,
}

/// Action wrapper wrapping action-specific result object (`create`, `index`, `update`, `delete`).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BulkItemWrapper {
    /// Result for a create action.
    #[serde(default)]
    pub create: Option<BulkItem>,
    /// Result for an index action.
    #[serde(default)]
    pub index: Option<BulkItem>,
    /// Result for an update action.
    #[serde(default)]
    pub update: Option<BulkItem>,
    /// Result for a delete action.
    #[serde(default)]
    pub delete: Option<BulkItem>,
}

impl BulkItemWrapper {
    /// Returns a reference to the inner `BulkItem` regardless of action type.
    #[must_use]
    pub fn item(&self) -> Option<&BulkItem> {
        self.create
            .as_ref()
            .or(self.index.as_ref())
            .or(self.update.as_ref())
            .or(self.delete.as_ref())
    }

    /// Returns the HTTP status code of the inner action item, if present.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        self.item().map(|i| i.status)
    }

    /// Returns the error details of the inner action item, if present.
    #[must_use]
    pub fn error(&self) -> Option<&BulkItemError> {
        self.item().and_then(|i| i.error.as_ref())
    }
}

/// Top-level response structure returned by the Elasticsearch/`OpenSearch` Bulk API.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BulkResponse {
    /// Time in milliseconds spent by the cluster processing the bulk request.
    #[serde(default)]
    pub took: u64,
    /// Whether any document operations in the bulk request encountered an error.
    pub errors: bool,
    /// Array of action results for each document in the bulk request.
    #[serde(default)]
    pub items: Vec<BulkItemWrapper>,
}

/// Internal cluster information returned by `GET /`.
#[derive(Debug, Deserialize)]
struct ClusterInfo {
    #[serde(default)]
    version: Option<ClusterVersionInfo>,
}

/// Cluster version details returned within `GET /`.
#[derive(Debug, Deserialize)]
struct ClusterVersionInfo {
    #[serde(default)]
    number: String,
}

/// Composable index template query response returned by `GET /_index_template/<pattern>`.
#[derive(Debug, Deserialize)]
struct IndexTemplatesResponse {
    #[serde(default)]
    index_templates: Vec<serde_json::Value>,
}

/// High-performance HTTP client for Elasticsearch and `OpenSearch` clusters.
///
/// Manages connection pooling, client-side round-robin endpoint selection,
/// TLS configuration, authentication header injection, and retry loops.
#[derive(Debug, Clone)]
pub struct HttpClient {
    /// Underlying reqwest HTTP client with persistent connection pool.
    client: reqwest::Client,
    /// List of normalized cluster endpoint base URLs (without trailing slashes).
    endpoints: Vec<String>,
    /// Shared atomic counter for round-robin endpoint distribution.
    current_endpoint: Arc<AtomicUsize>,
    /// Configured authentication credentials.
    auth: ElasticsearchAuthConfig,
    /// Whether gzip compression is enabled for bulk request payloads.
    gzip_compression: bool,
    /// Maximum number of retry attempts for transient failures (429/503/network).
    max_retries: usize,
    /// Delay between retries in seconds.
    retry_interval_secs: u64,
}

impl HttpClient {
    /// Constructs a new `HttpClient` from the given sink configuration.
    ///
    /// Validates endpoint list, configures TLS root CA and verification options,
    /// sets connection and request timeouts, and initializes TCP keepalive.
    pub fn try_new(config: &ElasticsearchSinkConfig) -> Result<Self, ElasticsearchError> {
        if config.endpoints.is_empty() {
            return Err(ElasticsearchError::StartupValidation(
                "at least one endpoint must be configured".to_string(),
            ));
        }

        let endpoints: Vec<String> = config
            .endpoints
            .iter()
            .map(|ep| ep.trim().trim_end_matches('/').to_string())
            .collect();

        if endpoints.iter().any(String::is_empty) {
            return Err(ElasticsearchError::StartupValidation(
                "endpoint URL cannot be empty".to_string(),
            ));
        }

        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true);

        if config.tls.insecure_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(ca_path) = &config.tls.ca_cert_path {
            let pem_bytes = std::fs::read(ca_path).map_err(|e| {
                ElasticsearchError::StartupValidation(format!(
                    "failed to read CA certificate from '{ca_path}': {e}"
                ))
            })?;
            let cert = reqwest::Certificate::from_pem(&pem_bytes)?;
            builder = builder.add_root_certificate(cert);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            endpoints,
            current_endpoint: Arc::new(AtomicUsize::new(0)),
            auth: config.auth.clone(),
            gzip_compression: config.gzip_compression,
            max_retries: config.max_retries,
            retry_interval_secs: config.retry_interval_secs,
        })
    }

    /// Returns the next cluster endpoint base URL in round-robin sequence.
    #[must_use]
    pub fn next_endpoint(&self) -> &str {
        let idx = self.current_endpoint.fetch_add(1, Ordering::Relaxed);
        &self.endpoints[idx % self.endpoints.len()]
    }

    /// Returns the slice of configured cluster endpoint base URLs.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Returns a reference to the active authentication configuration.
    #[must_use]
    pub fn auth(&self) -> &ElasticsearchAuthConfig {
        &self.auth
    }

    /// Returns whether gzip compression is enabled for bulk requests.
    #[must_use]
    pub const fn gzip_compression(&self) -> bool {
        self.gzip_compression
    }

    /// Injects authentication credentials into an outgoing HTTP request builder.
    pub fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            ElasticsearchAuthConfig::None => req,
            ElasticsearchAuthConfig::Basic { username, password } => {
                req.basic_auth(username, password.as_deref())
            }
            ElasticsearchAuthConfig::ApiKey { api_key } => {
                req.header(reqwest::header::AUTHORIZATION, format!("ApiKey {api_key}"))
            }
            ElasticsearchAuthConfig::Bearer { token } => req.bearer_auth(token),
            #[cfg(feature = "aws")]
            ElasticsearchAuthConfig::AwsSigv4 { region, service } => {
                // When AWS feature is enabled, set custom AWS signing headers or pass-through
                // if standard AWS environment credentials are provided.
                if let (Ok(key), Ok(secret)) = (
                    std::env::var("AWS_ACCESS_KEY_ID"),
                    std::env::var("AWS_SECRET_ACCESS_KEY"),
                ) {
                    let mut r = req.header("X-Amz-Region", region.clone());
                    r = r.header("X-Amz-Service", service.clone());
                    if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
                        r = r.header("X-Amz-Security-Token", token);
                    }
                    r.basic_auth(key, Some(secret))
                } else {
                    req
                }
            }
        }
    }

    /// Sends a serialized NDJSON bulk payload targeting `POST /<data_stream>/_bulk`.
    ///
    /// Applies round-robin node selection, gzip compression (if enabled), authentication,
    /// and a jittered exponential backoff retry loop on transient failures (429/503/network).
    pub async fn send_bulk(
        &self,
        data_stream: &str,
        payload: Bytes,
    ) -> Result<BulkResponse, ElasticsearchError> {
        let (body, is_gzipped) = if self.gzip_compression && !payload.is_empty() {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            std::io::Write::write_all(&mut encoder, &payload).map_err(|e| {
                ElasticsearchError::Serialization(format!("gzip compression failed: {e}"))
            })?;
            let compressed = encoder.finish().map_err(|e| {
                ElasticsearchError::Serialization(format!("gzip compression finalize failed: {e}"))
            })?;
            (Bytes::from(compressed), true)
        } else {
            (payload, false)
        };

        let mut attempt: usize = 0;
        let max_attempts = self.max_retries.saturating_add(1);

        loop {
            let endpoint = self.next_endpoint();
            let url = format!("{endpoint}/{data_stream}/_bulk");

            let mut req = self
                .client
                .post(&url)
                .header(CONTENT_TYPE, "application/x-ndjson")
                .body(body.clone());

            if is_gzipped {
                req = req.header(CONTENT_ENCODING, "gzip");
            }

            req = self.apply_auth(req);

            match req.send().await {
                Ok(response) => {
                    let status = response.status();

                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                        || status == reqwest::StatusCode::BAD_GATEWAY
                        || status == reqwest::StatusCode::GATEWAY_TIMEOUT
                    {
                        attempt = attempt.saturating_add(1);
                        if attempt >= max_attempts {
                            return Err(ElasticsearchError::BulkFailed {
                                retries: self.max_retries,
                                message: format!(
                                    "HTTP {status} after {attempt} attempts against {endpoint}"
                                ),
                            });
                        }
                        self.sleep_backoff(attempt).await;
                        continue;
                    }

                    if status == reqwest::StatusCode::UNAUTHORIZED
                        || status == reqwest::StatusCode::FORBIDDEN
                    {
                        let err_text = response.text().await.unwrap_or_default();
                        return Err(ElasticsearchError::AuthenticationFailed(format!(
                            "HTTP {status} from {endpoint}: {err_text}"
                        )));
                    }

                    if !status.is_success() {
                        let err_text = response.text().await.unwrap_or_default();
                        return Err(ElasticsearchError::BulkFailed {
                            retries: attempt,
                            message: format!("HTTP {status} from {endpoint}: {err_text}"),
                        });
                    }

                    let bulk_resp: BulkResponse =
                        response.json().await.map_err(ElasticsearchError::Http)?;
                    return Ok(bulk_resp);
                }
                Err(err) => {
                    attempt = attempt.saturating_add(1);
                    if attempt >= max_attempts {
                        return Err(ElasticsearchError::BulkFailed {
                            retries: self.max_retries,
                            message: format!(
                                "network error after {attempt} attempts against {endpoint}: {err}"
                            ),
                        });
                    }
                    self.sleep_backoff(attempt).await;
                }
            }
        }
    }

    /// Performs a startup cluster health check by sending `GET /`.
    ///
    /// Validates that the cluster is reachable, authenticated, and returns a valid version number.
    pub async fn health_check(&self) -> Result<(), ElasticsearchError> {
        let endpoint = self.next_endpoint();
        let url = format!("{endpoint}/");

        let req = self.client.get(&url);
        let req = self.apply_auth(req);

        let response = req.send().await.map_err(|e| {
            ElasticsearchError::StartupValidation(format!(
                "failed to connect to cluster endpoint '{endpoint}': {e}"
            ))
        })?;

        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(ElasticsearchError::AuthenticationFailed(format!(
                "health check authentication failed with HTTP {status}"
            )));
        }

        if !status.is_success() {
            return Err(ElasticsearchError::StartupValidation(format!(
                "health check against '{endpoint}' returned HTTP {status}"
            )));
        }

        let info: ClusterInfo = response.json().await.map_err(|e| {
            ElasticsearchError::StartupValidation(format!(
                "failed to parse cluster info JSON from '{endpoint}': {e}"
            ))
        })?;

        let version_number = info
            .version
            .map(|v| v.number)
            .filter(|n| !n.trim().is_empty())
            .ok_or_else(|| {
                ElasticsearchError::StartupValidation(format!(
                    "cluster info from '{endpoint}' did not contain a valid version number"
                ))
            })?;

        tracing::info!(
            endpoint = %endpoint,
            version = %version_number,
            "Elasticsearch/OpenSearch cluster health check succeeded"
        );

        Ok(())
    }

    /// Validates that a composable index template exists for the given data stream.
    ///
    /// Queries `GET /_index_template/<data_stream>` and returns an error if not found.
    pub async fn validate_index_template(
        &self,
        data_stream: &str,
    ) -> Result<(), ElasticsearchError> {
        let endpoint = self.next_endpoint();
        let url = format!("{endpoint}/_index_template/{data_stream}");

        let req = self.client.get(&url);
        let req = self.apply_auth(req);

        let response = req.send().await.map_err(|e| {
            ElasticsearchError::StartupValidation(format!(
                "failed to query index template for '{data_stream}' on '{endpoint}': {e}"
            ))
        })?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(ElasticsearchError::StartupValidation(format!(
                "index template for data stream '{data_stream}' not found (HTTP 404)"
            )));
        }

        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(ElasticsearchError::AuthenticationFailed(format!(
                "unauthorized to validate index template for '{data_stream}': HTTP {status}"
            )));
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ElasticsearchError::StartupValidation(format!(
                "index template check for '{data_stream}' returned HTTP {status}: {body}"
            )));
        }

        let parsed: IndexTemplatesResponse = response.json().await.map_err(|e| {
            ElasticsearchError::StartupValidation(format!(
                "failed to parse index templates response for '{data_stream}': {e}"
            ))
        })?;

        if parsed.index_templates.is_empty() {
            return Err(ElasticsearchError::StartupValidation(format!(
                "index template for data stream '{data_stream}' returned empty template list"
            )));
        }

        tracing::info!(
            endpoint = %endpoint,
            data_stream = %data_stream,
            "Index template validation succeeded"
        );

        Ok(())
    }

    /// Executes a jittered exponential backoff sleep.
    async fn sleep_backoff(&self, attempt: usize) {
        if self.retry_interval_secs == 0 {
            tokio::task::yield_now().await;
            return;
        }

        let base_ms = self.retry_interval_secs.saturating_mul(1000).max(100);
        let shift = attempt.saturating_sub(1).min(6);
        let multiplier = 1u64 << shift;
        let base_backoff = base_ms.saturating_mul(multiplier);

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let jitter_ms = u64::from(nanos % 500);

        let total_backoff_ms = base_backoff.saturating_add(jitter_ms);
        tokio::time::sleep(Duration::from_millis(total_backoff_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DataStreamMapping;
    use crate::tls::TlsConfig;
    use flate2::read::GzDecoder;
    use std::io::Read;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn make_test_config(endpoints: Vec<String>) -> ElasticsearchSinkConfig {
        ElasticsearchSinkConfig {
            endpoints,
            auth: ElasticsearchAuthConfig::None,
            data_streams: DataStreamMapping {
                logs: "logs-otel-default".to_string(),
                metrics: "metrics-otel-default".to_string(),
                traces: "traces-otel-default".to_string(),
            },
            tls: TlsConfig::default(),
            unpack_attributes: true,
            gzip_compression: true,
            max_concurrent_requests: 8,
            max_payload_bytes: 20_971_520,
            connect_timeout_secs: 5,
            request_timeout_secs: 10,
            max_retries: 3,
            retry_interval_secs: 0, // 0 for instantaneous tests
            validate_on_startup: true,
            batching: None,
            order_by: None,
        }
    }

    #[test]
    fn test_round_robin_endpoint_cycling() {
        let endpoints = vec![
            "http://node1:9200".to_string(),
            "http://node2:9200".to_string(),
            "http://node3:9200/".to_string(),
        ];
        let config = make_test_config(endpoints);
        let client = HttpClient::try_new(&config).unwrap();

        // Check normalization: trailing slash removed
        assert_eq!(
            client.endpoints(),
            &[
                "http://node1:9200",
                "http://node2:9200",
                "http://node3:9200"
            ]
        );

        // Verify round-robin sequence cycles evenly
        assert_eq!(client.next_endpoint(), "http://node1:9200");
        assert_eq!(client.next_endpoint(), "http://node2:9200");
        assert_eq!(client.next_endpoint(), "http://node3:9200");
        assert_eq!(client.next_endpoint(), "http://node1:9200");
        assert_eq!(client.next_endpoint(), "http://node2:9200");
        assert_eq!(client.next_endpoint(), "http://node3:9200");
    }

    #[test]
    fn test_single_endpoint_selection() {
        let endpoints = vec!["http://localhost:9200/".to_string()];
        let config = make_test_config(endpoints);
        let client = HttpClient::try_new(&config).unwrap();

        assert_eq!(client.next_endpoint(), "http://localhost:9200");
        assert_eq!(client.next_endpoint(), "http://localhost:9200");
    }

    #[test]
    fn test_empty_endpoints_rejected() {
        let config = make_test_config(vec![]);
        let err = HttpClient::try_new(&config).unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(
            err.to_string()
                .contains("at least one endpoint must be configured")
        );
    }

    #[test]
    fn test_blank_endpoint_rejected() {
        let config = make_test_config(vec!["   ".to_string()]);
        let err = HttpClient::try_new(&config).unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(err.to_string().contains("endpoint URL cannot be empty"));
    }

    #[test]
    fn test_auth_headers_none() {
        let config = make_test_config(vec!["http://localhost:9200".to_string()]);
        let client = HttpClient::try_new(&config).unwrap();

        let req = client.client.get("http://localhost:9200");
        let req = client.apply_auth(req);
        let request = req.build().unwrap();

        assert!(request.headers().get("authorization").is_none());
    }

    #[test]
    fn test_auth_headers_basic() {
        let mut config = make_test_config(vec!["http://localhost:9200".to_string()]);
        config.auth = ElasticsearchAuthConfig::Basic {
            username: "admin".to_string(),
            password: Some("secret123".to_string()),
        };
        let client = HttpClient::try_new(&config).unwrap();

        let req = client.client.get("http://localhost:9200");
        let req = client.apply_auth(req);
        let request = req.build().unwrap();

        let auth_hdr = request.headers().get("authorization").unwrap();
        // admin:secret123 base64 is YWRtaW46c2VjcmV0MTIz
        assert_eq!(auth_hdr.to_str().unwrap(), "Basic YWRtaW46c2VjcmV0MTIz");
    }

    #[test]
    fn test_auth_headers_basic_without_password() {
        let mut config = make_test_config(vec!["http://localhost:9200".to_string()]);
        config.auth = ElasticsearchAuthConfig::Basic {
            username: "readonly".to_string(),
            password: None,
        };
        let client = HttpClient::try_new(&config).unwrap();

        let req = client.client.get("http://localhost:9200");
        let req = client.apply_auth(req);
        let request = req.build().unwrap();

        let auth_hdr = request.headers().get("authorization").unwrap();
        // readonly: base64 is cmVhZG9ubHk6
        assert_eq!(auth_hdr.to_str().unwrap(), "Basic cmVhZG9ubHk6");
    }

    #[test]
    fn test_auth_headers_api_key() {
        let mut config = make_test_config(vec!["http://localhost:9200".to_string()]);
        config.auth = ElasticsearchAuthConfig::ApiKey {
            api_key: "VnVhQ2ZHY0JDZGJrUW0tZTVhT3k6dWkybHAyYXhUTm1xWUY1QXdqd1JRdw==".to_string(),
        };
        let client = HttpClient::try_new(&config).unwrap();

        let req = client.client.get("http://localhost:9200");
        let req = client.apply_auth(req);
        let request = req.build().unwrap();

        let auth_hdr = request.headers().get("authorization").unwrap();
        assert_eq!(
            auth_hdr.to_str().unwrap(),
            "ApiKey VnVhQ2ZHY0JDZGJrUW0tZTVhT3k6dWkybHAyYXhUTm1xWUY1QXdqd1JRdw=="
        );
    }

    #[test]
    fn test_auth_headers_bearer() {
        let mut config = make_test_config(vec!["http://localhost:9200".to_string()]);
        config.auth = ElasticsearchAuthConfig::Bearer {
            token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9".to_string(),
        };
        let client = HttpClient::try_new(&config).unwrap();

        let req = client.client.get("http://localhost:9200");
        let req = client.apply_auth(req);
        let request = req.build().unwrap();

        let auth_hdr = request.headers().get("authorization").unwrap();
        assert_eq!(
            auth_hdr.to_str().unwrap(),
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        );
    }

    #[test]
    fn test_bulk_response_parse_success() {
        let json_str = r#"{
            "took": 42,
            "errors": false,
            "items": [
                {
                    "create": {
                        "_index": "logs-otel-default-2026.09.16-000001",
                        "_id": "doc1",
                        "status": 201
                    }
                },
                {
                    "create": {
                        "_index": "logs-otel-default-2026.09.16-000001",
                        "_id": "doc2",
                        "status": 201
                    }
                }
            ]
        }"#;

        let resp: BulkResponse = serde_json::from_str(json_str).unwrap();
        assert_eq!(resp.took, 42);
        assert!(!resp.errors);
        assert_eq!(resp.items.len(), 2);

        let item0 = resp.items[0].item().unwrap();
        assert_eq!(
            item0.index.as_deref(),
            Some("logs-otel-default-2026.09.16-000001")
        );
        assert_eq!(item0.id.as_deref(), Some("doc1"));
        assert_eq!(item0.status, 201);
        assert!(item0.error.is_none());
        assert_eq!(resp.items[0].status(), Some(201));
        assert!(resp.items[0].error().is_none());
    }

    #[test]
    fn test_bulk_response_parse_with_errors() {
        let json_str = r#"{
            "took": 88,
            "errors": true,
            "items": [
                {
                    "create": {
                        "_index": "logs-otel-default",
                        "_id": "doc1",
                        "status": 201
                    }
                },
                {
                    "create": {
                        "_index": "logs-otel-default",
                        "_id": "doc2",
                        "status": 429,
                        "error": {
                            "type": "es_rejected_execution_exception",
                            "reason": "rejected execution of queue capacity = 200"
                        }
                    }
                },
                {
                    "create": {
                        "_index": "logs-otel-default",
                        "_id": "doc3",
                        "status": 400,
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse field [host.ip] of type [ip]"
                        }
                    }
                }
            ]
        }"#;

        let resp: BulkResponse = serde_json::from_str(json_str).unwrap();
        assert_eq!(resp.took, 88);
        assert!(resp.errors);
        assert_eq!(resp.items.len(), 3);

        // First item: success
        assert_eq!(resp.items[0].status(), Some(201));
        assert!(resp.items[0].error().is_none());

        // Second item: 429 rejected
        assert_eq!(resp.items[1].status(), Some(429));
        let err1 = resp.items[1].error().unwrap();
        assert_eq!(err1.error_type, "es_rejected_execution_exception");
        assert_eq!(err1.reason, "rejected execution of queue capacity = 200");

        // Third item: 400 mapping error
        assert_eq!(resp.items[2].status(), Some(400));
        let err2 = resp.items[2].error().unwrap();
        assert_eq!(err2.error_type, "mapper_parsing_exception");
        assert_eq!(err2.reason, "failed to parse field [host.ip] of type [ip]");
    }

    #[test]
    fn test_tls_config_insecure_skip_verify() {
        let mut config = make_test_config(vec!["https://localhost:9200".to_string()]);
        config.tls.insecure_skip_verify = true;
        let client = HttpClient::try_new(&config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_tls_config_missing_ca_file() {
        let mut config = make_test_config(vec!["https://localhost:9200".to_string()]);
        config.tls.ca_cert_path = Some("/nonexistent/ca.pem".to_string());
        let err = HttpClient::try_new(&config).unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(
            err.to_string()
                .contains("failed to read CA certificate from '/nonexistent/ca.pem'")
        );
    }

    #[tokio::test]
    async fn test_send_bulk_success_with_gzip() {
        let server = MockServer::start().await;

        let bulk_response = r#"{
            "took": 15,
            "errors": false,
            "items": [
                {"create": {"_index": "logs-otel-default", "_id": "1", "status": 201}}
            ]
        }"#;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(header("content-type", "application/x-ndjson"))
            .and(header("content-encoding", "gzip"))
            .respond_with(ResponseTemplate::new(200).set_body_string(bulk_response))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();
        assert!(client.gzip_compression());

        let payload = Bytes::from("{\"create\":{}}\n{\"body\":\"test message\"}\n");
        let resp = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap();

        assert!(!resp.errors);
        assert_eq!(resp.took, 15);
        assert_eq!(resp.items.len(), 1);
        assert_eq!(resp.items[0].status(), Some(201));
    }

    #[tokio::test]
    async fn test_send_bulk_without_gzip() {
        let server = MockServer::start().await;

        let bulk_response = r#"{"took": 5, "errors": false, "items": []}"#;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(header("content-type", "application/x-ndjson"))
            .respond_with(ResponseTemplate::new(200).set_body_string(bulk_response))
            .mount(&server)
            .await;

        let mut config = make_test_config(vec![server.uri()]);
        config.gzip_compression = false;
        let client = HttpClient::try_new(&config).unwrap();
        assert!(!client.gzip_compression());

        let payload = Bytes::from("{\"create\":{}}\n{\"body\":\"plain ndjson\"}\n");
        let resp = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap();

        assert!(!resp.errors);
    }

    #[tokio::test]
    async fn test_send_bulk_gzip_decompression_verification() {
        let server = MockServer::start().await;

        let raw_ndjson = "{\"create\":{}}\n{\"service_name\":\"api-gateway\",\"status\":200}\n";

        // Decompress body in matcher callback
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .and(header("content-encoding", "gzip"))
            .and(move |req: &wiremock::Request| {
                let mut decoder = GzDecoder::new(&req.body[..]);
                let mut decompressed = String::new();
                if decoder.read_to_string(&mut decompressed).is_ok() {
                    decompressed == raw_ndjson
                } else {
                    false
                }
            })
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"took": 10, "errors": false, "items": []}"#),
            )
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let payload = Bytes::from(raw_ndjson);
        let resp = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap();
        assert!(!resp.errors);
    }

    #[tokio::test]
    async fn test_send_bulk_retry_on_429_then_succeeds() {
        let server = MockServer::start().await;

        // First attempt returns 429
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Too Many Requests"))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Second attempt returns 200 OK
        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"took": 20, "errors": false, "items": []}"#),
            )
            .mount(&server)
            .await;

        let mut config = make_test_config(vec![server.uri()]);
        config.max_retries = 2;
        let client = HttpClient::try_new(&config).unwrap();

        let payload = Bytes::from("{\"create\":{}}\n{\"body\":\"retry test\"}\n");
        let resp = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap();
        assert!(!resp.errors);
    }

    #[tokio::test]
    async fn test_send_bulk_retry_on_503_then_succeeds() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/metrics-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
            .up_to_n_times(2)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/metrics-otel-default/_bulk"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"took": 3, "errors": false, "items": []}"#),
            )
            .mount(&server)
            .await;

        let mut config = make_test_config(vec![server.uri()]);
        config.max_retries = 3;
        let client = HttpClient::try_new(&config).unwrap();

        let payload = Bytes::from("{\"create\":{}}\n{\"metric\":\"cpu\"}\n");
        let resp = client
            .send_bulk("metrics-otel-default", payload)
            .await
            .unwrap();
        assert!(!resp.errors);
    }

    #[tokio::test]
    async fn test_send_bulk_retries_exhausted() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(429).set_body_string("cluster saturated"))
            .mount(&server)
            .await;

        let mut config = make_test_config(vec![server.uri()]);
        config.max_retries = 2;
        let client = HttpClient::try_new(&config).unwrap();

        let payload = Bytes::from("{\"create\":{}}\n{\"body\":\"drop me\"}\n");
        let err = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap_err();

        match err {
            ElasticsearchError::BulkFailed { retries, message } => {
                assert_eq!(retries, 2);
                assert!(message.contains("HTTP 429"));
            }
            other => panic!("expected BulkFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_send_bulk_auth_failure_no_retry() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/logs-otel-default/_bulk"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        let mut config = make_test_config(vec![server.uri()]);
        config.max_retries = 3;
        let client = HttpClient::try_new(&config).unwrap();

        let payload = Bytes::from("{\"create\":{}}\n{\"body\":\"unauthorized\"}\n");
        let err = client
            .send_bulk("logs-otel-default", payload)
            .await
            .unwrap_err();

        assert!(matches!(err, ElasticsearchError::AuthenticationFailed(_)));
        assert!(err.to_string().contains("HTTP 401"));
    }

    #[tokio::test]
    async fn test_health_check_success() {
        let server = MockServer::start().await;

        let info_json = r#"{
            "name": "opensearch-node-1",
            "cluster_name": "otel-cluster",
            "version": {
                "distribution": "opensearch",
                "number": "2.17.0"
            },
            "tagline": "The OpenSearch Project: https://opensearch.org/"
        }"#;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(info_json))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        assert!(client.health_check().await.is_ok());
    }

    #[tokio::test]
    async fn test_health_check_missing_version_number() {
        let server = MockServer::start().await;

        let bad_json = r#"{"name": "broken-node", "version": {}}"#;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(bad_json))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let err = client.health_check().await.unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(err.to_string().contains("valid version number"));
    }

    #[tokio::test]
    async fn test_health_check_unauthorized() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let err = client.health_check().await.unwrap_err();
        assert!(matches!(err, ElasticsearchError::AuthenticationFailed(_)));
        assert!(err.to_string().contains("HTTP 403"));
    }

    #[tokio::test]
    async fn test_validate_index_template_success() {
        let server = MockServer::start().await;

        let template_json = r#"{
            "index_templates": [
                {
                    "name": "logs-otel-default",
                    "index_template": {
                        "index_patterns": ["logs-otel-default*"],
                        "data_stream": {}
                    }
                }
            ]
        }"#;

        Mock::given(method("GET"))
            .and(path("/_index_template/logs-otel-default"))
            .respond_with(ResponseTemplate::new(200).set_body_string(template_json))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        assert!(
            client
                .validate_index_template("logs-otel-default")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_validate_index_template_not_found() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/_index_template/missing-template"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let err = client
            .validate_index_template("missing-template")
            .await
            .unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(err.to_string().contains("not found (HTTP 404)"));
    }

    #[tokio::test]
    async fn test_validate_index_template_unauthorized() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/_index_template/traces-otel-default"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let err = client
            .validate_index_template("traces-otel-default")
            .await
            .unwrap_err();
        assert!(matches!(err, ElasticsearchError::AuthenticationFailed(_)));
        assert!(err.to_string().contains("HTTP 401"));
    }

    #[tokio::test]
    async fn test_validate_index_template_empty_array() {
        let server = MockServer::start().await;

        let template_json = r#"{"index_templates": []}"#;

        Mock::given(method("GET"))
            .and(path("/_index_template/logs-otel-default"))
            .respond_with(ResponseTemplate::new(200).set_body_string(template_json))
            .mount(&server)
            .await;

        let config = make_test_config(vec![server.uri()]);
        let client = HttpClient::try_new(&config).unwrap();

        let err = client
            .validate_index_template("logs-otel-default")
            .await
            .unwrap_err();
        assert!(matches!(err, ElasticsearchError::StartupValidation(_)));
        assert!(err.to_string().contains("empty template list"));
    }
}
