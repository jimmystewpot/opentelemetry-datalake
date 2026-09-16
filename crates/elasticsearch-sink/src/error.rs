//! Domain error definitions and pipeline error conversions for the Elasticsearch sink.

use thiserror::Error;

/// Domain errors for the Elasticsearch/`OpenSearch` sink.
#[derive(Error, Debug)]
pub enum ElasticsearchError {
    /// HTTP transport or connection failure.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Bulk response indicated all items failed after retries exhausted.
    #[error("bulk request failed after {retries} retries: {message}")]
    BulkFailed {
        /// Number of retry attempts made.
        retries: usize,
        /// Diagnostic message from the last attempt.
        message: String,
    },

    /// Authentication or authorization failure (401/403).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Startup validation failure (cluster unreachable or missing index templates).
    #[error("startup validation failed: {0}")]
    StartupValidation(String),

    /// Serialization error during NDJSON construction.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Payload exceeds configured maximum size.
    #[error("payload size {actual} bytes exceeds limit {limit} bytes")]
    PayloadTooLarge {
        /// Actual serialized payload size.
        actual: usize,
        /// Configured maximum payload size.
        limit: usize,
    },
}

impl From<ElasticsearchError> for pipeline_core::error::PipelineError {
    fn from(err: ElasticsearchError) -> Self {
        Self::Storage(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline_core::error::PipelineError;

    #[test]
    fn test_error_display_bulk_failed() {
        let err = ElasticsearchError::BulkFailed {
            retries: 3,
            message: "all items rejected with 429".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("bulk request failed after 3 retries"));
        assert!(msg.contains("all items rejected with 429"));
    }

    #[test]
    fn test_error_display_authentication_failed() {
        let err = ElasticsearchError::AuthenticationFailed("invalid api key".to_string());
        assert_eq!(err.to_string(), "authentication failed: invalid api key");
    }

    #[test]
    fn test_error_display_startup_validation() {
        let err = ElasticsearchError::StartupValidation("cluster unreachable".to_string());
        assert_eq!(
            err.to_string(),
            "startup validation failed: cluster unreachable"
        );
    }

    #[test]
    fn test_error_display_serialization() {
        let err = ElasticsearchError::Serialization("unsupported data type".to_string());
        assert_eq!(
            err.to_string(),
            "serialization error: unsupported data type"
        );
    }

    #[test]
    fn test_error_display_payload_too_large() {
        let err = ElasticsearchError::PayloadTooLarge {
            actual: 25_000_000,
            limit: 20_000_000,
        };
        assert_eq!(
            err.to_string(),
            "payload size 25000000 bytes exceeds limit 20000000 bytes"
        );
    }

    #[test]
    fn test_pipeline_error_conversion_preserves_root_cause_in_storage() {
        let auth_err = ElasticsearchError::AuthenticationFailed("invalid api key 123".to_string());
        let pipeline_err: PipelineError = auth_err.into();
        assert!(matches!(pipeline_err, PipelineError::Storage(_)));
        assert!(pipeline_err.to_string().contains("invalid api key 123"));

        let bulk_err = ElasticsearchError::BulkFailed {
            retries: 3,
            message: "cluster saturated with 429".to_string(),
        };
        let pipeline_err2: PipelineError = bulk_err.into();
        assert!(matches!(pipeline_err2, PipelineError::Storage(_)));
        assert!(
            pipeline_err2
                .to_string()
                .contains("cluster saturated with 429")
        );
    }

    #[test]
    fn test_pipeline_error_conversion_other_variants_to_storage() {
        let startup_err = ElasticsearchError::StartupValidation("missing template".to_string());
        let pipeline_err: PipelineError = startup_err.into();
        assert!(matches!(pipeline_err, PipelineError::Storage(_)));
        assert!(pipeline_err.to_string().contains("missing template"));

        let serial_err = ElasticsearchError::Serialization("format issue".to_string());
        let pipeline_err2: PipelineError = serial_err.into();
        assert!(matches!(pipeline_err2, PipelineError::Storage(_)));
        assert!(pipeline_err2.to_string().contains("format issue"));

        let size_err = ElasticsearchError::PayloadTooLarge {
            actual: 100,
            limit: 50,
        };
        let pipeline_err3: PipelineError = size_err.into();
        assert!(matches!(pipeline_err3, PipelineError::Storage(_)));
        assert!(
            pipeline_err3
                .to_string()
                .contains("payload size 100 bytes exceeds limit 50 bytes")
        );
    }
}
