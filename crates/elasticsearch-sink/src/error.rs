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
        match err {
            ElasticsearchError::AuthenticationFailed(_) | ElasticsearchError::BulkFailed { .. } => {
                Self::DownstreamClosed
            }
            other => Self::Internal(other.to_string()),
        }
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
    fn test_pipeline_error_conversion_downstream_closed() {
        let auth_err = ElasticsearchError::AuthenticationFailed("unauthorized".to_string());
        let pipeline_err: PipelineError = auth_err.into();
        assert!(matches!(pipeline_err, PipelineError::DownstreamClosed));

        let bulk_err = ElasticsearchError::BulkFailed {
            retries: 3,
            message: "cluster saturated".to_string(),
        };
        let pipeline_err2: PipelineError = bulk_err.into();
        assert!(matches!(pipeline_err2, PipelineError::DownstreamClosed));
    }

    #[test]
    fn test_pipeline_error_conversion_internal() {
        let startup_err = ElasticsearchError::StartupValidation("missing template".to_string());
        let pipeline_err: PipelineError = startup_err.into();
        match pipeline_err {
            PipelineError::Internal(msg) => {
                assert!(msg.contains("startup validation failed: missing template"));
            }
            other => panic!("expected PipelineError::Internal, got {other:?}"),
        }

        let serial_err = ElasticsearchError::Serialization("format issue".to_string());
        let pipeline_err2: PipelineError = serial_err.into();
        match pipeline_err2 {
            PipelineError::Internal(msg) => {
                assert!(msg.contains("serialization error: format issue"));
            }
            other => panic!("expected PipelineError::Internal, got {other:?}"),
        }

        let size_err = ElasticsearchError::PayloadTooLarge {
            actual: 100,
            limit: 50,
        };
        let pipeline_err3: PipelineError = size_err.into();
        match pipeline_err3 {
            PipelineError::Internal(msg) => {
                assert!(msg.contains("payload size 100 bytes exceeds limit 50 bytes"));
            }
            other => panic!("expected PipelineError::Internal, got {other:?}"),
        }
    }
}
