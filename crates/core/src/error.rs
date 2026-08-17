use thiserror::Error;

/// Core domain error types for the pipeline operations.
///
/// Preserves causal chains via `#[from]` and `#[source]` so that
/// downstream consumers can inspect the original error type for
/// programmatic handling (e.g., retry on transient catalog errors).
#[derive(Error, Debug)]
pub enum PipelineError {
    /// An expected metadata field was not found.
    #[error("missing metadata: {0}")]
    MissingMetadata(String),

    /// Configuration loading or validation failed.
    #[error("configuration error: {0}")]
    Configuration(#[source] Box<figment::Error>),

    /// An Arrow operation (schema creation, compute, IPC) failed.
    #[error("arrow operation failed")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A storage backend (Iceberg, Delta, etc.) operation failed.
    /// Uses a boxed source to avoid coupling `pipeline-core` to specific backends.
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// An unstructured internal error for cases that don't fit other variants.
    #[error("internal error: {0}")]
    Internal(String),

    /// A downstream mpsc channel receiver was dropped.
    #[error("downstream channel closed")]
    DownstreamClosed,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MissingMetadata must include the field name in its Display output.
    #[test]
    fn test_pipeline_error_display_missing_metadata() {
        let err = PipelineError::MissingMetadata("x-batch-id".to_string());
        let msg = err.to_string();
        assert!(msg.contains("x-batch-id"), "Display must include field name: {msg}");
        assert!(msg.contains("missing metadata"), "Display prefix must be present: {msg}");
    }

    /// DownstreamClosed must display a human-readable message.
    #[test]
    fn test_pipeline_error_display_downstream_closed() {
        let err = PipelineError::DownstreamClosed;
        assert_eq!(err.to_string(), "downstream channel closed");
    }

    /// Internal error must include the context string in its Display output.
    #[test]
    fn test_pipeline_error_display_internal() {
        let err = PipelineError::Internal("something went wrong".to_string());
        let msg = err.to_string();
        assert!(msg.contains("something went wrong"), "Display must include context: {msg}");
    }

    /// Arrow errors must be convertible to `PipelineError` via the `From` impl.
    #[test]
    fn test_pipeline_error_from_arrow_error() {
        let arrow_err = arrow::error::ArrowError::CastError("bad cast".to_string());
        let err: PipelineError = arrow_err.into();
        assert!(
            matches!(err, PipelineError::Arrow(_)),
            "Arrow error must convert to PipelineError::Arrow"
        );
        // Display must show the static message (the Arrow variant)
        assert_eq!(err.to_string(), "arrow operation failed");
    }
}
