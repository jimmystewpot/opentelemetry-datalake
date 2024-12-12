use thiserror::Error;

/// Core domain error types for the pipeline operations.
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("missing metadata: {0}")]
    MissingMetadata(String),

    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("downstream channel closed")]
    DownstreamClosed,
}
