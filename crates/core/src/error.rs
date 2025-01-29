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
