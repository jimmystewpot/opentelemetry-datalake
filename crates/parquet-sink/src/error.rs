//! Domain error definitions and pipeline error conversions for the Parquet sink.

use thiserror::Error;

/// Domain errors for the Parquet streaming sink.
#[derive(Error, Debug)]
pub enum ParquetSinkError {
    /// `OpenDAL` storage or object store I/O failure.
    #[error("OpenDAL storage error: {0}")]
    OpenDal(#[from] opendal::Error),

    /// Parquet serialization or low-level writer failure.
    #[error("Parquet writing failure: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow array processing or compute kernel failure.
    #[error("Arrow array processing error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Variant binary encoding or metadata construction failure.
    #[error("Variant encoding error: {0}")]
    VariantEncoding(String),

    /// Configuration parsing or validation failure.
    #[error("Configuration validation error: {0}")]
    Config(String),

    /// Internal pipeline or channel communication failure.
    #[error("Internal pipeline failure: {0}")]
    Internal(String),
}

impl From<ParquetSinkError> for pipeline_core::error::PipelineError {
    fn from(err: ParquetSinkError) -> Self {
        Self::Storage(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeline_core::error::PipelineError;

    #[test]
    fn test_error_display_variant_encoding() {
        let err = ParquetSinkError::VariantEncoding("invalid header byte".to_string());
        assert_eq!(
            err.to_string(),
            "Variant encoding error: invalid header byte"
        );
    }

    #[test]
    fn test_error_display_config() {
        let err = ParquetSinkError::Config("storage_uri cannot be empty".to_string());
        assert_eq!(
            err.to_string(),
            "Configuration validation error: storage_uri cannot be empty"
        );
    }

    #[test]
    fn test_error_display_internal() {
        let err = ParquetSinkError::Internal("worker thread panicked".to_string());
        assert_eq!(
            err.to_string(),
            "Internal pipeline failure: worker thread panicked"
        );
    }

    #[test]
    fn test_error_from_arrow() {
        let arrow_err = arrow::error::ArrowError::DivideByZero;
        let err = ParquetSinkError::from(arrow_err);
        assert!(matches!(err, ParquetSinkError::Arrow(_)));
        assert!(err.to_string().contains("Arrow array processing error"));
    }

    #[test]
    fn test_pipeline_error_conversion() {
        let err = ParquetSinkError::Config("bad config".to_string());
        let pipeline_err: PipelineError = err.into();
        assert!(matches!(pipeline_err, PipelineError::Storage(_)));
        assert!(pipeline_err.to_string().contains("bad config"));
    }
}
