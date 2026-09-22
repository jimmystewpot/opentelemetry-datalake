//! SDK error types for WASM guest modules.

use thiserror::Error;

/// Error type returned by WASM SDK operations and helper functions.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SdkError {
    /// Attempted to drop or nullify an immutable OpenTelemetry core column.
    #[error("Cannot modify or nullify immutable OpenTelemetry core field: {0}")]
    ImmutableFieldViolation(String),

    /// An Arrow record batch or array manipulation error occurred.
    #[error("Arrow error: {0}")]
    Arrow(String),

    /// The specified column was not found in the record batch schema.
    #[error("Column not found in schema: {0}")]
    ColumnNotFound(String),

    /// An IPC or serialization error occurred.
    #[error("IPC error: {0}")]
    Ipc(String),

    /// An invalid signal type raw code was encountered.
    #[error("Invalid signal type: {0}")]
    InvalidSignalType(u32),

    /// Target schema does not match the record batch schema.
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
}
