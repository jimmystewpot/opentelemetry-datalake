//! Core trait and types for WASM batch transformation modules.

use arrow::record_batch::RecordBatch;

/// OpenTelemetry signal type processed by the WASM transformer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    /// OpenTelemetry log records.
    Logs,
    /// OpenTelemetry metric data points.
    Metrics,
    /// OpenTelemetry spans / traces.
    Traces,
}

/// Result returned by a [`BatchTransformer::transform`] execution.
#[derive(Debug)]
pub enum TransformResult {
    /// Processing succeeded and produced zero or more transformed record batches.
    Continue(Vec<RecordBatch>),
    /// Discard the record batch (drop the records).
    Discard,
    /// Reject the batch with a specified error message.
    Reject {
        /// Reason explaining why the batch was rejected.
        reason: String,
    },
    /// Transformation encountered an execution error.
    Error {
        /// Reason explaining why the transformation failed.
        reason: String,
    },
}

/// Trait implemented by WASM guest transform plugins.
pub trait BatchTransformer {
    /// Initializes the transformer instance with the target signal type and optional JSON configuration.
    fn init(signal: SignalType, config_json: Option<&str>) -> Result<Self, String>
    where
        Self: Sized;

    /// Transforms an incoming Arrow [`RecordBatch`].
    fn transform(&mut self, batch: RecordBatch) -> TransformResult;
}
