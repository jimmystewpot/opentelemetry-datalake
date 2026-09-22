//! Core trait and types for WASM batch transformation modules.

use arrow::record_batch::RecordBatch;

/// OpenTelemetry signal type processed by the WASM transformer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SignalType {
    /// OpenTelemetry log records.
    Logs = 0,
    /// OpenTelemetry metric data points.
    Metrics = 1,
    /// OpenTelemetry spans / traces.
    Traces = 2,
}

impl SignalType {
    /// Converts a raw `u32` value into a [`SignalType`], matching C-ABI v1.
    #[must_use]
    pub const fn from_u32(val: u32) -> Option<Self> {
        match val {
            0 => Some(Self::Logs),
            1 => Some(Self::Metrics),
            2 => Some(Self::Traces),
            _ => None,
        }
    }
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

impl TransformResult {
    /// Creates a [`TransformResult::Continue`] containing a single transformed record batch.
    #[must_use]
    pub fn ok(batch: RecordBatch) -> Self {
        Self::Continue(vec![batch])
    }

    /// Creates a [`TransformResult::Continue`] containing multiple transformed record batches.
    #[must_use]
    pub fn ok_multiple(batches: Vec<RecordBatch>) -> Self {
        Self::Continue(batches)
    }

    /// Creates an empty [`TransformResult::Continue`] (records dropped / filtered out).
    #[must_use]
    pub const fn ok_empty() -> Self {
        Self::Continue(Vec::new())
    }

    /// Creates a [`TransformResult::Discard`] result.
    #[must_use]
    pub const fn discard() -> Self {
        Self::Discard
    }

    /// Creates a [`TransformResult::Reject`] result with the specified reason.
    #[must_use]
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    /// Creates a [`TransformResult::Error`] result with the specified reason.
    #[must_use]
    pub fn error(reason: impl Into<String>) -> Self {
        Self::Error {
            reason: reason.into(),
        }
    }
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
