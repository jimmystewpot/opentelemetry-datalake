use crate::error::PipelineError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Strongly typed event wrapper for Arrow batches indicating the signal type.
#[derive(Debug, Clone)]
pub enum SignalBatch {
    /// Batch containing OpenTelemetry log data.
    Logs(RecordBatch),
    /// Batch containing OpenTelemetry metric data.
    Metrics(RecordBatch),
    /// Batch containing OpenTelemetry trace data.
    Traces(RecordBatch),
}

/// Sender type for pushing records into the pipeline.
pub type PipelineSender = mpsc::Sender<SignalBatch>;

/// Receiver type for pulling records from the pipeline.
pub type PipelineReceiver = mpsc::Receiver<SignalBatch>;

/// Represents a source capable of pushing into multiple signal-specific downstreams.
#[async_trait]
pub trait Source: Send + Sync {
    /// Start the source task.
    async fn run(&mut self) -> Result<(), PipelineError>;
}

/// Represents a stateless transformation on the record batches.
#[async_trait]
pub trait Transform: Send + Sync {
    /// Process an input stream into an output stream.
    async fn transform(
        &mut self,
        input: PipelineReceiver,
        output: PipelineSender,
    ) -> Result<(), PipelineError>;
}

/// Represents a sink writing batches to an external system.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Start the sink processing task.
    async fn run(&mut self, input: PipelineReceiver) -> Result<(), PipelineError>;
}

/// Fanout structure handling routing to multiple downstreams.
#[derive(Debug, Clone)]
pub struct Fanout {
    outputs: Vec<PipelineSender>,
}

impl Fanout {
    /// Creates a new Fanout instance.
    #[must_use]
    pub fn new(outputs: Vec<PipelineSender>) -> Self {
        Self { outputs }
    }

    /// Sends a batch to all downstreams.
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::DownstreamClosed` if any receiver has hung up.
    pub async fn send(&self, batch: SignalBatch) -> Result<(), PipelineError> {
        for output in &self.outputs {
            output
                .send(batch.clone())
                .await
                .map_err(|_| PipelineError::DownstreamClosed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_fanout_send() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let fanout = Fanout::new(vec![tx1, tx2]);

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let signal = SignalBatch::Logs(batch);

        fanout.send(signal).await.unwrap();

        assert!(rx1.recv().await.is_some());
        assert!(rx2.recv().await.is_some());
    }
}
