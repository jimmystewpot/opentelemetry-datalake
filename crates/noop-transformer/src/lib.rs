use async_trait::async_trait;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, PipelineSender, Transform};

/// Pass-through transformer that forwards all received `SignalBatch`es downstream.
#[derive(Default)]
pub struct NoopTransformer;

impl NoopTransformer {
    /// Creates a new `NoopTransformer`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Transform for NoopTransformer {
    async fn transform(
        &mut self,
        mut input: PipelineReceiver,
        output: PipelineSender,
    ) -> Result<(), PipelineError> {
        while let Some(batch) = input.recv().await {
            output
                .send(batch)
                .await
                .map_err(|_| PipelineError::DownstreamClosed)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use pipeline_core::pipeline::SignalBatch;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_noop_transformer() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        let mut transformer = NoopTransformer::new();

        let handle = tokio::spawn(async move {
            let _ = transformer.transform(input_rx, output_tx).await;
        });

        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let signal = SignalBatch::Logs(batch);

        input_tx.send(signal).await.unwrap();
        drop(input_tx);

        let received = output_rx.recv().await.unwrap();
        match received {
            SignalBatch::Logs(b) => {
                assert_eq!(b.num_rows(), 3);
            }
            _ => panic!("Expected Logs"),
        }

        handle.await.unwrap();
    }
}
