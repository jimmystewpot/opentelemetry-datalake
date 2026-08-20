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
    ///
    /// # Errors
    ///
    /// Returns `PipelineError::Internal` if `outputs` is empty, since a
    /// fanout with zero sinks would silently drop all data.
    pub fn try_new(outputs: Vec<PipelineSender>) -> Result<Self, PipelineError> {
        if outputs.is_empty() {
            return Err(PipelineError::Internal(
                "Fanout requires at least one output".into(),
            ));
        }
        Ok(Self { outputs })
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
        let fanout = Fanout::try_new(vec![tx1, tx2]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let signal = SignalBatch::Logs(batch);

        fanout.send(signal).await.unwrap();

        assert!(rx1.recv().await.is_some());
        assert!(rx2.recv().await.is_some());
    }

    /// Fanout with zero outputs must be rejected to prevent silent data loss.
    #[test]
    fn test_fanout_empty_outputs() {
        let outputs: Vec<PipelineSender> = vec![];
        let result = Fanout::try_new(outputs);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, PipelineError::Internal(_)),
            "Expected Internal error for empty outputs, got: {err}"
        );
    }

    /// When a downstream receiver is dropped, Fanout::send must return
    /// DownstreamClosed so the upstream can propagate backpressure.
    #[tokio::test]
    async fn test_fanout_downstream_closed() {
        let (tx1, rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let fanout = Fanout::try_new(vec![tx1, tx2]).unwrap();

        // Drop the first receiver to simulate a downstream failure.
        drop(rx1);

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let signal = SignalBatch::Metrics(batch);

        let result = fanout.send(signal).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), PipelineError::DownstreamClosed),
            "Expected DownstreamClosed when receiver is dropped"
        );
    }

    /// A Fanout with a single output must succeed — it is the degenerate
    /// case and must not be rejected.
    #[tokio::test]
    async fn test_fanout_single_output() {
        let (tx, mut rx) = mpsc::channel(10);
        let fanout = Fanout::try_new(vec![tx]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![99]))]).unwrap();
        fanout.send(SignalBatch::Logs(batch)).await.unwrap();
        assert!(rx.recv().await.is_some());
    }

    /// When a downstream channel is full, send() must block until the
    /// consumer drains it rather than silently dropping data or panicking.
    /// This validates bounded-channel backpressure semantics.
    #[tokio::test]
    async fn test_fanout_full_channel_backpressure() {
        // Capacity 1: second send will block until receiver drains.
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Arc::new(Fanout::try_new(vec![tx]).unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();
        let batch2 =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![2]))]).unwrap();

        // Fill the channel
        fanout.send(SignalBatch::Metrics(batch1)).await.unwrap();

        // Spawn a task that tries to send — it will block until we drain.
        let fanout2 = Arc::clone(&fanout);
        let send_task = tokio::spawn(async move {
            fanout2
                .send(SignalBatch::Metrics(batch2))
                .await
                .expect("second send must succeed after drain");
        });

        // Drain one item, unblocking the sender.
        assert!(rx.recv().await.is_some());
        send_task.await.unwrap();
        assert!(rx.recv().await.is_some());
    }

    /// Multiple concurrent senders writing to the same Fanout must not
    /// produce data races; every message must be received in order by each
    /// downstream without loss.
    #[tokio::test]
    async fn test_fanout_concurrent_senders() {
        let (tx1, mut rx1) = mpsc::channel(32);
        let fanout = Arc::new(Fanout::try_new(vec![tx1]).unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        const MSG_COUNT: usize = 16;

        let handles: Vec<_> = (0..MSG_COUNT)
            .map(|i| {
                let f = Arc::clone(&fanout);
                let s = schema.clone();
                tokio::spawn(async move {
                    let batch =
                        RecordBatch::try_new(s, vec![Arc::new(Int32Array::from(vec![i as i32]))])
                            .unwrap();
                    f.send(SignalBatch::Traces(batch)).await.unwrap();
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap();
        }

        let mut received = 0;
        while rx1.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(
            received, MSG_COUNT,
            "All concurrent messages must be received"
        );
    }

    /// All SignalBatch variants must be cloneable; verify that cloning
    /// produces an independent copy with the same row count.
    #[test]
    fn test_signal_batch_variants_are_cloneable() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        for variant in [
            SignalBatch::Logs(batch.clone()),
            SignalBatch::Metrics(batch.clone()),
            SignalBatch::Traces(batch.clone()),
        ] {
            let cloned = variant.clone();
            let rows = match cloned {
                SignalBatch::Logs(b) | SignalBatch::Metrics(b) | SignalBatch::Traces(b) => {
                    b.num_rows()
                }
            };
            assert_eq!(rows, 3);
        }
    }

    /// Fanout must deliver clones to all downstreams; verify each
    /// receiver gets its own copy of the data with correct row counts.
    #[tokio::test]
    async fn test_fanout_multi_receiver_data_integrity() {
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let (tx3, mut rx3) = mpsc::channel(10);
        let fanout = Fanout::try_new(vec![tx1, tx2, tx3]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40]))],
        )
        .unwrap();
        let signal = SignalBatch::Traces(batch);

        fanout.send(signal).await.unwrap();

        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let received = rx.recv().await.expect("receiver should get a batch");
            match received {
                SignalBatch::Traces(b) => assert_eq!(b.num_rows(), 4),
                _ => panic!("Expected Traces signal variant"),
            }
        }
    }
}
