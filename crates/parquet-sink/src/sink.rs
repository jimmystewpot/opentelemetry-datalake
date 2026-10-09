//! Implementation of the `Sink` pipeline trait and graceful shutdown drain for `ParquetSink`.

use std::time::Duration;

use async_trait::async_trait;
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, Sink};

use crate::config::ParquetSinkConfig;
use crate::partition::PartitionManager;
use crate::router::SignalRouter;

/// Streaming Parquet sink implementing the [`Sink`] trait.
///
/// Ingests incoming OpenTelemetry [`pipeline_core::pipeline::SignalBatch`] payloads,
/// optionally encodes semi-structured attributes using the Variant binary format,
/// routes records to temporal directory partitions, writes compressed Parquet files,
/// and streams them to storage backends via `OpenDAL`.
#[derive(Debug)]
pub struct ParquetSink {
    config: ParquetSinkConfig,
    manager: PartitionManager,
    router: SignalRouter,
}

impl ParquetSink {
    /// Creates a new `ParquetSink` from configuration.
    ///
    /// Initializes the `OpenDAL` storage operator, signal router, and partition manager.
    ///
    /// # Errors
    /// Returns [`PipelineError`] if the storage operator cannot be constructed.
    pub fn try_new(config: ParquetSinkConfig) -> Result<Self, PipelineError> {
        let operator = config.build_operator()?;
        let router = SignalRouter::new(config.variant_encoding);
        let manager = PartitionManager::new(config.clone(), operator);
        Ok(Self {
            config,
            manager,
            router,
        })
    }

    /// Returns a reference to the sink configuration.
    #[must_use]
    pub fn config(&self) -> &ParquetSinkConfig {
        &self.config
    }

    /// Returns a reference to the partition manager.
    #[must_use]
    pub fn manager(&self) -> &PartitionManager {
        &self.manager
    }

    /// Returns a reference to the signal router.
    #[must_use]
    pub fn router(&self) -> &SignalRouter {
        &self.router
    }
}

#[async_trait]
impl Sink for ParquetSink {
    /// Runs the sink processing loop, consuming signal batches from `input` and sweeping idle writers.
    ///
    /// Periodically checks active writers and rolls idle partitions. When the input channel
    /// closes (EOF or graceful shutdown), all active partition writers are flushed and
    /// any in-flight background uploads are awaited before returning.
    ///
    /// # Errors
    /// Returns [`PipelineError`] if batch routing, flushing, or background uploading fails.
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        let sweep_interval = 1;
        let mut ticker = tokio::time::interval(Duration::from_secs(sweep_interval));

        loop {
            tokio::select! {
                maybe_batch = input.recv() => {
                    if let Some(batch) = maybe_batch {
                        run_blocking(|| {
                            let prepared = self.router.route_and_prepare(batch)?;
                            self.manager.route_prepared_batch(&prepared)
                        })??;
                    } else {
                        tracing::debug!("ParquetSink input channel closed; draining all partition writers");
                        break;
                    }
                }
                _ = ticker.tick() => {
                    run_blocking(|| self.manager.sweep_idle_writers())??;
                }
            }
        }

        run_blocking(|| self.manager.flush_all())??;
        self.manager.wait_for_all_uploads().await?;
        tracing::info!("ParquetSink successfully drained and committed all pending uploads");

        Ok(())
    }
}

/// Executes a closure using `block_in_place` to prevent executor starvation.
/// Rejects execution on single-threaded runtimes to prevent deadlocks and panics.
fn run_blocking<F, R>(f: F) -> Result<R, PipelineError>
where
    F: FnOnce() -> R,
{
    let is_multithread = tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);

    if !is_multithread {
        return Err(PipelineError::Internal(
            "ParquetSink requires a multi-threaded Tokio runtime to safely execute synchronous file uploads. \
             Current-thread runtimes will deadlock and panic.".to_string(),
        ));
    }

    Ok(tokio::task::block_in_place(f))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_blocking_rejects_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        rt.block_on(async {
            let res = run_blocking(|| 42);
            assert!(res.is_err());
            assert!(
                res.unwrap_err()
                    .to_string()
                    .contains("Current-thread runtimes will deadlock")
            );
        });
    }
}
