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
    #[allow(clippy::collapsible_if)]
    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {
        let sweep_interval = if self.config.max_file_interval_sec > 0 {
            self.config.max_file_interval_sec.clamp(1, 5)
        } else {
            60
        };
        let mut ticker = tokio::time::interval(Duration::from_secs(sweep_interval));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut final_res = Ok(());

        loop {
            tokio::select! {
                maybe_batch = input.recv() => {
                    if let Some(batch) = maybe_batch {
                        let res = run_blocking(|| {
                            let prepared = self.router.route_and_prepare(batch)?;
                            self.manager.route_prepared_batch(&prepared)
                        });

                        match res {
                            Ok(Ok(())) => {},
                            Err(e) => {
                                final_res = Err(pipeline_core::error::PipelineError::Internal(e.to_string()));
                                break;
                            },
                            Ok(Err(e)) => {
                                final_res = Err(e.into());
                                break;
                            }
                        }
                    } else {
                        tracing::debug!("ParquetSink input channel closed; draining all partition writers");
                        break;
                    }
                }
                _ = ticker.tick() => {
                    let res = run_blocking(|| self.manager.sweep_idle_writers());
                    match res {
                        Ok(Ok(())) => {},
                        Err(e) => {
                            final_res = Err(pipeline_core::error::PipelineError::Internal(e.to_string()));
                            break;
                        },
                        Ok(Err(e)) => {
                            final_res = Err(e.into());
                            break;
                        }
                    }
                }
            }
        }

        let flush_res = run_blocking(|| self.manager.flush_all());
        if let Ok(Err(e)) = flush_res {
            if final_res.is_ok() {
                final_res = Err(e.into());
            }
        } else if let Err(e) = flush_res {
            if final_res.is_ok() {
                final_res = Err(pipeline_core::error::PipelineError::Internal(e.to_string()));
            }
        }

        let wait_res = self.manager.wait_for_all_uploads().await;
        if let Err(e) = wait_res {
            if final_res.is_ok() {
                final_res = Err(e.into());
            }
        }

        if final_res.is_ok() {
            tracing::info!("ParquetSink successfully drained and committed all pending uploads");
        } else {
            tracing::error!(
                "ParquetSink failed, but deterministically flushed and awaited all uploads"
            );
        }

        final_res
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

    #[test]
    fn test_parquet_sink_accessors_and_invalid_config() {
        let valid_config = ParquetSinkConfig {
            storage_uri: "memory://test-sink".to_string(),
            ..Default::default()
        };
        let sink = ParquetSink::try_new(valid_config.clone()).unwrap();
        assert_eq!(sink.config().storage_uri, "memory://test-sink");
        assert_eq!(sink.manager().active_writer_count(), 0);
        assert!(sink.router().is_variant_enabled());

        let invalid_config = ParquetSinkConfig {
            storage_uri: "unsupported_scheme://bucket/prefix".to_string(),
            ..Default::default()
        };
        let err = ParquetSink::try_new(invalid_config);
        assert!(err.is_err());
    }
}
