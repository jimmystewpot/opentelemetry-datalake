//! Least-loaded batch dispatcher distributing telemetry across sandboxed WASM workers.
//!
//! Manages a pool of isolated [`WasmWorker`] instances running on independent tokio
//! tasks, distributing incoming Arrow batches using non-blocking `try_send` with
//! round-robin fallback backpressure, and draining gracefully on input channel closure.

use crate::engine::EngineCache;
use crate::error::WasmTransformError;
use crate::host_calls::MetricRegistry;
use crate::worker::{WasmWorker, WorkerOutcome};
use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, WasmTransformerConfig};
use pipeline_core::pipeline::{PipelineReceiver, PipelineSender, SignalBatch};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use wasmtime::Module;

/// Configuration for the WASM batch dispatcher and worker pool.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Number of concurrent worker instances to run.
    pub concurrency: usize,
    /// Channel capacity for each individual worker task.
    pub worker_channel_capacity: usize,
}

/// Dispatcher distributing incoming telemetry batches across isolated WASM workers.
///
/// Dispatches batches using a least-loaded non-blocking `try_send` strategy across
/// `concurrency` worker tasks, with round-robin fallback `send` providing backpressure.
/// Results from worker executions are routed to primary output or DLQ channels according
/// to configured error and rejection policies.
pub struct WasmDispatcher {
    config: DispatcherConfig,
    engine: Arc<EngineCache>,
    module: Arc<Module>,
    transformer_config: WasmTransformerConfig,
    output: PipelineSender,
    reroute_error: Option<crate::DlqOutput>,
    reroute_reject: Option<crate::DlqOutput>,
    registry: Arc<crate::host_calls::MetricRegistry>,
}

impl WasmDispatcher {
    /// Creates a new `WasmDispatcher`.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: DispatcherConfig,
        engine: Arc<EngineCache>,
        module: Arc<Module>,
        transformer_config: WasmTransformerConfig,
        output: PipelineSender,
        reroute_error: Option<crate::DlqOutput>,
        reroute_reject: Option<crate::DlqOutput>,
        registry: Arc<crate::host_calls::MetricRegistry>,
    ) -> Self {
        Self {
            config,
            engine,
            module,
            transformer_config,
            output,
            reroute_error,
            reroute_reject,
            registry,
        }
    }

    /// Returns a reference to the shared [`MetricRegistry`].
    #[must_use]
    pub fn registry(&self) -> &Arc<MetricRegistry> {
        &self.registry
    }

    /// Runs the dispatcher loop until the `input` channel is closed, then drains workers.
    ///
    /// # Errors
    ///
    /// Returns [`WasmTransformError`] if dispatcher processing encounters an unrecoverable failure.
    pub async fn run(self, mut input: PipelineReceiver) -> Result<(), WasmTransformError> {
        let concurrency = self.config.concurrency.max(1);
        let cap = self.config.worker_channel_capacity.max(1);
        let (worker_txs, worker_handles, init_rxs) = self.spawn_workers(concurrency, cap);

        let mut successful_workers = 0;
        let mut last_err = String::new();
        for rx in init_rxs {
            if let Ok(res) = rx.await {
                match res {
                    Ok(()) => successful_workers += 1,
                    Err(e) => last_err = e,
                }
            }
        }

        if successful_workers == 0 {
            for handle in &worker_handles {
                handle.abort();
            }
            return Err(WasmTransformError::Pipeline(format!(
                "All workers failed to initialize. Last error: {last_err}"
            )));
        }

        if successful_workers < concurrency {
            warn!(
                successful = successful_workers,
                total = concurrency,
                "Dispatcher initialized in degraded state: some workers failed to start"
            );
        }

        let mut next_worker = 0usize;
        while let Some(batch) = input.recv().await {
            if !Self::dispatch_batch(batch, &worker_txs, &mut next_worker, concurrency).await {
                return Err(WasmTransformError::Pipeline(
                    "Dispatcher worker channel closed".into(),
                ));
            }
        }

        drop(worker_txs);
        Self::drain_workers(worker_handles, &self.transformer_config.drain_timeout).await;

        Ok(())
    }

    /// Spawns worker tasks and returns their channel senders, task join handles, and initialization result receivers.
    #[allow(clippy::type_complexity, clippy::too_many_lines)]
    fn spawn_workers(
        &self,
        concurrency: usize,
        cap: usize,
    ) -> (
        Vec<mpsc::Sender<SignalBatch>>,
        Vec<JoinHandle<()>>,
        Vec<tokio::sync::oneshot::Receiver<Result<(), String>>>,
    ) {
        let mut worker_txs = Vec::with_capacity(concurrency);
        let mut worker_handles = Vec::with_capacity(concurrency);
        let mut init_rxs = Vec::with_capacity(concurrency);

        for worker_id in 0..concurrency {
            let (wtx, mut wrx) = mpsc::channel::<SignalBatch>(cap);
            worker_txs.push(wtx);

            let (init_tx, init_rx) = tokio::sync::oneshot::channel();
            init_rxs.push(init_rx);

            let engine = Arc::clone(&self.engine);
            let module = Arc::clone(&self.module);
            let tf_cfg = self.transformer_config.clone();
            let output = self.output.clone();
            let err_tx = self.reroute_error.clone();
            let rej_tx = self.reroute_reject.clone();
            let registry = Arc::clone(&self.registry);

            worker_handles.push(tokio::spawn(async move {
                let tf_cfg_init = tf_cfg.clone();
                let init_outcome = tokio::task::spawn_blocking(move || {
                    match WasmWorker::new(worker_id, engine, module, tf_cfg_init, registry) {
                        Ok(w) => (Ok(()), Some(w)),
                        Err(e) => {
                            warn!(worker_id, "Worker initialization failed: {e}");
                            (Err(e.to_string()), None)
                        }
                    }
                })
                .await;

                let (worker_init_status, worker_opt) = match init_outcome {
                    Ok(pair) => pair,
                    Err(join_err) => {
                        error!(
                            worker_id,
                            "Worker init blocking task join failed: {join_err}"
                        );
                        (Err(format!("Worker init join error: {join_err}")), None)
                    }
                };

                let _ = init_tx.send(worker_init_status);
                let Some(mut worker) = worker_opt else {
                    return;
                };

                while let Some(batch) = wrx.recv().await {
                    let mut w = worker;
                    let Ok((res, returned_worker)) = tokio::task::spawn_blocking(move || {
                        // execute_batch is synchronous, so we can call it inside the blocking closure
                        let res = w.execute_batch(batch);
                        (res, w)
                    })
                    .await
                    else {
                        error!(
                            worker_id,
                            "WasmWorker blocking task failed or was cancelled; terminating worker"
                        );
                        return;
                    };
                    worker = returned_worker;

                    match res {
                        Ok(outcome) => {
                            if !Self::handle_worker_outcome(
                                worker_id,
                                outcome,
                                &tf_cfg,
                                &output,
                                err_tx.as_ref(),
                                rej_tx.as_ref(),
                            )
                            .await
                            {
                                warn!(
                                    worker_id,
                                    "Downstream output channel closed; draining buffered batches"
                                );
                                while let Ok(unprocessed) = wrx.try_recv() {
                                    if let Some(ref dlq) = err_tx {
                                        let _ = dlq.send(unprocessed).await;
                                    }
                                }
                                break;
                            }
                        }
                        Err((original_batch, e)) => {
                            warn!(worker_id, "Worker execution trap or error: {e}");
                            let rejuv_res = worker.rejuvenate();
                            let errored_handled = Self::handle_errored(
                                worker_id,
                                e.to_string(),
                                original_batch,
                                &tf_cfg,
                                &output,
                                err_tx.as_ref(),
                            )
                            .await;
                            if let Err(rejuv_err) = rejuv_res {
                                error!(
                                    worker_id,
                                    "Failed to rejuvenate worker after trap; terminating worker task: {rejuv_err}"
                                );
                                while let Ok(unprocessed) = wrx.try_recv() {
                                    if let Some(ref dlq) = err_tx {
                                        let _ = dlq.send(unprocessed).await;
                                    }
                                }
                                break;
                            }
                            if !errored_handled {
                                warn!(
                                    worker_id,
                                    "Downstream output channel closed; draining buffered batches"
                                );
                                while let Ok(unprocessed) = wrx.try_recv() {
                                    if let Some(ref dlq) = err_tx {
                                        let _ = dlq.send(unprocessed).await;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                info!(worker_id, "Worker drain complete");
            }));
        }

        (worker_txs, worker_handles, init_rxs)
    }

    /// Handles an individual execution outcome from a worker.
    ///
    /// Returns `true` if processing should continue, or `false` if downstream channels
    /// are closed and the worker should terminate.
    async fn handle_worker_outcome(
        worker_id: usize,
        outcome: WorkerOutcome,
        tf_cfg: &WasmTransformerConfig,
        output: &PipelineSender,
        err_tx: Option<&crate::DlqOutput>,
        rej_tx: Option<&crate::DlqOutput>,
    ) -> bool {
        match outcome {
            WorkerOutcome::Emitted(batches) => {
                for b in batches {
                    if let Err(e) = output.send(b).await {
                        warn!(worker_id, "Failed to forward emitted batch to output: {e}");
                        return false;
                    }
                }
                true
            }
            WorkerOutcome::Discarded => true,
            WorkerOutcome::Rejected { reason, original } => {
                Self::handle_rejected(worker_id, reason, original, tf_cfg.on_reject, rej_tx).await
            }
            WorkerOutcome::Errored { reason, original } => {
                Self::handle_errored(worker_id, reason, original, tf_cfg, output, err_tx).await
            }
        }
    }

    /// Routes a rejected batch according to the rejection policy.
    ///
    /// Returns `true` if downstream channel is healthy, or `false` if DLQ channel closed.
    async fn handle_rejected(
        worker_id: usize,
        reason: String,
        original: SignalBatch,
        policy: OnRejectPolicy,
        rej_tx: Option<&crate::DlqOutput>,
    ) -> bool {
        let mut healthy = true;
        if policy == OnRejectPolicy::Reroute {
            if let Some(rtx) = rej_tx {
                if let Err(e) = rtx.send(original).await {
                    warn!(worker_id, "Failed to send rejected batch to DLQ: {e}");
                    healthy = false;
                }
            } else {
                warn!(
                    worker_id,
                    "Reject policy is Reroute, but no DLQ reject channel configured; dropping batch"
                );
            }
        }
        warn!(worker_id, %reason, "Batch rejected by guest");
        healthy
    }

    /// Routes an errored batch according to the error policy.
    ///
    /// Returns `true` if downstream channel is healthy, or `false` if output channel closed.
    async fn handle_errored(
        worker_id: usize,
        reason: String,
        original: SignalBatch,
        tf_cfg: &WasmTransformerConfig,
        output: &PipelineSender,
        err_tx: Option<&crate::DlqOutput>,
    ) -> bool {
        let mut healthy = true;
        match tf_cfg.on_error {
            OnErrorPolicy::Reroute => {
                if let Some(etx) = err_tx {
                    if let Err(e) = etx.send(original).await {
                        warn!(worker_id, "Failed to send errored batch to DLQ: {e}");
                        healthy = false;
                    }
                } else {
                    warn!(
                        worker_id,
                        "Error policy is Reroute, but no DLQ error channel configured; dropping batch"
                    );
                }
            }
            OnErrorPolicy::Passthrough => {
                if tf_cfg.allow_unmasked_passthrough {
                    if let Err(e) = output.send(original).await {
                        warn!(worker_id, "Failed to send passthrough batch to output: {e}");
                        healthy = false;
                    }
                } else {
                    warn!(
                        worker_id,
                        "Dropping errored batch: allow_unmasked_passthrough is false"
                    );
                }
            }
            OnErrorPolicy::Drop => {}
        }
        warn!(worker_id, %reason, "Batch error in guest execution");
        healthy
    }

    /// Dispatches an incoming batch to the least-loaded worker with backpressure fallback.
    async fn dispatch_batch(
        batch: SignalBatch,
        worker_txs: &[mpsc::Sender<SignalBatch>],
        next_worker: &mut usize,
        concurrency: usize,
    ) -> bool {
        let mut pending_batch = Some(batch);
        let mut first_full_idx: Option<usize> = None;

        for offset in 0..concurrency {
            let idx = (next_worker.saturating_add(offset)) % concurrency;
            let Some(target_tx) = worker_txs.get(idx) else {
                continue;
            };
            if let Some(b) = pending_batch.take() {
                match target_tx.try_send(b) {
                    Ok(()) => {
                        *next_worker = (idx.saturating_add(1)) % concurrency;
                        return true;
                    }
                    Err(mpsc::error::TrySendError::Full(returned)) => {
                        if first_full_idx.is_none() {
                            first_full_idx = Some(idx);
                        }
                        pending_batch = Some(returned);
                    }
                    Err(mpsc::error::TrySendError::Closed(returned)) => {
                        pending_batch = Some(returned);
                    }
                }
            }
        }

        if let Some(b) = pending_batch {
            let Some(full_idx) = first_full_idx else {
                warn!(
                    "All worker channels are closed. Aborting dispatch loop to propagate backpressure and prevent data loss."
                );
                return false;
            };

            let Some(target_tx) = worker_txs.get(full_idx) else {
                return false;
            };

            if let Err(mpsc::error::SendError(returned_batch)) = target_tx.send(b).await {
                warn!(
                    full_idx,
                    "Worker channel closed during backpressure send. Attempting retry across surviving workers..."
                );
                let mut routed = false;
                for retry_offset in 1..concurrency {
                    let retry_idx = (full_idx.saturating_add(retry_offset)) % concurrency;
                    if let Some(retry_tx) = worker_txs.get(retry_idx)
                        && retry_tx.try_send(returned_batch.clone()).is_ok()
                    {
                        routed = true;
                        *next_worker = (retry_idx.saturating_add(1)) % concurrency;
                        break;
                    }
                }
                if !routed {
                    warn!(
                        "Could not reroute pending batch across surviving workers; aborting dispatch loop"
                    );
                    return false;
                }
            } else {
                *next_worker = (full_idx.saturating_add(1)) % concurrency;
            }
        }

        true
    }

    /// Drains worker tasks up to the configured drain timeout.
    async fn drain_workers(handles: Vec<JoinHandle<()>>, drain_timeout_str: &str) {
        let timeout_dur =
            parse_duration(drain_timeout_str).unwrap_or_else(|| std::time::Duration::from_secs(10));

        let abort_handles: Vec<_> = handles.iter().map(JoinHandle::abort_handle).collect();

        let drain_result = tokio::time::timeout(timeout_dur, async {
            for handle in handles {
                if let Err(e) = handle.await {
                    warn!("Worker task panicked during drain: {e:?}");
                }
            }
        })
        .await;

        if drain_result.is_err() {
            warn!("Worker drain timed out after {timeout_dur:?}; aborting lingering workers");
            for abort_handle in abort_handles {
                abort_handle.abort();
            }
        }
    }
}

use crate::worker::parse_duration;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    fn empty_batch() -> SignalBatch {
        SignalBatch::Logs(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }

    #[tokio::test]
    async fn test_dispatch_batch_backpressures_on_live_worker_when_another_is_closed() {
        let (tx0, mut rx0) = mpsc::channel::<SignalBatch>(1);
        let (tx1, rx1) = mpsc::channel::<SignalBatch>(1);

        // Fill tx0 so try_send to tx0 will return Full
        tx0.try_send(empty_batch()).expect("fill tx0");

        // Close rx1 so tx1 is closed
        drop(rx1);

        // Spawn a background task to drain rx0 so the backpressure send succeeds without dropping the receiver
        let drain_handle = tokio::spawn(async move {
            let _ = rx0.recv().await;
            let _ = rx0.recv().await;
        });

        let worker_txs = vec![tx0, tx1];
        let mut next_worker = 1;

        let live =
            WasmDispatcher::dispatch_batch(empty_batch(), &worker_txs, &mut next_worker, 2).await;
        assert!(
            live,
            "dispatcher must backpressure on live worker and succeed rather than aborting on closed worker"
        );
        drain_handle.await.expect("drain task");
    }

    #[tokio::test]
    async fn test_dispatch_batch_aborts_when_all_workers_are_closed() {
        let (tx0, rx0) = mpsc::channel::<SignalBatch>(1);
        let (tx1, rx1) = mpsc::channel::<SignalBatch>(1);

        // Close both worker receivers
        drop(rx0);
        drop(rx1);

        let worker_txs = vec![tx0, tx1];
        let mut next_worker = 0;

        let live =
            WasmDispatcher::dispatch_batch(empty_batch(), &worker_txs, &mut next_worker, 2).await;
        assert!(
            !live,
            "dispatcher must abort when all worker channels are closed"
        );
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("500ms"),
            Some(std::time::Duration::from_millis(500))
        );
        assert_eq!(
            parse_duration("5s"),
            Some(std::time::Duration::from_secs(5))
        );
        assert_eq!(
            parse_duration("1m"),
            Some(std::time::Duration::from_secs(60))
        );
        assert_eq!(
            parse_duration("10"),
            Some(std::time::Duration::from_secs(10))
        );
    }

    #[tokio::test]
    async fn test_drain_workers_aborts_lingering_tasks_on_timeout() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct DropDetector(Arc<AtomicBool>);
        impl Drop for DropDetector {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_clone = Arc::clone(&dropped);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _detector = DropDetector(dropped_clone);
            let _ = started_tx.send(());
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        });

        let _ = started_rx.await;

        WasmDispatcher::drain_workers(vec![handle], "10ms").await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            dropped.load(Ordering::SeqCst),
            "Lingering worker task must be aborted and dropped on drain timeout"
        );
    }
}
