import re

with open('crates/parquet-sink/src/sink.rs', 'r') as f:
    content = f.read()

# We need to change:
#                     if let Some(batch) = maybe_batch {
#                         run_blocking(|| {
#                             let prepared = self.router.route_and_prepare(batch)?;
#                             self.manager.route_prepared_batch(&prepared)
#                         })??;
#                     }

new_loop = """
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
            tracing::error!("ParquetSink failed, but deterministically flushed and awaited all uploads");
        }

        final_res
"""

content = re.sub(
    r'loop \{.*Ok\(\(\)\)\n',
    new_loop + '\n',
    content,
    flags=re.DOTALL
)

with open('crates/parquet-sink/src/sink.rs', 'w') as f:
    f.write(content)

