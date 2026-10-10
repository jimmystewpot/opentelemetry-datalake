//! Asynchronous multipart and streaming uploader using `OpenDAL`.
//!
//! Provides bounded channel buffering between chunk producers (synchronous or
//! asynchronous) and an asynchronous `OpenDAL` writer background task, ensuring
//! drop abort safety and clean completion for both small (< 5MB) and large files.

use crate::error::ParquetSinkError;
use opendal::Operator;

/// Default capacity for the bounded chunk channel.
const DEFAULT_CHANNEL_CAPACITY: usize = 8;

/// Sender handle for streaming byte chunks into the asynchronous uploader.
#[derive(Debug)]
pub enum UploaderMessage {
    Chunk(bytes::Bytes),
    Finish,
}

#[derive(Debug)]
pub struct UploaderSender {
    tx: tokio::sync::mpsc::Sender<UploaderMessage>,
}

impl UploaderSender {
    /// Sends a chunk of bytes to the background uploader synchronously.
    ///
    /// This method is intended for synchronous callers (such as Parquet writers running
    /// inside worker threads or `spawn_blocking`). If the bounded channel is full,
    /// it blocks until buffer space becomes available.
    pub fn send_chunk(&self, chunk: bytes::Bytes) -> Result<(), ParquetSinkError> {
        match self.tx.try_send(UploaderMessage::Chunk(chunk)) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(
                ParquetSinkError::Internal("Uploader channel closed".to_string()),
            ),
            Err(tokio::sync::mpsc::error::TrySendError::Full(chunk)) => self
                .tx
                .blocking_send(chunk)
                .map_err(|e| ParquetSinkError::Internal(format!("Uploader channel closed: {e}"))),
        }
    }

    /// Sends a chunk of bytes to the background uploader asynchronously.
    ///
    /// Awaits until space is available in the bounded buffer before enqueuing.
    pub async fn send_chunk_async(&self, chunk: bytes::Bytes) -> Result<(), ParquetSinkError> {
        self.tx
            .send(UploaderMessage::Chunk(chunk))
            .await
            .map_err(|e| ParquetSinkError::Internal(format!("Uploader channel closed: {e}")))
    }

    /// Explicitly completes the sender, closing the channel and notifying the
    /// background task that all chunks have been emitted.
    pub fn finish(self) -> Result<(), ParquetSinkError> {
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) =
            self.tx.try_send(UploaderMessage::Finish)
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        let _ = self.tx.send(msg).await;
                    });
                });
            } else {
                let _ = self.tx.blocking_send(msg);
            }
        }
        drop(self);
        Ok(())
    }
}

/// Completion handle for an active background upload.
///
/// If dropped before [`Self::wait_for_completion`] successfully returns, the background
/// writer task is instructed to abort the upload, preventing orphaned multipart uploads.
#[derive(Debug)]
pub struct UploaderHandle {
    join_handle: Option<tokio::task::JoinHandle<Result<(), ParquetSinkError>>>,
    abort_tx: Option<tokio::sync::oneshot::Sender<()>>,
    completed: bool,
}

impl UploaderHandle {
    /// Awaits completion of the background upload task and finalizes file commit.
    pub async fn wait_for_completion(mut self) -> Result<(), ParquetSinkError> {
        let join_handle = self.join_handle.take().ok_or_else(|| {
            ParquetSinkError::Internal("Uploader task already completed".to_string())
        })?;

        let res = match join_handle.await {
            Ok(inner_res) => inner_res,
            Err(join_err) => {
                if join_err.is_cancelled() {
                    Err(ParquetSinkError::Internal(
                        "Uploader task was cancelled".to_string(),
                    ))
                } else {
                    Err(ParquetSinkError::Internal(format!(
                        "Uploader task panicked: {join_err}"
                    )))
                }
            }
        };

        if res.is_ok() {
            self.completed = true;
        }
        res
    }
}

impl Drop for UploaderHandle {
    fn drop(&mut self) {
        let abort_tx = if self.completed {
            None
        } else {
            self.abort_tx.take()
        };
        if let Some(tx) = abort_tx {
            let _ = tx.send(());
        }
    }
}

/// Factory for launching asynchronous streaming uploads via `OpenDAL`.
#[derive(Debug)]
pub struct AsyncUploader;

impl AsyncUploader {
    /// Starts an asynchronous upload to the target path on the given `OpenDAL` operator.
    ///
    /// Spawns a background task that receives chunks over a bounded channel of capacity 8
    /// and streams them into an `opendal::Writer`.
    pub fn start(
        op: &Operator,
        path: &str,
    ) -> Result<(UploaderSender, UploaderHandle), ParquetSinkError> {
        if path.trim().is_empty() {
            return Err(ParquetSinkError::Config(
                "Storage path cannot be empty".to_string(),
            ));
        }

        if tokio::runtime::Handle::try_current().is_err() {
            return Err(ParquetSinkError::Internal(
                "AsyncUploader::start must be called within an active Tokio runtime".to_string(),
            ));
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<UploaderMessage>(DEFAULT_CHANNEL_CAPACITY);
        let (abort_tx, mut abort_rx) = tokio::sync::oneshot::channel::<()>();

        let path_owned = path.to_string();
        let op_clone = op.clone();

        let join_handle = tokio::spawn(async move {
            let mut writer = match op_clone.writer(&path_owned).await {
                Ok(w) => w,
                Err(e) => return Err(ParquetSinkError::from(e)),
            };

            let mut aborted = false;

            loop {
                tokio::select! {
                    biased;

                    _ = &mut abort_rx => {
                        aborted = true;
                        break;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(UploaderMessage::Chunk(bytes)) => {
                                if let Err(e) = writer.write(bytes).await {
                                    let _ = writer.abort().await;
                                    return Err(ParquetSinkError::from(e));
                                }
                            }
                            Some(UploaderMessage::Finish) => {
                                break;
                            }
                            None => {
                                aborted = true;
                                break;
                            }
                        }
                    }
                }
            }

            if aborted {
                let _ = writer.abort().await;
                return Err(ParquetSinkError::Internal(format!(
                    "Upload aborted for path: {path_owned}"
                )));
            }

            tokio::select! {
                biased;

                _ = &mut abort_rx => {
                    let _ = writer.abort().await;
                    Err(ParquetSinkError::Internal(format!(
                        "Upload aborted for path: {path_owned}"
                    )))
                }
                close_res = writer.close() => {
                    match close_res {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            let _ = writer.abort().await;
                            Err(ParquetSinkError::from(e))
                        }
                    }
                }
            }
        });

        let sender = UploaderSender { tx };
        let handle = UploaderHandle {
            join_handle: Some(join_handle),
            abort_tx: Some(abort_tx),
            completed: false,
        };

        Ok((sender, handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Operator;
    use opendal::services::Memory;

    #[tokio::test]
    async fn test_upload_file_smaller_than_5mb_succeeds_on_complete() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/small.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        // Send a 1MB chunk (less than 5MB S3 limit)
        let chunk = bytes::Bytes::from(vec![0u8; 1024 * 1024]);
        sender.send_chunk(chunk).unwrap();
        sender.finish().unwrap();

        handle.wait_for_completion().await.unwrap();

        let meta = op.stat(path).await.unwrap();
        assert_eq!(meta.content_length(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_upload_multiple_chunks_succeeds() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/multi_chunk.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        let chunk_size = 512 * 1024;
        for _ in 0..4 {
            let chunk = bytes::Bytes::from(vec![42u8; chunk_size]);
            sender.send_chunk(chunk).unwrap();
        }
        sender.finish().unwrap();

        handle.wait_for_completion().await.unwrap();

        let meta = op.stat(path).await.unwrap();
        assert_eq!(meta.content_length(), (4 * chunk_size) as u64);
    }

    #[tokio::test]
    async fn test_upload_async_sender_succeeds() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/async_chunks.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        let chunk = bytes::Bytes::from_static(b"hello world via async send");
        sender.send_chunk_async(chunk).await.unwrap();
        sender.finish().unwrap();

        handle.wait_for_completion().await.unwrap();

        let meta = op.stat(path).await.unwrap();
        assert_eq!(meta.content_length(), 26);
    }

    #[tokio::test]
    async fn test_uploader_drop_handle_aborts_upload() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/dropped_handle.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        let chunk = bytes::Bytes::from(vec![1u8; 1024 * 1024]);
        sender.send_chunk(chunk).unwrap();

        // Drop handle prematurely before calling wait_for_completion
        drop(handle);

        // Allow background task to process the abort signal
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // The file should not be finalized in storage
        let exists = op.exists(path).await.unwrap();
        assert!(!exists, "Aborted upload must not exist in storage");
    }

    #[tokio::test]
    async fn test_uploader_empty_path_rejected() {
        let op = Operator::new(Memory::default()).unwrap();
        let res = AsyncUploader::start(&op, "");
        assert!(res.is_err(), "Empty path must be rejected");
    }

    #[tokio::test]
    async fn test_uploader_send_chunk_fails_when_aborted() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/aborted_sender.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        // Drop handle prematurely
        drop(handle);

        // Allow background task to abort and drop receiver
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Subsequent send should fail due to closed channel
        let chunk = bytes::Bytes::from_static(b"late data");
        let res = sender.send_chunk(chunk);
        assert!(res.is_err(), "Send after abort must fail");
    }

    #[test]
    fn test_uploader_outside_tokio_runtime_returns_error() {
        let op = Operator::new(Memory::default()).unwrap();
        let res = AsyncUploader::start(&op, "test/no_runtime.parquet");
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("active Tokio runtime")
        );
    }

    #[tokio::test]
    async fn test_uploader_spawn_blocking_drains_many_chunks() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/many_chunks_blocking.parquet";
        let (sender, handle) = AsyncUploader::start(&op, path).unwrap();

        // 25 chunks of 64KB exceeds channel capacity of 8
        let jh = tokio::task::spawn_blocking(move || {
            for i in 0u8..25 {
                let chunk = bytes::Bytes::from(vec![i; 64 * 1024]);
                sender.send_chunk(chunk)?;
            }
            sender.finish()
        });

        jh.await.unwrap().unwrap();
        handle.wait_for_completion().await.unwrap();

        let meta = op.stat(path).await.unwrap();
        assert_eq!(meta.content_length(), (25 * 64 * 1024) as u64);
    }
}
