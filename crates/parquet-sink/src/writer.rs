//! Decoupled synchronous Parquet writer with Bloom filters and selectable compression.
//!
//! Provides [`PartitionWriter`], which encapsulates Apache Arrow's [`ArrowWriter`]
//! configured with Parquet 2.0 properties, dictionary encoding, page-level statistics,
//! and Bloom filters on `trace_id` and `span_id` columns, streaming encoded byte chunks
//! into an asynchronous [`UploaderSender`].

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

use crate::config::ParquetSinkConfig;
use crate::error::ParquetSinkError;
use crate::uploader::UploaderSender;

/// Default buffer capacity for the internal writer chunk buffer (256 KB).
const DEFAULT_CHUNK_BUFFER_SIZE: usize = 256 * 1024;

/// Target false positive probability for Bloom filters on tracing identifiers.
const BLOOM_FILTER_FPP: f64 = 0.01;

/// Distinct value estimate (NDV) for Bloom filters on tracing identifiers.
const BLOOM_FILTER_NDV: u64 = 1_000_000;

/// An adapter implementing [`std::io::Write`] that buffers bytes and streams chunks
/// to an [`UploaderSender`].
#[derive(Debug)]
pub struct ChannelWriter {
    sender: Option<UploaderSender>,
    buffer: Vec<u8>,
    buffer_capacity: usize,
    bytes_written: usize,
}

impl ChannelWriter {
    /// Creates a new `ChannelWriter` that sends byte chunks to the provided `uploader`.
    #[must_use]
    pub fn new(uploader: UploaderSender, buffer_capacity: usize) -> Self {
        let capacity = if buffer_capacity == 0 {
            DEFAULT_CHUNK_BUFFER_SIZE
        } else {
            buffer_capacity
        };
        Self {
            sender: Some(uploader),
            buffer: Vec::with_capacity(capacity),
            buffer_capacity: capacity,
            bytes_written: 0,
        }
    }

    /// Flushes any buffered bytes and marks the uploader channel complete.
    pub fn finish(&mut self) -> Result<(), ParquetSinkError> {
        self.flush_buffer()
            .map_err(|e| ParquetSinkError::Internal(e.to_string()))?;
        if let Some(sender) = self.sender.take() {
            sender.finish()?;
        }
        Ok(())
    }

    /// Returns the total number of bytes written to this adapter.
    #[must_use]
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn flush_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = bytes::Bytes::copy_from_slice(&self.buffer);
        self.buffer.clear();
        self.send_chunk(chunk)
    }

    fn send_chunk(&self, chunk: bytes::Bytes) -> std::io::Result<()> {
        let sender = self.sender.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "uploader sender already closed",
            )
        })?;
        sender
            .send_chunk(chunk)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e.to_string()))
    }
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Flush existing buffered data if new slice would exceed capacity
        if !self.buffer.is_empty() && self.buffer.len() + buf.len() > self.buffer_capacity {
            self.flush_buffer()?;
        }

        // If slice alone is at or above capacity, stream directly without buffering
        if buf.len() >= self.buffer_capacity {
            let chunk = bytes::Bytes::copy_from_slice(buf);
            self.send_chunk(chunk)?;
            self.bytes_written += buf.len();
            return Ok(buf.len());
        }

        self.buffer.extend_from_slice(buf);
        self.bytes_written += buf.len();

        if self.buffer.len() >= self.buffer_capacity {
            self.flush_buffer()?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buffer()
    }
}

impl Drop for ChannelWriter {
    fn drop(&mut self) {
        if self.sender.is_some() {
            let _ = self.flush_buffer();
            if let Some(sender) = self.sender.take() {
                let _ = sender.finish();
            }
        }
    }
}

/// Builds Parquet [`WriterProperties`] based on the sink configuration.
fn build_writer_properties(config: &ParquetSinkConfig) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(config.compression.to_parquet_compression())
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page);

    if config.max_file_size_bytes > 0 {
        builder = builder.set_max_row_group_bytes(Some(config.max_file_size_bytes));
    }
    if config.max_records > 0 {
        builder = builder.set_max_row_group_row_count(Some(config.max_records));
    }

    builder = builder
        .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
        .set_column_bloom_filter_fpp(ColumnPath::from("trace_id"), BLOOM_FILTER_FPP)
        .set_column_bloom_filter_max_ndv(ColumnPath::from("trace_id"), BLOOM_FILTER_NDV)
        .set_column_bloom_filter_enabled(ColumnPath::from("span_id"), true)
        .set_column_bloom_filter_fpp(ColumnPath::from("span_id"), BLOOM_FILTER_FPP)
        .set_column_bloom_filter_max_ndv(ColumnPath::from("span_id"), BLOOM_FILTER_NDV);

    builder.build()
}

/// Decoupled synchronous Parquet writer with Bloom filter support and selectable compression.
///
/// Encapsulates an [`ArrowWriter`] writing into an asynchronous [`UploaderSender`] via a
/// bounded chunk channel, configuring dictionary encoding, page statistics, and Bloom filters
/// for tracing identifiers (`trace_id` and `span_id`).
#[derive(Debug)]
pub struct PartitionWriter {
    arrow_writer: ArrowWriter<ChannelWriter>,
    records_written: usize,
}

impl PartitionWriter {
    /// Creates a new `PartitionWriter` with the given schema, uploader sender, and configuration.
    pub fn try_new(
        schema: Arc<Schema>,
        uploader: UploaderSender,
        config: &ParquetSinkConfig,
    ) -> Result<Self, ParquetSinkError> {
        let props = build_writer_properties(config);
        let channel_writer = ChannelWriter::new(uploader, DEFAULT_CHUNK_BUFFER_SIZE);
        let arrow_writer = ArrowWriter::try_new(channel_writer, schema, Some(props))?;
        Ok(Self {
            arrow_writer,
            records_written: 0,
        })
    }

    /// Alias for [`Self::try_new`].
    pub fn new(
        schema: Arc<Schema>,
        uploader: UploaderSender,
        config: &ParquetSinkConfig,
    ) -> Result<Self, ParquetSinkError> {
        Self::try_new(schema, uploader, config)
    }

    /// Writes an Arrow [`RecordBatch`] into the Parquet file.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), ParquetSinkError> {
        self.arrow_writer.write(batch)?;
        self.records_written += batch.num_rows();
        Ok(())
    }

    /// Closes and finalizes the Parquet writer.
    ///
    /// Writes the Parquet metadata footer, flushes all remaining buffered bytes
    /// into the underlying [`UploaderSender`], and signals completion to the uploader.
    pub fn close(self) -> Result<(), ParquetSinkError> {
        let mut channel_writer = self.arrow_writer.into_inner()?;
        channel_writer.finish()?;
        Ok(())
    }

    /// Flushes in-progress Arrow row groups to the underlying uploader.
    pub fn flush(&mut self) -> Result<(), ParquetSinkError> {
        self.arrow_writer.flush()?;
        Ok(())
    }

    /// Returns the total number of records successfully written so far.
    #[must_use]
    pub fn records_written(&self) -> usize {
        self.records_written
    }

    /// Returns the total number of bytes written to the underlying uploader so far.
    #[must_use]
    pub fn bytes_written(&self) -> usize {
        self.arrow_writer.bytes_written()
    }

    /// Returns the estimated in-progress row group memory size in bytes.
    #[must_use]
    pub fn in_progress_size(&self) -> usize {
        self.arrow_writer.in_progress_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use opendal::Operator;
    use opendal::services::Memory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[tokio::test]
    async fn test_writer_with_zstd_and_bloom_filter() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/test.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let config = crate::config::ParquetSinkConfig {
            compression: crate::config::CompressionCodec::Zstd { level: Some(3) },
            ..Default::default()
        };

        let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["trace-12345", "trace-67890"])),
                Arc::new(Int64Array::from(vec![42, 99])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        assert_eq!(writer.records_written(), 2);
        assert!(writer.in_progress_size() > 0);

        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let meta = op.stat(path).await.unwrap();
        assert!(meta.content_length() > 0);

        // Read back the parquet file and verify bloom filter & compression
        let data = op.read(path).await.unwrap();
        let bytes = Bytes::from(data.to_vec());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let parquet_meta = reader.metadata();
        let row_group = parquet_meta.row_group(0);

        // trace_id column should have bloom filter enabled
        let trace_id_col = row_group.column(0);
        assert!(
            trace_id_col.bloom_filter_offset().is_some(),
            "trace_id column must have a bloom filter offset"
        );
        assert!(
            matches!(
                trace_id_col.compression(),
                parquet::basic::Compression::ZSTD(_)
            ),
            "trace_id column must use Zstd compression"
        );

        // val column should NOT have bloom filter enabled
        let val_col = row_group.column(1);
        assert!(
            val_col.bloom_filter_offset().is_none(),
            "val column must not have a bloom filter offset"
        );

        // Verify records read back
        let mut batch_reader = reader.build().unwrap();
        let read_batch = batch_reader.next().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_writer_with_span_id_bloom_filter() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/span_test.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("span_id", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
        ]));

        let config = crate::config::ParquetSinkConfig::default();
        let mut writer = PartitionWriter::new(schema.clone(), sender, &config).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["span-aaa", "span-bbb"])),
                Arc::new(StringArray::from(vec!["msg1", "msg2"])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let data = op.read(path).await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec())).unwrap();
        let span_col = reader.metadata().row_group(0).column(0);
        assert!(
            span_col.bloom_filter_offset().is_some(),
            "span_id column must have a bloom filter offset"
        );
    }

    #[tokio::test]
    async fn test_writer_multiple_batches_and_counters() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/multi_batch.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let config = crate::config::ParquetSinkConfig::default();
        let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();

        for i in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![format!("trace-{i}")])),
                    Arc::new(Int64Array::from(vec![i64::try_from(i).unwrap()])),
                ],
            )
            .unwrap();
            writer.write_batch(&batch).unwrap();
            assert_eq!(writer.records_written(), i + 1);
        }

        assert_eq!(writer.records_written(), 5);
        assert!(writer.in_progress_size() > 0);

        writer.flush().unwrap();
        let final_bytes = writer.bytes_written();
        assert!(final_bytes > 0);

        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let data = op.read(path).await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec())).unwrap();
        let mut batch_reader = reader.build().unwrap();
        let mut total_rows = 0;
        while let Some(Ok(b)) = batch_reader.next() {
            total_rows += b.num_rows();
        }
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_writer_compression_codecs() {
        let codecs = vec![
            (
                crate::config::CompressionCodec::Snappy,
                parquet::basic::Compression::SNAPPY,
            ),
            (
                crate::config::CompressionCodec::Lz4Raw,
                parquet::basic::Compression::LZ4_RAW,
            ),
            (
                crate::config::CompressionCodec::Uncompressed,
                parquet::basic::Compression::UNCOMPRESSED,
            ),
            (
                crate::config::CompressionCodec::Gzip,
                parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default()),
            ),
        ];

        for (idx, (codec, expected_parquet_comp)) in codecs.into_iter().enumerate() {
            let op = Operator::new(Memory::default()).unwrap();
            let path = format!("test/comp_{idx}.parquet");
            let (sender, handle) = crate::uploader::AsyncUploader::start(&op, &path).unwrap();

            let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));

            let config = crate::config::ParquetSinkConfig {
                compression: codec,
                ..Default::default()
            };

            let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
            )
            .unwrap();

            writer.write_batch(&batch).unwrap();
            writer.close().unwrap();
            handle.wait_for_completion().await.unwrap();

            let data = op.read(&path).await.unwrap();
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec())).unwrap();
            assert_eq!(
                reader.metadata().row_group(0).column(0).compression(),
                expected_parquet_comp
            );
        }
    }

    #[tokio::test]
    async fn test_writer_empty_batch() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/empty_batch.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));

        let config = crate::config::ParquetSinkConfig::default();
        let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();

        // Write empty batch
        let empty_batch = RecordBatch::new_empty(schema.clone());
        writer.write_batch(&empty_batch).unwrap();
        assert_eq!(writer.records_written(), 0);

        // Write non-empty batch
        let non_empty =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))]).unwrap();
        writer.write_batch(&non_empty).unwrap();
        assert_eq!(writer.records_written(), 2);

        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let data = op.read(path).await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec())).unwrap();
        let mut batch_reader = reader.build().unwrap();
        let read = batch_reader.next().unwrap().unwrap();
        assert_eq!(read.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_writer_schema_mismatch_returns_error() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/mismatch.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));

        let config = crate::config::ParquetSinkConfig::default();
        let mut writer = PartitionWriter::try_new(schema, sender, &config).unwrap();

        let wrong_schema = Arc::new(Schema::new(vec![Field::new("str", DataType::Utf8, false)]));
        let bad_batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();

        let res = writer.write_batch(&bad_batch);
        assert!(res.is_err());

        drop(writer);
        drop(handle);
    }

    #[tokio::test]
    async fn test_channel_writer_buffering_and_direct_send() {
        use std::io::Write;

        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/channel_writer.bin";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let mut channel_writer = ChannelWriter::new(sender, 1024);

        // 1. Small write (< 1024 bytes) - buffered
        let small_slice = [42u8; 100];
        let written = channel_writer.write(&small_slice).unwrap();
        assert_eq!(written, 100);
        assert_eq!(channel_writer.bytes_written(), 100);

        // 2. Large write (>= 1024 bytes) - flushes buffer and directly sends
        let large_slice = [99u8; 2048];
        let written = channel_writer.write(&large_slice).unwrap();
        assert_eq!(written, 2048);
        assert_eq!(channel_writer.bytes_written(), 2148);

        // 3. Finish and check upload
        channel_writer.finish().unwrap();
        handle.wait_for_completion().await.unwrap();

        let data = op.read(path).await.unwrap();
        assert_eq!(data.len(), 2148);
        assert_eq!(&data.to_vec()[..100], &small_slice);
        assert_eq!(&data.to_vec()[100..], &large_slice);
    }

    #[tokio::test]
    async fn test_channel_writer_error_when_closed() {
        use std::io::Write;

        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/closed_channel.bin";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let mut channel_writer = ChannelWriter::new(sender, 1024);

        // Abort upload by dropping handle
        drop(handle);
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let chunk = [1u8; 2048];
        let res = channel_writer.write(&chunk);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_writer_dictionary_and_page_statistics() {
        let op = Operator::new(Memory::default()).unwrap();
        let path = "test/dict_stats.parquet";
        let (sender, handle) = crate::uploader::AsyncUploader::start(&op, path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("num", DataType::Int64, false),
        ]));

        let config = crate::config::ParquetSinkConfig::default();
        let mut writer = PartitionWriter::try_new(schema.clone(), sender, &config).unwrap();

        // Repeated values benefit from dictionary encoding
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["cat_a", "cat_a", "cat_b", "cat_b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        writer.write_batch(&batch).unwrap();
        writer.close().unwrap();
        handle.wait_for_completion().await.unwrap();

        let data = op.read(path).await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.to_vec())).unwrap();
        let row_group = reader.metadata().row_group(0);

        let cat_col = row_group.column(0);
        // Column chunk statistics should be present
        assert!(cat_col.statistics().is_some());

        let num_col = row_group.column(1);
        assert!(num_col.statistics().is_some());
    }
}
