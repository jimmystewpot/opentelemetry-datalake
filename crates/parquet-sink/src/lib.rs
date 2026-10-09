//! Parquet streaming sink with Variant binary format support.

pub mod config;
pub mod error;
pub mod naming;
pub mod partition;
pub mod router;
pub mod uploader;
pub mod variant;
pub mod writer;

pub use config::{CompressionCodec, ParquetSinkConfig};
pub use error::ParquetSinkError;
pub use naming::FileNamer;
pub use partition::{PartitionId, PartitionManager};
pub use router::{PreparedBatch, SignalRouter};
pub use uploader::{AsyncUploader, UploaderHandle, UploaderSender};
pub use variant::{VariantEncoder, VariantTransformer};
pub use writer::{ChannelWriter, PartitionWriter};
