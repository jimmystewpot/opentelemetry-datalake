//! Parquet streaming sink with Variant binary format support.

pub mod config;
pub mod error;
pub mod naming;

pub use config::{CompressionCodec, ParquetSinkConfig};
pub use error::ParquetSinkError;
pub use naming::FileNamer;
