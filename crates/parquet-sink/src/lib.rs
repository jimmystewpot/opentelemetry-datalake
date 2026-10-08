//! Parquet streaming sink with Variant binary format support.

pub mod config;
pub mod error;

pub use config::{CompressionCodec, ParquetSinkConfig};
pub use error::ParquetSinkError;
