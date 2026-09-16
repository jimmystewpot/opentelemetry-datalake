//! Elasticsearch and `OpenSearch` sink for the `opentelemetry-datalake` pipeline.

pub mod client;
pub mod config;
pub mod error;
pub mod serializer;
pub mod tls;

pub use config::ElasticsearchSinkConfig;
pub use error::ElasticsearchError;
