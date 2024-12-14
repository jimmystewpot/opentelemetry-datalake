pub mod iceberg;

use serde::{Deserialize, Serialize};

/// Management mode for target table schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMode {
    /// Validate incoming payloads against fixed layout.
    Fixed,
    /// Dynamically update table schema with additive columns.
    Auto,
    /// Fetch the target table schema directly from the catalog.
    Catalog,
}

/// Granularity of physical table partitioning on storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionGranularity {
    /// Partition layout: year=YYYY/month=MM/day=DD/hour=HH/
    Hourly,
    /// Partition layout: year=YYYY/month=MM/day=DD/
    Daily,
}

/// Target Iceberg Catalog API type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum CatalogType {
    /// standard REST catalog interface.
    Rest,
    /// AWS Glue Catalog service.
    #[cfg(feature = "aws")]
    Glue,
    /// Amazon S3 Tables catalog service.
    #[cfg(feature = "aws")]
    S3Tables,
}
