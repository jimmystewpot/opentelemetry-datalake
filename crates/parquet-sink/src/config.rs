//! Configuration definitions and `OpenDAL` operator builder for the Parquet sink.

use std::collections::HashMap;

use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use serde::{Deserialize, Deserializer, Serialize, de};

use crate::error::ParquetSinkError;

/// Supported compression codecs for Parquet data pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// Zstandard compression with optional compression level (1-22).
    Zstd {
        /// Compression level. Defaults to 3 if not specified.
        level: Option<i32>,
    },
    /// Snappy compression.
    Snappy,
    /// LZ4 raw compression format.
    Lz4Raw,
    /// Gzip compression.
    Gzip,
    /// Uncompressed data pages.
    Uncompressed,
}

impl CompressionCodec {
    /// Converts this codec into the underlying Parquet [`Compression`] setting.
    pub fn to_parquet_compression(&self) -> Result<Compression, ParquetSinkError> {
        match *self {
            Self::Zstd { level } => {
                let zstd_level = match level {
                    Some(lvl) => ZstdLevel::try_new(lvl).map_err(|e| {
                        ParquetSinkError::Config(format!("Invalid Zstd level: {e}"))
                    })?,
                    None => ZstdLevel::default(),
                };
                Ok(Compression::ZSTD(zstd_level))
            }
            Self::Snappy => Ok(Compression::SNAPPY),
            Self::Lz4Raw => Ok(Compression::LZ4_RAW),
            Self::Gzip => Ok(Compression::GZIP(GzipLevel::default())),
            Self::Uncompressed => Ok(Compression::UNCOMPRESSED),
        }
    }
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::Zstd { level: Some(3) }
    }
}

impl<'de> Deserialize<'de> for CompressionCodec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Structured {
            Zstd {
                level: Option<i32>,
            },
            Snappy,
            #[serde(alias = "lz4_raw", alias = "lz4")]
            Lz4Raw,
            Gzip,
            #[serde(alias = "none")]
            Uncompressed,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Str(String),
            Structured(Structured),
        }

        match Helper::deserialize(deserializer)? {
            Helper::Str(s) => match s.to_ascii_lowercase().as_str() {
                "zstd" => Ok(Self::Zstd { level: Some(3) }),
                "snappy" | "snap" => Ok(Self::Snappy),
                "lz4" | "lz4raw" | "lz4_raw" => Ok(Self::Lz4Raw),
                "gzip" | "gz" => Ok(Self::Gzip),
                "uncompressed" | "none" => Ok(Self::Uncompressed),
                other => Err(de::Error::custom(format!(
                    "unknown compression codec: '{other}', expected one of: 'zstd', 'snappy', 'lz4_raw', 'gzip', 'uncompressed'"
                ))),
            },
            Helper::Structured(s) => match s {
                Structured::Zstd { level } => {
                    #[allow(clippy::collapsible_if)]
                    if let Some(lvl) = level {
                        if let Err(e) = ZstdLevel::try_new(lvl) {
                            return Err(de::Error::custom(format!(
                                "invalid zstd compression level: {e}"
                            )));
                        }
                    }
                    Ok(Self::Zstd { level })
                }
                Structured::Snappy => Ok(Self::Snappy),
                Structured::Lz4Raw => Ok(Self::Lz4Raw),
                Structured::Gzip => Ok(Self::Gzip),
                Structured::Uncompressed => Ok(Self::Uncompressed),
            },
        }
    }
}

/// Configuration parameters for the Parquet streaming sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetSinkConfig {
    /// Target storage URI (e.g. `file:///var/data`, `s3://bucket/prefix`).
    #[serde(default = "default_storage_uri")]
    pub storage_uri: String,

    /// Unique ingestion node identifier used to prevent file name collisions.
    #[serde(default = "default_node_id")]
    pub node_id: String,

    /// Compression codec applied to Parquet columns and data pages.
    #[serde(default = "default_compression")]
    pub compression: CompressionCodec,

    /// Optional compression level applied when compression codec is Zstd.
    #[serde(default)]
    pub compression_level: Option<i32>,

    /// Maximum file size in bytes before rolling a partition file (default: 64 MB).
    #[serde(default = "default_max_file_size_bytes")]
    pub max_file_size_bytes: usize,

    /// Maximum interval in seconds before rolling an idle partition file (default: 60s).
    #[serde(default = "default_max_file_interval_sec")]
    pub max_file_interval_sec: u64,

    /// Maximum active concurrently open partition writers (default: 16).
    #[serde(default = "default_max_open_partitions")]
    pub max_open_partitions: usize,

    /// Maximum active concurrent background uploads across all partitions (default: 16).
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,

    /// Global memory ceiling for active partition buffers in bytes (default: 1 GB).
    #[serde(default = "default_global_memory_limit_bytes")]
    pub global_memory_limit_bytes: usize,

    /// Whether to encode semi-structured attributes using the Variant binary format.
    #[serde(default = "default_variant_encoding")]
    pub variant_encoding: bool,

    /// Maximum records per file before rolling (default: 500,000).
    #[serde(default = "default_max_records")]
    pub max_records: usize,

    /// Hive-style partition pattern template (default: `signal={signal}/date={date}/hour={hour}`).
    #[serde(default = "default_partition_pattern")]
    pub partition_pattern: String,

    /// Additional backend storage options (e.g., `aws_region`, `endpoint`).
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

fn default_storage_uri() -> String {
    "file://./data".to_string()
}

fn default_node_id() -> String {
    "default-node".to_string()
}

fn default_compression() -> CompressionCodec {
    CompressionCodec::Zstd { level: Some(3) }
}

const fn default_max_file_size_bytes() -> usize {
    67_108_864
}

const fn default_max_file_interval_sec() -> u64 {
    60
}

const fn default_max_open_partitions() -> usize {
    16
}

const fn default_max_concurrent_uploads() -> usize {
    16
}

const fn default_global_memory_limit_bytes() -> usize {
    1_073_741_824
}

const fn default_variant_encoding() -> bool {
    true
}

const fn default_max_records() -> usize {
    500_000
}

fn default_partition_pattern() -> String {
    "signal={signal}/date={date}/hour={hour}".to_string()
}

impl Default for ParquetSinkConfig {
    fn default() -> Self {
        Self {
            storage_uri: default_storage_uri(),
            node_id: default_node_id(),
            compression: default_compression(),
            compression_level: None,
            max_file_size_bytes: default_max_file_size_bytes(),
            max_file_interval_sec: default_max_file_interval_sec(),
            max_open_partitions: default_max_open_partitions(),
            max_concurrent_uploads: default_max_concurrent_uploads(),
            global_memory_limit_bytes: default_global_memory_limit_bytes(),
            variant_encoding: default_variant_encoding(),
            max_records: default_max_records(),
            partition_pattern: default_partition_pattern(),
            storage_options: HashMap::new(),
        }
    }
}

impl ParquetSinkConfig {
    /// Returns the effective compression codec, applying `compression_level` when configured.
    ///
    /// # Errors
    /// Returns [`ParquetSinkError::Config`] if `compression_level` is set but represents an invalid Zstd level.
    pub fn effective_compression(&self) -> Result<CompressionCodec, ParquetSinkError> {
        if let Some(level) = self.compression_level
            && matches!(self.compression, CompressionCodec::Zstd { .. })
        {
            ZstdLevel::try_new(level).map_err(|e| {
                ParquetSinkError::Config(format!("Invalid Zstd compression level: {e}"))
            })?;
            return Ok(CompressionCodec::Zstd { level: Some(level) });
        }
        Ok(self.compression)
    }

    /// Builds an `OpenDAL` [`opendal::Operator`] configured according to `storage_uri` and `storage_options`.
    #[allow(clippy::too_many_lines)]
    pub fn build_operator(&self) -> Result<opendal::Operator, ParquetSinkError> {
        let uri = self.storage_uri.trim();
        if uri.is_empty() {
            return Err(ParquetSinkError::Config(
                "storage_uri cannot be empty".to_string(),
            ));
        }

        let op = if let Some(path) = uri.strip_prefix("file://") {
            let atomic_dir = self
                .storage_options
                .get("atomic_write_dir")
                .map_or(path, String::as_str);
            let builder = opendal::services::Fs::default()
                .root(path)
                .atomic_write_dir(atomic_dir);
            opendal::Operator::new(builder)?
        } else if let Some(s3_path) = uri.strip_prefix("s3://") {
            let (bucket, root) = match s3_path.find('/') {
                Some(idx) => (&s3_path[..idx], &s3_path[idx..]),
                None => (s3_path, "/"),
            };
            if bucket.is_empty() {
                return Err(ParquetSinkError::Config(
                    "S3 storage URI missing bucket name".to_string(),
                ));
            }
            let mut builder = opendal::services::S3::default().bucket(bucket);
            if !root.is_empty() {
                builder = builder.root(root);
            }
            for (k, v) in &self.storage_options {
                match k.as_str() {
                    "region" | "aws_region" => {
                        builder = builder.region(v);
                    }
                    "endpoint" | "aws_endpoint" => {
                        builder = builder.endpoint(v);
                    }
                    "access_key_id" | "aws_access_key_id" => {
                        builder = builder.access_key_id(v);
                    }
                    "secret_access_key" | "aws_secret_access_key" => {
                        builder = builder.secret_access_key(v);
                    }
                    "session_token" | "aws_session_token" | "security_token" => {
                        builder = builder.session_token(v);
                    }
                    "role_arn" | "aws_role_arn" => {
                        builder = builder.role_arn(v);
                    }
                    "enable_virtual_host_style" => {
                        if v.eq_ignore_ascii_case("true") || v == "1" {
                            builder = builder.enable_virtual_host_style();
                        }
                    }
                    "allow_anonymous" | "skip_signature" => {
                        if v.eq_ignore_ascii_case("true") || v == "1" {
                            builder = builder.skip_signature();
                        }
                    }
                    "server_side_encryption" => {
                        builder = builder.server_side_encryption(v);
                    }
                    "server_side_encryption_aws_kms_key_id" => {
                        builder = builder.server_side_encryption_aws_kms_key_id(v);
                    }
                    other => {
                        tracing::warn!("Unrecognized or unhandled S3 storage option: '{other}'");
                    }
                }
            }
            opendal::Operator::new(builder)?
        } else if let Some(gcs_path) = uri
            .strip_prefix("gs://")
            .or_else(|| uri.strip_prefix("gcs://"))
        {
            let (bucket, root) = match gcs_path.find('/') {
                Some(idx) => (&gcs_path[..idx], &gcs_path[idx..]),
                None => (gcs_path, "/"),
            };
            if bucket.is_empty() {
                return Err(ParquetSinkError::Config(
                    "GCS storage URI missing bucket name".to_string(),
                ));
            }
            let mut builder = opendal::services::Gcs::default().bucket(bucket);
            if !root.is_empty() {
                builder = builder.root(root);
            }
            for (k, v) in &self.storage_options {
                match k.as_str() {
                    "endpoint" | "gcs_endpoint" => {
                        builder = builder.endpoint(v);
                    }
                    "credential" | "gcs_credential" | "credentials" => {
                        builder = builder.credential(v);
                    }
                    "credential_path" | "gcs_credential_path" => {
                        builder = builder.credential_path(v);
                    }
                    "service_account" | "gcs_service_account" => {
                        builder = builder.service_account(v);
                    }
                    "allow_anonymous" | "skip_signature" => {
                        if v.eq_ignore_ascii_case("true") || v == "1" {
                            builder = builder.skip_signature();
                        }
                    }
                    other => {
                        tracing::warn!("Unrecognized or unhandled GCS storage option: '{other}'");
                    }
                }
            }
            opendal::Operator::new(builder)?
        } else if let Some(abfs_path) = uri
            .strip_prefix("azblob://")
            .or_else(|| uri.strip_prefix("abfs://"))
        {
            let (container, root) = match abfs_path.find('/') {
                Some(idx) => (&abfs_path[..idx], &abfs_path[idx..]),
                None => (abfs_path, "/"),
            };
            if container.is_empty() {
                return Err(ParquetSinkError::Config(
                    "Azblob storage URI missing container name".to_string(),
                ));
            }
            let mut builder = opendal::services::Azblob::default().container(container);
            if !root.is_empty() {
                builder = builder.root(root);
            }
            for (k, v) in &self.storage_options {
                match k.as_str() {
                    "endpoint" | "azure_endpoint" => {
                        builder = builder.endpoint(v);
                    }
                    "account_name" | "azure_account_name" => {
                        builder = builder.account_name(v);
                    }
                    "account_key" | "azure_account_key" => {
                        builder = builder.account_key(v);
                    }
                    "sas_token" | "azure_sas_token" => {
                        builder = builder.sas_token(v);
                    }
                    other => {
                        tracing::warn!(
                            "Unrecognized or unhandled Azblob storage option: '{other}'"
                        );
                    }
                }
            }
            opendal::Operator::new(builder)?
        } else if uri.starts_with("memory://") {
            #[cfg(any(test, feature = "services-memory"))]
            {
                let builder = opendal::services::Memory::default();
                opendal::Operator::new(builder)?
            }
            #[cfg(not(any(test, feature = "services-memory")))]
            {
                return Err(ParquetSinkError::Config(
                    "memory storage service is not enabled".to_string(),
                ));
            }
        } else if !uri.contains("://") {
            let atomic_dir = self
                .storage_options
                .get("atomic_write_dir")
                .map_or(uri, String::as_str);
            let builder = opendal::services::Fs::default()
                .root(uri)
                .atomic_write_dir(atomic_dir);
            opendal::Operator::new(builder)?
        } else {
            return Err(ParquetSinkError::Config(format!(
                "unsupported storage URI scheme: '{uri}'"
            )));
        };

        Ok(op.layer(opendal::layers::RetryLayer::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ParquetSinkConfig::default();
        assert_eq!(
            config.compression,
            CompressionCodec::Zstd { level: Some(3) }
        );
        assert_eq!(config.max_file_size_bytes, 67_108_864);
        assert_eq!(config.max_file_interval_sec, 60);
        assert_eq!(config.max_open_partitions, 16);
        assert_eq!(config.max_concurrent_uploads, 16);
        assert_eq!(config.global_memory_limit_bytes, 1_073_741_824);
        assert!(config.variant_encoding);
    }

    #[test]
    fn test_deserialize_toml() {
        let toml_str = r#"
            storage_uri = "s3://my-bucket/telemetry"
            node_id = "test-node"
            compression = "snappy"
            max_file_size_bytes = 10485760
            max_open_partitions = 32
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage_uri, "s3://my-bucket/telemetry");
        assert_eq!(config.node_id, "test-node");
        assert_eq!(config.compression, CompressionCodec::Snappy);
        assert_eq!(config.max_file_size_bytes, 10_485_760);
        assert_eq!(config.max_open_partitions, 32);
    }

    #[test]
    fn test_deserialize_compression_variants() {
        let toml_zstd = r#"
            compression = "zstd"
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_zstd).unwrap();
        assert_eq!(
            config.compression,
            CompressionCodec::Zstd { level: Some(3) }
        );

        let toml_snappy = r#"
            compression = "snappy"
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_snappy).unwrap();
        assert_eq!(config.compression, CompressionCodec::Snappy);

        let toml_lz4 = r#"
            compression = "lz4_raw"
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_lz4).unwrap();
        assert_eq!(config.compression, CompressionCodec::Lz4Raw);

        let toml_gzip = r#"
            compression = "gzip"
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_gzip).unwrap();
        assert_eq!(config.compression, CompressionCodec::Gzip);

        let toml_uncompressed = r#"
            compression = "uncompressed"
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_uncompressed).unwrap();
        assert_eq!(config.compression, CompressionCodec::Uncompressed);
    }

    #[test]
    fn test_to_parquet_compression() {
        assert_eq!(
            CompressionCodec::Snappy.to_parquet_compression().unwrap(),
            Compression::SNAPPY
        );
        assert_eq!(
            CompressionCodec::Lz4Raw.to_parquet_compression().unwrap(),
            Compression::LZ4_RAW
        );
        assert_eq!(
            CompressionCodec::Uncompressed
                .to_parquet_compression()
                .unwrap(),
            Compression::UNCOMPRESSED
        );
        assert!(matches!(
            CompressionCodec::Zstd { level: Some(5) }
                .to_parquet_compression()
                .unwrap(),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            CompressionCodec::Gzip.to_parquet_compression().unwrap(),
            Compression::GZIP(_)
        ));
    }

    #[test]
    fn test_invalid_zstd_level_returns_error() {
        let codec = CompressionCodec::Zstd { level: Some(999) };
        assert!(matches!(
            codec.to_parquet_compression(),
            Err(ParquetSinkError::Config(_))
        ));

        let toml_invalid = r"
            compression = { zstd = { level = 999 } }
        ";
        let res: Result<ParquetSinkConfig, _> = toml::from_str(toml_invalid);
        assert!(res.is_err());
    }

    #[test]
    fn test_build_operator_fs() {
        let config = ParquetSinkConfig {
            storage_uri: "file:///tmp/telemetry".to_string(),
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok());
    }

    #[test]
    fn test_build_operator_raw_fs_path() {
        let config = ParquetSinkConfig {
            storage_uri: "/tmp/telemetry".to_string(),
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok());
    }

    #[test]
    fn test_build_operator_s3() {
        let mut storage_options = HashMap::new();
        storage_options.insert("aws_region".to_string(), "us-east-1".to_string());
        let config = ParquetSinkConfig {
            storage_uri: "s3://my-bucket/telemetry".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok());
    }

    #[test]
    fn test_build_operator_memory() {
        let config = ParquetSinkConfig {
            storage_uri: "memory://test".to_string(),
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok());
    }

    #[test]
    fn test_build_operator_gcs() {
        let mut storage_options = HashMap::new();
        storage_options.insert(
            "gcs_credential".to_string(),
            "my-credential-json".to_string(),
        );
        let config = ParquetSinkConfig {
            storage_uri: "gs://my-bucket/telemetry".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok());
    }

    #[test]
    fn test_build_operator_azblob() {
        let mut storage_options = HashMap::new();
        storage_options.insert("azure_account_name".to_string(), "myaccount".to_string());
        storage_options.insert(
            "azure_endpoint".to_string(),
            "https://myaccount.blob.core.windows.net".to_string(),
        );
        let config = ParquetSinkConfig {
            storage_uri: "azblob://my-container/telemetry".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok(), "op is err: {:?}", op.err());
    }

    #[test]
    fn test_build_operator_empty_uri_fails() {
        let config = ParquetSinkConfig {
            storage_uri: "   ".to_string(),
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(matches!(op, Err(ParquetSinkError::Config(_))));
    }

    #[test]
    fn test_build_operator_unsupported_scheme() {
        let config = ParquetSinkConfig {
            storage_uri: "ftp://my-server/telemetry".to_string(),
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(matches!(op, Err(ParquetSinkError::Config(_))));
    }

    #[test]
    fn test_build_operator_all_s3_storage_options() {
        let mut storage_options = HashMap::new();
        storage_options.insert("aws_region".to_string(), "eu-west-1".to_string());
        storage_options.insert(
            "aws_endpoint".to_string(),
            "http://localhost:9000".to_string(),
        );
        storage_options.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
        storage_options.insert(
            "aws_secret_access_key".to_string(),
            "minioadmin".to_string(),
        );
        storage_options.insert("aws_session_token".to_string(), "session123".to_string());
        storage_options.insert(
            "aws_role_arn".to_string(),
            "arn:aws:iam::123456789012:role/S3Access".to_string(),
        );
        storage_options.insert("enable_virtual_host_style".to_string(), "true".to_string());
        storage_options.insert("skip_signature".to_string(), "true".to_string());
        storage_options.insert("server_side_encryption".to_string(), "aws:kms".to_string());
        storage_options.insert(
            "server_side_encryption_aws_kms_key_id".to_string(),
            "kms-key-id".to_string(),
        );
        storage_options.insert(
            "custom_unknown_option".to_string(),
            "ignored_val".to_string(),
        );

        let config = ParquetSinkConfig {
            storage_uri: "s3://my-bucket/prefix/path".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok(), "op is err: {:?}", op.err());

        // Test empty S3 bucket
        let empty_bucket_config = ParquetSinkConfig {
            storage_uri: "s3:///prefix".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            empty_bucket_config.build_operator(),
            Err(ParquetSinkError::Config(_))
        ));
    }

    #[test]
    fn test_build_operator_all_gcs_storage_options() {
        let mut storage_options = HashMap::new();
        storage_options.insert(
            "gcs_endpoint".to_string(),
            "http://localhost:4443".to_string(),
        );
        storage_options.insert("gcs_credential".to_string(), "{}".to_string());
        storage_options.insert(
            "gcs_credential_path".to_string(),
            "/path/to/key.json".to_string(),
        );
        storage_options.insert(
            "gcs_service_account".to_string(),
            "sa@proj.iam.gserviceaccount.com".to_string(),
        );
        storage_options.insert("allow_anonymous".to_string(), "true".to_string());
        storage_options.insert("custom_gcs_option".to_string(), "ignored".to_string());

        let config = ParquetSinkConfig {
            storage_uri: "gcs://my-gcs-bucket/telemetry".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok(), "op is err: {:?}", op.err());

        let empty_gcs_config = ParquetSinkConfig {
            storage_uri: "gs:///telemetry".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            empty_gcs_config.build_operator(),
            Err(ParquetSinkError::Config(_))
        ));
    }

    #[test]
    fn test_build_operator_all_azblob_storage_options() {
        let mut storage_options = HashMap::new();
        storage_options.insert("account_name".to_string(), "myaccount".to_string());
        storage_options.insert(
            "account_key".to_string(),
            "c2VjcmV0a2V5MTIzNDU2".to_string(),
        );
        storage_options.insert("sas_token".to_string(), "sastoken123".to_string());
        storage_options.insert(
            "endpoint".to_string(),
            "https://myaccount.blob.core.windows.net".to_string(),
        );
        storage_options.insert("custom_az_opt".to_string(), "ignored".to_string());

        let config = ParquetSinkConfig {
            storage_uri: "abfs://my-container/telemetry".to_string(),
            storage_options,
            ..Default::default()
        };
        let op = config.build_operator();
        assert!(op.is_ok(), "op is err: {:?}", op.err());

        let empty_az_config = ParquetSinkConfig {
            storage_uri: "azblob:///telemetry".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            empty_az_config.build_operator(),
            Err(ParquetSinkError::Config(_))
        ));
    }

    #[test]
    fn test_build_operator_local_fs_path_without_scheme_and_unsupported_scheme() {
        let fs_config = ParquetSinkConfig {
            storage_uri: "./local_data_dir".to_string(),
            ..Default::default()
        };
        let op = fs_config.build_operator();
        assert!(
            op.is_ok(),
            "Local fs without scheme should build: {:?}",
            op.err()
        );

        let unsupported_config = ParquetSinkConfig {
            storage_uri: "ftp://remote.host/data".to_string(),
            ..Default::default()
        };
        let err = unsupported_config.build_operator();
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("unsupported storage URI scheme")
        );
    }

    #[test]
    fn test_compression_codec_invalid_zstd_level_and_deserialize_errors() {
        let invalid_codec = CompressionCodec::Zstd { level: Some(999) };
        assert!(invalid_codec.to_parquet_compression().is_err());

        let unknown_err = serde_json::from_str::<CompressionCodec>(r#""bogus_compression""#);
        assert!(unknown_err.is_err());

        let invalid_zstd_err =
            serde_json::from_str::<CompressionCodec>(r#"{"zstd": {"level": 999}}"#);
        assert!(invalid_zstd_err.is_err());
    }

    #[test]
    fn test_compression_level_toml_parsing_and_override() {
        let toml_str = r#"
            storage_uri = "file://./data"
            compression = "zstd"
            compression_level = 7
        "#;
        let config: ParquetSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.compression_level, Some(7));
        assert_eq!(
            config.effective_compression().unwrap(),
            CompressionCodec::Zstd { level: Some(7) }
        );

        let invalid_toml = r#"
            storage_uri = "file://./data"
            compression = "zstd"
            compression_level = 999
        "#;
        let invalid_config: ParquetSinkConfig = toml::from_str(invalid_toml).unwrap();
        assert!(invalid_config.effective_compression().is_err());
    }
}
