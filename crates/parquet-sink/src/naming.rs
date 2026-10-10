//! Distributed collision-free file naming for Parquet sink.
//!
//! Provides [`FileNamer`], which generates filenames conforming to:
//! `{partition_prefix}/{timestamp_nano:020}_{node_id}_{uuidv7}_{sequence:04}.parquet`
//!
//! This format guarantees:
//! - Lexicographical time-ordering for object store bucket listings.
//! - Collision-free naming across distributed collector nodes.
//! - Monotonic sequence ordering within high-frequency write cycles.

use std::fmt::Write;
use std::sync::atomic::{AtomicI64, Ordering};

/// Generates unique, lexicographically ordered filenames for Parquet data sinks.
///
/// Encapsulates the node identity and a monotonic nanosecond timestamp generator
/// to ensure that listings in cloud object stores remain ordered while preventing
/// file collisions across distributed worker instances.
#[derive(Debug)]
pub struct FileNamer {
    node_id: String,
    last_nanos: AtomicI64,
}

impl Clone for FileNamer {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            last_nanos: AtomicI64::new(self.last_nanos.load(Ordering::Relaxed)),
        }
    }
}

impl FileNamer {
    /// Creates a new [`FileNamer`] with the specified node identifier.
    #[must_use]
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            last_nanos: AtomicI64::new(0),
        }
    }

    /// Returns the node identifier configured for this namer.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Generates a unique, collision-free, lexicographically time-ordered filename.
    ///
    /// The generated path follows the layout:
    /// `{partition_prefix}/{timestamp_nano:020}_{node_id}_{uuidv7}_{sequence:04}.parquet`
    ///
    /// If `partition_prefix` is empty, the leading directory separator is omitted:
    /// `{timestamp_nano:020}_{node_id}_{uuidv7}_{sequence:04}.parquet`
    ///
    /// # Parameters
    /// - `partition_prefix`: The directory/partition prefix path (e.g. `signal=logs/date=2026-10-09`).
    /// - `sequence`: Sequence counter for sub-nanosecond or batch-internal rotations.
    #[must_use]
    pub fn generate_filename(&self, partition_prefix: &str, sequence: u16) -> String {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);

        let timestamp_nano = self
            .last_nanos
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                if now > prev {
                    Some(now)
                } else {
                    Some(prev.saturating_add(1))
                }
            })
            .map_or(now, |prev| {
                if now > prev {
                    now
                } else {
                    prev.saturating_add(1)
                }
            });

        let uuid = uuid::Uuid::now_v7();
        let mut uuid_buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
        let uuid_str = uuid.as_hyphenated().encode_lower(&mut uuid_buf);

        let prefix = partition_prefix.trim_end_matches('/');
        let prefix_separator_len = usize::from(!prefix.is_empty());
        // Pre-calculate exact or upper-bound capacity:
        // prefix + optional '/' + 20 (timestamp) + 1 ('_') + node_id + 1 ('_') + 36 (uuid) + 1 ('_') + 5 (seq) + 8 (".parquet")
        let capacity =
            prefix.len() + prefix_separator_len + 20 + 1 + self.node_id.len() + 1 + 36 + 1 + 5 + 8;
        let mut filename = String::with_capacity(capacity);

        if prefix.is_empty() {
            let _ = write!(
                filename,
                "{timestamp_nano:020}_{}_{uuid_str}_{sequence:04}.parquet",
                self.node_id
            );
        } else {
            let _ = write!(
                filename,
                "{prefix}/{timestamp_nano:020}_{}_{uuid_str}_{sequence:04}.parquet",
                self.node_id
            );
        }

        filename
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_naming_format_and_lexicographical_order() {
        let namer = FileNamer::new("collector-node-01".to_string());
        let file1 = namer.generate_filename("signal=logs/date=2026-10-09", 1);
        let file2 = namer.generate_filename("signal=logs/date=2026-10-09", 2);

        assert!(file1.starts_with("signal=logs/date=2026-10-09/"));
        assert!(file1.ends_with(".parquet"));
        assert!(file1.contains("collector-node-01"));
        assert!(
            file1 < file2,
            "Expected lexicographical time/sequence ordering"
        );
    }

    #[test]
    fn test_naming_monotonic_sequence_same_nanosecond() {
        let namer = FileNamer::new("node-a".to_string());
        let mut seen_filenames = HashSet::new();
        for seq in 0..1000 {
            let name = namer.generate_filename("p", seq);
            assert!(
                seen_filenames.insert(name),
                "Collision detected in sequence generator!"
            );
        }
    }

    #[test]
    fn test_naming_empty_partition_prefix() {
        let namer = FileNamer::new("node-b".to_string());
        let file = namer.generate_filename("", 1);
        assert!(!file.starts_with('/'));
        assert!(file.contains("_node-b_"));
        assert!(file.ends_with("_0001.parquet"));
    }

    #[test]
    fn test_naming_trailing_slash_normalization() {
        let namer = FileNamer::new("node-c".to_string());
        let file_slash = namer.generate_filename("prefix/dir/", 1);
        assert!(file_slash.starts_with("prefix/dir/"));
        assert!(!file_slash.contains("//"));
    }

    #[test]
    fn test_node_id_accessor() {
        let namer = FileNamer::new("my-custom-node".to_string());
        assert_eq!(namer.node_id(), "my-custom-node");
    }

    #[test]
    fn test_cloned_namer_preserves_node_id() {
        let namer = FileNamer::new("orig-node".to_string());
        let cloned = namer.clone();
        assert_eq!(cloned.node_id(), "orig-node");
        let name = cloned.generate_filename("part", 42);
        assert!(name.contains("orig-node"));
        assert!(name.ends_with("_0042.parquet"));
    }

    #[test]
    fn test_naming_slash_only_prefix() {
        let namer = FileNamer::new("node-slash".to_string());
        let file = namer.generate_filename("///", 10);
        assert!(!file.starts_with('/'));
        assert!(file.contains("_node-slash_"));
        assert!(file.ends_with("_0010.parquet"));
    }

    #[test]
    fn test_naming_concurrent_threads() {
        use std::sync::Arc;
        let namer = Arc::new(FileNamer::new("concurrent-node".to_string()));
        let mut handles = Vec::new();

        for t in 0..8 {
            let namer_clone = Arc::clone(&namer);
            handles.push(std::thread::spawn(move || {
                let mut generated_files = Vec::new();
                for i in 0..100 {
                    let seq = u16::try_from(t * 100 + i).unwrap();
                    generated_files.push(namer_clone.generate_filename("part", seq));
                }
                generated_files
            }));
        }

        let mut all_files = HashSet::new();
        for h in handles {
            let thread_names = h.join().unwrap();
            for n in thread_names {
                assert!(all_files.insert(n), "Filename collision under concurrency!");
            }
        }
    }
}
