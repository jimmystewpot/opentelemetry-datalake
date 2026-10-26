use crate::error::PipelineError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Signal type alias used across the sink buffer subsystem.
pub type SignalKind = crate::sort::SignalType;

/// Storage backend mode for a [` crate::buffer::SinkBuffer `].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BufferMode {
    /// In-memory zero-copy Arrow `RecordBatch` ring buffer.
    #[default]
    Memory,
    /// Local `NVMe` segmented log buffer with framing and crash recovery.
    Disk,
}

/// Action taken when a signal ring reaches its byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Block incoming `BufferSender::send` calls until in-flight leases are acknowledged.
    #[default]
    Block,
    /// Evict the oldest unleased (`Queued`) batches to make room for new arrivals.
    DropOldest,
}

/// Action taken when graceful shutdown exceeds `drain_timeout_sec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainTimeoutPolicy {
    /// Flush and fsync unacknowledged segments to disk for replay on next startup (`Disk` mode only).
    Persist,
    /// Discard remaining unacknowledged batches and record shutdown timeout drops.
    Discard,
}

/// Legacy `[<sink>.batching]` configuration mapped onto [`BufferConfig`] triggers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyBatchingConfig {
    /// Maximum accumulated byte size before triggering a flush lease.
    pub max_batch_size_bytes: usize,
    /// Optional maximum accumulated row count before triggering a flush lease.
    pub max_batch_records: Option<usize>,
    /// Maximum time interval in seconds before triggering a flush lease.
    pub max_batch_interval_sec: u64,
}

const fn default_segment_size_bytes() -> u64 {
    67_108_864 // 64 MiB
}

const fn default_max_buffer_bytes() -> u64 {
    805_306_368 // 768 MiB (256 MiB per signal)
}

const fn default_max_batch_size_bytes() -> usize {
    33_554_432 // 32 MiB
}

const fn default_max_batch_interval_sec() -> u64 {
    60
}

const fn default_max_inflight_leases() -> usize {
    1
}

const fn default_initial_backoff_ms() -> u64 {
    500
}

const fn default_max_backoff_sec() -> u64 {
    60
}

const fn default_max_attempts() -> u32 {
    3
}

const fn default_max_dead_letter_bytes() -> u64 {
    1_073_741_824 // 1 GiB
}

const fn default_drain_timeout_sec() -> u64 {
    30
}

const fn default_readiness_stall_sec() -> u64 {
    10
}

/// Unified configuration for a per-sink [`crate::buffer::SinkBuffer`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BufferConfig {
    /// Storage backend mode (`memory` or `disk`).
    #[serde(default)]
    pub mode: BufferMode,
    /// Directory path for segment and watermark storage when `mode = "disk"`.
    #[serde(default)]
    pub disk_path: Option<PathBuf>,
    /// Maximum size in bytes of a single on-disk segment file before rolling.
    #[serde(default = "default_segment_size_bytes")]
    pub segment_size_bytes: u64,
    /// Whether to `sync_data` on every write batch before `send` returns.
    #[serde(default)]
    pub sync_on_write: bool,
    /// Total buffer byte budget across all three signals (`Logs`, `Metrics`, `Traces`).
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: u64,
    /// Behavior when a signal ring reaches its per-signal byte budget.
    #[serde(default)]
    pub overflow_policy: OverflowPolicy,
    /// Target byte threshold for coalescing queued batches into a lease.
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    /// Optional target record threshold for coalescing queued batches into a lease.
    #[serde(default)]
    pub max_batch_records: Option<usize>,
    /// Maximum flush interval in seconds.
    #[serde(default = "default_max_batch_interval_sec")]
    pub max_batch_interval_sec: u64,
    /// Maximum number of concurrent in-flight leases per signal.
    #[serde(default = "default_max_inflight_leases")]
    pub max_inflight_leases: usize,
    /// Initial retry backoff in milliseconds for `FailureClass::Transient`.
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    /// Maximum retry backoff cap in seconds for `FailureClass::Transient`.
    #[serde(default = "default_max_backoff_sec")]
    pub max_backoff_sec: u64,
    /// Maximum delivery attempts for `FailureClass::Rejected` before dead-letter quarantine.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Maximum duration in seconds to retry `Transient` failures (`0` = unlimited).
    #[serde(default)]
    pub max_transient_duration_sec: u64,
    /// Optional directory override for dead-letter Arrow IPC quarantine files.
    #[serde(default)]
    pub dead_letter_path: Option<PathBuf>,
    /// Maximum total bytes stored in the dead-letter quarantine directory.
    #[serde(default = "default_max_dead_letter_bytes")]
    pub max_dead_letter_bytes: u64,
    /// Graceful shutdown drain timeout in seconds.
    #[serde(default = "default_drain_timeout_sec")]
    pub drain_timeout_sec: u64,
    /// Policy applied if graceful shutdown exceeds `drain_timeout_sec`.
    #[serde(default)]
    pub on_drain_timeout: Option<DrainTimeoutPolicy>,
    /// Continuous blocked-send duration in seconds before `/readyz` reports not-ready.
    #[serde(default = "default_readiness_stall_sec")]
    pub readiness_stall_sec: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            mode: BufferMode::Memory,
            disk_path: None,
            segment_size_bytes: default_segment_size_bytes(),
            sync_on_write: false,
            max_buffer_bytes: default_max_buffer_bytes(),
            overflow_policy: OverflowPolicy::Block,
            max_batch_size_bytes: default_max_batch_size_bytes(),
            max_batch_records: None,
            max_batch_interval_sec: default_max_batch_interval_sec(),
            max_inflight_leases: default_max_inflight_leases(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_sec: default_max_backoff_sec(),
            max_attempts: default_max_attempts(),
            max_transient_duration_sec: 0,
            dead_letter_path: None,
            max_dead_letter_bytes: default_max_dead_letter_bytes(),
            drain_timeout_sec: default_drain_timeout_sec(),
            on_drain_timeout: None,
            readiness_stall_sec: default_readiness_stall_sec(),
        }
    }
}

impl BufferConfig {
    /// Returns a memory-mode passthrough configuration where each lease yields a single batch
    /// without coalescing or waiting for an interval timer.
    #[must_use]
    pub fn passthrough() -> Self {
        Self {
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..Self::default()
        }
    }

    /// Resolves a sink's effective [`BufferConfig`] from optional `[<sink>.buffer]` and
    /// legacy `[<sink>.batching]` tables, enforcing rule V4 and validating rules V1–V5.
    pub fn resolve(
        sink_name: &str,
        buffer: Option<Self>,
        legacy_batching: Option<LegacyBatchingConfig>,
        default_passthrough: bool,
    ) -> Result<Self, PipelineError> {
        let resolved = match (buffer, legacy_batching) {
            (Some(_), Some(_)) => {
                return Err(Self::config_err(format!(
                    "[{sink_name}]: cannot specify both [{sink_name}.buffer] and legacy [{sink_name}.batching] (V4)"
                )));
            }
            (Some(cfg), None) => cfg,
            (None, Some(legacy)) => {
                let min_buf = (legacy.max_batch_size_bytes as u64).saturating_mul(3);
                Self {
                    mode: BufferMode::Memory,
                    max_buffer_bytes: default_max_buffer_bytes().max(min_buf),
                    max_batch_size_bytes: legacy.max_batch_size_bytes,
                    max_batch_records: legacy.max_batch_records,
                    max_batch_interval_sec: legacy.max_batch_interval_sec,
                    ..Self::default()
                }
            }
            (None, None) => {
                if default_passthrough {
                    Self::passthrough()
                } else {
                    Self::default()
                }
            }
        };
        resolved.validate(sink_name)?;
        Ok(resolved)
    }

    /// Validates startup configuration invariants V1–V5 (`docs/buffer.md` §5.3).
    pub fn validate(&self, sink_name: &str) -> Result<(), PipelineError> {
        // V5: Non-zero limits
        if self.max_inflight_leases == 0 {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: max_inflight_leases must be >= 1 (V5)"
            )));
        }
        if self.max_attempts == 0 {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: max_attempts must be >= 1 (V5)"
            )));
        }
        if self.drain_timeout_sec == 0 {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: drain_timeout_sec must be >= 1 (V5)"
            )));
        }
        if self.max_batch_size_bytes == 0 {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: max_batch_size_bytes must be >= 1 (V5)"
            )));
        }
        if self.max_batch_records == Some(0) {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: max_batch_records must be >= 1 when set (V5)"
            )));
        }

        // V1: max_buffer_bytes must accommodate at least one full batch per signal (3 signals)
        let min_buffer_bytes = (self.max_batch_size_bytes as u64).saturating_mul(3);
        if self.max_buffer_bytes < min_buffer_bytes {
            return Err(Self::config_err(format!(
                "[{sink_name}.buffer]: max_buffer_bytes ({}) must be >= 3 * max_batch_size_bytes ({min_buffer_bytes}) (V1)",
                self.max_buffer_bytes
            )));
        }

        // V3: Disk mode requires disk_path; Persist drain timeout policy requires Disk mode
        match self.mode {
            BufferMode::Disk => {
                let has_valid_path = self
                    .disk_path
                    .as_ref()
                    .is_some_and(|p| !p.as_os_str().is_empty());
                if !has_valid_path {
                    return Err(Self::config_err(format!(
                        "[{sink_name}.buffer]: mode = \"disk\" requires a non-empty disk_path (V3)"
                    )));
                }
                // V2: Disk segment size must hold at least 2 batches
                let min_segment_bytes = (self.max_batch_size_bytes as u64).saturating_mul(2);
                if self.segment_size_bytes < min_segment_bytes {
                    return Err(Self::config_err(format!(
                        "[{sink_name}.buffer]: segment_size_bytes ({}) must be >= 2 * max_batch_size_bytes ({min_segment_bytes}) in disk mode (V2)",
                        self.segment_size_bytes
                    )));
                }
            }
            BufferMode::Memory => {
                if self.on_drain_timeout == Some(DrainTimeoutPolicy::Persist) {
                    return Err(Self::config_err(format!(
                        "[{sink_name}.buffer]: on_drain_timeout = \"persist\" requires mode = \"disk\" (V3)"
                    )));
                }
            }
        }

        Ok(())
    }

    fn config_err(msg: String) -> PipelineError {
        PipelineError::Configuration(Box::new(figment::Error::from(msg)))
    }

    /// Returns the byte budget allocated to each of the three signal rings (`max_buffer_bytes / 3`).
    #[must_use]
    pub const fn per_signal_budget(&self) -> u64 {
        self.max_buffer_bytes / 3
    }

    /// Returns the maximum allowed on-disk `OTLB` frame IPC payload size in bytes (`2 * segment_size_bytes`,
    /// capped at `u32::MAX`).
    #[must_use]
    pub const fn max_disk_frame_payload_bytes(&self) -> u32 {
        let doubled = self.segment_size_bytes.saturating_mul(2);
        if doubled > u32::MAX as u64 {
            u32::MAX
        } else {
            doubled as u32
        }
    }

    /// Returns the effective drain timeout policy, defaulting to `Discard` for memory mode
    /// and `Persist` for disk mode.
    #[must_use]
    pub const fn effective_drain_policy(&self) -> DrainTimeoutPolicy {
        match self.on_drain_timeout {
            Some(policy) => policy,
            None => match self.mode {
                BufferMode::Memory => DrainTimeoutPolicy::Discard,
                BufferMode::Disk => DrainTimeoutPolicy::Persist,
            },
        }
    }

    /// Returns the effective dead-letter quarantine root directory:
    /// `dead_letter_path` if explicitly set, `<disk_path>/dead-letter` in `Disk` mode,
    /// or `./dlq/buffer` in `Memory` mode (`docs/buffer.md` §4.3).
    #[must_use]
    pub fn effective_dead_letter_path(&self) -> PathBuf {
        if let Some(ref dl_path) = self.dead_letter_path {
            dl_path.clone()
        } else if self.mode == BufferMode::Disk
            && let Some(ref disk_dir) = self.disk_path
        {
            disk_dir.join("dead-letter")
        } else {
            PathBuf::from("./dlq/buffer")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_rejects_max_buffer_bytes_smaller_than_three_batches() {
        let cfg = BufferConfig {
            max_buffer_bytes: 300,
            max_batch_size_bytes: 200,
            ..BufferConfig::default()
        };
        let err = cfg.validate("iceberg").unwrap_err();
        assert!(matches!(err, PipelineError::Configuration(_)));
    }

    #[test]
    fn test_v2_rejects_disk_segment_smaller_than_two_batches() {
        let cfg = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(PathBuf::from("/tmp/otel-datalake-buf-v2")),
            segment_size_bytes: 100,
            max_batch_size_bytes: 60,
            max_buffer_bytes: 1_000,
            ..BufferConfig::default()
        };
        let err = cfg.validate("starrocks").unwrap_err();
        assert!(matches!(err, PipelineError::Configuration(_)));
    }

    #[test]
    fn test_v3_rejects_disk_mode_without_disk_path_or_persist_on_memory() {
        let disk_without_path = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: None,
            ..BufferConfig::default()
        };
        assert!(matches!(
            disk_without_path.validate("elasticsearch"),
            Err(PipelineError::Configuration(_))
        ));

        let memory_with_persist = BufferConfig {
            mode: BufferMode::Memory,
            on_drain_timeout: Some(DrainTimeoutPolicy::Persist),
            ..BufferConfig::default()
        };
        assert!(matches!(
            memory_with_persist.validate("elasticsearch"),
            Err(PipelineError::Configuration(_))
        ));
    }

    #[test]
    fn test_v4_rejects_both_buffer_and_legacy_batching() {
        let buf = BufferConfig::default();
        let legacy = LegacyBatchingConfig {
            max_batch_size_bytes: 1024,
            max_batch_records: Some(100),
            max_batch_interval_sec: 10,
        };
        let err = BufferConfig::resolve("starrocks", Some(buf), Some(legacy), true).unwrap_err();
        assert!(matches!(err, PipelineError::Configuration(_)));
    }

    #[test]
    fn test_v5_rejects_zero_limits() {
        let zero_inflight = BufferConfig {
            max_inflight_leases: 0,
            ..BufferConfig::default()
        };
        assert!(matches!(
            zero_inflight.validate("kafka"),
            Err(PipelineError::Configuration(_))
        ));

        let zero_attempts = BufferConfig {
            max_attempts: 0,
            ..BufferConfig::default()
        };
        assert!(matches!(
            zero_attempts.validate("kafka"),
            Err(PipelineError::Configuration(_))
        ));

        let zero_drain = BufferConfig {
            drain_timeout_sec: 0,
            ..BufferConfig::default()
        };
        assert!(matches!(
            zero_drain.validate("kafka"),
            Err(PipelineError::Configuration(_))
        ));
    }

    #[test]
    fn test_signal_kind_u8_and_str_roundtrip() {
        for (sig, expected_u8, expected_str) in [
            (SignalKind::Logs, 0u8, "logs"),
            (SignalKind::Metrics, 1u8, "metrics"),
            (SignalKind::Traces, 2u8, "traces"),
        ] {
            assert_eq!(sig.as_u8(), expected_u8);
            assert_eq!(SignalKind::from_u8(expected_u8), Some(sig));
            assert_eq!(sig.as_str(), expected_str);
            let json = serde_json::to_string(&sig).unwrap();
            assert_eq!(json, format!("\"{expected_str}\""));
            let decoded: SignalKind = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, sig);
        }
        assert_eq!(SignalKind::from_u8(3), None);
        assert_eq!(SignalKind::from_u8(255), None);
    }

    #[test]
    fn test_resolve_legacy_and_defaults_and_drain_policy() {
        let passthrough = BufferConfig::resolve("kafka", None, None, true).unwrap();
        assert_eq!(passthrough.max_batch_records, Some(1));
        assert_eq!(passthrough.max_batch_interval_sec, 0);
        assert_eq!(
            passthrough.effective_drain_policy(),
            DrainTimeoutPolicy::Discard
        );

        let buffered = BufferConfig::resolve("iceberg", None, None, false).unwrap();
        assert_eq!(buffered.max_batch_records, None);
        assert_eq!(buffered.per_signal_budget(), buffered.max_buffer_bytes / 3);

        let legacy = LegacyBatchingConfig {
            max_batch_size_bytes: 4096,
            max_batch_records: Some(500),
            max_batch_interval_sec: 15,
        };
        let from_legacy = BufferConfig::resolve("starrocks", None, Some(legacy), true).unwrap();
        assert_eq!(from_legacy.max_batch_size_bytes, 4096);
        assert_eq!(from_legacy.max_batch_records, Some(500));
        assert_eq!(from_legacy.max_batch_interval_sec, 15);

        let disk_cfg = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(PathBuf::from("/tmp/otel-dl-disk")),
            ..BufferConfig::default()
        };
        assert_eq!(
            disk_cfg.effective_drain_policy(),
            DrainTimeoutPolicy::Persist
        );
    }
}
