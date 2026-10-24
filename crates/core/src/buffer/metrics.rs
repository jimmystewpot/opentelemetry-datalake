use crate::buffer::config::{BufferConfig, BufferMode, SignalKind};
use crate::buffer::dead_letter::current_unix_ms;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const ALL_SIGNALS: [SignalKind; 3] = [SignalKind::Logs, SignalKind::Metrics, SignalKind::Traces];

const fn mode_as_str(mode: BufferMode) -> &'static str {
    match mode {
        BufferMode::Memory => "memory",
        BufferMode::Disk => "disk",
    }
}

/// Per-signal atomic gauge and counter state backing [`BufferMetrics`].
#[derive(Debug)]
pub struct SignalMetricsState {
    /// Current number of buffered records (`Queued + InFlight`) for this signal.
    pub buffered_records: AtomicU64,
    /// Current number of buffered bytes (`Queued + InFlight`) for this signal.
    pub buffered_bytes: AtomicU64,
    /// Unix epoch timestamp in milliseconds of the oldest unacknowledged batch (`0` if empty).
    pub oldest_enqueued_unix_ms: AtomicU64,
    /// Current number of in-flight leases for this signal.
    pub inflight_leases: AtomicU64,
    /// Configured per-signal byte budget.
    pub max_bytes: AtomicU64,
    /// Configured per-signal record threshold (`0` if unbounded).
    pub max_events: AtomicU64,
    /// Cumulative records committed via [`crate::buffer::Lease::ack`].
    pub committed_records: AtomicU64,
    /// Cumulative records dropped due to `OverflowPolicy::DropOldest`.
    pub overflow_drops: AtomicU64,
    /// Cumulative records quarantined to the dead-letter store.
    pub dead_letter_drops: AtomicU64,
    /// Cumulative records dropped due to on-disk frame corruption during recovery.
    pub corruption_drops: AtomicU64,
    /// Cumulative records discarded on graceful shutdown drain timeout (`DrainTimeoutPolicy::Discard`).
    pub shutdown_timeout_drops: AtomicU64,
    /// Cumulative records replayed from disk segments during startup recovery.
    pub replayed_records: AtomicU64,
    /// Cumulative lease nacks with `FailureClass::Transient`.
    pub transient_nacks: AtomicU64,
    /// Cumulative lease nacks with `FailureClass::Rejected`.
    pub rejected_nacks: AtomicU64,
    /// Cumulative lease nacks with `FailureClass::Fatal`.
    pub fatal_nacks: AtomicU64,
    signal_sink_attrs: [KeyValue; 2],
    signal_sink_buffer_mode_attrs: [KeyValue; 3],
    signal_sink_mode_attrs: [KeyValue; 3],
}

impl SignalMetricsState {
    fn new(sink_name: &str, signal: SignalKind, config: &BufferConfig) -> Self {
        let sig_str = signal.as_str();
        let mode_str = mode_as_str(config.mode);
        let max_bytes = config.per_signal_budget();
        let max_events = config
            .max_batch_records
            .and_then(|r| u64::try_from(r).ok())
            .unwrap_or(0);
        Self {
            buffered_records: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            oldest_enqueued_unix_ms: AtomicU64::new(0),
            inflight_leases: AtomicU64::new(0),
            max_bytes: AtomicU64::new(max_bytes),
            max_events: AtomicU64::new(max_events),
            committed_records: AtomicU64::new(0),
            overflow_drops: AtomicU64::new(0),
            dead_letter_drops: AtomicU64::new(0),
            corruption_drops: AtomicU64::new(0),
            shutdown_timeout_drops: AtomicU64::new(0),
            replayed_records: AtomicU64::new(0),
            transient_nacks: AtomicU64::new(0),
            rejected_nacks: AtomicU64::new(0),
            fatal_nacks: AtomicU64::new(0),
            signal_sink_attrs: [
                KeyValue::new("signal", sig_str),
                KeyValue::new("sink", sink_name.to_string()),
            ],
            signal_sink_buffer_mode_attrs: [
                KeyValue::new("signal", sig_str),
                KeyValue::new("sink", sink_name.to_string()),
                KeyValue::new("buffer_mode", mode_str),
            ],
            signal_sink_mode_attrs: [
                KeyValue::new("signal", sig_str),
                KeyValue::new("sink", sink_name.to_string()),
                KeyValue::new("mode", mode_str),
            ],
        }
    }
}

/// Shared atomic gauge state observed by OpenTelemetry asynchronous gauge callbacks.
#[derive(Debug)]
pub struct BufferGaugeState {
    mode: BufferMode,
    signals: [SignalMetricsState; 3],
}

impl BufferGaugeState {
    fn new(sink_name: &str, config: &BufferConfig) -> Self {
        Self {
            mode: config.mode,
            signals: [
                SignalMetricsState::new(sink_name, SignalKind::Logs, config),
                SignalMetricsState::new(sink_name, SignalKind::Metrics, config),
                SignalMetricsState::new(sink_name, SignalKind::Traces, config),
            ],
        }
    }

    /// Returns the [`SignalMetricsState`] for `signal`.
    #[must_use]
    pub const fn signal_state(&self, signal: SignalKind) -> &SignalMetricsState {
        &self.signals[signal.as_u8() as usize]
    }
}

/// OpenTelemetry metrics instruments and atomic state for a [`crate::buffer::SinkBuffer`].
#[derive(Debug)]
pub struct BufferMetrics {
    sink_name: String,
    mode_str: &'static str,
    gauges: Arc<BufferGaugeState>,
    _buffer_records_gauge: ObservableGauge<u64>,
    _buffer_events_gauge: ObservableGauge<u64>,
    _buffer_bytes_gauge: ObservableGauge<u64>,
    _buffer_max_events_gauge: ObservableGauge<u64>,
    _buffer_max_bytes_gauge: ObservableGauge<u64>,
    _buffer_disk_bytes_gauge: ObservableGauge<u64>,
    _buffer_disk_max_bytes_gauge: ObservableGauge<u64>,
    _buffer_oldest_age_gauge: ObservableGauge<f64>,
    _buffer_leases_inflight_gauge: ObservableGauge<u64>,
    records_committed_total: Counter<u64>,
    records_dropped_total: Counter<u64>,
    buffer_dropped_records: Counter<u64>,
    lease_nacks_total: Counter<u64>,
    buffer_lease_retries: Counter<u64>,
    buffer_replayed_records_total: Counter<u64>,
    buffer_replayed_records: Counter<u64>,
    flush_duration_seconds: Histogram<f64>,
    buffer_flush_duration_ms: Histogram<f64>,
}

impl BufferMetrics {
    /// Registers OpenTelemetry instruments in meter `"pipeline_core"` for `sink_name`.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn new(sink_name: &str, config: &BufferConfig) -> Self {
        let meter = opentelemetry::global::meter("pipeline_core");
        let gauges = Arc::new(BufferGaugeState::new(sink_name, config));

        let weak_records = Arc::downgrade(&gauges);
        let buffer_records_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_records")
            .with_unit("{record}")
            .with_description("Records currently buffered in the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_records.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.buffered_records.load(Ordering::Relaxed),
                            &sig.signal_sink_buffer_mode_attrs,
                        );
                    }
                }
            })
            .build();

        let weak_events = Arc::downgrade(&gauges);
        let buffer_events_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_events")
            .with_unit("{record}")
            .with_description("Events currently buffered in the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_events.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.buffered_records.load(Ordering::Relaxed),
                            &sig.signal_sink_mode_attrs,
                        );
                    }
                }
            })
            .build();

        let weak_bytes = Arc::downgrade(&gauges);
        let buffer_bytes_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_bytes")
            .with_unit("By")
            .with_description("Bytes currently buffered in the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_bytes.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.buffered_bytes.load(Ordering::Relaxed),
                            &sig.signal_sink_buffer_mode_attrs,
                        );
                    }
                }
            })
            .build();

        let weak_max_events = Arc::downgrade(&gauges);
        let buffer_max_events_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_max_events")
            .with_unit("{record}")
            .with_description("Configured batch record threshold for the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_max_events.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.max_events.load(Ordering::Relaxed),
                            &sig.signal_sink_mode_attrs,
                        );
                    }
                }
            })
            .build();

        let weak_max_bytes = Arc::downgrade(&gauges);
        let buffer_max_bytes_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_max_bytes")
            .with_unit("By")
            .with_description("Configured per-signal byte budget for the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_max_bytes.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.max_bytes.load(Ordering::Relaxed),
                            &sig.signal_sink_mode_attrs,
                        );
                    }
                }
            })
            .build();

        let weak_disk_bytes = Arc::downgrade(&gauges);
        let buffer_disk_bytes_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_disk_bytes")
            .with_unit("By")
            .with_description("Bytes currently buffered on disk in the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_disk_bytes.upgrade() {
                    for sig in &state.signals {
                        let val = if state.mode == BufferMode::Disk {
                            sig.buffered_bytes.load(Ordering::Relaxed)
                        } else {
                            0
                        };
                        observer.observe(val, &sig.signal_sink_mode_attrs);
                    }
                }
            })
            .build();

        let weak_disk_max = Arc::downgrade(&gauges);
        let buffer_disk_max_bytes_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_disk_max_bytes")
            .with_unit("By")
            .with_description("Configured per-signal disk byte budget for the sink buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_disk_max.upgrade() {
                    for sig in &state.signals {
                        let val = if state.mode == BufferMode::Disk {
                            sig.max_bytes.load(Ordering::Relaxed)
                        } else {
                            0
                        };
                        observer.observe(val, &sig.signal_sink_mode_attrs);
                    }
                }
            })
            .build();

        let weak_age = Arc::downgrade(&gauges);
        let buffer_oldest_age_gauge = meter
            .f64_observable_gauge("datalake.sink.buffer_oldest_age_seconds")
            .with_unit("s")
            .with_description("Age in seconds of the oldest unacknowledged batch in the buffer")
            .with_callback(move |observer| {
                if let Some(state) = weak_age.upgrade() {
                    let now_ms = current_unix_ms();
                    for sig in &state.signals {
                        let oldest_ms = sig.oldest_enqueued_unix_ms.load(Ordering::Relaxed);
                        #[allow(clippy::cast_precision_loss)]
                        let age_secs = if oldest_ms == 0 {
                            0.0
                        } else {
                            (now_ms.saturating_sub(oldest_ms) as f64) / 1_000.0
                        };
                        observer.observe(age_secs, &sig.signal_sink_attrs);
                    }
                }
            })
            .build();

        let weak_leases = Arc::downgrade(&gauges);
        let buffer_leases_inflight_gauge = meter
            .u64_observable_gauge("datalake.sink.buffer_leases_inflight")
            .with_unit("{lease}")
            .with_description("Number of currently active in-flight leases per signal")
            .with_callback(move |observer| {
                if let Some(state) = weak_leases.upgrade() {
                    for sig in &state.signals {
                        observer.observe(
                            sig.inflight_leases.load(Ordering::Relaxed),
                            &sig.signal_sink_attrs,
                        );
                    }
                }
            })
            .build();

        let records_committed_total = meter
            .u64_counter("datalake.sink.records_committed_total")
            .with_unit("{record}")
            .with_description("Records committed to persistent storage via Lease::ack")
            .build();

        let records_dropped_total = meter
            .u64_counter("datalake.sink.records_dropped_total")
            .with_unit("{record}")
            .with_description(
                "Records dropped due to overflow, dead-letter, corruption, or shutdown timeout",
            )
            .build();

        let buffer_dropped_records = meter
            .u64_counter("datalake.sink.buffer_dropped_records")
            .with_unit("{record}")
            .with_description("Records dropped by the sink buffer")
            .build();

        let lease_nacks_total = meter
            .u64_counter("datalake.sink.lease_nacks_total")
            .with_unit("{lease}")
            .with_description("Leases nacked by sinks, partitioned by failure_class")
            .build();

        let buffer_lease_retries = meter
            .u64_counter("datalake.sink.buffer_lease_retries")
            .with_unit("{lease}")
            .with_description("Lease retry nacks in the sink buffer")
            .build();

        let buffer_replayed_records_total = meter
            .u64_counter("datalake.sink.buffer_replayed_records_total")
            .with_unit("{record}")
            .with_description("Records replayed from disk buffer segments on startup recovery")
            .build();

        let buffer_replayed_records = meter
            .u64_counter("datalake.sink.buffer_replayed_records")
            .with_unit("{record}")
            .with_description("Records replayed from disk buffer segments on startup recovery")
            .build();

        let flush_duration_seconds = meter
            .f64_histogram("datalake.sink.flush_duration_seconds")
            .with_unit("s")
            .with_description("Duration in seconds from lease acquisition to acknowledgement")
            .build();

        let buffer_flush_duration_ms = meter
            .f64_histogram("datalake.sink.buffer_flush_duration_ms")
            .with_unit("ms")
            .with_description("Duration in milliseconds from lease acquisition to acknowledgement")
            .build();

        Self {
            sink_name: sink_name.to_string(),
            mode_str: mode_as_str(config.mode),
            gauges,
            _buffer_records_gauge: buffer_records_gauge,
            _buffer_events_gauge: buffer_events_gauge,
            _buffer_bytes_gauge: buffer_bytes_gauge,
            _buffer_max_events_gauge: buffer_max_events_gauge,
            _buffer_max_bytes_gauge: buffer_max_bytes_gauge,
            _buffer_disk_bytes_gauge: buffer_disk_bytes_gauge,
            _buffer_disk_max_bytes_gauge: buffer_disk_max_bytes_gauge,
            _buffer_oldest_age_gauge: buffer_oldest_age_gauge,
            _buffer_leases_inflight_gauge: buffer_leases_inflight_gauge,
            records_committed_total,
            records_dropped_total,
            buffer_dropped_records,
            lease_nacks_total,
            buffer_lease_retries,
            buffer_replayed_records_total,
            buffer_replayed_records,
            flush_duration_seconds,
            buffer_flush_duration_ms,
        }
    }

    /// Updates the observable gauge atomics for `signal`.
    pub fn update_gauges(
        &self,
        signal: SignalKind,
        buffered_records: usize,
        buffered_bytes: usize,
        inflight_leases: usize,
        oldest_enqueued_unix_ms: u64,
    ) {
        let sig_state = self.gauges.signal_state(signal);
        sig_state.buffered_records.store(
            u64::try_from(buffered_records).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        sig_state.buffered_bytes.store(
            u64::try_from(buffered_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        sig_state.inflight_leases.store(
            u64::try_from(inflight_leases).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        sig_state
            .oldest_enqueued_unix_ms
            .store(oldest_enqueued_unix_ms, Ordering::Relaxed);
    }

    /// Records committed records and lease flush duration on [`crate::buffer::Lease::ack`].
    pub fn record_committed(&self, signal: SignalKind, rows: u64, duration: Duration) {
        let sig_state = self.gauges.signal_state(signal);
        sig_state
            .committed_records
            .fetch_add(rows, Ordering::Relaxed);
        self.records_committed_total
            .add(rows, &sig_state.signal_sink_attrs);
        let secs = duration.as_secs_f64();
        self.flush_duration_seconds
            .record(secs, &sig_state.signal_sink_attrs);
        self.buffer_flush_duration_ms
            .record(secs * 1_000.0, &sig_state.signal_sink_mode_attrs);
    }

    /// Records dropped records for `reason` ∈ `{"overflow", "dead_letter", "corruption", "shutdown_timeout"}`.
    pub fn record_dropped(&self, signal: SignalKind, rows: u64, reason: &'static str) {
        if rows == 0 {
            return;
        }
        let sig_state = self.gauges.signal_state(signal);
        match reason {
            "overflow" => {
                sig_state.overflow_drops.fetch_add(rows, Ordering::Relaxed);
            }
            "dead_letter" => {
                sig_state
                    .dead_letter_drops
                    .fetch_add(rows, Ordering::Relaxed);
            }
            "corruption" => {
                sig_state
                    .corruption_drops
                    .fetch_add(rows, Ordering::Relaxed);
            }
            "shutdown_timeout" => {
                sig_state
                    .shutdown_timeout_drops
                    .fetch_add(rows, Ordering::Relaxed);
            }
            _ => {}
        }
        let attrs = [
            KeyValue::new("signal", signal.as_str()),
            KeyValue::new("sink", self.sink_name.clone()),
            KeyValue::new("reason", reason),
        ];
        self.records_dropped_total.add(rows, &attrs);
        let mode_attrs = [
            KeyValue::new("signal", signal.as_str()),
            KeyValue::new("sink", self.sink_name.clone()),
            KeyValue::new("mode", self.mode_str),
            KeyValue::new("reason", reason),
        ];
        self.buffer_dropped_records.add(rows, &mode_attrs);
    }

    /// Records a lease nack for `failure_class` ∈ `{"transient", "rejected", "fatal"}`.
    pub fn record_nack(&self, signal: SignalKind, failure_class: &'static str) {
        let sig_state = self.gauges.signal_state(signal);
        match failure_class {
            "transient" => {
                sig_state.transient_nacks.fetch_add(1, Ordering::Relaxed);
            }
            "rejected" => {
                sig_state.rejected_nacks.fetch_add(1, Ordering::Relaxed);
            }
            "fatal" => {
                sig_state.fatal_nacks.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let attrs = [
            KeyValue::new("signal", signal.as_str()),
            KeyValue::new("sink", self.sink_name.clone()),
            KeyValue::new("failure_class", failure_class),
        ];
        self.lease_nacks_total.add(1, &attrs);
        let retry_attrs = [
            KeyValue::new("signal", signal.as_str()),
            KeyValue::new("sink", self.sink_name.clone()),
            KeyValue::new("mode", self.mode_str),
            KeyValue::new("class", failure_class),
        ];
        self.buffer_lease_retries.add(1, &retry_attrs);
    }

    /// Records records replayed from disk during startup recovery.
    pub fn record_replayed(&self, signal: SignalKind, rows: u64) {
        if rows == 0 {
            return;
        }
        let sig_state = self.gauges.signal_state(signal);
        sig_state
            .replayed_records
            .fetch_add(rows, Ordering::Relaxed);
        self.buffer_replayed_records_total
            .add(rows, &sig_state.signal_sink_attrs);
        self.buffer_replayed_records
            .add(rows, &sig_state.signal_sink_mode_attrs);
    }

    /// Returns cumulative dropped records for `reason` across `scope` (or all signals if `None`).
    #[must_use]
    pub fn dropped_records(&self, scope: Option<SignalKind>, reason: &str) -> u64 {
        let signals: &[SignalKind] = match scope {
            Some(ref sig) => std::slice::from_ref(sig),
            None => &ALL_SIGNALS,
        };
        signals.iter().fold(0u64, |acc, &sig| {
            let st = self.gauges.signal_state(sig);
            let count = match reason {
                "overflow" => st.overflow_drops.load(Ordering::Acquire),
                "dead_letter" => st.dead_letter_drops.load(Ordering::Acquire),
                "corruption" => st.corruption_drops.load(Ordering::Acquire),
                "shutdown_timeout" => st.shutdown_timeout_drops.load(Ordering::Acquire),
                _ => 0,
            };
            acc.saturating_add(count)
        })
    }

    /// Returns cumulative committed records across `scope` (or all signals if `None`).
    #[must_use]
    pub fn committed_records(&self, scope: Option<SignalKind>) -> u64 {
        let signals: &[SignalKind] = match scope {
            Some(ref sig) => std::slice::from_ref(sig),
            None => &ALL_SIGNALS,
        };
        signals.iter().fold(0u64, |acc, &sig| {
            acc.saturating_add(
                self.gauges
                    .signal_state(sig)
                    .committed_records
                    .load(Ordering::Acquire),
            )
        })
    }

    /// Returns cumulative lease nacks for `failure_class` across `scope` (or all signals if `None`).
    #[must_use]
    pub fn lease_nacks(&self, scope: Option<SignalKind>, failure_class: &str) -> u64 {
        let signals: &[SignalKind] = match scope {
            Some(ref sig) => std::slice::from_ref(sig),
            None => &ALL_SIGNALS,
        };
        signals.iter().fold(0u64, |acc, &sig| {
            let st = self.gauges.signal_state(sig);
            let count = match failure_class {
                "transient" => st.transient_nacks.load(Ordering::Acquire),
                "rejected" => st.rejected_nacks.load(Ordering::Acquire),
                "fatal" => st.fatal_nacks.load(Ordering::Acquire),
                _ => 0,
            };
            acc.saturating_add(count)
        })
    }
}
