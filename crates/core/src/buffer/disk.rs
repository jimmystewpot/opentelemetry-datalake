use crate::buffer::config::{BufferConfig, SignalKind};
use crate::buffer::memory::{BatchPayload, LeaseSlotState, QueuedBatch, SignalRingState};
use crate::error::PipelineError;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Instant;

/// 4-byte ASCII magic header (`"OTLB"`) prefixing every on-disk buffer frame.
pub const FRAME_MAGIC: [u8; 4] = *b"OTLB";

/// Current on-disk frame format version (`1`).
pub const FRAME_VERSION: u8 = 1;

/// Fixed byte length of an `OTLB` frame header (`28` bytes).
pub const FRAME_HEADER_LEN: usize = 28;
const FRAME_HEADER_LEN_U32: u32 = 28;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn storage_err(
    msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> PipelineError {
    PipelineError::Storage(msg.into())
}

/// Decoded 28-byte little-endian `OTLB` frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    /// Telemetry signal encoded in the frame.
    pub signal: SignalKind,
    /// Monotonic per-signal sequence number (`>= 1`).
    pub seq_id: u64,
    /// Number of rows inside the encoded Arrow `RecordBatch`.
    pub row_count: u32,
    /// Byte length of the Arrow IPC stream payload following the 28-byte header.
    pub payload_len: u32,
    /// CRC32 checksum computed over header bytes `4..24` and the IPC payload.
    pub crc32c: u32,
}

/// Errors encountered while decoding an on-disk `OTLB` frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameDecodeError {
    /// Truncated header or payload at EOF (e.g. torn tail write during power loss).
    #[error("torn OTLB frame at end of segment")]
    TornFrame,

    /// Frame header magic, version, signal type, or payload length is invalid.
    #[error("invalid OTLB frame header: {reason}")]
    InvalidHeader {
        /// Diagnostic description of the header validation failure.
        reason: String,
    },

    /// Payload CRC32 did not match the header's recorded checksum.
    #[error(
        "OTLB frame CRC32 mismatch at seq {}: expected {:#010x}, got {:#010x}",
        header.seq_id,
        header.crc32c,
        actual_crc
    )]
    CrcMismatch {
        /// Parsed 28-byte header of the corrupted frame.
        header: FrameHeader,
        /// Actual CRC32 computed over header bytes `4..24` and payload.
        actual_crc: u32,
    },

    /// Arrow IPC stream payload failed to decode into a `RecordBatch`.
    #[error("OTLB frame IPC decode error at seq {}: {source}", header.seq_id)]
    IpcDecode {
        /// Parsed 28-byte header of the frame whose IPC stream failed to decode.
        header: FrameHeader,
        /// Underlying Arrow error.
        #[source]
        source: arrow::error::ArrowError,
    },

    /// Underlying filesystem I/O error.
    #[error("OTLB frame I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Encodes `batch` into `out` with a 28-byte little-endian `OTLB` header followed by
/// an Arrow IPC stream payload.
pub fn encode_frame(
    signal: SignalKind,
    seq_id: u64,
    batch: &RecordBatch,
    out: &mut Vec<u8>,
) -> Result<FrameHeader, PipelineError> {
    let row_count = u32::try_from(batch.num_rows())
        .map_err(|_| PipelineError::Storage("RecordBatch row_count exceeds u32::MAX".into()))?;

    let header_start = out.len();
    out.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    let payload_start = out.len();

    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut *out, batch.schema().as_ref())
                .map_err(PipelineError::Arrow)?;
        writer.write(batch).map_err(PipelineError::Arrow)?;
        writer.finish().map_err(PipelineError::Arrow)?;
    }

    let payload_len = u32::try_from(out.len().saturating_sub(payload_start))
        .map_err(|_| PipelineError::Storage("OTLB frame payload exceeds u32::MAX".into()))?;

    {
        let hdr = &mut out[header_start..payload_start];
        hdr[0..4].copy_from_slice(&FRAME_MAGIC);
        hdr[4] = FRAME_VERSION;
        hdr[5] = signal.as_u8();
        hdr[6..8].copy_from_slice(&0u16.to_le_bytes());
        hdr[8..16].copy_from_slice(&seq_id.to_le_bytes());
        hdr[16..20].copy_from_slice(&row_count.to_le_bytes());
        hdr[20..24].copy_from_slice(&payload_len.to_le_bytes());
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&out[header_start + 4..header_start + 24]);
    hasher.update(&out[payload_start..]);
    let crc32c = hasher.finalize();

    out[header_start + 24..header_start + 28].copy_from_slice(&crc32c.to_le_bytes());

    Ok(FrameHeader {
        signal,
        seq_id,
        row_count,
        payload_len,
        crc32c,
    })
}

/// Pre-encoded `OTLB` frame produced prior to reserving a sequence number in the signal ring.
#[derive(Debug, Clone)]
pub(crate) struct PreEncodedFrame {
    pub header: FrameHeader,
    pub bytes: Vec<u8>,
    pub batch: RecordBatch,
}

/// Patches `seq_id` into `frame_bytes[8..16]` and recomputes `crc32c` (`frame_bytes[24..28]`)
/// in place without re-encoding the Arrow IPC payload.
pub fn patch_frame_seq_id(
    frame_bytes: &mut [u8],
    header: &mut FrameHeader,
    seq_id: u64,
) -> Result<(), PipelineError> {
    let expected_len = FRAME_HEADER_LEN.saturating_add(header.payload_len as usize);
    if frame_bytes.len() < FRAME_HEADER_LEN || frame_bytes.len() != expected_len {
        return Err(storage_err(format!(
            "cannot patch OTLB frame seq_id: buffer length ({}) does not match expected frame length ({expected_len})",
            frame_bytes.len()
        )));
    }

    frame_bytes[8..16].copy_from_slice(&seq_id.to_le_bytes());

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&frame_bytes[4..24]);
    hasher.update(&frame_bytes[FRAME_HEADER_LEN..]);
    let crc32c = hasher.finalize();

    frame_bytes[24..28].copy_from_slice(&crc32c.to_le_bytes());
    header.seq_id = seq_id;
    header.crc32c = crc32c;
    Ok(())
}

/// Encodes `batch` into one or more [`PreEncodedFrame`]s (with placeholder `seq_id = 0`),
/// splitting by row bisection whenever a frame's `payload_len > max_payload_len`.
/// Returns `Err(PipelineError::Storage)` if an unsplittable (`<= 1` row) batch exceeds `max_payload_len`.
pub(crate) fn split_and_encode_frames_for_disk(
    signal: SignalKind,
    batch: RecordBatch,
    max_payload_len: u32,
) -> Result<Vec<PreEncodedFrame>, PipelineError> {
    let mut out = Vec::with_capacity(1);
    split_and_encode_recursive(signal, batch, max_payload_len, &mut out)?;
    Ok(out)
}

fn split_and_encode_recursive(
    signal: SignalKind,
    batch: RecordBatch,
    max_payload_len: u32,
    out: &mut Vec<PreEncodedFrame>,
) -> Result<(), PipelineError> {
    let mut bytes = Vec::with_capacity(
        FRAME_HEADER_LEN.saturating_add(batch.get_array_memory_size().min(64 * 1024)),
    );
    let header = encode_frame(signal, 0, &batch, &mut bytes)?;
    if header.payload_len <= max_payload_len {
        out.push(PreEncodedFrame {
            header,
            bytes,
            batch,
        });
        return Ok(());
    }

    let rows = batch.num_rows();
    if rows <= 1 {
        return Err(storage_err(format!(
            "single-row RecordBatch encoded OTLB payload ({} bytes) exceeds max_payload_len ({max_payload_len} bytes)",
            header.payload_len
        )));
    }

    let mid = rows / 2;
    let left = batch.slice(0, mid);
    let right = batch.slice(mid, rows - mid);
    split_and_encode_recursive(signal, left, max_payload_len, out)?;
    split_and_encode_recursive(signal, right, max_payload_len, out)?;
    Ok(())
}

/// Decodes the next 28-byte `OTLB` frame and Arrow IPC `RecordBatch` from `reader`,
/// returning `Ok(None)` on clean EOF at a frame boundary.
pub fn decode_frame<R: Read>(
    reader: &mut R,
    max_payload_len: u32,
) -> Result<Option<(FrameHeader, RecordBatch)>, FrameDecodeError> {
    let mut hdr = [0u8; FRAME_HEADER_LEN];
    let mut filled = 0usize;
    while filled < FRAME_HEADER_LEN {
        match reader.read(&mut hdr[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(FrameDecodeError::TornFrame);
            }
            Ok(n) => {
                filled = filled.saturating_add(n);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(FrameDecodeError::TornFrame);
            }
            Err(e) => return Err(FrameDecodeError::Io(e)),
        }
    }

    if hdr[0..4] != FRAME_MAGIC {
        return Err(FrameDecodeError::InvalidHeader {
            reason: format!("unexpected frame magic: {:02x?}", &hdr[0..4]),
        });
    }
    if hdr[4] != FRAME_VERSION {
        return Err(FrameDecodeError::InvalidHeader {
            reason: format!("unsupported frame version: {}", hdr[4]),
        });
    }
    let Some(signal) = SignalKind::from_u8(hdr[5]) else {
        return Err(FrameDecodeError::InvalidHeader {
            reason: format!("invalid signal type byte: {}", hdr[5]),
        });
    };

    let mut seq_bytes = [0u8; 8];
    seq_bytes.copy_from_slice(&hdr[8..16]);
    let seq_id = u64::from_le_bytes(seq_bytes);

    let mut row_bytes = [0u8; 4];
    row_bytes.copy_from_slice(&hdr[16..20]);
    let row_count = u32::from_le_bytes(row_bytes);

    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&hdr[20..24]);
    let payload_len = u32::from_le_bytes(len_bytes);

    let mut crc_bytes = [0u8; 4];
    crc_bytes.copy_from_slice(&hdr[24..28]);
    let crc32c = u32::from_le_bytes(crc_bytes);

    if payload_len > max_payload_len {
        return Err(FrameDecodeError::InvalidHeader {
            reason: format!(
                "frame payload_len ({payload_len}) exceeds max_payload_len ({max_payload_len})"
            ),
        });
    }

    let header = FrameHeader {
        signal,
        seq_id,
        row_count,
        payload_len,
        crc32c,
    };

    let mut payload = vec![0u8; payload_len as usize];
    if let Err(e) = reader.read_exact(&mut payload) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(FrameDecodeError::TornFrame);
        }
        return Err(FrameDecodeError::Io(e));
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&hdr[4..24]);
    hasher.update(&payload);
    let actual_crc = hasher.finalize();
    if actual_crc != crc32c {
        return Err(FrameDecodeError::CrcMismatch { header, actual_crc });
    }

    let cursor = Cursor::new(&payload);
    let mut ipc_reader =
        arrow::ipc::reader::StreamReader::try_new(cursor, None).map_err(|source| {
            FrameDecodeError::IpcDecode {
                header: header.clone(),
                source,
            }
        })?;

    let batch = match ipc_reader.next() {
        Some(Ok(b)) => b,
        Some(Err(source)) => {
            return Err(FrameDecodeError::IpcDecode { header, source });
        }
        None => {
            return Err(FrameDecodeError::IpcDecode {
                header,
                source: arrow::error::ArrowError::IpcError(
                    "OTLB frame contained no RecordBatch in IPC stream".into(),
                ),
            });
        }
    };

    Ok(Some((header, batch)))
}

/// Persisted contiguous low-watermark sequence numbers across all three signals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskWatermark {
    /// Durable 64-bit buffer instance identifier persisted across crash recovery.
    #[serde(default)]
    pub instance_id: u64,
    /// Highest contiguous acknowledged sequence number for `Logs`.
    #[serde(default)]
    pub logs: u64,
    /// Highest contiguous acknowledged sequence number for `Metrics`.
    #[serde(default)]
    pub metrics: u64,
    /// Highest contiguous acknowledged sequence number for `Traces`.
    #[serde(default)]
    pub traces: u64,
}

impl DiskWatermark {
    #[must_use]
    pub(crate) const fn for_signal(self, signal: SignalKind) -> u64 {
        match signal {
            SignalKind::Logs => self.logs,
            SignalKind::Metrics => self.metrics,
            SignalKind::Traces => self.traces,
        }
    }

    #[must_use]
    pub(crate) fn from_rings(instance_id: u64, rings: &[SignalRingState; 3]) -> Self {
        Self {
            instance_id,
            logs: rings[SignalKind::Logs.as_u8() as usize]
                .committed_seq
                .load(Ordering::Acquire),
            metrics: rings[SignalKind::Metrics.as_u8() as usize]
                .committed_seq
                .load(Ordering::Acquire),
            traces: rings[SignalKind::Traces.as_u8() as usize]
                .committed_seq
                .load(Ordering::Acquire),
        }
    }

    /// Monotonically merges `incoming` into `self` so out-of-order concurrent snapshots
    /// never regress any signal's committed watermark. Returns `true` iff any field changed.
    pub const fn merge_monotonic(&mut self, incoming: Self) -> bool {
        let prev_instance = self.instance_id;
        let prev_logs = self.logs;
        let prev_metrics = self.metrics;
        let prev_traces = self.traces;

        if self.instance_id == 0 && incoming.instance_id != 0 {
            self.instance_id = incoming.instance_id;
        }
        if incoming.logs > self.logs {
            self.logs = incoming.logs;
        }
        if incoming.metrics > self.metrics {
            self.metrics = incoming.metrics;
        }
        if incoming.traces > self.traces {
            self.traces = incoming.traces;
        }

        self.instance_id != prev_instance
            || self.logs != prev_logs
            || self.metrics != prev_metrics
            || self.traces != prev_traces
    }
}

/// Location of a frame written to a segment file by the writer thread.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WrittenFrame {
    pub(crate) segment_id: u64,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

type AppendReply = tokio::sync::oneshot::Sender<Result<WrittenFrame, String>>;
type WatermarkOneshotReply = tokio::sync::oneshot::Sender<Result<(), String>>;
type WatermarkSyncReply = std::sync::mpsc::SyncSender<Result<(), String>>;

enum WriterCommand {
    AppendEncoded {
        signal: SignalKind,
        seq_id: u64,
        rows: u32,
        frame_bytes: Vec<u8>,
        reply: AppendReply,
    },
    UpdateWatermark {
        watermark: DiskWatermark,
        reply: Option<WatermarkOneshotReply>,
    },
    FlushWatermarkSync {
        watermark: DiskWatermark,
        reply: WatermarkSyncReply,
    },
    Shutdown,
}

#[derive(Debug)]
struct ActiveSegment {
    segment_id: u64,
    path: PathBuf,
    file: File,
    bytes_written: u64,
    max_seq: [u64; 3],
    has_frames: bool,
}

#[derive(Debug, Clone)]
struct SealedSegmentMeta {
    path: PathBuf,
    max_seq: [u64; 3],
    has_frames: bool,
}

impl SealedSegmentMeta {
    const fn is_fully_acked(&self, watermark: DiskWatermark) -> bool {
        self.has_frames
            && self.max_seq[0] <= watermark.logs
            && self.max_seq[1] <= watermark.metrics
            && self.max_seq[2] <= watermark.traces
    }
}

#[must_use]
pub(crate) fn segment_file_name(segment_id: u64) -> String {
    format!("segment-{segment_id:020}.bin")
}

#[must_use]
pub(crate) fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(segment_file_name(segment_id))
}

fn parse_segment_id(file_name: &str) -> Option<u64> {
    let rest = file_name.strip_prefix("segment-")?;
    let digits = rest.strip_suffix(".bin")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()
}

fn sync_dir(dir: &Path) {
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_data();
    }
}

fn write_watermark_atomic(dir: &Path, watermark: DiskWatermark) -> Result<(), String> {
    let tmp_path = dir.join("watermark.json.tmp");
    let final_path = dir.join("watermark.json");
    let payload = serde_json::to_vec_pretty(&watermark)
        .map_err(|e| format!("failed to serialize watermark.json: {e}"))?;
    {
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| format!("failed to open {}: {e}", tmp_path.display()))?;
        tmp_file
            .write_all(&payload)
            .map_err(|e| format!("failed to write {}: {e}", tmp_path.display()))?;
        tmp_file
            .sync_data()
            .map_err(|e| format!("failed to sync {}: {e}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| {
        format!(
            "failed to rename {} to {}: {e}",
            tmp_path.display(),
            final_path.display()
        )
    })?;
    sync_dir(dir);
    Ok(())
}

fn gc_sealed_segments(
    dir: &Path,
    sealed_segments: &mut Vec<SealedSegmentMeta>,
    watermark: DiskWatermark,
) {
    let mut removed_any = false;
    sealed_segments.retain(|seg| {
        if seg.is_fully_acked(watermark) {
            let _ = std::fs::remove_file(&seg.path);
            removed_any = true;
            false
        } else {
            true
        }
    });
    if removed_any {
        sync_dir(dir);
    }
}

/// Reads and decodes a single on-disk `OTLB` frame at `(segment_id, offset, len)`.
pub(crate) fn read_frame_at(
    dir: &Path,
    segment_id: u64,
    offset: u64,
    len: u32,
    max_payload_len: u32,
) -> Result<RecordBatch, PipelineError> {
    let path = segment_path(dir, segment_id);
    let mut file = File::open(&path).map_err(|e| {
        storage_err(format!(
            "failed to open segment {} for lease prepare: {e}",
            path.display()
        ))
    })?;
    file.seek(SeekFrom::Start(offset)).map_err(|e| {
        storage_err(format!(
            "failed to seek segment {} to offset {offset}: {e}",
            path.display()
        ))
    })?;
    let mut limited = file.take(u64::from(len));
    match decode_frame(&mut limited, max_payload_len) {
        Ok(Some((_hdr, batch))) => Ok(batch),
        Ok(None) => Err(storage_err(format!(
            "unexpected EOF reading frame at offset {offset} in {}",
            path.display()
        ))),
        Err(e) => Err(storage_err(format!(
            "failed to decode frame at offset {offset} in {}: {e}",
            path.display()
        ))),
    }
}

/// `NVMe` segmented disk backend managing the advisory lock, startup recovery, and group-commit writer thread.
pub(crate) struct DiskBackend {
    dir: PathBuf,
    instance_id: u64,
    cmd_tx: Mutex<Option<std::sync::mpsc::Sender<WriterCommand>>>,
    writer_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    _lock_file: File,
}

impl std::fmt::Debug for DiskBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskBackend")
            .field("dir", &self.dir)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl DiskBackend {
    #[must_use]
    pub(crate) const fn instance_id(&self) -> u64 {
        self.instance_id
    }

    pub(crate) fn open_and_recover(
        sink_name: &str,
        config: &BufferConfig,
        rings: &[SignalRingState; 3],
    ) -> Result<Self, PipelineError> {
        let dir = config.disk_path.clone().ok_or_else(|| {
            storage_err(format!(
                "[{sink_name}.buffer]: disk mode requires disk_path"
            ))
        })?;

        std::fs::create_dir_all(&dir).map_err(|e| {
            storage_err(format!(
                "failed to create buffer disk_path '{}': {e}",
                dir.display()
            ))
        })?;

        let lock_file = Self::acquire_advisory_lock(&dir)?;
        let watermark = Self::load_initial_watermark(&dir, rings)?;
        let instance_id = watermark.instance_id;
        let (max_segment_id, sealed_segments) =
            Self::recover_segments(sink_name, &dir, config, watermark, rings)?;

        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<WriterCommand>();
        let writer_dir = dir.clone();
        let segment_size_bytes = config.segment_size_bytes;
        let max_payload_len = config.max_disk_frame_payload_bytes();
        let sync_on_write = config.sync_on_write;
        let next_segment_id = max_segment_id.saturating_add(1);

        let writer_handle = std::thread::Builder::new()
            .name(format!("dl-buf-{sink_name}"))
            .spawn(move || {
                Self::writer_loop(
                    &writer_dir,
                    &cmd_rx,
                    segment_size_bytes,
                    max_payload_len,
                    sync_on_write,
                    next_segment_id,
                    sealed_segments,
                    watermark,
                );
            })
            .map_err(|e| {
                storage_err(format!(
                    "failed to spawn disk buffer writer thread 'dl-buf-{sink_name}': {e}"
                ))
            })?;

        Ok(Self {
            dir,
            instance_id,
            cmd_tx: Mutex::new(Some(cmd_tx)),
            writer_handle: Mutex::new(Some(writer_handle)),
            _lock_file: lock_file,
        })
    }

    fn acquire_advisory_lock(dir: &Path) -> Result<File, PipelineError> {
        let lock_path = dir.join("buffer.lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                storage_err(format!(
                    "failed to open buffer lock file '{}': {e}",
                    lock_path.display()
                ))
            })?;

        lock_file.try_lock().map_err(|e| {
            storage_err(format!(
                "buffer disk_path '{}' lock failed: {e}",
                dir.display()
            ))
        })?;
        Ok(lock_file)
    }

    fn load_initial_watermark(
        dir: &Path,
        rings: &[SignalRingState; 3],
    ) -> Result<DiskWatermark, PipelineError> {
        let tmp_wm = dir.join("watermark.json.tmp");
        if tmp_wm.exists() {
            let _ = std::fs::remove_file(&tmp_wm);
        }

        let watermark_path = dir.join("watermark.json");
        let mut watermark = if watermark_path.exists() {
            let bytes = std::fs::read(&watermark_path).map_err(|e| {
                storage_err(format!(
                    "failed to read '{}': {e}",
                    watermark_path.display()
                ))
            })?;
            serde_json::from_slice::<DiskWatermark>(&bytes).map_err(|e| {
                storage_err(format!(
                    "failed to parse '{}': {e}",
                    watermark_path.display()
                ))
            })?
        } else {
            DiskWatermark::default()
        };

        if watermark.instance_id == 0 {
            watermark.instance_id = crate::buffer::generate_instance_id();
            write_watermark_atomic(dir, watermark).map_err(storage_err)?;
        }

        for sig in [SignalKind::Logs, SignalKind::Metrics, SignalKind::Traces] {
            let wm_val = watermark.for_signal(sig);
            let ring_state = &rings[sig.as_u8() as usize];
            ring_state.committed_seq.store(wm_val, Ordering::Release);
            let mut ring = lock_unpoisoned(&ring_state.ring);
            ring.next_seq = wm_val.saturating_add(1);
        }
        Ok(watermark)
    }

    fn recover_segments(
        sink_name: &str,
        dir: &Path,
        config: &BufferConfig,
        watermark: DiskWatermark,
        rings: &[SignalRingState; 3],
    ) -> Result<(u64, Vec<SealedSegmentMeta>), PipelineError> {
        let mut segments: Vec<(u64, PathBuf)> = Vec::new();
        let read_dir = std::fs::read_dir(dir).map_err(|e| {
            storage_err(format!(
                "failed to read buffer disk_path '{}': {e}",
                dir.display()
            ))
        })?;
        for entry_res in read_dir {
            let entry = entry_res
                .map_err(|e| storage_err(format!("failed to read directory entry: {e}")))?;
            let file_name = entry.file_name();
            if let Some(name_str) = file_name.to_str()
                && let Some(segment_num) = parse_segment_id(name_str)
            {
                segments.push((segment_num, entry.path()));
            }
        }
        segments.sort_by_key(|(segment_num, _)| *segment_num);

        let max_payload_len = config.max_disk_frame_payload_bytes();
        let mut max_segment_id = 0u64;
        let mut sealed_segments = Vec::new();

        for (segment_num, seg_path) in segments {
            max_segment_id = max_segment_id.max(segment_num);
            if let Some(sealed) = Self::recover_single_segment(
                sink_name,
                dir,
                segment_num,
                seg_path,
                max_payload_len,
                watermark,
                rings,
            )? {
                sealed_segments.push(sealed);
            }
        }
        Ok((max_segment_id, sealed_segments))
    }

    fn recover_single_segment(
        sink_name: &str,
        dir: &Path,
        segment_num: u64,
        seg_path: PathBuf,
        max_payload_len: u32,
        watermark: DiskWatermark,
        rings: &[SignalRingState; 3],
    ) -> Result<Option<SealedSegmentMeta>, PipelineError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&seg_path)
            .map_err(|e| {
                storage_err(format!(
                    "failed to open segment '{}' for recovery: {e}",
                    seg_path.display()
                ))
            })?;

        let mut offset = 0u64;
        let mut max_seq = [0u64; 3];
        let mut unacked_frames = 0usize;
        let mut total_valid_frames = 0usize;

        loop {
            file.seek(SeekFrom::Start(offset)).map_err(|e| {
                storage_err(format!(
                    "failed to seek segment '{}' to {offset}: {e}",
                    seg_path.display()
                ))
            })?;

            match decode_frame(&mut file, max_payload_len) {
                Ok(None) => break,
                Ok(Some((header, batch))) => {
                    let frame_len = FRAME_HEADER_LEN_U32.saturating_add(header.payload_len);
                    let sig_idx = header.signal.as_u8() as usize;
                    max_seq[sig_idx] = max_seq[sig_idx].max(header.seq_id);
                    total_valid_frames = total_valid_frames.saturating_add(1);

                    if Self::record_recovered_frame(
                        segment_num,
                        offset,
                        frame_len,
                        &header,
                        &batch,
                        watermark,
                        &rings[sig_idx],
                    ) {
                        unacked_frames = unacked_frames.saturating_add(1);
                    }
                    offset = offset.saturating_add(u64::from(frame_len));
                }
                Err(FrameDecodeError::TornFrame) => {
                    Self::truncate_torn_segment(sink_name, &file, &seg_path, offset)?;
                    break;
                }
                Err(
                    FrameDecodeError::CrcMismatch { header, .. }
                    | FrameDecodeError::IpcDecode { header, .. },
                ) => {
                    let frame_len = FRAME_HEADER_LEN_U32.saturating_add(header.payload_len);
                    let sig_idx = header.signal.as_u8() as usize;
                    max_seq[sig_idx] = max_seq[sig_idx].max(header.seq_id);
                    Self::record_corrupted_frame(
                        sink_name,
                        &seg_path,
                        &header,
                        watermark,
                        &rings[sig_idx],
                    );
                    offset = offset.saturating_add(u64::from(frame_len));
                }
                Err(FrameDecodeError::InvalidHeader { reason }) => {
                    tracing::error!(
                        sink = sink_name,
                        segment = %seg_path.display(),
                        offset,
                        reason = %reason,
                        "Stopping segment scan due to invalid OTLB frame header"
                    );
                    rings[0].corruption_drops.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(FrameDecodeError::Io(e)) => {
                    return Err(storage_err(format!(
                        "I/O error scanning segment '{}': {e}",
                        seg_path.display()
                    )));
                }
            }
        }

        if unacked_frames == 0 && total_valid_frames > 0 {
            let _ = std::fs::remove_file(&seg_path);
            sync_dir(dir);
            Ok(None)
        } else if unacked_frames > 0 {
            Ok(Some(SealedSegmentMeta {
                path: seg_path,
                max_seq,
                has_frames: true,
            }))
        } else {
            Ok(None)
        }
    }

    fn record_recovered_frame(
        segment_num: u64,
        offset: u64,
        frame_len: u32,
        header: &FrameHeader,
        batch: &RecordBatch,
        watermark: DiskWatermark,
        ring_state: &SignalRingState,
    ) -> bool {
        let mut ring = lock_unpoisoned(&ring_state.ring);
        ring.next_seq = ring.next_seq.max(header.seq_id.saturating_add(1));
        if header.seq_id <= watermark.for_signal(header.signal) {
            return false;
        }
        let byte_size = frame_len as usize;
        let row_count = header.row_count as usize;
        ring.bytes_in_flight = ring.bytes_in_flight.saturating_add(byte_size);
        ring.rows_in_flight = ring.rows_in_flight.saturating_add(row_count);
        ring.slots.insert(header.seq_id, LeaseSlotState::Queued);
        ring.enqueue_ordered(QueuedBatch {
            seq_id: header.seq_id,
            payload: BatchPayload::OnDisk {
                segment_id: segment_num,
                offset,
                len: frame_len,
                rows: header.row_count,
                schema: batch.schema(),
            },
            byte_size,
            row_count,
            enqueued_at: Instant::now(),
            attempt: 0,
            ready_at: None,
        });
        ring_state
            .replayed_records
            .fetch_add(u64::from(header.row_count), Ordering::Relaxed);
        true
    }

    fn record_corrupted_frame(
        sink_name: &str,
        seg_path: &Path,
        header: &FrameHeader,
        watermark: DiskWatermark,
        ring_state: &SignalRingState,
    ) {
        tracing::error!(
            sink = sink_name,
            signal = header.signal.as_str(),
            seq_id = header.seq_id,
            rows = header.row_count,
            segment = %seg_path.display(),
            "Skipping corrupted OTLB frame during crash recovery"
        );
        ring_state
            .corruption_drops
            .fetch_add(u64::from(header.row_count), Ordering::Relaxed);
        let mut ring = lock_unpoisoned(&ring_state.ring);
        ring.next_seq = ring.next_seq.max(header.seq_id.saturating_add(1));
        if header.seq_id > watermark.for_signal(header.signal) {
            ring.slots.insert(header.seq_id, LeaseSlotState::Acked);
        }
    }

    fn truncate_torn_segment(
        sink_name: &str,
        file: &File,
        seg_path: &Path,
        offset: u64,
    ) -> Result<(), PipelineError> {
        tracing::warn!(
            sink = sink_name,
            segment = %seg_path.display(),
            truncate_offset = offset,
            "Truncating torn tail frame at end of segment during crash recovery"
        );
        file.set_len(offset).map_err(|e| {
            storage_err(format!(
                "failed to truncate torn segment '{}': {e}",
                seg_path.display()
            ))
        })?;
        file.sync_data().map_err(|e| {
            storage_err(format!(
                "failed to sync truncated segment '{}': {e}",
                seg_path.display()
            ))
        })
    }

    pub(crate) async fn append_encoded_frame(
        &self,
        signal: SignalKind,
        seq_id: u64,
        rows: u32,
        frame_bytes: Vec<u8>,
    ) -> Result<WrittenFrame, PipelineError> {
        let sender = {
            let guard = lock_unpoisoned(&self.cmd_tx);
            guard
                .clone()
                .ok_or_else(|| storage_err("disk buffer writer thread is closed"))?
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        sender
            .send(WriterCommand::AppendEncoded {
                signal,
                seq_id,
                rows,
                frame_bytes,
                reply: tx,
            })
            .map_err(|_| storage_err("disk buffer writer thread exited"))?;

        let res = rx
            .await
            .map_err(|_| storage_err("disk buffer writer thread dropped append reply"))?;
        res.map_err(storage_err)
    }

    pub(crate) async fn update_watermark(
        &self,
        watermark: DiskWatermark,
    ) -> Result<(), PipelineError> {
        let sender = {
            let guard = lock_unpoisoned(&self.cmd_tx);
            guard
                .clone()
                .ok_or_else(|| storage_err("disk buffer writer thread is closed"))?
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        sender
            .send(WriterCommand::UpdateWatermark {
                watermark,
                reply: Some(tx),
            })
            .map_err(|_| storage_err("disk buffer writer thread exited"))?;

        let res = rx
            .await
            .map_err(|_| storage_err("disk buffer writer thread dropped watermark reply"))?;
        res.map_err(storage_err)
    }

    pub(crate) fn update_watermark_nonblocking(&self, watermark: DiskWatermark) {
        let sender = {
            let guard = lock_unpoisoned(&self.cmd_tx);
            guard.clone()
        };
        if let Some(tx) = sender {
            let _ = tx.send(WriterCommand::UpdateWatermark {
                watermark,
                reply: None,
            });
        }
    }

    pub(crate) fn flush_and_sync_watermark(
        &self,
        watermark: DiskWatermark,
    ) -> Result<(), PipelineError> {
        let sender = {
            let guard = lock_unpoisoned(&self.cmd_tx);
            guard
                .clone()
                .ok_or_else(|| storage_err("disk buffer writer thread is closed"))?
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        sender
            .send(WriterCommand::FlushWatermarkSync {
                watermark,
                reply: tx,
            })
            .map_err(|_| storage_err("disk buffer writer thread exited"))?;

        let res = rx
            .recv()
            .map_err(|_| storage_err("disk buffer writer thread dropped sync reply"))?;
        res.map_err(storage_err)
    }

    #[allow(clippy::too_many_arguments)]
    fn writer_loop(
        dir: &Path,
        cmd_rx: &std::sync::mpsc::Receiver<WriterCommand>,
        segment_size_bytes: u64,
        max_payload_len: u32,
        sync_on_write: bool,
        mut next_segment_id: u64,
        mut sealed_segments: Vec<SealedSegmentMeta>,
        mut current_watermark: DiskWatermark,
    ) {
        let mut active_segment: Option<ActiveSegment> = None;

        while let Ok(first_cmd) = cmd_rx.recv() {
            let mut batch_cmds = Vec::with_capacity(16);
            batch_cmds.push(first_cmd);
            while batch_cmds.len() < 64 {
                let Ok(cmd) = cmd_rx.try_recv() else { break };
                batch_cmds.push(cmd);
            }

            let mut append_replies: Vec<(AppendReply, Result<WrittenFrame, String>)> = Vec::new();
            let mut wm_oneshot_replies: Vec<WatermarkOneshotReply> = Vec::new();
            let mut wm_blocking_replies: Vec<WatermarkSyncReply> = Vec::new();
            let mut watermark_dirty = false;
            let mut force_sync_segment = false;
            let mut should_shutdown = false;

            for cmd in batch_cmds {
                match cmd {
                    WriterCommand::AppendEncoded {
                        signal,
                        seq_id,
                        rows,
                        frame_bytes,
                        reply,
                    } => {
                        let res = Self::write_encoded_frame(
                            dir,
                            &mut active_segment,
                            &mut sealed_segments,
                            &mut next_segment_id,
                            segment_size_bytes,
                            max_payload_len,
                            current_watermark,
                            signal,
                            seq_id,
                            rows,
                            &frame_bytes,
                        );
                        append_replies.push((reply, res));
                    }
                    WriterCommand::UpdateWatermark { watermark, reply } => {
                        if current_watermark.merge_monotonic(watermark) {
                            watermark_dirty = true;
                        }
                        if let Some(r) = reply {
                            wm_oneshot_replies.push(r);
                        }
                    }
                    WriterCommand::FlushWatermarkSync { watermark, reply } => {
                        let _ = current_watermark.merge_monotonic(watermark);
                        watermark_dirty = true;
                        force_sync_segment = true;
                        wm_blocking_replies.push(reply);
                    }
                    WriterCommand::Shutdown => {
                        should_shutdown = true;
                    }
                }
            }

            let mut sync_err: Option<String> = None;
            if ((!append_replies.is_empty() && sync_on_write) || force_sync_segment)
                && let Some(seg) = &mut active_segment
                && let Err(e) = seg.file.sync_data()
            {
                sync_err = Some(format!(
                    "failed to sync active segment '{}': {e}",
                    seg.path.display()
                ));
            }

            for (reply, write_res) in append_replies {
                let final_res = match (&sync_err, write_res) {
                    (Some(err), _) => Err(err.clone()),
                    (None, r) => r,
                };
                let _ = reply.send(final_res);
            }

            if watermark_dirty {
                let wm_res = match &sync_err {
                    Some(err) => Err(err.clone()),
                    None => write_watermark_atomic(dir, current_watermark),
                };
                if wm_res.is_ok() {
                    gc_sealed_segments(dir, &mut sealed_segments, current_watermark);
                }
                for reply in wm_oneshot_replies {
                    let _ = reply.send(wm_res.clone());
                }
                for reply in wm_blocking_replies {
                    let _ = reply.send(wm_res.clone());
                }
            } else {
                for reply in wm_oneshot_replies {
                    let _ = reply.send(Ok(()));
                }
            }

            if should_shutdown {
                break;
            }
        }

        if let Some(seg) = active_segment.take() {
            let _ = seg.file.sync_data();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_encoded_frame(
        dir: &Path,
        active_segment: &mut Option<ActiveSegment>,
        sealed_segments: &mut Vec<SealedSegmentMeta>,
        next_segment_id: &mut u64,
        segment_size_bytes: u64,
        max_payload_len: u32,
        current_watermark: DiskWatermark,
        signal: SignalKind,
        seq_id: u64,
        rows: u32,
        frame_bytes: &[u8],
    ) -> Result<WrittenFrame, String> {
        if frame_bytes.len() < FRAME_HEADER_LEN {
            return Err(format!(
                "encoded OTLB frame shorter than {FRAME_HEADER_LEN}-byte header ({} bytes)",
                frame_bytes.len()
            ));
        }
        let payload_len = u32::try_from(frame_bytes.len() - FRAME_HEADER_LEN)
            .map_err(|_| "frame payload length overflow".to_string())?;
        if payload_len > max_payload_len {
            return Err(format!(
                "OTLB frame payload_len ({payload_len}) exceeds max_payload_len ({max_payload_len})"
            ));
        }
        let mut hdr_row_bytes = [0u8; 4];
        hdr_row_bytes.copy_from_slice(&frame_bytes[16..20]);
        let hdr_rows = u32::from_le_bytes(hdr_row_bytes);
        if hdr_rows != rows {
            return Err(format!(
                "OTLB frame header row_count ({hdr_rows}) does not match expected rows ({rows})"
            ));
        }
        let frame_len_u64 = frame_bytes.len() as u64;
        let frame_len_u32 =
            u32::try_from(frame_bytes.len()).map_err(|_| "frame length overflow".to_string())?;

        // Roll active segment before writing if adding this frame would exceed segment_size_bytes
        if let Some(seg) = active_segment.as_ref()
            && seg.bytes_written > 0
            && seg.bytes_written.saturating_add(frame_len_u64) > segment_size_bytes
            && let Some(sealed) = active_segment.take()
        {
            sealed
                .file
                .sync_data()
                .map_err(|e| format!("failed to sync sealed segment: {e}"))?;
            sealed_segments.push(SealedSegmentMeta {
                path: sealed.path,
                max_seq: sealed.max_seq,
                has_frames: sealed.has_frames,
            });
            gc_sealed_segments(dir, sealed_segments, current_watermark);
        }

        if active_segment.is_none() {
            let new_segment_num = *next_segment_id;
            *next_segment_id = next_segment_id.saturating_add(1);
            let path = segment_path(dir, new_segment_num);
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| format!("failed to create segment '{}': {e}", path.display()))?;
            sync_dir(dir);
            *active_segment = Some(ActiveSegment {
                segment_id: new_segment_num,
                path,
                file,
                bytes_written: 0,
                max_seq: [0; 3],
                has_frames: false,
            });
        }

        let Some(seg) = active_segment.as_mut() else {
            return Err("internal error: active_segment missing after initialization".into());
        };

        let offset = seg.bytes_written;
        let segment_id = seg.segment_id;
        seg.file
            .write_all(frame_bytes)
            .map_err(|e| format!("failed to write to segment '{}': {e}", seg.path.display()))?;
        seg.bytes_written = seg.bytes_written.saturating_add(frame_len_u64);
        let sig_idx = signal.as_u8() as usize;
        seg.max_seq[sig_idx] = seg.max_seq[sig_idx].max(seq_id);
        seg.has_frames = true;

        if seg.bytes_written >= segment_size_bytes
            && let Some(sealed) = active_segment.take()
        {
            sealed
                .file
                .sync_data()
                .map_err(|e| format!("failed to sync sealed segment: {e}"))?;
            sealed_segments.push(SealedSegmentMeta {
                path: sealed.path,
                max_seq: sealed.max_seq,
                has_frames: sealed.has_frames,
            });
            gc_sealed_segments(dir, sealed_segments, current_watermark);
        }

        Ok(WrittenFrame {
            segment_id,
            offset,
            len: frame_len_u32,
        })
    }
}

impl Drop for DiskBackend {
    fn drop(&mut self) {
        if let Some(tx) = lock_unpoisoned(&self.cmd_tx).take() {
            let _ = tx.send(WriterCommand::Shutdown);
        }
        if let Some(handle) = lock_unpoisoned(&self.writer_handle).take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{BufferConfig, BufferMode, SinkBuffer};
    use crate::pipeline::SignalBatch;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TempDiskDir {
        path: PathBuf,
    }

    impl TempDiskDir {
        fn new(prefix: &str) -> Self {
            let id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let path = std::env::temp_dir().join(format!("otel-dl-disk-{prefix}-{pid}-{id}"));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDiskDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn make_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        let arr = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn test_frame_header_is_exactly_28_bytes_and_roundtrips_ipc() {
        let batch = make_batch(&[10, 20, 30, 40]);
        let mut buf = Vec::new();
        let header = encode_frame(SignalKind::Logs, 7, &batch, &mut buf).unwrap();

        assert_eq!(header.signal, SignalKind::Logs);
        assert_eq!(header.seq_id, 7);
        assert_eq!(header.row_count, 4);
        assert_eq!(buf.len(), FRAME_HEADER_LEN + header.payload_len as usize);
        assert_eq!(&buf[0..4], b"OTLB");
        assert_eq!(buf[4], FRAME_VERSION);
        assert_eq!(buf[5], SignalKind::Logs.as_u8());
        assert_eq!(&buf[6..8], &[0, 0]);

        let mut cursor = Cursor::new(&buf);
        let (decoded_hdr, decoded_batch) = decode_frame(&mut cursor, u32::MAX).unwrap().unwrap();
        assert_eq!(decoded_hdr, header);
        assert_eq!(decoded_batch, batch);
        assert!(decode_frame(&mut cursor, u32::MAX).unwrap().is_none());

        // Flip 1 bit in payload and verify CRC mismatch detection
        let mut corrupted = buf.clone();
        corrupted[FRAME_HEADER_LEN + 4] ^= 0x01;
        let mut bad_cursor = Cursor::new(&corrupted);
        let err = decode_frame(&mut bad_cursor, u32::MAX).unwrap_err();
        assert!(matches!(err, FrameDecodeError::CrcMismatch { .. }));
    }

    #[test]
    fn test_disk_frame_roundtrip_and_crc_verification() {
        test_frame_header_is_exactly_28_bytes_and_roundtrips_ipc();
    }

    #[tokio::test]
    async fn test_disk_recovery_torn_tail_vs_corrupt_crc() {
        let tmp = TempDiskDir::new("recovery");
        let b1 = make_batch(&[1, 2, 3]);
        let b2 = make_batch(&[4, 5]);
        let b3 = make_batch(&[6, 7, 8, 9]);

        let seg_path = tmp.path.join("segment-00000000000000000001.bin");
        let mut raw_bytes = Vec::new();
        encode_frame(SignalKind::Logs, 1, &b1, &mut raw_bytes).unwrap();
        let frame1_end = raw_bytes.len();
        encode_frame(SignalKind::Logs, 2, &b2, &mut raw_bytes).unwrap();
        let frame2_end = raw_bytes.len();
        encode_frame(SignalKind::Logs, 3, &b3, &mut raw_bytes).unwrap();

        // Write 3 frames and truncate 10 bytes off the end of frame 3 (simulating power loss)
        std::fs::write(&seg_path, &raw_bytes[..raw_bytes.len() - 10]).unwrap();

        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        // Reopen SinkBuffer: frames 1 and 2 must replay cleanly, file truncated to frame2_end, corruption_drops == 0
        {
            let (tx, mut buf) = SinkBuffer::new("test_recovery", config.clone()).unwrap();
            drop(tx);
            assert_eq!(buf.corruption_drops(), 0);
            let meta = std::fs::metadata(&seg_path).unwrap();
            assert_eq!(meta.len(), frame2_end as u64);

            let lease1 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease1.id().first_seq, 1);
            let rec1 = lease1.prepare(Ok).await.unwrap();
            assert_eq!(rec1, b1);
            lease1.ack().await.unwrap();

            let lease2 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease2.id().first_seq, 2);
            let rec2 = lease2.prepare(Ok).await.unwrap();
            assert_eq!(rec2, b2);
            lease2.ack().await.unwrap();

            assert!(buf.next_lease().await.unwrap().is_none());
        }

        // Flip 1 bit in frame 1's payload while frame 2 remains valid (reset watermark for part 2)
        let _ = std::fs::remove_file(tmp.path.join("watermark.json"));
        let mut seg_bytes = raw_bytes[..frame2_end].to_vec();
        seg_bytes[FRAME_HEADER_LEN + 8] ^= 0x04;
        std::fs::write(&seg_path, &seg_bytes).unwrap();

        {
            let (tx, mut buf) = SinkBuffer::new("test_recovery", config).unwrap();
            drop(tx);
            assert_eq!(buf.corruption_drops(), b1.num_rows() as u64);

            let lease2 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease2.id().first_seq, 2);
            assert_eq!(lease2.id().last_seq, 2);
            let rec2 = lease2.prepare(Ok).await.unwrap();
            assert_eq!(rec2, b2);
            lease2.ack().await.unwrap();

            assert!(buf.next_lease().await.unwrap().is_none());
            let _ = frame1_end;
        }
    }

    #[tokio::test]
    async fn test_disk_recovery_truncates_torn_tail_and_skips_corrupt_middle_frame() {
        let tmp = TempDiskDir::new("recovery_alias");
        let b1 = make_batch(&[10, 20]);
        let b2 = make_batch(&[30, 40, 50]);
        let b3 = make_batch(&[60]);

        let seg_path = tmp.path.join("segment-00000000000000000001.bin");
        let mut raw_bytes = Vec::new();
        encode_frame(SignalKind::Logs, 1, &b1, &mut raw_bytes).unwrap();
        encode_frame(SignalKind::Logs, 2, &b2, &mut raw_bytes).unwrap();
        let frame2_end = raw_bytes.len();
        encode_frame(SignalKind::Logs, 3, &b3, &mut raw_bytes).unwrap();

        std::fs::write(&seg_path, &raw_bytes[..raw_bytes.len() - 10]).unwrap();

        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        {
            let (tx, mut buf) = SinkBuffer::new("test_recovery_alias", config.clone()).unwrap();
            drop(tx);
            assert_eq!(buf.corruption_drops(), 0);
            assert_eq!(
                std::fs::metadata(&seg_path).unwrap().len(),
                frame2_end as u64
            );
            let l1 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(l1.id().first_seq, 1);
            l1.ack().await.unwrap();

            let l2 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(l2.id().first_seq, 2);
            l2.ack().await.unwrap();
            assert!(buf.next_lease().await.unwrap().is_none());
        }

        let _ = std::fs::remove_file(tmp.path.join("watermark.json"));
        let mut seg_bytes = raw_bytes[..frame2_end].to_vec();
        seg_bytes[FRAME_HEADER_LEN + 2] ^= 0x01;
        std::fs::write(&seg_path, &seg_bytes).unwrap();

        {
            let (tx, mut buf) = SinkBuffer::new("test_recovery_alias", config).unwrap();
            drop(tx);
            assert_eq!(buf.corruption_drops(), 2);
            let l2 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(l2.id().first_seq, 2);
            l2.ack().await.unwrap();
            assert!(buf.next_lease().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_disk_watermark_prevents_replay_of_acked_sequences() {
        let tmp = TempDiskDir::new("watermark");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        {
            let (tx, mut buf) = SinkBuffer::new("test_wm", config.clone()).unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[100])))
                .await
                .unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[200])))
                .await
                .unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[300])))
                .await
                .unwrap();

            let lease1 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease1.id().first_seq, 1);
            lease1.ack().await.unwrap();

            let lease2 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease2.id().first_seq, 2);
            lease2.ack().await.unwrap();

            buf.flush_and_sync_watermark().unwrap();
            assert!(tmp.path.join("watermark.json").exists());
        }

        // Reopen on same disk_path: only seq 3 should be replayed
        {
            let (tx, mut buf) = SinkBuffer::new("test_wm", config).unwrap();
            drop(tx);
            assert_eq!(buf.committed_seq(SignalKind::Logs), 2);

            let lease3 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease3.id().first_seq, 3);
            assert_eq!(lease3.id().last_seq, 3);
            let batch3 = lease3.prepare(Ok).await.unwrap();
            assert_eq!(batch3, make_batch(&[300]));
            lease3.ack().await.unwrap();

            assert!(buf.next_lease().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_disk_buffer_persists_watermark_and_recovers_only_unacked_sequences() {
        let tmp = TempDiskDir::new("watermark_alias");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        {
            let (tx, mut buf) = SinkBuffer::new("test_wm_alias", config.clone()).unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[1]))).await.unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[2]))).await.unwrap();
            tx.send(SignalBatch::Logs(make_batch(&[3]))).await.unwrap();

            buf.next_lease()
                .await
                .unwrap()
                .unwrap()
                .ack()
                .await
                .unwrap();
            buf.next_lease()
                .await
                .unwrap()
                .unwrap()
                .ack()
                .await
                .unwrap();
            buf.flush_and_sync_watermark().unwrap();
        }

        {
            let (tx, mut buf) = SinkBuffer::new("test_wm_alias", config).unwrap();
            drop(tx);
            let lease3 = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease3.id().first_seq, 3);
            lease3.ack().await.unwrap();
            assert!(buf.next_lease().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_signal_triad_shares_single_disk_lock_and_isolates_signals() {
        let tmp = TempDiskDir::new("triad_lock");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let [
            (logs_tx, mut logs_buf),
            (metrics_tx, mut metrics_buf),
            (traces_tx, mut traces_buf),
        ] = SinkBuffer::new_signal_triad("triad_disk", config.clone()).unwrap();

        assert!(tmp.path.join("buffer.lock").exists());

        // Attempt a second SinkBuffer::new on the same disk_path while triad is open -> must fail
        let second = SinkBuffer::new("second_disk", config);
        assert!(matches!(second, Err(PipelineError::Storage(_))));

        // Verify triad isolates signals on the shared disk backend
        logs_tx
            .send(SignalBatch::Logs(make_batch(&[11])))
            .await
            .unwrap();
        metrics_tx
            .send(SignalBatch::Metrics(make_batch(&[22])))
            .await
            .unwrap();
        traces_tx
            .send(SignalBatch::Traces(make_batch(&[33])))
            .await
            .unwrap();
        drop(logs_tx);
        drop(metrics_tx);
        drop(traces_tx);

        let l_lease = logs_buf.next_lease().await.unwrap().unwrap();
        assert_eq!(l_lease.signal(), SignalKind::Logs);
        assert_eq!(l_lease.prepare(Ok).await.unwrap(), make_batch(&[11]));
        l_lease.ack().await.unwrap();

        let m_lease = metrics_buf.next_lease().await.unwrap().unwrap();
        assert_eq!(m_lease.signal(), SignalKind::Metrics);
        assert_eq!(m_lease.prepare(Ok).await.unwrap(), make_batch(&[22]));
        m_lease.ack().await.unwrap();

        let t_lease = traces_buf.next_lease().await.unwrap().unwrap();
        assert_eq!(t_lease.signal(), SignalKind::Traces);
        assert_eq!(t_lease.prepare(Ok).await.unwrap(), make_batch(&[33]));
        t_lease.ack().await.unwrap();
    }

    #[tokio::test]
    async fn test_disk_buffer_rejects_second_instance_via_advisory_lock() {
        let tmp = TempDiskDir::new("advisory_lock");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let (_tx1, _buf1) = SinkBuffer::new("first", config.clone()).unwrap();
        let second = SinkBuffer::new("second", config);
        assert!(matches!(second, Err(PipelineError::Storage(_))));
    }

    #[tokio::test]
    async fn test_segment_deletion_when_all_frames_acked() {
        let tmp = TempDiskDir::new("seg_del");
        // Small segment_size_bytes so writing 2 frames rolls to segment 2
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            segment_size_bytes: 600,
            max_batch_size_bytes: 300,
            max_buffer_bytes: 100_000,
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let (tx, mut buf) = SinkBuffer::new("test_seg_del", config).unwrap();
        let batch = make_batch(&[1, 2, 3, 4, 5, 6, 7, 8]);

        // Frame 1 goes to segment 1; Frame 2 rolls to segment 2
        tx.send(SignalBatch::Logs(batch.clone())).await.unwrap();
        tx.send(SignalBatch::Logs(batch.clone())).await.unwrap();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();

        let seg1 = tmp.path.join("segment-00000000000000000001.bin");
        let seg2 = tmp.path.join("segment-00000000000000000002.bin");
        assert!(seg1.exists(), "segment 1 must exist before ack");
        assert!(seg2.exists(), "segment 2 must exist after roll");

        // Ack frame 1 (and frame 2 if segment 1 held 2 frames) until segment 1 is unlinked
        let lease1 = buf.next_lease().await.unwrap().unwrap();
        lease1.ack().await.unwrap();
        if seg1.exists() {
            let lease2 = buf.next_lease().await.unwrap().unwrap();
            lease2.ack().await.unwrap();
        }

        assert!(
            !seg1.exists(),
            "segment-00000000000000000001.bin must be unlinked once all its frames are acked"
        );
    }

    #[tokio::test]
    async fn test_disk_buffer_deletes_fully_acked_segment() {
        let tmp = TempDiskDir::new("seg_del_alias");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            segment_size_bytes: 600,
            max_batch_size_bytes: 300,
            max_buffer_bytes: 100_000,
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let (tx, mut buf) = SinkBuffer::new("test_seg_del_alias", config).unwrap();
        let batch = make_batch(&[10, 20, 30, 40, 50, 60, 70, 80]);

        tx.send(SignalBatch::Logs(batch.clone())).await.unwrap();
        tx.send(SignalBatch::Logs(batch)).await.unwrap();

        let seg1 = tmp.path.join("segment-00000000000000000001.bin");
        let seg2 = tmp.path.join("segment-00000000000000000002.bin");
        assert!(seg1.exists());
        assert!(seg2.exists());

        let lease1 = buf.next_lease().await.unwrap().unwrap();
        lease1.ack().await.unwrap();

        assert!(!seg1.exists());
        assert!(seg2.exists());
    }

    #[tokio::test]
    async fn test_disk_buffer_persists_instance_id_across_crash_recovery_even_before_first_ack() {
        let tmp = TempDiskDir::new("instance_id_recovery");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let first_instance_id = {
            let (tx, mut buf) = SinkBuffer::new("test_inst_id", config.clone()).unwrap();
            let inst_id = buf.instance_id();
            assert_ne!(inst_id, 0);
            tx.send(SignalBatch::Logs(make_batch(&[42, 43])))
                .await
                .unwrap();

            let lease = buf.next_lease().await.unwrap().unwrap();
            assert_eq!(lease.id().instance_id, inst_id);
            assert_eq!(lease.id().first_seq, 1);
            // Drop lease and buffer without calling ack() -> simulates process crash during in-flight commit
            inst_id
        };

        // Reopen on the same disk_path: must recover the exact same instance_id and sequence number
        {
            let (tx, mut reopened) = SinkBuffer::new("test_inst_id", config).unwrap();
            drop(tx);
            assert_eq!(reopened.instance_id(), first_instance_id);

            let replayed_lease = reopened.next_lease().await.unwrap().unwrap();
            assert_eq!(replayed_lease.id().instance_id, first_instance_id);
            assert_eq!(replayed_lease.id().first_seq, 1);
            assert_eq!(replayed_lease.id().last_seq, 1);
            replayed_lease.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_disk_watermark_merges_out_of_order_signal_snapshots_monotonically() {
        let tmp = TempDiskDir::new("monotonic_watermark_merge");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let (tx, buf) = SinkBuffer::new("test_mono_wm", config).unwrap();
        let inst_id = buf.instance_id();
        let disk = buf.inner.disk.as_ref().unwrap();

        // Snapshot 1 arrives first with metrics=10, traces=3, logs=5
        disk.update_watermark(DiskWatermark {
            instance_id: inst_id,
            logs: 5,
            metrics: 10,
            traces: 3,
        })
        .await
        .unwrap();

        // Snapshot 2 was captured earlier for metrics/traces (metrics=8, traces=2) while logs advanced to 6,
        // and arrives at the writer thread after Snapshot 1.
        disk.update_watermark(DiskWatermark {
            instance_id: inst_id,
            logs: 6,
            metrics: 8,
            traces: 2,
        })
        .await
        .unwrap();

        let persisted_bytes = std::fs::read(tmp.path.join("watermark.json")).unwrap();
        let persisted: DiskWatermark = serde_json::from_slice(&persisted_bytes).unwrap();
        assert_eq!(persisted.instance_id, inst_id);
        assert_eq!(persisted.logs, 6);
        assert_eq!(
            persisted.metrics, 10,
            "Out-of-order watermark snapshot must not regress metrics watermark from 10 to 8"
        );
        assert_eq!(
            persisted.traces, 3,
            "Out-of-order watermark snapshot must not regress traces watermark from 3 to 2"
        );
        drop(tx);
    }

    #[tokio::test]
    async fn test_disk_buffer_splits_oversized_multirow_batch_and_recovers_cleanly() {
        let tmp = TempDiskDir::new("oversized_multirow_split");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            segment_size_bytes: 4_096,
            max_batch_size_bytes: 2_048,
            max_buffer_bytes: 65_536,
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        // 2,048 i64 values = 16,384 bytes (> 2 * segment_size_bytes = 8,192 bytes)
        let values: Vec<i64> = (0..2_048).collect();
        let oversized_batch = make_batch(&values);
        assert!(oversized_batch.get_array_memory_size() > (config.segment_size_bytes as usize) * 2);

        {
            let (tx, _buf) = SinkBuffer::new("test_oversized_split", config.clone()).unwrap();
            tx.send(SignalBatch::Logs(oversized_batch)).await.unwrap();
        }

        // Reopen from disk to verify startup recovery AND Lease::prepare both succeed on all split frames
        {
            let (tx, mut reopened) = SinkBuffer::new("test_oversized_split", config).unwrap();
            drop(tx);
            assert_eq!(
                reopened.corruption_drops(),
                0,
                "Recovery must not drop any frame from a split oversized batch"
            );

            let mut recovered_values = Vec::new();
            while let Some(lease) = reopened.next_lease().await.unwrap() {
                let batch = lease.prepare(Ok).await.unwrap();
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..col.len() {
                    recovered_values.push(col.value(i));
                }
                lease.ack().await.unwrap();
            }
            assert_eq!(recovered_values, values);
        }
    }

    #[tokio::test]
    async fn test_disk_buffer_rejects_unsplittable_single_row_batch_exceeding_max_payload() {
        use arrow::array::StringArray;

        let tmp = TempDiskDir::new("oversized_single_row_reject");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            segment_size_bytes: 1_024,
            max_batch_size_bytes: 512,
            max_buffer_bytes: 16_384,
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };

        let (tx, mut buf) = SinkBuffer::new("test_single_row_reject", config).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let huge_str = "x".repeat(4_096);
        let single_row_oversized = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![huge_str.as_str()]))],
        )
        .unwrap();

        let err = tx
            .send(SignalBatch::Logs(single_row_oversized))
            .await
            .unwrap_err();
        assert!(
            matches!(err, PipelineError::Storage(_)),
            "Expected Storage error for unsplittable single-row batch > 2 * segment_size_bytes, got: {err:?}"
        );

        // Buffer must remain usable for subsequent valid batches
        tx.send(SignalBatch::Logs(make_batch(&[7, 8, 9])))
            .await
            .unwrap();
        drop(tx);

        let lease = buf.next_lease().await.unwrap().unwrap();
        let batch = lease.prepare(Ok).await.unwrap();
        assert_eq!(batch, make_batch(&[7, 8, 9]));
        lease.ack().await.unwrap();
    }

    #[test]
    fn test_patch_frame_seq_id_updates_header_and_crc_cleanly() {
        let batch = make_batch(&[10, 20, 30]);
        let mut buf = Vec::new();
        let mut hdr = encode_frame(SignalKind::Logs, 0, &batch, &mut buf).unwrap();
        assert_eq!(hdr.seq_id, 0);

        patch_frame_seq_id(&mut buf, &mut hdr, 42).unwrap();
        assert_eq!(hdr.seq_id, 42);

        let mut cursor = Cursor::new(&buf);
        let (decoded_hdr, decoded_batch) = decode_frame(&mut cursor, u32::MAX).unwrap().unwrap();
        assert_eq!(decoded_hdr.seq_id, 42);
        assert_eq!(decoded_hdr, hdr);
        assert_eq!(decoded_batch, batch);

        let mut short_buf = [0u8; 10];
        assert!(patch_frame_seq_id(&mut short_buf, &mut hdr, 99).is_err());
    }

    #[test]
    fn test_split_and_encode_frames_for_disk_produces_valid_pre_encoded_frames() {
        let values: Vec<i64> = (0..256).collect();
        let batch = make_batch(&values);
        let mut full_buf = Vec::new();
        let full_hdr = encode_frame(SignalKind::Metrics, 0, &batch, &mut full_buf).unwrap();
        let max_payload_len = full_hdr.payload_len / 2;

        let frames =
            split_and_encode_frames_for_disk(SignalKind::Metrics, batch, max_payload_len).unwrap();
        assert!(frames.len() >= 2);

        let mut recovered = Vec::new();
        for (idx, mut frame) in frames.into_iter().enumerate() {
            assert!(frame.header.payload_len <= max_payload_len);
            assert_eq!(
                frame.bytes.len(),
                FRAME_HEADER_LEN + frame.header.payload_len as usize
            );
            let seq_id = (idx as u64) + 100;
            patch_frame_seq_id(&mut frame.bytes, &mut frame.header, seq_id).unwrap();

            let mut cursor = Cursor::new(&frame.bytes);
            let (decoded_hdr, decoded_batch) =
                decode_frame(&mut cursor, max_payload_len).unwrap().unwrap();
            assert_eq!(decoded_hdr.seq_id, seq_id);
            assert_eq!(decoded_hdr, frame.header);
            assert_eq!(decoded_batch, frame.batch);

            let col = decoded_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                recovered.push(col.value(i));
            }
        }
        assert_eq!(recovered, values);
    }

    #[tokio::test]
    async fn test_disk_watermark_merge_monotonic_returns_false_when_unchanged() {
        let mut wm = DiskWatermark {
            instance_id: 77,
            logs: 10,
            metrics: 20,
            traces: 30,
        };
        assert!(!wm.merge_monotonic(DiskWatermark {
            instance_id: 77,
            logs: 10,
            metrics: 15,
            traces: 25,
        }));
        assert!(!wm.merge_monotonic(wm));
        assert!(wm.merge_monotonic(DiskWatermark {
            instance_id: 77,
            logs: 11,
            metrics: 20,
            traces: 30,
        }));
        assert_eq!(wm.logs, 11);

        let tmp = TempDiskDir::new("wm_merge_unchanged");
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, buf) = SinkBuffer::new("test_wm_unchanged", config).unwrap();
        let inst_id = buf.instance_id();
        let disk = buf.inner.disk.as_ref().unwrap();

        let wm1 = DiskWatermark {
            instance_id: inst_id,
            logs: 5,
            metrics: 4,
            traces: 3,
        };
        disk.update_watermark(wm1).await.unwrap();

        // Second update with identical or older values must still resolve Ok(()) promptly
        disk.update_watermark(wm1).await.unwrap();
        disk.update_watermark(DiskWatermark {
            instance_id: inst_id,
            logs: 2,
            metrics: 4,
            traces: 1,
        })
        .await
        .unwrap();
        drop(tx);
    }

    #[test]
    fn test_decode_frame_rejects_invalid_magic_version_signal_and_oversized_header() {
        let batch = make_batch(&[1, 2, 3]);
        let mut valid_buf = Vec::new();
        let hdr = encode_frame(SignalKind::Logs, 1, &batch, &mut valid_buf).unwrap();

        // 1. Partial 10-byte header EOF -> TornFrame
        let mut torn_hdr_cursor = Cursor::new(&valid_buf[..10]);
        assert!(matches!(
            decode_frame(&mut torn_hdr_cursor, u32::MAX),
            Err(FrameDecodeError::TornFrame)
        ));

        // 2. Bad magic -> InvalidHeader
        let mut bad_magic = valid_buf.clone();
        bad_magic[0..4].copy_from_slice(b"XXXX");
        let mut cursor = Cursor::new(&bad_magic);
        assert!(matches!(
            decode_frame(&mut cursor, u32::MAX),
            Err(FrameDecodeError::InvalidHeader { ref reason }) if reason.contains("magic")
        ));

        // 3. Bad version -> InvalidHeader
        let mut bad_ver = valid_buf.clone();
        bad_ver[4] = 99;
        let mut cursor = Cursor::new(&bad_ver);
        assert!(matches!(
            decode_frame(&mut cursor, u32::MAX),
            Err(FrameDecodeError::InvalidHeader { ref reason }) if reason.contains("version")
        ));

        // 4. Invalid signal byte -> InvalidHeader
        let mut bad_sig = valid_buf.clone();
        bad_sig[5] = 99;
        let mut cursor = Cursor::new(&bad_sig);
        assert!(matches!(
            decode_frame(&mut cursor, u32::MAX),
            Err(FrameDecodeError::InvalidHeader { ref reason }) if reason.contains("signal")
        ));

        // 5. payload_len > max_payload_len -> InvalidHeader
        let mut cursor = Cursor::new(&valid_buf);
        assert!(matches!(
            decode_frame(&mut cursor, hdr.payload_len.saturating_sub(1)),
            Err(FrameDecodeError::InvalidHeader { ref reason }) if reason.contains("payload_len")
        ));

        // 6. Valid CRC but invalid IPC payload -> IpcDecode
        let mut bad_ipc = valid_buf.clone();
        for b in &mut bad_ipc[FRAME_HEADER_LEN..] {
            *b = 0xFF;
        }
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bad_ipc[4..24]);
        hasher.update(&bad_ipc[FRAME_HEADER_LEN..]);
        let crc = hasher.finalize();
        bad_ipc[24..28].copy_from_slice(&crc.to_le_bytes());
        let mut cursor = Cursor::new(&bad_ipc);
        assert!(matches!(
            decode_frame(&mut cursor, u32::MAX),
            Err(FrameDecodeError::IpcDecode { .. })
        ));

        // 7. Segment recovery encountering InvalidHeader stops segment scan and increments corruption_drops
        let tmp = TempDiskDir::new("invalid_hdr_recovery");
        let seg_path = tmp.path.join("segment-00000000000000000001.bin");
        std::fs::write(&seg_path, &bad_magic).unwrap();
        let config = BufferConfig {
            mode: BufferMode::Disk,
            disk_path: Some(tmp.path.clone()),
            sync_on_write: true,
            max_batch_records: Some(1),
            max_batch_interval_sec: 0,
            ..BufferConfig::default()
        };
        let (tx, buf) = SinkBuffer::new("test_inv_hdr", config).unwrap();
        assert_eq!(buf.corruption_drops(), 1);
        drop(tx);
    }
}
