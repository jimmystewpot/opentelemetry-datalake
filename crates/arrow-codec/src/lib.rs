pub mod common;
pub mod compliance;
pub mod logs;
pub mod metrics;
pub mod traces;

// Explicitly re-expose the compliance and decoding API surface for downstream pipeline consumers,
// shielding them from the internal module structure.
pub use compliance::{ComplianceEngine, ComplianceMode, ComplianceOutput};
pub use logs::decode_logs;
pub use metrics::decode_metrics;
pub use traces::decode_traces;
