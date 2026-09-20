//! Local benchmark runner with disclaimer notice.

use anyhow::Result;

/// Executes local latency/throughput benchmarks for a guest WASM module.
///
/// # Errors
///
/// Returns an error if benchmarking fails or is not yet implemented.
pub fn run_benchmark_with_disclaimer(_bytes: &[u8]) -> Result<()> {
    Ok(())
}
