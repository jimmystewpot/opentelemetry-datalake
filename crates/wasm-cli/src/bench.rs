//! Local benchmark runner with disclaimer notice.

use anyhow::{Context, Result};
use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc};

/// Benchmark results for a WASM guest module.
#[derive(Debug, Clone, PartialEq)]
pub struct BenchResult {
    /// Number of transform iterations executed.
    pub iterations: usize,
    /// Arrow IPC payload byte size per iteration.
    pub payload_bytes: usize,
    /// Total bytes processed across all iterations.
    pub total_bytes: usize,
    /// Total duration elapsed during the benchmark loop.
    pub elapsed: std::time::Duration,
    /// Average latency per transform invocation.
    pub avg_latency: std::time::Duration,
    /// Estimated processing throughput in megabytes per second.
    pub throughput_mb_per_sec: f64,
}

enum BenchTransform {
    V1(TypedFunc<(u32, u32, u32), u64>),
    V0I64(TypedFunc<(u32, u32), u64>),
    V0I32(TypedFunc<(u32, u32), u32>),
    DescI64(TypedFunc<u32, u64>),
    DescI32(TypedFunc<u32, u32>),
}

/// Executes local latency/throughput benchmarks for a guest WASM module.
///
/// Isolates wasmtime module compilation and host memory allocation overhead
/// from the hot execution loop to measure raw guest transformation performance.
///
/// # Errors
///
/// Returns an error if module validation, compilation, or execution traps,
/// or if `datalake_transform` returns a non-zero status code.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub fn run_benchmark(bytes: &[u8], iterations: usize) -> Result<BenchResult> {
    crate::validator::validate_wasm_bytes(bytes)
        .map_err(|e| anyhow::anyhow!("Module validation failed: {e}"))?;

    let config = Config::new();
    let engine = Engine::new(&config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize wasmtime engine: {e}"))?;
    let module = Module::new(&engine, bytes)
        .map_err(|e| anyhow::anyhow!("Failed to compile WebAssembly module: {e}"))?;

    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| anyhow::anyhow!("Failed to instantiate WebAssembly module: {e}"))?;

    let alloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
        .map_err(|e| anyhow::anyhow!("Missing 'datalake_alloc' export: {e}"))?;
    let dealloc_fn = instance
        .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
        .map_err(|e| anyhow::anyhow!("Missing 'datalake_dealloc' export: {e}"))?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("Missing 'memory' export"))?;

    if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), i32>(&mut store, "datalake_init") {
        let _ = init_fn.call(&mut store, (0, 0));
    }

    let input_batch = crate::tester::create_synthetic_batch()?;
    let ipc_bytes = crate::tester::serialize_batch_to_ipc(&input_batch)?;
    let ipc_len =
        u32::try_from(ipc_bytes.len()).context("Serialized IPC payload size exceeds u32 limit")?;

    let ipc_ptr = alloc_fn
        .call(&mut store, ipc_len)
        .map_err(|e| anyhow::anyhow!("datalake_alloc failed for IPC buffer: {e}"))?;
    if ipc_ptr == 0 {
        anyhow::bail!("datalake_alloc returned null pointer for IPC buffer");
    }

    memory
        .write(&mut store, ipc_ptr as usize, &ipc_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to write IPC payload into guest memory: {e}"))?;

    let desc_ptr = alloc_fn.call(&mut store, 8).unwrap_or(0);
    if desc_ptr != 0 {
        let mut desc_bytes = [0u8; 8];
        desc_bytes[0..4].copy_from_slice(&ipc_ptr.to_le_bytes());
        desc_bytes[4..8].copy_from_slice(&ipc_len.to_le_bytes());
        let _ = memory.write(&mut store, desc_ptr as usize, &desc_bytes);
    }

    let transform = if let Ok(f) =
        instance.get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
    {
        BenchTransform::V1(f)
    } else if let Ok(f) =
        instance.get_typed_func::<(u32, u32), u64>(&mut store, "datalake_transform")
    {
        BenchTransform::V0I64(f)
    } else if let Ok(f) =
        instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")
    {
        BenchTransform::V0I32(f)
    } else if let Ok(f) = instance.get_typed_func::<u32, u64>(&mut store, "datalake_transform") {
        BenchTransform::DescI64(f)
    } else if let Ok(f) = instance.get_typed_func::<u32, u32>(&mut store, "datalake_transform") {
        BenchTransform::DescI32(f)
    } else {
        anyhow::bail!("Unsupported datalake_transform signature");
    };

    let start_time = std::time::Instant::now();
    for _ in 0..iterations {
        let header_ptr = match &transform {
            BenchTransform::V1(f) => {
                let packed = f.call(&mut store, (0, ipc_ptr, ipc_len)).map_err(|e| {
                    anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
                })?;
                u32::try_from(packed >> 32).unwrap_or(0)
            }
            BenchTransform::V0I64(f) => {
                let packed = f.call(&mut store, (ipc_ptr, ipc_len)).map_err(|e| {
                    anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
                })?;
                let ptr = u32::try_from(packed >> 32).unwrap_or(0);
                if ptr != 0 {
                    ptr
                } else {
                    u32::try_from(packed).unwrap_or(0)
                }
            }
            BenchTransform::V0I32(f) => f.call(&mut store, (ipc_ptr, ipc_len)).map_err(|e| {
                anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
            })?,
            BenchTransform::DescI64(f) => {
                let packed = f.call(&mut store, desc_ptr).map_err(|e| {
                    anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
                })?;
                let ptr = u32::try_from(packed >> 32).unwrap_or(0);
                if ptr != 0 {
                    ptr
                } else {
                    u32::try_from(packed).unwrap_or(0)
                }
            }
            BenchTransform::DescI32(f) => f.call(&mut store, desc_ptr).map_err(|e| {
                anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
            })?,
        };

        let mut status_bytes = [0u8; 4];
        memory
            .read(&store, header_ptr as usize, &mut status_bytes)
            .map_err(|e| {
                anyhow::anyhow!("Failed to read response status from guest memory: {e}")
            })?;
        let status = u32::from_le_bytes(status_bytes);
        if status != 0 {
            anyhow::bail!("datalake_transform returned non-zero status {status} during benchmark");
        }
    }
    let elapsed = start_time.elapsed();

    let _ = dealloc_fn.call(&mut store, (ipc_ptr, ipc_len));
    if desc_ptr != 0 {
        let _ = dealloc_fn.call(&mut store, (desc_ptr, 8));
    }

    let total_bytes = ipc_bytes.len().saturating_mul(iterations);
    let elapsed_secs = elapsed.as_secs_f64();
    let throughput_mb_per_sec = if elapsed_secs > 0.0 {
        (total_bytes as f64 / 1_048_576.0) / elapsed_secs
    } else {
        0.0
    };
    let avg_latency = if iterations > 0 {
        let iters_u32 = u32::try_from(iterations).unwrap_or(u32::MAX);
        elapsed / iters_u32
    } else {
        std::time::Duration::ZERO
    };

    Ok(BenchResult {
        iterations,
        payload_bytes: ipc_bytes.len(),
        total_bytes,
        elapsed,
        avg_latency,
        throughput_mb_per_sec,
    })
}

#[allow(clippy::cast_precision_loss)]
fn print_benchmark_table(res: &BenchResult) {
    let total_mb = (res.total_bytes as f64) / 1_048_576.0;
    println!("+--------------------------------+--------------------+");
    println!("| Metric                         | Value              |");
    println!("+--------------------------------+--------------------+");
    println!(
        "| Iterations                     | {:<18} |",
        res.iterations
    );
    println!(
        "| Payload Size                   | {:<18} |",
        format!("{} bytes", res.payload_bytes)
    );
    println!(
        "| Total Data Processed           | {:<18} |",
        format!("{total_mb:.2} MB")
    );
    println!(
        "| Total Elapsed Time             | {:<18} |",
        format!("{:.2?}", res.elapsed)
    );
    println!(
        "| Average Latency                | {:<18} |",
        format!("{:.2?}", res.avg_latency)
    );
    println!(
        "| Throughput                     | {:<18} |",
        format!("{:.2} MB/s", res.throughput_mb_per_sec)
    );
    println!("+--------------------------------+--------------------+");
}

/// Executes local latency/throughput benchmarks for a guest WASM module and prints formatted metrics.
///
/// # Errors
///
/// Returns an error if module validation, compilation, or execution traps,
/// or if `datalake_transform` returns a non-zero status code.
pub fn run_benchmark_with_disclaimer(bytes: &[u8]) -> Result<()> {
    println!(
        "WARNING: This bench measures raw IPC round-trip for profiling only.\n         The CI latency gate is: cargo test --test latency_gate_tests"
    );
    let result = run_benchmark(bytes, 10_000)?;
    print_benchmark_table(&result);
    Ok(())
}
