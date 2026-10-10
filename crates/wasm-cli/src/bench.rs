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
    /// Guest linear memory size allocated in bytes.
    pub allocated_memory_bytes: usize,
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
pub fn run_benchmark(
    bytes: &[u8],
    iterations: usize,
    signal: &str,
    config_json: &str,
) -> Result<BenchResult> {
    crate::validator::validate_wasm_bytes(bytes)
        .map_err(|e| anyhow::anyhow!("Module validation failed: {e}"))?;

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize wasmtime engine: {e}"))?;
    let module = Module::new(&engine, bytes)
        .map_err(|e| anyhow::anyhow!("Failed to compile WebAssembly module: {e}"))?;

    let mut store = Store::new(&engine, ());
    // Allocate 10,000,000 fuel units per iteration (minimum 100,000,000) to bound infinite loops
    let total_fuel = u64::try_from(iterations.max(1))
        .unwrap_or(u64::MAX)
        .saturating_mul(10_000_000)
        .max(100_000_000);
    store
        .set_fuel(total_fuel)
        .map_err(|e| anyhow::anyhow!("Failed to configure execution fuel: {e}"))?;

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
        let parsed_config = serde_json::from_str::<serde_json::Value>(config_json)
            .unwrap_or_else(|_| serde_json::json!({}));
        let init_payload = serde_json::json!({
            "signal": signal,
            "env": {},
            "config": parsed_config,
        });
        let init_bytes =
            serde_json::to_vec(&init_payload).context("Failed to serialize init payload")?;
        let init_len =
            u32::try_from(init_bytes.len()).context("Init payload size exceeds u32 limit")?;

        let init_ptr = alloc_fn
            .call(&mut store, init_len)
            .map_err(|e| anyhow::anyhow!("datalake_alloc failed for init payload: {e}"))?;

        if init_ptr == 0 {
            anyhow::bail!("datalake_alloc returned null pointer during initialization");
        }

        memory
            .write(&mut store, init_ptr as usize, &init_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to write init payload to guest memory: {e}"))?;

        let status = init_fn
            .call(&mut store, (init_ptr, init_len))
            .map_err(|e| anyhow::anyhow!("datalake_init trapped during initialization: {e}"))?;

        let _ = dealloc_fn.call(&mut store, (init_ptr, init_len));

        if status != 0 {
            anyhow::bail!("datalake_init returned non-zero status code: {status}");
        }
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

    let transform_fn = instance.get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        .map_err(|e| anyhow::anyhow!("Module must implement ABI v1 datalake_transform signature: {e}"))?;

    let signal_bytes = signal.as_bytes();
    let signal_len = u32::try_from(signal_bytes.len()).unwrap_or(0);
    let signal_ptr = alloc_fn.call(&mut store, signal_len).unwrap_or(0);
    if signal_ptr != 0 {
        let _ = memory.write(&mut store, signal_ptr as usize, signal_bytes);
    }

    let start_time = std::time::Instant::now();
    for _ in 0..iterations {
        let header_ptr = {
                let f = transform_fn.clone();

                let packed = f.call(&mut store, (signal_ptr, ipc_ptr, ipc_len)).map_err(|e| {
                    anyhow::anyhow!("datalake_transform execution trapped or failed: {e}")
                })?;
                let ptr = u32::try_from(packed >> 32).unwrap_or(0);
                let len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
                if ptr == 0 {
                    anyhow::bail!("datalake_transform returned a null pointer for response header");
                }
                if len < 20 {
                    anyhow::bail!("datalake_transform returned header length < 20: {len}");
                }
                ptr
            
            };
        let mut header_buf = [0u8; 20];
        memory
            .read(&store, header_ptr as usize, &mut header_buf)
            .map_err(|e| anyhow::anyhow!("Failed to read response header from guest memory: {e}"))?;
            
        let status = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        if status != 0 {
            anyhow::bail!("datalake_transform returned non-zero status {status} during benchmark");
        }
        
        let batch_count = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
        let batches_ptr = u32::from_le_bytes(header_buf[8..12].try_into().unwrap());
        let msg_ptr = u32::from_le_bytes(header_buf[12..16].try_into().unwrap());
        let msg_len = u32::from_le_bytes(header_buf[16..20].try_into().unwrap());

        if msg_ptr != 0 && msg_len > 0 {
            let _ = dealloc_fn.call(&mut store, (msg_ptr, msg_len));
        }

        if batch_count > 0 && batches_ptr != 0 {
            let mut desc_buf = vec![0u8; (batch_count * 8) as usize];
            if memory.read(&store, batches_ptr as usize, &mut desc_buf).is_ok() {
                for i in 0..batch_count as usize {
                    let b_ptr = u32::from_le_bytes(desc_buf[i*8 .. i*8+4].try_into().unwrap());
                    let b_len = u32::from_le_bytes(desc_buf[i*8+4 .. i*8+8].try_into().unwrap());
                    if b_ptr != 0 && b_len > 0 {
                        let _ = dealloc_fn.call(&mut store, (b_ptr, b_len));
                    }
                }
            }
            let _ = dealloc_fn.call(&mut store, (batches_ptr, batch_count * 8));
        }
        let _ = dealloc_fn.call(&mut store, (header_ptr, 20));

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
    let allocated_memory_bytes = memory.data_size(&store);

    let _ = dealloc_fn.call(&mut store, (signal_ptr, signal_len));

    Ok(BenchResult {
        iterations,
        payload_bytes: ipc_bytes.len(),
        total_bytes,
        elapsed,
        avg_latency,
        throughput_mb_per_sec,
        allocated_memory_bytes,
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
    println!(
        "| Guest Memory Allocated         | {:<18} |",
        format!("{} bytes", res.allocated_memory_bytes)
    );
    println!("+--------------------------------+--------------------+");
}

/// Executes local latency/throughput benchmarks for a guest WASM module and prints formatted metrics.
///
/// # Errors
///
/// Returns an error if module validation, compilation, or execution traps,
/// or if `datalake_transform` returns a non-zero status code.
pub fn run_benchmark_with_disclaimer(bytes: &[u8], signal: &str, config_json: &str) -> Result<()> {
    println!(
        "WARNING: This bench measures raw IPC round-trip for profiling only.\n         The CI latency gate is: cargo test --test latency_gate_tests"
    );
    let result = run_benchmark(bytes, 10_000, signal, config_json)?;
    print_benchmark_table(&result);
    Ok(())
}
