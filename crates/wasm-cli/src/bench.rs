//! Local benchmark runner with disclaimer notice.

use anyhow::{Context, Result};
use wasmtime::{Config, Engine, Module, Store};

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

    let parsed_config: serde_json::Value =
        serde_json::from_str(config_json).context("Invalid JSON provided for --config")?;

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize wasmtime engine: {e}"))?;
    let module = Module::new(&engine, bytes)
        .map_err(|e| anyhow::anyhow!("Failed to compile WebAssembly module: {e}"))?;

    let mut store = Store::new(&engine, crate::default_wasi_ctx());
    // Allocate 10,000,000 fuel units per iteration (minimum 100,000,000) to bound infinite loops
    let total_fuel = u64::try_from(iterations.max(1))
        .unwrap_or(u64::MAX)
        .saturating_mul(10_000_000)
        .max(100_000_000);
    store
        .set_fuel(total_fuel)
        .map_err(|e| anyhow::anyhow!("Failed to configure execution fuel: {e}"))?;

    let linker = crate::build_cli_linker(&engine)
        .map_err(|e| anyhow::anyhow!("Failed to configure host linker: {e}"))?;
    let instance = linker
        .instantiate(&mut store, &module)
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

    let transform_fn = instance
        .get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        .map_err(|e| {
            anyhow::anyhow!("Module must implement ABI v1 datalake_transform signature: {e}")
        })?;

    let signal_type = crate::parse_signal_type(signal);

    let start_time = std::time::Instant::now();
    for _ in 0..iterations {
        let packed = transform_fn
            .call(&mut store, (signal_type, ipc_ptr, ipc_len))
            .map_err(|e| anyhow::anyhow!("datalake_transform execution trapped or failed: {e}"))?;
        let header_ptr = u32::try_from(packed >> 32).unwrap_or(0);
        let header_len = u32::try_from(packed & 0xFFFF_FFFF).unwrap_or(0);
        if header_ptr == 0 {
            anyhow::bail!("datalake_transform returned a null pointer for response header");
        }
        if header_len < 20 {
            anyhow::bail!("datalake_transform returned header length < 20: {header_len}");
        }
        if (header_ptr as usize).saturating_add(header_len as usize) > memory.data_size(&store) {
            anyhow::bail!(
                "datalake_transform response header out of bounds (ptr={header_ptr}, len={header_len})"
            );
        }

        let mut header_buf = [0u8; 20];
        memory
            .read(&store, header_ptr as usize, &mut header_buf)
            .map_err(|e| {
                anyhow::anyhow!("Failed to read response header from guest memory: {e}")
            })?;

        let status =
            u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        if status != 0 {
            anyhow::bail!("datalake_transform returned non-zero status {status} during benchmark");
        }

        let batch_count =
            u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
        let batches_ptr =
            u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);
        let msg_ptr = u32::from_le_bytes([
            header_buf[12],
            header_buf[13],
            header_buf[14],
            header_buf[15],
        ]);
        let msg_len = u32::from_le_bytes([
            header_buf[16],
            header_buf[17],
            header_buf[18],
            header_buf[19],
        ]);

        let mem_size = memory.data_size(&store);
        if msg_ptr != 0
            && msg_len > 0
            && (msg_ptr as usize).saturating_add(msg_len as usize) <= mem_size
        {
            let _ = dealloc_fn.call(&mut store, (msg_ptr, msg_len));
        }

        let desc_table_len = (batch_count as usize).saturating_mul(8);
        if batch_count > 0
            && batches_ptr != 0
            && (batches_ptr as usize).saturating_add(desc_table_len) <= mem_size
        {
            let mut desc_buf = vec![0u8; desc_table_len];
            if memory
                .read(&store, batches_ptr as usize, &mut desc_buf)
                .is_ok()
            {
                for chunk in desc_buf.as_chunks::<8>().0 {
                    let b_ptr = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    let b_len = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
                    if b_ptr != 0 && b_len > 0 && b_ptr != ipc_ptr {
                        let _ = dealloc_fn.call(&mut store, (b_ptr, b_len));
                    }
                }
            }
            let _ = dealloc_fn.call(&mut store, (batches_ptr, batch_count.saturating_mul(8)));
        }
        let _ = dealloc_fn.call(&mut store, (header_ptr, header_len));
    }
    let elapsed = start_time.elapsed();

    let _ = dealloc_fn.call(&mut store, (ipc_ptr, ipc_len));

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_benchmark_supports_wasi_and_env_host_imports_without_double_free() {
        let wat_src = r#"
            (module
                (import "wasi_snapshot_preview1" "sched_yield" (func $sched_yield (result i32)))
                (import "env" "datalake_host_metric_emit" (func $metric_emit (param i32 i32 i32 i64)))
                (import "env" "datalake_host_log" (func $host_log (param i32 i32 i32)))
                (import "env" "datalake_host_has_capability" (func $has_cap (param i32 i32) (result i32)))
                (import "env" "datalake_host_now_nanos" (func $now_nanos (result i64)))
                (memory (export "memory") 2)
                (global $bump (mut i32) (i32.const 4096))
                (global $ipc_ptr_seen (mut i32) (i32.const 0))
                (global $ipc_free_count (mut i32) (i32.const 0))
                (func (export "datalake_abi_version") (result i32)
                    i32.const 1
                )
                (func (export "datalake_alloc") (param $size i32) (result i32)
                    (local $ptr i32)
                    global.get $bump
                    local.set $ptr
                    global.get $bump
                    local.get $size
                    i32.add
                    global.set $bump
                    local.get $ptr
                )
                (func (export "datalake_dealloc") (param $ptr i32) (param $size i32)
                    (if (i32.and
                            (i32.ne (global.get $ipc_ptr_seen) (i32.const 0))
                            (i32.eq (local.get $ptr) (global.get $ipc_ptr_seen)))
                        (then
                            (global.set $ipc_free_count
                                (i32.add (global.get $ipc_free_count) (i32.const 1)))
                            (if (i32.gt_u (global.get $ipc_free_count) (i32.const 1))
                                (then (unreachable))
                            )
                        )
                    )
                )
                (func (export "datalake_init") (param $ptr i32) (param $len i32) (result i32)
                    (drop (call $sched_yield))
                    (call $metric_emit (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 1))
                    (call $host_log (i32.const 0) (i32.const 0) (i32.const 0))
                    (drop (call $has_cap (i32.const 0) (i32.const 0)))
                    (drop (call $now_nanos))
                    i32.const 0
                )
                (func (export "datalake_transform") (param $signal i32) (param $ptr i32) (param $len i32) (result i64)
                    ;; Verify signal == 2 ("traces")
                    (if (i32.ne (local.get $signal) (i32.const 2))
                        (then (unreachable))
                    )
                    (drop (call $sched_yield))
                    (call $metric_emit (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 1))
                    (call $host_log (i32.const 0) (i32.const 0) (i32.const 0))
                    (drop (call $has_cap (i32.const 0) (i32.const 0)))
                    (drop (call $now_nanos))
                    ;; Record $ptr so datalake_dealloc can detect double-free across iterations
                    (global.set $ipc_ptr_seen (local.get $ptr))
                    ;; Write batch descriptor at offset 128 echoing ($ptr, $len)
                    (i32.store (i32.const 128) (local.get $ptr))
                    (i32.store (i32.const 132) (local.get $len))
                    ;; Write 20-byte response header at offset 64:
                    ;; status = 0, batch_count = 1, batches_ptr = 128, msg_ptr = 0, msg_len = 0
                    (i32.store (i32.const 64) (i32.const 0))
                    (i32.store (i32.const 68) (i32.const 1))
                    (i32.store (i32.const 72) (i32.const 128))
                    (i32.store (i32.const 76) (i32.const 0))
                    (i32.store (i32.const 80) (i32.const 0))
                    ;; Return (64 << 32) | 20
                    (i64.or
                        (i64.shl (i64.const 64) (i64.const 32))
                        (i64.const 20))
                )
            )
        "#;
        let wasm = wat::parse_str(wat_src).unwrap();
        assert!(run_benchmark(&wasm, 5, "traces", "{}").is_ok());
    }

    #[test]
    fn test_run_benchmark_rejects_invalid_config_json() {
        let wat_src = r#"
            (module
                (memory (export "memory") 2)
                (func (export "datalake_abi_version") (result i32) i32.const 1)
                (func (export "datalake_alloc") (param i32) (result i32) i32.const 4096)
                (func (export "datalake_dealloc") (param i32 i32))
                (func (export "datalake_transform") (param i32 i32 i32) (result i64) i64.const 0)
            )
        "#;
        let wasm = wat::parse_str(wat_src).unwrap();
        let res = run_benchmark(&wasm, 5, "traces", "{invalid-json}");
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Invalid JSON provided for --config"));
    }
}
