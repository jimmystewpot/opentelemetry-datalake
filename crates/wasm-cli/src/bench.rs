//! Local benchmark runner with disclaimer notice.

use std::time::Instant;

use anyhow::Result;
use wasmtime::{Config, Engine, Module, Store};

use crate::tester::{
    build_canonical_test_batch_for_signal, extract_transform_payloads, reclaim_transform_response,
    serialize_batch_to_ipc,
};
use crate::validator::validate_wasm_bytes;

fn wasm_err<E: std::fmt::Display>(err: E) -> anyhow::Error {
    anyhow::anyhow!("{err}")
}

/// Executes local latency/throughput benchmarks for a guest WASM module
/// with an explicit signal type and optional initialization configuration payload.
///
/// # Concurrency Characteristics
///
/// This function creates an isolated Wasmtime [`Engine`] and [`Store`], executing entirely
/// within the calling thread. Multiple threads can call this function concurrently with
/// independent guest modules.
///
/// # Errors
///
/// Returns an error if benchmarking fails.
pub fn run_benchmark_with_options(
    bytes: &[u8],
    signal: u32,
    config_payload: Option<&str>,
) -> Result<()> {
    println!(
        "WARNING: This bench measures raw IPC round-trip for profiling only.\n         The CI latency gate is: cargo test --test latency_gate_tests"
    );

    validate_wasm_bytes(bytes)?;

    let batch = build_canonical_test_batch_for_signal(signal)?;
    let buffer = serialize_batch_to_ipc(&batch)?;
    let num_records = batch.num_rows();

    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config).map_err(wasm_err)?;
    let module = Module::new(&engine, bytes).map_err(wasm_err)?;
    let mut store: Store<()> = Store::new(&engine, ());
    store.set_fuel(10_000_000_000).map_err(wasm_err)?;

    let linker = crate::create_default_linker(&engine, &module).map_err(wasm_err)?;
    let instance = linker.instantiate(&mut store, &module).map_err(wasm_err)?;

    let alloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
        .map_err(wasm_err)?;
    let dealloc_fn = instance
        .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
        .map_err(wasm_err)?;
    let transform_fn = instance
        .get_typed_func::<(u32, u32, u32), u64>(&mut store, "datalake_transform")
        .map_err(wasm_err)?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("Module missing memory export"))?;

    if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_init") {
        let (config_ptr, config_len) = if let Some(cfg) = config_payload {
            let cfg_bytes = cfg.as_bytes();
            let len = u32::try_from(cfg_bytes.len())?;
            if len > 0 {
                let ptr = alloc_fn.call(&mut store, len).map_err(wasm_err)?;
                if ptr == 0 {
                    anyhow::bail!(
                        "datalake_alloc returned null pointer when allocating config buffer of length {len}"
                    );
                }
                let mem_len = memory.data(&store).len();
                let start = ptr as usize;
                let end = start
                    .checked_add(len as usize)
                    .ok_or_else(|| anyhow::anyhow!("Overflow computing config buffer end"))?;
                if end > mem_len {
                    anyhow::bail!("Allocated config buffer out of guest memory bounds");
                }
                memory.data_mut(&mut store)[start..end].copy_from_slice(cfg_bytes);
                (ptr, len)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        };

        let init_res = init_fn
            .call(&mut store, (config_ptr, config_len))
            .map_err(wasm_err)?;

        if config_ptr != 0 && config_len > 0 {
            dealloc_fn
                .call(&mut store, (config_ptr, config_len))
                .map_err(wasm_err)?;
        }

        if init_res != 0 {
            anyhow::bail!("datalake_init returned non-zero code: {init_res}");
        }
    }

    let input_len = u32::try_from(buffer.len())?;
    let input_ptr = alloc_fn.call(&mut store, input_len).map_err(wasm_err)?;
    if input_ptr == 0 && input_len > 0 {
        anyhow::bail!(
            "datalake_alloc returned null pointer when allocating input buffer of length {input_len}"
        );
    }

    let mem_len = memory.data(&store).len();
    let start = input_ptr as usize;
    let end = start
        .checked_add(input_len as usize)
        .ok_or_else(|| anyhow::anyhow!("Overflow computing buffer end"))?;
    if end > mem_len {
        anyhow::bail!("Allocated input buffer out of guest memory bounds");
    }
    memory.data_mut(&mut store)[start..end].copy_from_slice(&buffer);

    for _ in 0..5 {
        let packed = transform_fn
            .call(&mut store, (signal, input_ptr, input_len))
            .map_err(wasm_err)?;
        let header_ptr = (packed >> 32) as u32;
        let header_len = (packed & 0xffff_ffff) as u32;
        extract_transform_payloads(&memory, &store, header_ptr, header_len)?;
        reclaim_transform_response(&memory, &mut store, &dealloc_fn, header_ptr, header_len)?;
    }

    let iterations: u32 = 50;
    let start_time = Instant::now();
    for _ in 0..iterations {
        let packed = transform_fn
            .call(&mut store, (signal, input_ptr, input_len))
            .map_err(wasm_err)?;
        let header_ptr = (packed >> 32) as u32;
        let header_len = (packed & 0xffff_ffff) as u32;
        extract_transform_payloads(&memory, &store, header_ptr, header_len)?;
        reclaim_transform_response(&memory, &mut store, &dealloc_fn, header_ptr, header_len)?;
    }
    let total_elapsed = start_time.elapsed();

    let mean_latency = total_elapsed / iterations;
    #[allow(clippy::cast_precision_loss)]
    let total_records = f64::from(iterations) * (num_records as f64);
    let total_secs = total_elapsed.as_secs_f64();
    let throughput = if total_secs > 0.0 {
        total_records / total_secs
    } else {
        0.0
    };

    println!(
        "Benchmark results (50 iterations):\n  Mean latency: {mean_latency:?}\n  Throughput: {throughput:.0} records/sec"
    );

    dealloc_fn
        .call(&mut store, (input_ptr, input_len))
        .map_err(wasm_err)?;
    Ok(())
}

/// Executes local latency/throughput benchmarks for a guest WASM module
/// using default signal (`Logs` / `0`) and no configuration.
///
/// # Concurrency Characteristics
///
/// This function creates an isolated Wasmtime [`Engine`] and [`Store`], executing entirely
/// within the calling thread. Multiple threads can call this function concurrently with
/// independent guest modules.
///
/// # Errors
///
/// Returns an error if benchmarking fails.
pub fn run_benchmark_with_disclaimer(bytes: &[u8]) -> Result<()> {
    run_benchmark_with_options(bytes, 0, None)
}
