//! Local benchmark runner with disclaimer notice.

use std::time::Instant;

use anyhow::Result;
use wasmtime::{Config, Engine, Module, Store};

use crate::tester::{build_canonical_test_batch, serialize_batch_to_ipc, verify_transform_status};
use crate::validator::validate_wasm_bytes;

fn wasm_err<E: std::fmt::Display>(err: E) -> anyhow::Error {
    anyhow::anyhow!("{err}")
}

/// Executes local latency/throughput benchmarks for a guest WASM module.
///
/// # Errors
///
/// Returns an error if benchmarking fails.
pub fn run_benchmark_with_disclaimer(bytes: &[u8]) -> Result<()> {
    println!(
        "WARNING: This bench measures raw IPC round-trip for profiling only.\n         The CI latency gate is: cargo test --test latency_gate_tests"
    );

    validate_wasm_bytes(bytes)?;

    let batch = build_canonical_test_batch()?;
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

    if let Ok(init_fn) = instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_init") {
        let init_res = init_fn.call(&mut store, (0, 0)).map_err(wasm_err)?;
        if init_res != 0 {
            anyhow::bail!("datalake_init returned non-zero code: {init_res}");
        }
    }

    let alloc_fn = instance
        .get_typed_func::<u32, u32>(&mut store, "datalake_alloc")
        .map_err(wasm_err)?;
    let dealloc_fn = instance
        .get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")
        .map_err(wasm_err)?;
    let transform_fn = instance
        .get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")
        .map_err(wasm_err)?;

    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow::anyhow!("Module missing memory export"))?;

    let input_len = u32::try_from(buffer.len())?;
    let input_ptr = alloc_fn.call(&mut store, input_len).map_err(wasm_err)?;

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
        let header_ptr = transform_fn
            .call(&mut store, (input_ptr, input_len))
            .map_err(wasm_err)?;
        verify_transform_status(&memory, &store, header_ptr)?;
    }

    let iterations: u32 = 50;
    let start_time = Instant::now();
    for _ in 0..iterations {
        let header_ptr = transform_fn
            .call(&mut store, (input_ptr, input_len))
            .map_err(wasm_err)?;
        verify_transform_status(&memory, &store, header_ptr)?;
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
