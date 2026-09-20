//! Hard CI latency gate on real WebAssembly boundary execution.
//! Executes 2,000-row batches through Wasmtime 48.

#![allow(clippy::print_stdout)]

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::pipeline::SignalBatch;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wasm_transformer::engine::EngineCache;
use wasm_transformer::worker::WasmWorker;

const WARMUP: usize = 50;
const SAMPLES: usize = 1_000;
const P95_LIMIT: Duration = Duration::from_micros(1_500);
const P99_LIMIT: Duration = Duration::from_micros(3_000);

fn passthrough_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 16)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

fn make_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ids: Vec<String> = (0..rows).map(|i| format!("trace_{i:032x}")).collect();
    let bodies: Vec<String> = (0..rows).map(|i| format!("body_{i}")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                ids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                bodies.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("valid record batch")
}

#[tokio::test]
async fn test_real_wasm_boundary_latency_within_budget() {
    let cache = Arc::new(
        EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("pooling allocator should initialize"),
    );
    let wat_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let module = cache
        .compile_module(&wat_bytes)
        .expect("module compilation should succeed");

    let cfg = WasmTransformerConfig {
        id: "latency_gate".into(),
        r#type: "wasm".into(),
        module_path: "test.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 2,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 100_000,
        init_timeout: "2s".into(),
        on_error: OnErrorPolicy::Reroute,
        allow_unmasked_passthrough: false,
        on_reject: OnRejectPolicy::Reroute,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
    };

    let mut worker = WasmWorker::new(0, cache, module, cfg).expect("worker should initialize");
    let batch = make_batch(2000);

    // Warmup
    for _ in 0..WARMUP {
        let _ = worker
            .execute_batch(SignalBatch::Logs(batch.clone()))
            .await
            .expect("warmup execution should succeed");
    }

    // Measured samples
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t0 = Instant::now();
        let _ = worker
            .execute_batch(SignalBatch::Logs(batch.clone()))
            .await
            .expect("measured execution should succeed");
        samples.push(t0.elapsed());
    }

    samples.sort_unstable();
    let p50 = samples[SAMPLES / 2];
    let p95 = samples[(SAMPLES * 95) / 100];
    let p99 = samples[(SAMPLES * 99) / 100];

    println!("WASM FFI Round-trip (2,000 rows): p50={p50:?} p95={p95:?} p99={p99:?}");
    assert!(
        p95 <= P95_LIMIT,
        "p95 latency {p95:?} exceeds budget {P95_LIMIT:?}"
    );
    assert!(
        p99 <= P99_LIMIT,
        "p99 latency {p99:?} exceeds budget {P99_LIMIT:?}"
    );
}
