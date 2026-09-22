//! Criterion benchmark measuring WASM boundary crossing latency across batch sizes.
//!
//! Evaluates round-trip IPC serialization, guest execution, and deserialization
//! for batch sizes of 100, 500, 2,000, and 10,000 rows.

#![allow(clippy::unwrap_used)]

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::pipeline::SignalBatch;
use std::sync::Arc;
use wasm_transformer::engine::EngineCache;
use wasm_transformer::host_calls::MetricRegistry;
use wasm_transformer::worker::WasmWorker;

fn passthrough_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 64)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param $ptr i32) (param $len i32) (result i32)
            ;; TransformResponseHeader: status=0, batch_count=1, batches_ptr=24, message_ptr=0, message_len=0
            (i32.store (i32.const 0) (i32.const 0))
            (i32.store (i32.const 4) (i32.const 1))
            (i32.store (i32.const 8) (i32.const 24))
            (i32.store (i32.const 12) (i32.const 0))
            (i32.store (i32.const 16) (i32.const 0))
            ;; BatchDescriptor: ptr=$ptr, len=$len
            (i32.store (i32.const 24) (local.get $ptr))
            (i32.store (i32.const 28) (local.get $len))
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
    // 2,000 rows with ~200-byte bodies yields an uncompressed Arrow IPC stream of ~500 KB,
    // matching Section 3.5 of the WASM transformer design specification.
    let pad = "x".repeat(200);
    let bodies: Vec<String> = (0..rows).map(|i| format!("body_{i}_{pad}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let body_refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(id_refs)),
            Arc::new(StringArray::from(body_refs)),
        ],
    )
    .expect("valid record batch")
}

fn bench_wasm_boundary(c: &mut Criterion) {
    let cache = Arc::new(
        EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("pooling allocator should initialize"),
    );
    let wat_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let module = cache
        .compile_module(&wat_bytes)
        .expect("module compilation should succeed");

    let cfg = WasmTransformerConfig {
        id: "bench_trans".into(),
        r#type: "wasm".into(),
        module_path: "test.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 10_000,
        concurrency: 2,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 1_000_000,
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

    let registry = Arc::new(MetricRegistry::new("bench"));
    let mut worker =
        WasmWorker::new(0, cache, module, cfg, registry).expect("worker should initialize");

    let mut group = c.benchmark_group("wasm_boundary_roundtrip");
    for rows in [100, 500, 2000, 10_000] {
        let batch = make_batch(rows);
        group.bench_with_input(BenchmarkId::new("rows", rows), &rows, |b, _| {
            b.iter(|| {
                let _ = worker
                    .execute_batch(SignalBatch::Logs(batch.clone()))
                    .expect("execute batch should succeed");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_wasm_boundary);
criterion_main!(benches);
