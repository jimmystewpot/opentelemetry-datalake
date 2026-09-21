use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::pipeline::SignalBatch;
use std::sync::Arc;
use tokio::sync::mpsc;
use wasm_transformer::dispatcher::{DispatcherConfig, WasmDispatcher};
use wasm_transformer::engine::EngineCache;

fn passthrough_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
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

fn discard_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 1))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

fn reject_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 2))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

fn error_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32)
            (i32.store (i32.const 0) (i32.const 3))
            (i32.store (i32.const 4) (i32.const 0))
            (i32.const 0)
        )
    )"#
}

fn test_config(
    on_error: OnErrorPolicy,
    on_reject: OnRejectPolicy,
    concurrency: usize,
) -> WasmTransformerConfig {
    WasmTransformerConfig {
        id: "disp_test".into(),
        r#type: "wasm".into(),
        module_path: "test.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "5s".into(),
        max_batch_rows: 5000,
        concurrency,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error,
        allow_unmasked_passthrough: false,
        on_reject,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::default(),
        config: None,
        enable_sighup: false,
    }
}

fn logs_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace_abc"])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    )
    .expect("valid record batch")
}

#[tokio::test]
async fn test_dispatcher_executes_batches_and_drains_on_close() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Reroute, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        None,
        None,
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx); // Close input

    let mut received = 0;
    while output_rx.recv().await.is_some() {
        received += 1;
    }
    assert_eq!(
        received, 2,
        "dispatcher must execute and forward all batches before drain completes"
    );
}

#[tokio::test]
async fn test_dispatcher_error_reroute_policy_routes_to_reroute_error() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(error_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Reroute, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);
    let (err_tx, mut err_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        Some(err_tx),
        None,
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx);

    let mut err_received = 0;
    while err_rx.recv().await.is_some() {
        err_received += 1;
    }
    assert_eq!(
        err_received, 2,
        "all errored batches must route to reroute_error DLQ"
    );

    let mut out_received = 0;
    while output_rx.try_recv().is_ok() {
        out_received += 1;
    }
    assert_eq!(
        out_received, 0,
        "no errored batches should be sent to primary output"
    );
}

#[tokio::test]
async fn test_dispatcher_error_passthrough_policy_routes_to_output() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(error_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Passthrough, OnRejectPolicy::Reroute, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);
    let (err_tx, mut err_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        Some(err_tx),
        None,
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx);

    let mut out_received = 0;
    while output_rx.recv().await.is_some() {
        out_received += 1;
    }
    assert_eq!(
        out_received, 2,
        "passthrough policy must forward original batches to output"
    );

    let mut err_received = 0;
    while err_rx.try_recv().is_ok() {
        err_received += 1;
    }
    assert_eq!(
        err_received, 0,
        "passthrough policy must not forward to DLQ error channel"
    );
}

#[tokio::test]
async fn test_dispatcher_reject_reroute_policy_routes_to_reroute_reject() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(reject_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Reroute, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);
    let (rej_tx, mut rej_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        None,
        Some(rej_tx),
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx);

    let mut rej_received = 0;
    while rej_rx.recv().await.is_some() {
        rej_received += 1;
    }
    assert_eq!(
        rej_received, 2,
        "all rejected batches must route to reroute_reject DLQ"
    );

    let mut out_received = 0;
    while output_rx.try_recv().is_ok() {
        out_received += 1;
    }
    assert_eq!(
        out_received, 0,
        "no rejected batches should be sent to primary output"
    );
}

#[tokio::test]
async fn test_dispatcher_reject_drop_policy_drops_batch() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(reject_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Drop, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);
    let (rej_tx, mut rej_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        None,
        Some(rej_tx),
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx);

    // Wait until output is closed on drain completion
    assert!(output_rx.recv().await.is_none());

    let mut rej_received = 0;
    while rej_rx.try_recv().is_ok() {
        rej_received += 1;
    }
    assert_eq!(
        rej_received, 0,
        "drop policy must not route to DLQ reject channel"
    );
}

#[tokio::test]
async fn test_dispatcher_discard_outcome_drops_batch() {
    let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(discard_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Reroute, 2);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency: 2,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        None,
        None,
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 1");
    input_tx
        .send(SignalBatch::Logs(logs_batch()))
        .await
        .expect("send batch 2");
    drop(input_tx);

    assert!(
        output_rx.recv().await.is_none(),
        "discarded batches must be dropped without emission"
    );
}

#[tokio::test]
async fn test_dispatcher_concurrent_drain_completes_without_dropping_batches() {
    let concurrency = 4;
    let cache =
        Arc::new(EngineCache::new_pooling(concurrency, 64 * 1024 * 1024).expect("cache init"));
    let module = cache
        .compile_module(&wat::parse_str(passthrough_wat()).expect("valid wat"))
        .expect("compile module");

    let cfg = test_config(OnErrorPolicy::Reroute, OnRejectPolicy::Reroute, concurrency);

    let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(16);
    let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(64);

    let dispatcher = WasmDispatcher::new(
        DispatcherConfig {
            concurrency,
            worker_channel_capacity: 1,
        },
        Arc::clone(&cache),
        module,
        cfg,
        output_tx,
        None,
        None,
    );

    tokio::spawn(async move {
        dispatcher.run(input_rx).await.expect("dispatcher run");
    });

    let total_batches = 50;
    for _ in 0..total_batches {
        input_tx
            .send(SignalBatch::Logs(logs_batch()))
            .await
            .expect("send batch");
    }
    drop(input_tx); // Trigger drain

    let mut received = 0;
    while output_rx.recv().await.is_some() {
        received += 1;
    }
    assert_eq!(
        received, total_batches,
        "concurrent drain must complete without dropping any in-flight or buffered batches"
    );
}

fn test_tf_cfg() -> WasmTransformerConfig {
    WasmTransformerConfig {
        id: "test".to_string(),
        r#type: "wasm".to_string(),
        module_path: "dummy".to_string(),
        sha256: None,
        max_execution_duration: "1s".to_string(),
        drain_timeout: "10ms".to_string(),
        max_batch_rows: 1000,
        concurrency: 1,
        worker_channel_capacity: 10,
        max_memory: "1MB".to_string(),
        rejuvenate_threshold: "0".to_string(),
        rejuvenate_batches: 0,
        init_timeout: "1s".to_string(),
        allow_unmasked_passthrough: false,
        on_error: OnErrorPolicy::Passthrough,
        on_reject: OnRejectPolicy::Drop,
        schema_guard: SchemaGuardMode::Defensive,
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
        env_whitelist: vec![],
    }
}

#[tokio::test]
async fn test_dispatcher_worker_init_failure() {
    let engine = Arc::new(EngineCache::new_pooling(1, 1024 * 1024).unwrap());

    // Provide a valid module initially, but we'll mess up the config or module to cause WasmWorker::new to fail.
    // It calls `engine.instantiate(module)`. If the module needs imports that aren't provided by the linker.
    let bad_wat = r#"(module (import "env" "missing" (func)))"#;
    let module = engine
        .compile_module(&wat::parse_str(bad_wat).unwrap())
        .unwrap();

    let cfg = DispatcherConfig {
        concurrency: 1,
        worker_channel_capacity: 1,
    };

    let tf_cfg = test_tf_cfg();

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(1);
    let (out_tx, _out_rx) = tokio::sync::mpsc::channel(1);
    let (err_tx, _err_rx) = tokio::sync::mpsc::channel(1);
    let (rej_tx, _rej_rx) = tokio::sync::mpsc::channel(1);

    let dispatcher = WasmDispatcher::new(
        cfg,
        engine,
        module,
        tf_cfg,
        out_tx,
        Some(err_tx),
        Some(rej_tx),
    );

    // The worker spawn task will fail initializing WasmWorker, causing it to return early.
    // The worker channels are dropped, so sending to them will fail, causing run() to return Err.
    let run_handle = tokio::spawn(async move { dispatcher.run(in_rx).await });

    // Yield so the worker can die.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send a batch.
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();
    let _ = in_tx.send(SignalBatch::Logs(batch)).await;

    let res = run_handle.await.unwrap();
    assert!(
        res.is_err(),
        "Dispatcher should error because worker channel closed"
    );
}

#[tokio::test]
async fn test_dispatcher_output_channel_closed() {
    let engine = Arc::new(EngineCache::new_pooling(2, 1024 * 1024).unwrap());
    let module = engine
        .compile_module(&wat::parse_str(passthrough_wat()).unwrap())
        .unwrap();

    let cfg = DispatcherConfig {
        concurrency: 1,
        worker_channel_capacity: 1,
    };

    let tf_cfg = test_tf_cfg();

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(1);
    let (out_tx, out_rx) = tokio::sync::mpsc::channel(1);
    let (err_tx, _err_rx) = tokio::sync::mpsc::channel(1);
    let (rej_tx, _rej_rx) = tokio::sync::mpsc::channel(1);

    let dispatcher = WasmDispatcher::new(
        cfg,
        engine,
        module,
        tf_cfg,
        out_tx,
        Some(err_tx),
        Some(rej_tx),
    );
    let run_handle = tokio::spawn(async move { dispatcher.run(in_rx).await });

    // Drop out_rx BEFORE sending the batch, so that output.send(b) inside the worker fails.
    drop(out_rx);

    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["test"]))]).unwrap();
    let _ = in_tx.send(SignalBatch::Logs(batch)).await;

    // Send another to trigger the worker loop if first dropped silently
    // But since output closed, the worker will `break` out of its processing loop and terminate.
    drop(in_tx);

    let res = run_handle.await.unwrap();
    assert!(
        res.is_ok(),
        "Run should finish gracefully when input closes"
    );
}
