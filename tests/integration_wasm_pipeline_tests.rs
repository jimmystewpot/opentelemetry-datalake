use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::error::PipelineError;
use pipeline_core::pipeline::{PipelineReceiver, PipelineSender, SignalBatch, Transform};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use wasm_transformer::WasmTransformer;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempWasmFile {
    path: std::path::PathBuf,
}

impl TempWasmFile {
    fn new(bytes: &[u8]) -> Self {
        let count = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("test_wasm_{}_{count}.wasm", std::process::id()));
        std::fs::write(&path, bytes).expect("failed to write temp wasm file");
        Self { path }
    }

    fn path_str(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}

impl Drop for TempWasmFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

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

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["auth_service", "payment_service"])),
            Arc::new(StringArray::from(vec![
                "login successful",
                "payment processed",
            ])),
        ],
    )
    .expect("valid record batch")
}

#[test]
fn test_topological_dlq_sink_missing_error_reroute() {
    let cfg = WasmTransformerConfig {
        id: "dlq_check".into(),
        r#type: "wasm".into(),
        module_path: "nonexistent.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 1,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error: OnErrorPolicy::Reroute,
        allow_unmasked_passthrough: false,
        on_reject: OnRejectPolicy::Drop,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
    };

    let res = WasmTransformer::new(cfg, None, None);
    assert!(
        matches!(res, Err(PipelineError::TopologicalSinkMissing(ref s)) if s.contains("dlq_check.__reroute_errored"))
    );
}

#[test]
fn test_topological_dlq_sink_missing_reject_reroute() {
    let (err_tx, _err_rx) = mpsc::channel(10);
    let cfg = WasmTransformerConfig {
        id: "reject_check".into(),
        r#type: "wasm".into(),
        module_path: "nonexistent.wasm".into(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 1,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
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

    // Provide reroute_error so on_error passes, but reroute_reject is None
    let res = WasmTransformer::new(cfg, Some(err_tx), None);
    assert!(
        matches!(res, Err(PipelineError::TopologicalSinkMissing(ref s)) if s.contains("reject_check.__reroute_rejected"))
    );
}

#[test]
fn test_passthrough_security_audit_and_successful_initialization() {
    let wasm_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let temp_file = TempWasmFile::new(&wasm_bytes);

    let cfg = WasmTransformerConfig {
        id: "passthrough_test".into(),
        r#type: "wasm".into(),
        module_path: temp_file.path_str(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 2,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error: OnErrorPolicy::Passthrough,
        allow_unmasked_passthrough: true,
        on_reject: OnRejectPolicy::Drop,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
    };

    let transformer = WasmTransformer::new(cfg, None, None).expect("initialization should succeed");
    assert_eq!(transformer.config().id, "passthrough_test");
    assert!(transformer.engine().module().is_some());
}

#[test]
fn test_successful_initialization_with_dlq_channels() {
    let wasm_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let temp_file = TempWasmFile::new(&wasm_bytes);

    let (err_tx, _err_rx) = mpsc::channel(10);
    let (rej_tx, _rej_rx) = mpsc::channel(10);

    let cfg = WasmTransformerConfig {
        id: "dlq_configured".into(),
        r#type: "wasm".into(),
        module_path: temp_file.path_str(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 2,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
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

    let transformer = WasmTransformer::new(cfg, Some(err_tx), Some(rej_tx))
        .expect("initialization should succeed");
    assert_eq!(transformer.config().id, "dlq_configured");
}

#[test]
fn test_sha256_integrity_verification() {
    let wasm_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let temp_file = TempWasmFile::new(&wasm_bytes);

    let mut hasher = Sha256::new();
    hasher.update(&wasm_bytes);
    let actual_hash = hex::encode(hasher.finalize());

    // 1. Correct hash succeeds
    let mut cfg = WasmTransformerConfig {
        id: "sha_check".into(),
        r#type: "wasm".into(),
        module_path: temp_file.path_str(),
        sha256: Some(actual_hash),
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 1,
        worker_channel_capacity: 1,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error: OnErrorPolicy::Drop,
        allow_unmasked_passthrough: false,
        on_reject: OnRejectPolicy::Drop,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
    };

    let res = WasmTransformer::new(cfg.clone(), None, None);
    assert!(res.is_ok(), "Matching hash must succeed");

    // 2. Tampered hash fails
    cfg.sha256 = Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into());
    let res = WasmTransformer::new(cfg, None, None);
    assert!(matches!(res, Err(PipelineError::Internal(ref s)) if s.contains("SHA256 mismatch")));
}

#[tokio::test]
async fn test_wasm_transformer_transform_trait_pipeline_execution() {
    let wasm_bytes = wat::parse_str(passthrough_wat()).expect("valid wat");
    let temp_file = TempWasmFile::new(&wasm_bytes);

    let cfg = WasmTransformerConfig {
        id: "pipeline_transform".into(),
        r#type: "wasm".into(),
        module_path: temp_file.path_str(),
        sha256: None,
        max_execution_duration: "500ms".into(),
        drain_timeout: "10s".into(),
        max_batch_rows: 5000,
        concurrency: 2,
        worker_channel_capacity: 2,
        max_memory: "64MiB".into(),
        rejuvenate_threshold: "16MiB".into(),
        rejuvenate_batches: 10_000,
        init_timeout: "2s".into(),
        on_error: OnErrorPolicy::Drop,
        allow_unmasked_passthrough: false,
        on_reject: OnRejectPolicy::Drop,
        schema_guard: SchemaGuardMode::Defensive,
        env_whitelist: vec![],
        env: std::collections::HashMap::new(),
        config: None,
        enable_sighup: false,
    };

    let mut transformer =
        WasmTransformer::new(cfg, None, None).expect("transformer creation failed");

    let (input_tx, input_rx): (PipelineSender, PipelineReceiver) = mpsc::channel(10);
    let (output_tx, mut output_rx): (PipelineSender, PipelineReceiver) = mpsc::channel(10);

    let transform_handle =
        tokio::spawn(async move { transformer.transform(input_rx, output_tx).await });

    let batch = sample_batch();
    input_tx
        .send(SignalBatch::Logs(batch))
        .await
        .expect("input send failed");

    // Close input channel to initiate dispatcher drain
    drop(input_tx);

    let received = output_rx
        .recv()
        .await
        .expect("output batch must be received");
    match received {
        SignalBatch::Logs(rb) => {
            assert_eq!(rb.num_rows(), 2);
            assert_eq!(rb.num_columns(), 2);
        }
        _ => panic!("Expected Logs signal variant"),
    }

    let transform_res = transform_handle.await.expect("task panicked");
    assert!(transform_res.is_ok());
}
