use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::error::PipelineError;

#[test]
fn test_wasm_config_deserializes_explicit_fields() {
    let toml_str = r#"
        id = "test_wasm"
        type = "wasm"
        module_path = "transforms/test.wasm"
        on_error = "reroute"
        on_reject = "drop"
        worker_channel_capacity = 1
        rejuvenate_threshold = "16MiB"
        concurrency = 4
        enable_sighup = true
    "#;
    let cfg: WasmTransformerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.id, "test_wasm");
    assert_eq!(cfg.r#type, "wasm");
    assert_eq!(cfg.module_path, "transforms/test.wasm");
    assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
    assert_eq!(cfg.on_reject, OnRejectPolicy::Drop);
    assert_eq!(cfg.worker_channel_capacity, 1);
    assert_eq!(cfg.rejuvenate_threshold, "16MiB");
    assert_eq!(cfg.concurrency, 4);
    assert!(cfg.enable_sighup);
}

#[test]
fn test_wasm_config_defaults_on_error_is_reroute() {
    let toml_str = r#"
        id = "minimal"
        type = "wasm"
        module_path = "transforms/minimal.wasm"
    "#;
    let cfg: WasmTransformerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.id, "minimal");
    assert_eq!(cfg.r#type, "wasm");
    assert_eq!(cfg.module_path, "transforms/minimal.wasm");
    assert_eq!(cfg.sha256, None);
    assert_eq!(cfg.max_execution_duration, "500ms");
    assert_eq!(cfg.drain_timeout, "10s");
    assert_eq!(cfg.max_batch_rows, 5000);
    assert_eq!(cfg.concurrency, 4);
    assert_eq!(cfg.worker_channel_capacity, 1);
    assert_eq!(cfg.max_memory, "64MiB");
    assert_eq!(cfg.rejuvenate_threshold, "16MiB");
    assert_eq!(cfg.rejuvenate_batches, 10_000);
    assert_eq!(cfg.init_timeout, "2s");
    assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
    assert!(!cfg.allow_unmasked_passthrough);
    assert_eq!(cfg.on_reject, OnRejectPolicy::Reroute);
    assert_eq!(cfg.schema_guard, SchemaGuardMode::Defensive);
    assert!(cfg.env_whitelist.is_empty());
    assert!(cfg.env.is_empty());
    assert_eq!(cfg.config, None);
    assert!(!cfg.enable_sighup);
}

#[test]
fn test_policy_explicit_defaults() {
    // Crucial requirement: Default MUST be Reroute, not derived from first variant position
    assert_eq!(OnErrorPolicy::default(), OnErrorPolicy::Reroute);
    assert_eq!(OnRejectPolicy::default(), OnRejectPolicy::Reroute);
    assert_eq!(SchemaGuardMode::default(), SchemaGuardMode::Defensive);
}

#[test]
fn test_policy_serde_roundtrip() {
    // Test OnErrorPolicy variants
    assert_eq!(
        serde_json::to_string(&OnErrorPolicy::Reroute).unwrap(),
        "\"reroute\""
    );
    assert_eq!(
        serde_json::to_string(&OnErrorPolicy::Drop).unwrap(),
        "\"drop\""
    );
    assert_eq!(
        serde_json::to_string(&OnErrorPolicy::Passthrough).unwrap(),
        "\"passthrough\""
    );

    for policy in [
        OnErrorPolicy::Reroute,
        OnErrorPolicy::Drop,
        OnErrorPolicy::Passthrough,
    ] {
        let serialized = serde_json::to_string(&policy).unwrap();
        let deserialized: OnErrorPolicy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(policy, deserialized);
    }

    // Test OnRejectPolicy variants
    assert_eq!(
        serde_json::to_string(&OnRejectPolicy::Reroute).unwrap(),
        "\"reroute\""
    );
    assert_eq!(
        serde_json::to_string(&OnRejectPolicy::Drop).unwrap(),
        "\"drop\""
    );

    for policy in [OnRejectPolicy::Reroute, OnRejectPolicy::Drop] {
        let serialized = serde_json::to_string(&policy).unwrap();
        let deserialized: OnRejectPolicy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(policy, deserialized);
    }

    // Test SchemaGuardMode variants
    assert_eq!(
        serde_json::to_string(&SchemaGuardMode::Defensive).unwrap(),
        "\"defensive\""
    );
    assert_eq!(
        serde_json::to_string(&SchemaGuardMode::Strict).unwrap(),
        "\"strict\""
    );

    for mode in [SchemaGuardMode::Defensive, SchemaGuardMode::Strict] {
        let serialized = serde_json::to_string(&mode).unwrap();
        let deserialized: SchemaGuardMode = serde_json::from_str(&serialized).unwrap();
        assert_eq!(mode, deserialized);
    }
}

#[test]
fn test_pipeline_error_topological_sink_missing() {
    let err = PipelineError::TopologicalSinkMissing("dlq_sink".to_string());
    assert_eq!(err.to_string(), "Topological DLQ sink missing: dlq_sink");
}
