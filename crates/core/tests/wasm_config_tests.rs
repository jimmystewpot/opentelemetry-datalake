use pipeline_core::config::{
    OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig,
};
use pipeline_core::error::PipelineError;
use std::time::Duration;

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
    assert_eq!(cfg.worker_channel_capacity.get(), 1);
    assert_eq!(cfg.rejuvenate_threshold, 16 * 1024 * 1024);
    assert_eq!(cfg.concurrency.get(), 4);
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
    assert_eq!(cfg.max_execution_duration, Duration::from_millis(500));
    assert_eq!(cfg.drain_timeout, Duration::from_secs(10));
    assert_eq!(cfg.max_batch_rows.get(), 5000);
    assert_eq!(cfg.concurrency.get(), 4);
    assert_eq!(cfg.worker_channel_capacity.get(), 1);
    assert_eq!(cfg.max_memory, 64 * 1024 * 1024);
    assert_eq!(cfg.rejuvenate_threshold, 16 * 1024 * 1024);
    assert_eq!(cfg.rejuvenate_batches, 10_000);
    assert_eq!(cfg.init_timeout, Duration::from_secs(2));
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
fn test_wasm_config_rejects_zero_concurrency() {
    let toml_str = r#"
        id = "test_zero_concurrency"
        type = "wasm"
        module_path = "transforms/test.wasm"
        concurrency = 0
    "#;
    let result: Result<WasmTransformerConfig, _> = toml::from_str(toml_str);
    assert!(
        result.is_err(),
        "concurrency = 0 must fail at deserialization"
    );
}

#[test]
fn test_wasm_config_rejects_zero_worker_channel_capacity() {
    let toml_str = r#"
        id = "test_zero_capacity"
        type = "wasm"
        module_path = "transforms/test.wasm"
        worker_channel_capacity = 0
    "#;
    let result: Result<WasmTransformerConfig, _> = toml::from_str(toml_str);
    assert!(
        result.is_err(),
        "worker_channel_capacity = 0 must fail at deserialization to prevent channel panic"
    );
}

#[test]
fn test_wasm_config_rejects_zero_max_batch_rows() {
    let toml_str = r#"
        id = "test_zero_max_batch_rows"
        type = "wasm"
        module_path = "transforms/test.wasm"
        max_batch_rows = 0
    "#;
    let result: Result<WasmTransformerConfig, _> = toml::from_str(toml_str);
    assert!(
        result.is_err(),
        "max_batch_rows = 0 must fail at deserialization"
    );
}

#[test]
fn test_wasm_config_memory_units_and_raw_bytes() {
    let cases = [
        ("64MiB", 64 * 1024 * 1024),
        ("16MB", 16 * 1024 * 1024),
        ("1GiB", 1024 * 1024 * 1024),
        ("2GB", 2 * 1024 * 1024 * 1024),
        ("1024KiB", 1024 * 1024),
        ("512KB", 512 * 1024),
        ("2048B", 2048),
        ("100bytes", 100),
        ("4096", 4096),
    ];

    for (input, expected_bytes) in cases {
        let toml_str = format!(
            r#"
            id = "mem_test"
            type = "wasm"
            module_path = "transforms/test.wasm"
            max_memory = "{input}"
            "#
        );
        let cfg: WasmTransformerConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(
            cfg.max_memory, expected_bytes,
            "Failed parsing '{input}' into expected bytes"
        );
    }

    // Also verify integer byte deserialization directly
    let toml_int = r#"
        id = "mem_test_int"
        type = "wasm"
        module_path = "transforms/test.wasm"
        max_memory = 33554432
    "#;
    let cfg_int: WasmTransformerConfig = toml::from_str(toml_int).unwrap();
    assert_eq!(cfg_int.max_memory, 33_554_432);
}

#[test]
fn test_wasm_config_rejects_invalid_memory_strings() {
    for invalid in ["", "   ", "invalid", "64Foo", "-10MiB"] {
        let toml_str = format!(
            r#"
            id = "invalid_mem"
            type = "wasm"
            module_path = "transforms/test.wasm"
            max_memory = "{invalid}"
            "#
        );
        let result: Result<WasmTransformerConfig, _> = toml::from_str(&toml_str);
        assert!(
            result.is_err(),
            "Invalid memory string '{invalid}' must fail deserialization"
        );
    }
}

#[test]
fn test_wasm_config_rejects_invalid_duration_strings() {
    for invalid in ["", "not_a_duration", "500foobars", "-10s"] {
        let toml_str = format!(
            r#"
            id = "invalid_duration"
            type = "wasm"
            module_path = "transforms/test.wasm"
            max_execution_duration = "{invalid}"
            "#
        );
        let result: Result<WasmTransformerConfig, _> = toml::from_str(&toml_str);
        assert!(
            result.is_err(),
            "Invalid duration string '{invalid}' must fail deserialization"
        );
    }
}

#[test]
fn test_wasm_config_roundtrip_serde() {
    let toml_str = r#"
        id = "test_roundtrip"
        type = "wasm"
        module_path = "transforms/roundtrip.wasm"
        max_execution_duration = "1s"
        drain_timeout = "5s"
        max_batch_rows = 2000
        concurrency = 2
        worker_channel_capacity = 8
        max_memory = "32MiB"
        rejuvenate_threshold = "8MiB"
        rejuvenate_batches = 5000
        init_timeout = "1s"
        on_error = "drop"
        allow_unmasked_passthrough = true
        on_reject = "reroute"
        schema_guard = "strict"
        enable_sighup = true
    "#;
    let cfg: WasmTransformerConfig = toml::from_str(toml_str).unwrap();

    let serialized = serde_json::to_string(&cfg).unwrap();
    let deserialized: WasmTransformerConfig = serde_json::from_str(&serialized).unwrap();
    assert_eq!(cfg, deserialized);
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
