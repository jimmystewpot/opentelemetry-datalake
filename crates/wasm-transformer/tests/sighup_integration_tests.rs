use std::sync::Arc;
use wasm_transformer::{
    engine::EngineCache,
    reload::{spawn_sighup_listener, spawn_sighup_listener_multi},
};

fn valid_wat() -> &'static str {
    r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
        (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
        (func (export "datalake_dealloc") (param i32 i32))
        (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
        (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
    )"#
}

#[cfg(unix)]
static SIGHUP_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload() {
    #[cfg(unix)]
    {
        let _guard = SIGHUP_MUTEX.lock().await;
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
        tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle =
            spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), None, true).unwrap();

        // Give it a tiny bit of time to register the listener
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send SIGHUP to self
        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();

        // Wait for it to process
        let mut reloaded = false;
        for _ in 0..50 {
            if engine.module_generation() == 1 {
                reloaded = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(reloaded, "module generation should be 1 after first SIGHUP");

        // Send again to verify multiple reloads
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();

        let mut reloaded2 = false;
        for _ in 0..50 {
            if engine.module_generation() == 2 {
                reloaded2 = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(
            reloaded2,
            "module generation should be 2 after second SIGHUP"
        );

        handle.abort();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload_failure_missing_file() {
    #[cfg(unix)]
    {
        let _guard = SIGHUP_MUTEX.lock().await;
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_missing_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // File doesn't exist

        assert_eq!(engine.module_generation(), 0);

        let handle =
            spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), None, true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(engine.module_generation(), 0);
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload_failure_invalid_wasm() {
    #[cfg(unix)]
    {
        let _guard = SIGHUP_MUTEX.lock().await;
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_invalid_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        tokio::fs::write(&module_path, b"invalid").await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle =
            spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), None, true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(engine.module_generation(), 0);

        handle.abort();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_with_matching_expected_sha() {
    #[cfg(unix)]
    {
        use wasm_transformer::reload::compute_sha256;

        let _guard = SIGHUP_MUTEX.lock().await;
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_match_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
        let sha = compute_sha256(&wasm_bytes);
        tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle =
            spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), Some(sha), true)
                .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();

        let mut reloaded = false;
        for _ in 0..50 {
            if engine.module_generation() == 1 {
                reloaded = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(
            reloaded,
            "module generation should be 1 after SIGHUP with matching expected_sha"
        );

        handle.abort();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_with_mismatching_expected_sha() {
    #[cfg(unix)]
    {
        let _guard = SIGHUP_MUTEX.lock().await;
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_mismatch_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
        tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle = spawn_sighup_listener(
            Arc::clone(&engine),
            module_path.clone(),
            Some("0000000000000000000000000000000000000000000000000000000000000000".to_string()),
            true,
        )
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            engine.module_generation(),
            0,
            "module generation should remain 0 when expected_sha mismatches"
        );

        handle.abort();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_multi_triggers_reload() {
    #[cfg(unix)]
    {
        let _guard = SIGHUP_MUTEX.lock().await;
        let engine1 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        let engine2 = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());

        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_multi_{}_{}.wasm",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
        tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

        assert_eq!(engine1.module_generation(), 0);
        assert_eq!(engine2.module_generation(), 0);

        let handle = spawn_sighup_listener_multi(
            vec![Arc::clone(&engine1), Arc::clone(&engine2)],
            module_path.clone(),
            None,
            true,
        )
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill")
            .arg("-HUP")
            .arg(&pid)
            .status()
            .unwrap();

        let mut reloaded = false;
        for _ in 0..50 {
            if engine1.module_generation() == 1 && engine2.module_generation() == 1 {
                reloaded = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert!(
            reloaded,
            "both engine generations should be 1 after SIGHUP with multi-engine listener"
        );

        handle.abort();
        let _ = handle.await;
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}
