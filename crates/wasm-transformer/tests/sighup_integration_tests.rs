use std::{path::PathBuf, sync::Arc};
use wasm_transformer::{engine::EngineCache, reload::spawn_sighup_listener};

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

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload() {
    #[cfg(unix)]
    {
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        
        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_{}.wasm",
            std::process::id()
        ));
        
        let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
        tokio::fs::write(&module_path, &wasm_bytes).await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle = spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), true).unwrap();

        // Give it a tiny bit of time to register the listener
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Send SIGHUP to self
        let pid = std::process::id().to_string();
        std::process::Command::new("kill").arg("-HUP").arg(&pid).status().unwrap();

        // Wait for it to process
        for _ in 0..10 {
            if engine.module_generation() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(engine.module_generation(), 1);

        // Send again to verify multiple reloads
        std::process::Command::new("kill").arg("-HUP").arg(&pid).status().unwrap();
        
        for _ in 0..10 {
            if engine.module_generation() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        
        assert_eq!(engine.module_generation(), 2);

        handle.abort();
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload_failure_missing_file() {
    #[cfg(unix)]
    {
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        
        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_missing_{}.wasm",
            std::process::id()
        ));
        // File doesn't exist

        assert_eq!(engine.module_generation(), 0);

        let handle = spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill").arg("-HUP").arg(&pid).status().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(engine.module_generation(), 0);
        handle.abort();
    }
}

#[tokio::test]
async fn test_spawn_sighup_listener_triggers_reload_failure_invalid_wasm() {
    #[cfg(unix)]
    {
        let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
        
        let temp_dir = std::env::temp_dir();
        let module_path = temp_dir.join(format!(
            "sighup_test_module_invalid_{}.wasm",
            std::process::id()
        ));
        
        tokio::fs::write(&module_path, b"invalid").await.unwrap();

        assert_eq!(engine.module_generation(), 0);

        let handle = spawn_sighup_listener(Arc::clone(&engine), module_path.clone(), true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let pid = std::process::id().to_string();
        std::process::Command::new("kill").arg("-HUP").arg(&pid).status().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(engine.module_generation(), 0);

        handle.abort();
        let _ = tokio::fs::remove_file(&module_path).await;
    }
}
