use std::collections::HashMap;
use wasm_transformer::wasi_env::filter_environment_variables;

#[test]
fn test_ambient_denied_static_override_wins() {
    // In Rust 2024 edition, std::env::set_var is unsafe
    unsafe {
        std::env::set_var("HOST_SECRET", "super_secret");
        std::env::set_var("APP_ENV", "host_dev");
    }
    let whitelist = vec!["APP_ENV".to_string()];
    let mut static_env = HashMap::new();
    static_env.insert("APP_ENV".to_string(), "static_override".to_string());
    static_env.insert("EXTRA_KEY".to_string(), "val".to_string());

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert!(!filtered.contains_key("HOST_SECRET"));
    assert_eq!(
        filtered.get("APP_ENV").map(String::as_str),
        Some("static_override")
    );
    assert_eq!(filtered.get("EXTRA_KEY").map(String::as_str), Some("val"));
}

#[test]
fn test_whitelist_allows_ambient_env() {
    unsafe {
        std::env::set_var("ALLOWED_VAR", "ambient_val");
    }
    let whitelist = vec!["ALLOWED_VAR".to_string()];
    let static_env = HashMap::new();

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert_eq!(
        filtered.get("ALLOWED_VAR").map(String::as_str),
        Some("ambient_val")
    );
}

#[test]
fn test_missing_ambient_var_ignored() {
    unsafe {
        std::env::remove_var("NON_EXISTENT_VAR");
    }
    let whitelist = vec!["NON_EXISTENT_VAR".to_string()];
    let static_env = HashMap::new();

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert!(!filtered.contains_key("NON_EXISTENT_VAR"));
}

#[test]
fn test_sensitive_ambient_var_excluded() {
    unsafe {
        std::env::set_var("DATABASE_PASSWORD", "secret123");
        std::env::set_var("API_TOKEN", "tok456");
        std::env::set_var("PRIVATE_KEY", "pk789");
    }
    let whitelist = vec![];
    let static_env = HashMap::new();

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert!(!filtered.contains_key("DATABASE_PASSWORD"));
    assert!(!filtered.contains_key("API_TOKEN"));
    assert!(!filtered.contains_key("PRIVATE_KEY"));
}

#[test]
fn test_explicitly_whitelisted_sensitive_var_allowed() {
    unsafe {
        std::env::set_var("MY_SERVICE_TOKEN", "token_val");
    }
    let whitelist = vec!["MY_SERVICE_TOKEN".to_string()];
    let static_env = HashMap::new();

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert_eq!(
        filtered.get("MY_SERVICE_TOKEN").map(String::as_str),
        Some("token_val")
    );
}
