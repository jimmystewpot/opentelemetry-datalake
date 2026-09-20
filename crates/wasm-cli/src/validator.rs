//! Module validation for WASM guest transform modules.
//!
//! Validates that compiled WebAssembly binaries satisfy C-ABI v1 requirements,
//! including required function and memory exports and matching ABI versions.

use wasmtime::{Engine, Instance, Module, Store};

/// The set of required symbol exports defined by the datalake WASM ABI v1.
const REQUIRED_EXPORTS: &[&str] = &[
    "datalake_abi_version",
    "datalake_alloc",
    "datalake_dealloc",
    "datalake_init",
    "datalake_transform",
    "memory",
];

/// Validates raw WASM bytes against the C-ABI v1 specification.
///
/// # Errors
///
/// Returns an error string if:
/// - The bytecode is invalid WebAssembly.
/// - Any required export is missing from the module interface.
/// - The module cannot be instantiated.
/// - Calling `datalake_abi_version` fails or produces an unexpected version.
pub fn validate_wasm_bytes(bytes: &[u8]) -> Result<(), String> {
    let engine = Engine::default();
    let module = Module::new(&engine, bytes).map_err(|e| format!("Invalid WASM: {e}"))?;
    for &required in REQUIRED_EXPORTS {
        if !module.exports().any(|e| e.name() == required) {
            return Err(format!("Missing export '{required}'"));
        }
    }
    let mut store: Store<()> = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &module, &[])
        .map_err(|e| format!("Instantiation failed: {e}"))?;
    let abi_fn = instance
        .get_typed_func::<(), u32>(&mut store, "datalake_abi_version")
        .map_err(|e| format!("Cannot call datalake_abi_version: {e}"))?;
    let version = abi_fn
        .call(&mut store, ())
        .map_err(|e| format!("datalake_abi_version trap: {e}"))?;
    if version != opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION {
        return Err(format!(
            "ABI version mismatch: expected {}, got {version}",
            opentelemetry_datalake_wasm_sdk::abi::ABI_VERSION
        ));
    }
    Ok(())
}
