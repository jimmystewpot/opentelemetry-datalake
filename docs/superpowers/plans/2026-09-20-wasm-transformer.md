# WebAssembly (WASM) Whole-Batch Arrow Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an ultra-high-performance, sandboxed WebAssembly (WASM) transformation engine executing Whole-Batch Apache Arrow transformations on OpenTelemetry telemetry streams with zero-copy IPC exchange, three-tier immutability, symmetric DLQ rerouting, and sub-millisecond boundary latency.

**Architecture:** The system decouples guest transformations from the host runtime using a low-overhead C-ABI v1. The guest SDK (`opentelemetry-datalake-wasm-sdk`) provides typed abstractions and call-site fail-fast guards over Arrow IPC streams. The host runtime (`crates/wasm-transformer`) leverages Wasmtime's Pooling Allocator with `madvise(MADV_DONTNEED)` resets, lock-free least-loaded batch dispatching, $O(1)$ structural immutability verification, typed null backfilling, and zero-downtime hot-reloading. A standalone CLI (`datalake-wasm`) provides pre-deployment validation, synthetic testing, and latency benchmarking.

**Tech Stack:** Rust 2024, Apache Arrow 59, Wasmtime 48 (48.0.2 — current stable, with Pooling Allocator & WASI), Tokio 1.37, DashMap 6, sha2 0.10, hex 0.4, Thiserror 2.0, Criterion 0.8.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints

- **Zero-Panic Production Rule**: No `unwrap()`, `expect()`, `panic!()`, or `todo!()` in production paths (`src/`). All failures must propagate via `thiserror` domain errors.
- **Latency Budget**: Round-trip boundary overhead for a 2,000-row batch (~500 KB uncompressed Arrow IPC stream) must be p95 ≤ 1.5ms and p99 ≤ 3.0ms. Enforced by a standalone CI `#[test]` using `std::time::Instant` warm loops in `tests/latency_gate_tests.rs`. Criterion is for developer profiling only — it cannot fail CI on SLO breach.
- **Core Telemetry Immutability**: Core fields (`trace_id`, `span_id` in traces; `timestamp` in logs; `name`, `type` in metrics) cannot be dropped or nullified. Host checks this in O(1) via `col.null_count() == col.len()`.
- **Memory Safety & Reset**: Each WASM instance runs in a pre-allocated pooling memory slot (`max_memory = 64MiB`) with physical pages reclaimed via `madvise(MADV_DONTNEED)`. Dual-trigger rejuvenation (16 MiB soft threshold or 10,000 batches).
- **Concurrency & Backpressure**: Single `input: PipelineReceiver` is dispatched across N workers using non-blocking `try_send` with configurable queue depth (`worker_channel_capacity = 1`). Workers await on shutdown with full `JoinHandle` tracking.
- **Signal Isolation**: Three independent `WasmTransformer` instances are instantiated — one per signal type (Logs, Metrics, Traces) — mirroring the existing `NoopTransformer × 3` pattern in `src/main.rs`. Each has its own `PipelineReceiver`/`PipelineSender`. Signal type is passed to `datalake_init` via config JSON payload `{"signal":"logs"}`.
- **Wasmtime Version**: Use `wasmtime = "48"` and `wasmtime-wasi = "48"` (48.0.2). Do NOT use any 29.x version.
- **Workspace Dependency Rule**: All new external dependencies MUST be added to `[workspace.dependencies]` in the root `Cargo.toml` before any crate uses them. Crate `Cargo.toml` files MUST reference them via `{ workspace = true }`. Exception: `wat = "1.220"` (test-only WAT compiler) cannot be a workspace dep — add it directly to each crate's `[dev-dependencies]`.
- **Canonical Immutability List**: `IMMUTABLE_COLUMNS` is defined once in `crates/wasm-sdk/src/helpers.rs` and imported by the CLI conformance harness and host-side guard. Never duplicate it.
- **Code Standards**: Zero Clippy warnings under `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`. Format with `cargo fmt`. Prefer `crate::` over `super::` in non-test source code.
- **git**: Files under `docs/superpowers/` are gitignored. Use `git add -f` for any files in that directory.

---

## Modular PR Breakdown Strategy

To enable concurrent execution by parallel sub-agents and ensure fast, stand-alone reviews, the implementation is decomposed into **8 modular PRs** plus a prerequisite workspace task (PR 0):

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    PREREQUISITE: PR 0 — Workspace Dependencies                   │
│  Root Cargo.toml: wasmtime 48, wasmtime-wasi 48, dashmap, sha2, hex,            │
│  arrow+ipc feature. Must land before any other PR compiles.                      │
└────────────────────────────────────┬─────────────────────────────────────────────┘
                                     │ (all Layer 1 PRs depend on PR 0)
┌────────────────────────────────────▼─────────────────────────────────────────────┐
│                             INDEPENDENT PR LAYER 1                               │
├───────────────────────────────┬───────────────────────────────┬──────────────────┤
│ PR 1: Standalone Guest SDK    │ PR 2: Core Config & DLQ Rules │ PR 3: Dev CLI    │
│ (crates/wasm-sdk)             │ (crates/core)                 │ (crates/wasm-cli)│
│ - ABI v1 C-types & protocol   │ - WasmTransformerConfig       │ - validate cmd   │
│ - BatchTransformer & results  │ - OnError / OnReject enums    │ - test suite     │
│ - IMMUTABLE_COLUMNS constant  │ - Topological DLQ assertions  │ - bench tool     │
│ - Client-side immutability    │ - Unit tests & TOML fixtures  │ - synthetic IPC  │
│ - Metrics (f64 bitcast)       │                               │ NOTE: imports    │
│   (single source of truth)    │                               │ IMMUTABLE_COLUMNS│
│                               │                               │ from PR 1 —      │
│                               │                               │ must land after  │
└───────────────┬───────────────┴───────────────┬───────────────┴──────────────────┘
                │                               │
                ▼                               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             INDEPENDENT PR LAYER 2                               │
├───────────────────────────────────────────────┬──────────────────────────────────┤
│ PR 4: Host Engine, Pool & WASI Sandbox        │ PR 5: Host Guards & ABI Linker   │
│ (crates/wasm-transformer: engine, pool, wasi) │ (crates/wasm-transformer: guard) │
│ - Wasmtime 48 Engine & Module cache           │ - O(1) Immutability Check        │
│ - Pooling Allocator & soft rejuvenation       │ - Defensive Typed Null Backfill  │
│ - Zero-trust WASI env filter                  │ - Concurrent DashMap Metrics     │
│ - Diagnostic OOM telemetry                    │ - datalake_host_v1 linker        │
└───────────────────────┬───────────────────────┴──────────────────┬───────────────┘
                        │                                          │
                        ▼                                          ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             INTEGRATION PR LAYER 3                               │
├───────────────────────────────────────────────┬──────────────────────────────────┤
│ PR 6: Dispatcher, Drain & Pipeline Transform  │ PR 7: Hot-Reload Controller      │
│ (crates/wasm-transformer + src/main.rs)       │ (crates/wasm-transformer: reload)│
│ - Least-loaded try_send fan-out               │ - Single-read SHA-256 check      │
│ - Worker JoinHandle tracking & drain timeout  │ - Atomic generation fencing      │
│ - 3x signal-isolated Transform instances      │ - SIGHUP & admin router endpoint │
│ - Passthrough security audit warning          │ - Zero-drift worker swap         │
└───────────────────────────────────────────────┴──────────────────────────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  PR 8: CI Latency Gate,    │
                       │  Criterion Bench & Sample  │
                       │  tests/ + benches/ +        │
                       │  examples/transforms/       │
                       └────────────────────────────┘
```

> **PR 3 dependency note:** PR 3 (`wasm-cli`) imports `IMMUTABLE_COLUMNS` from `opentelemetry-datalake-wasm-sdk` (PR 1). A sub-agent implementing PR 3 must wait for PR 1 to land. Do not duplicate the constant.

---

## PR 0: Workspace Dependency Registration (Root `Cargo.toml`)

**Scope:** Register all new external dependencies in `[workspace.dependencies]` so all subsequent PRs reference them via `{ workspace = true }`. This MUST land first — it unblocks all of Layer 1.
**Independence:** Pure `Cargo.toml` change. No source code.

### Task 0.1: Add New Workspace Dependencies

**Files:**
- Modify: `Cargo.toml` (root workspace)

**Interfaces:**
- Produces: `wasmtime`, `wasmtime-wasi`, `dashmap`, `sha2`, `hex` available as workspace deps. Arrow `ipc` feature added.

- [ ] **Step 1: Verify arrow ipc feature is NOT yet present**

  ```bash
  grep -n '"ipc"' Cargo.toml
  ```
  Expected: no output

- [ ] **Step 2: Add workspace dependencies**

  In root `Cargo.toml`, in `[workspace.dependencies]`, add after the `# Arrow` section:

  ```toml
  # WebAssembly Runtime (Wasmtime 48 — current stable, Rust 1.97+ required)
  wasmtime = { version = "48", default-features = false, features = ["cranelift", "pooling-allocator"] }
  wasmtime-wasi = { version = "48", default-features = false }

  # Concurrent Collections
  dashmap = { version = "6.0", default-features = false }

  # Cryptography (SHA-256 module integrity)
  sha2 = { version = "0.10", default-features = false }
  hex = { version = "0.4", default-features = false }
  ```

  Update the existing `arrow` workspace dep to add the `ipc` feature (currently `features = ["json"]`):

  ```toml
  arrow = { version = "59", default-features = false, features = ["json", "ipc"] }
  ```

  > **Note on `wat`:** Cargo does not support workspace-level dev-dependencies. Add `wat = "1.220"` directly to `[dev-dependencies]` in each crate that needs it (PRs 4 and 7). This is the only dep that cannot be workspace-managed.

- [ ] **Step 3: Verify workspace resolves cleanly**

  ```bash
  cargo metadata --format-version 1 > /dev/null
  ```
  Expected: exits 0

- [ ] **Step 4: Commit**

  ```bash
  git add Cargo.toml
  git commit -m "chore(workspace): add wasmtime 48, dashmap, sha2, hex, and arrow ipc feature"
  ```

---




## PR 1: Standalone Guest SDK (`crates/wasm-sdk`)

**Scope:** Self-contained crate `opentelemetry-datalake-wasm-sdk` defining C-ABI v1 data structures, public `BatchTransformer` trait, `TransformResult` enum, client-side checked Arrow IPC helpers, metrics emission (f64 bitcast gauge, u64 nanosecond duration), panic hook forwarder, and mock testing harness.
**Independence:** Depends only on `arrow` and standard libraries. Zero dependencies on host runtime or `pipeline-core`.

### Task 1.1: SDK Crate Scaffolding & ABI v1 Data Structures
**Files:**
- Create: `crates/wasm-sdk/Cargo.toml`
- Create: `crates/wasm-sdk/src/lib.rs`
- Create: `crates/wasm-sdk/src/abi.rs`
- Test: `crates/wasm-sdk/tests/abi_tests.rs`

**Interfaces:**
- Consumes: Standard Rust types, Arrow IPC stream types.
- Produces: `TransformResponseHeader`, `BatchDescriptor`, `HostLogRecord`, ABI exports `datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`.

- [ ] **Step 1: Write failing test for ABI v1 memory layout & version handshake**

```rust
// crates/wasm-sdk/tests/abi_tests.rs
use opentelemetry_datalake_wasm_sdk::abi::{BatchDescriptor, TransformResponseHeader};

#[test]
fn test_abi_v1_header_memory_layout() {
    assert_eq!(std::mem::size_of::<TransformResponseHeader>(), 20);
    assert_eq!(std::mem::align_of::<TransformResponseHeader>(), 4);
    assert_eq!(std::mem::size_of::<BatchDescriptor>(), 8);
    assert_eq!(std::mem::align_of::<BatchDescriptor>(), 4);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test abi_tests`
Expected: FAIL with "package not found" or "unresolved import"

- [ ] **Step 3: Write minimal implementation**
Create `crates/wasm-sdk/Cargo.toml`:
```toml
[package]
name = "opentelemetry-datalake-wasm-sdk"
version = "0.1.0"
edition = "2024"
license = "MPL-2.0"

[dependencies]
arrow = { workspace = true, features = ["ipc"] }
```
Create `crates/wasm-sdk/src/abi.rs`:
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransformResponseHeader {
    pub status: u32,
    pub batch_count: u32,
    pub batches_ptr: u32,
    pub message_ptr: u32,
    pub message_len: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchDescriptor {
    pub ptr: u32,
    pub len: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostLogRecord {
    pub level: u32,
    pub msg_ptr: u32,
    pub msg_len: u32,
    pub target_ptr: u32,
    pub target_len: u32,
    pub file_ptr: u32,
    pub file_len: u32,
    pub line: u32,
}

#[no_mangle]
pub extern "C" fn datalake_abi_version() -> u32 {
    1
}

#[no_mangle]
pub extern "C" fn datalake_alloc(size: u32) -> u32 {
    let mut buf = Vec::<u8>::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr as usize as u32
}

#[no_mangle]
pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
    if ptr != 0 && size != 0 {
        unsafe {
            let _ = Vec::<u8>::from_raw_parts(ptr as *mut u8, 0, size as usize);
        }
    }
}
```
In `crates/wasm-sdk/src/lib.rs`:
```rust
pub mod abi;
```

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test abi_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-sdk/
git commit -m "feat(wasm-sdk): scaffold sdk crate and abi v1 definitions"
```

---

### Task 1.2: Trait Definition, TransformResult & Client-Side Immutability Guards
**Files:**
- Create: `crates/wasm-sdk/src/traits.rs`
- Create: `crates/wasm-sdk/src/helpers.rs`
- Create: `crates/wasm-sdk/src/error.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/guard_tests.rs`

**Interfaces:**
- Consumes: `RecordBatch` from Arrow.
- Produces: `BatchTransformer` trait, `TransformResult`, `sdk::helpers::nullify_column`, `SdkError::ImmutableFieldViolation`.

- [ ] **Step 1: Write failing test for call-site immutability violation**

```rust
// crates/wasm-sdk/tests/guard_tests.rs
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use opentelemetry_datalake_wasm_sdk::error::SdkError;
use opentelemetry_datalake_wasm_sdk::helpers::nullify_column;
use std::sync::Arc;

#[test]
fn test_nullify_immutable_column_fails_fast() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(StringArray::from(vec!["attr"])),
        ],
    ).unwrap();

    let err = nullify_column(&batch, "trace_id").unwrap_err();
    assert!(matches!(err, SdkError::ImmutableFieldViolation(col) if col == "trace_id"));

    // Auxiliary field succeeds
    let ok_batch = nullify_column(&batch, "scope_attributes").unwrap();
    assert!(ok_batch.column(1).null_count() == 1);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test guard_tests`
Expected: FAIL with "cannot find module `helpers`"

- [ ] **Step 3: Write minimal implementation**
Create `crates/wasm-sdk/src/error.rs`:
```rust
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SdkError {
    #[error("Cannot modify or nullify immutable OpenTelemetry core field: {0}")]
    ImmutableFieldViolation(String),
    #[error("Arrow error: {0}")]
    Arrow(String),
    #[error("Column not found in schema: {0}")]
    ColumnNotFound(String),
}
```
Create `crates/wasm-sdk/src/helpers.rs`:
```rust
use crate::error::SdkError;
use arrow::array::new_null_array;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

const IMMUTABLE_COLUMNS: &[&str] = &[
    "trace_id", "span_id", "timestamp", "observed_timestamp", "name", "type",
];

pub fn is_immutable_column(column: &str) -> bool {
    IMMUTABLE_COLUMNS.contains(&column)
}

pub fn nullify_column(batch: &RecordBatch, column_name: &str) -> Result<RecordBatch, SdkError> {
    if is_immutable_column(column_name) {
        return Err(SdkError::ImmutableFieldViolation(column_name.to_string()));
    }
    let schema = batch.schema();
    let idx = schema.index_of(column_name).map_err(|_| SdkError::ColumnNotFound(column_name.to_string()))?;
    let mut columns = batch.columns().to_vec();
    let field = schema.field(idx);
    columns[idx] = new_null_array(field.data_type(), batch.num_rows());
    RecordBatch::try_new(schema, columns).map_err(|e| SdkError::Arrow(e.to_string()))
}
```
Create `crates/wasm-sdk/src/traits.rs` with `SignalType`, `TransformResult`, and `BatchTransformer` per spec section 4.1.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test guard_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-sdk/
git commit -m "feat(wasm-sdk): add BatchTransformer trait, results, and client immutability guards"
```

---

### Task 1.3: Controlled Metrics Emission & Panic Hook
**Files:**
- Create: `crates/wasm-sdk/src/metrics.rs`
- Create: `crates/wasm-sdk/src/panic.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/metric_tests.rs`

**Interfaces:**
- Produces: `sdk::metrics::counter`, `sdk::metrics::gauge` (f64 bitcast), `sdk::metrics::duration` (nanoseconds), `init_panic_hook`.

- [ ] **Step 1: Write failing test for f64 gauge bitcast & nanosecond duration**

```rust
// crates/wasm-sdk/tests/metric_tests.rs
use opentelemetry_datalake_wasm_sdk::metrics::{gauge_to_bits, duration_to_nanos};
use std::time::Duration;

#[test]
fn test_gauge_f64_bitcast_preserves_negative_and_fractions() {
    let original = -12.375_f64;
    let bits = gauge_to_bits(original);
    assert_eq!(f64::from_bits(bits), original);
}

#[test]
fn test_duration_standardized_on_nanos() {
    let dur = Duration::from_millis(1500);
    assert_eq!(duration_to_nanos(dur), 1_500_000_000_u64);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test metric_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
Create `crates/wasm-sdk/src/metrics.rs`:
```rust
use std::time::Duration;

pub fn gauge_to_bits(value: f64) -> u64 {
    value.to_bits()
}

pub fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos() as u64
}

#[cfg(target_arch = "wasm32")]
extern "C" {
    fn datalake_host_metric_emit(metric_type: u32, name_ptr: u32, name_len: u32, value: u64);
}

pub fn counter(name: &str, value: u64) {
    #[cfg(target_arch = "wasm32")]
    unsafe { datalake_host_metric_emit(0, name.as_ptr() as u32, name.len() as u32, value); }
}

pub fn gauge(name: &str, value: f64) {
    #[cfg(target_arch = "wasm32")]
    unsafe { datalake_host_metric_emit(1, name.as_ptr() as u32, name.len() as u32, gauge_to_bits(value)); }
}

pub fn duration(name: &str, duration: Duration) {
    #[cfg(target_arch = "wasm32")]
    unsafe { datalake_host_metric_emit(2, name.as_ptr() as u32, name.len() as u32, duration_to_nanos(duration)); }
}
```
Create `crates/wasm-sdk/src/panic.rs` installing `std::panic::set_hook`.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test metric_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-sdk/
git commit -m "feat(wasm-sdk): add metrics f64 bitcast, nanosecond duration, and panic hook"
```

---

## PR 2: Core Configuration & DLQ Routing Rules (`crates/core`)

**Scope:** Updates `crates/core` to include `WasmTransformerConfig`, `OnErrorPolicy`, `OnRejectPolicy`, and validation for topological DLQ virtual streams (`<id>._reroute_errored`, `<id>._reroute_rejected`).
**Independence:** Self-contained in `crates/core`. No dependency on host runtime execution code.

### Task 2.1: WasmTransformerConfig & Routing Policy Types
**Files:**
- Modify: `crates/core/src/config.rs`
- Modify: `crates/core/src/error.rs`
- Test: `crates/core/tests/wasm_config_tests.rs`

**Interfaces:**
- Produces: `WasmTransformerConfig`, `OnErrorPolicy`, `OnRejectPolicy`, `SchemaGuardMode`, `PipelineError::TopologicalSinkMissing`.

- [ ] **Step 1: Write failing test for TOML deserialization and validation**

```rust
// crates/core/tests/wasm_config_tests.rs
use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, WasmTransformerConfig};

#[test]
fn test_wasm_config_deserializes_and_validates_policies() {
    let toml_str = r#"
        id = "test_wasm"
        type = "wasm"
        module_path = "transforms/test.wasm"
        on_error = "reroute"
        on_reject = "drop"
        worker_channel_capacity = 1
        rejuvenate_threshold = "16MiB"
        concurrency = 4
    "#;
    let cfg: WasmTransformerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.id, "test_wasm");
    assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
    assert_eq!(cfg.on_reject, OnRejectPolicy::Drop);
    assert_eq!(cfg.worker_channel_capacity, 1);
    assert_eq!(cfg.concurrency, 4);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p pipeline-core --test wasm_config_tests`
Expected: FAIL with "cannot find type `WasmTransformerConfig`"

- [ ] **Step 3: Write minimal implementation**
In `crates/core/src/config.rs`:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnErrorPolicy {
    Reroute,
    Drop,
    Passthrough,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnRejectPolicy {
    Reroute,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaGuardMode {
    #[default]
    Defensive,
    Strict,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct WasmTransformerConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub module_path: String,
    pub sha256: Option<String>,
    #[serde(default = "default_max_execution_duration")]
    pub max_execution_duration: String,
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout: String,
    #[serde(default = "default_max_batch_rows")]
    pub max_batch_rows: usize,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_worker_channel_capacity")]
    pub worker_channel_capacity: usize,
    #[serde(default = "default_max_memory")]
    pub max_memory: String,
    #[serde(default = "default_rejuvenate_threshold")]
    pub rejuvenate_threshold: String,
    #[serde(default = "default_rejuvenate_batches")]
    pub rejuvenate_batches: u64,
    #[serde(default = "default_init_timeout")]
    pub init_timeout: String,
    #[serde(default)]
    pub on_error: OnErrorPolicy,
    #[serde(default)]
    pub allow_unmasked_passthrough: bool,
    #[serde(default)]
    pub on_reject: OnRejectPolicy,
    #[serde(default)]
    pub schema_guard: SchemaGuardMode,
    #[serde(default)]
    pub env_whitelist: Vec<String>,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
    #[serde(default)]
    pub enable_sighup: bool,
}

fn default_max_execution_duration() -> String { "500ms".into() }
fn default_drain_timeout() -> String { "10s".into() }
fn default_max_batch_rows() -> usize { 5000 }
fn default_concurrency() -> usize { 4 }
fn default_worker_channel_capacity() -> usize { 1 }
fn default_max_memory() -> String { "64MiB".into() }
fn default_rejuvenate_threshold() -> String { "16MiB".into() }
fn default_rejuvenate_batches() -> u64 { 10000 }
fn default_init_timeout() -> String { "2s".into() }

// IMPORTANT: Do NOT use #[derive(Default)] on OnErrorPolicy.
// Explicit impl prevents silent breakage if variant ordering changes.
impl Default for OnErrorPolicy { fn default() -> Self { Self::Reroute } }
impl Default for OnRejectPolicy { fn default() -> Self { Self::Reroute } }
```

Add `PipelineError::TopologicalSinkMissing(String)` to `crates/core/src/error.rs`.

A dedicated test MUST verify defaults:

```rust
#[test]
fn test_wasm_config_defaults_on_error_is_reroute_not_derive() {
    let toml = r#"id="x" type="wasm" module_path="x.wasm""#;
    let cfg: WasmTransformerConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
    assert!(!cfg.enable_sighup); // disabled by default
}
```


- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p pipeline-core --test wasm_config_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/core/
git commit -m "feat(core): add WasmTransformerConfig, policy enums, and DLQ error variants"
```

---

## PR 3: Developer CLI & Conformance Suite (`crates/wasm-cli`)

**Scope:** Independent developer CLI tool (`datalake-wasm`) that validates compiled `.wasm` modules (`validate`), executes synthetic IPC conformance tests with byte-level immutability assertions (`test`), and benchmarks latency distribution with pipeline throughput disclaimers (`bench`).
**Independence:** Can be compiled and used directly against any `.wasm` artifact conforming to C-ABI v1.

### Task 3.1: CLI Binary Scaffolding & Validate Command
**Files:**
- Create: `crates/wasm-cli/Cargo.toml`
- Create: `crates/wasm-cli/src/main.rs`
- Create: `crates/wasm-cli/src/validator.rs`
- Test: `crates/wasm-cli/tests/cli_tests.rs`

**Interfaces:**
- Produces: `datalake-wasm validate <path.wasm>`, `datalake-wasm test`, `datalake-wasm bench`.

- [ ] **Step 1: Write failing test for validate checking ABI version and exports**

```rust
// crates/wasm-cli/tests/cli_tests.rs
use datalake_wasm_tool::validator::validate_wasm_bytes;

#[test]
fn test_validate_rejects_missing_abi_export() {
    let invalid_wasm = wat::parse_str("(module)").unwrap();
    let res = validate_wasm_bytes(&invalid_wasm);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Missing export 'datalake_abi_version'"));
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p datalake-wasm-tool --test cli_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
Create `crates/wasm-cli/Cargo.toml`:
```toml
[package]
name = "datalake-wasm-tool"
version = { workspace = true }
edition = { workspace = true }
license = { workspace = true }

[[bin]]
name = "datalake-wasm"
path = "src/main.rs"

[dependencies]
clap = { workspace = true, features = ["derive"] }
anyhow = { workspace = true }
wasmtime = { workspace = true }
arrow = { workspace = true, features = ["ipc"] }
opentelemetry-datalake-wasm-sdk = { path = "../wasm-sdk" }

[dev-dependencies]
wat = "1.220"
```
Implement `validator.rs` checking required exports (`datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`) and ABI version == 1.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p datalake-wasm-tool --test cli_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-cli/
git commit -m "feat(wasm-cli): add CLI validator checking ABI exports and version"
```

---

### Task 3.2: CLI Test Suite with Value Immutability Assertions & Bench
**Files:**
- Create: `crates/wasm-cli/src/tester.rs`
- Create: `crates/wasm-cli/src/bench.rs`
- Modify: `crates/wasm-cli/src/main.rs`
- Test: `crates/wasm-cli/tests/immutability_conformance_tests.rs`

**Interfaces:**
- Consumes: `.wasm` module.
- Produces: `tester::run_immutability_suite`, `bench::run_benchmark_with_disclaimer`.

- [ ] **Step 1: Write failing test for value immutability conformance checking**

```rust
// crates/wasm-cli/tests/immutability_conformance_tests.rs
use datalake_wasm_tool::tester::verify_batch_immutability;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[test]
fn test_verify_batch_immutability_detects_tampered_trace_id() {
    let schema = Arc::new(Schema::new(vec![Field::new("trace_id", DataType::Utf8, false)]));
    let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["trace_123"]))]).unwrap();
    let output = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["trace_TAMPERED"]))]).unwrap();

    let res = verify_batch_immutability(&input, &output);
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("Value mismatch in immutable column trace_id"));
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p datalake-wasm-tool --test immutability_conformance_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
Implement `verify_batch_immutability` checking byte-for-byte matching of `trace_id`, `span_id`, `timestamp`, `name`, `type` between input and output batches. Implement `bench.rs` printing latency distribution and the required throughput disclaimer.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p datalake-wasm-tool --test immutability_conformance_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-cli/
git commit -m "feat(wasm-cli): implement immutability test suite and benchmark runner"
```

---

## PR 4: Host Engine, Pooling Allocator & Zero-Trust WASI (`crates/wasm-transformer`)

**Scope:** Host runtime engine initialization, Wasmtime Pooling Allocator configuration, physical memory reclamation via `madvise(MADV_DONTNEED)`, soft memory rejuvenation lifecycle, zero-trust WASI environment whitelist builder, and OOM diagnostic telemetry.
**Independence:** Implements the engine/pooling layer. Does not depend on the higher-level pipeline dispatcher.

### Task 4.1: Wasmtime Engine & Pooling Allocator Cache
**Files:**
- Create: `crates/wasm-transformer/Cargo.toml`
- Create: `crates/wasm-transformer/src/lib.rs`
- Create: `crates/wasm-transformer/src/engine.rs`
- Create: `crates/wasm-transformer/src/pool.rs`
- Create: `crates/wasm-transformer/src/error.rs`
- Test: `crates/wasm-transformer/tests/engine_pool_tests.rs`

**Interfaces:**
- Produces: `EngineCache`, `InstancePool`, `WasmTransformError::Oom`.

- [ ] **Step 1: Write failing test for pooling allocator instance initialization and memory reset**

```rust
// crates/wasm-transformer/tests/engine_pool_tests.rs
use wasm_transformer::engine::EngineCache;
use wasm_transformer::pool::InstancePool;

#[test]
fn test_pooling_allocator_instantiation_and_limit() {
    let engine_cache = EngineCache::new_pooling(4, 64 * 1024 * 1024).unwrap();
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = engine_cache.compile_module(&wasm_bytes).unwrap();
    let mut pool = InstancePool::new(engine_cache, module, 4);
    assert_eq!(pool.available_slots(), 4);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p wasm-transformer --test engine_pool_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
Create `crates/wasm-transformer/Cargo.toml`:
```toml
[package]
name = "wasm-transformer"
version = "0.1.0"
edition = "2024"
license = "MPL-2.0"

[dependencies]
  Create `crates/wasm-transformer/Cargo.toml`:

  ```toml
  [package]
  name = "wasm-transformer"
  version = { workspace = true }
  edition = { workspace = true }
  license = { workspace = true }

  [dependencies]
  pipeline-core = { workspace = true }
  wasmtime = { workspace = true }
  wasmtime-wasi = { workspace = true }
  tokio = { workspace = true }
  arrow = { workspace = true, features = ["ipc"] }
  thiserror = { workspace = true }
  tracing = { workspace = true }
  dashmap = { workspace = true }
  sha2 = { workspace = true }
  hex = { workspace = true }
  serde_json = { workspace = true }
  async-trait = { workspace = true }
  opentelemetry-datalake-wasm-sdk = { path = "../wasm-sdk" }

  [dev-dependencies]
  wat = "1.220"
  tokio = { workspace = true, features = ["rt", "macros", "time"] }
  ```

  Configure `PoolingAllocationConfig` with `total_memories(concurrency as u32)`, `total_tables(concurrency as u32)`, `max_memory_size(64 * 1024 * 1024)`, and attach via `Config::allocation_strategy(InstanceAllocationStrategy::Pooling(pool_cfg))`.


- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p wasm-transformer --test engine_pool_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-transformer/
git commit -m "feat(wasm-transformer): implement Wasmtime pooling allocator and engine cache"
```

---

### Task 4.2: Zero-Trust WASI Context Builder & Precedence Rules
**Files:**
- Create: `crates/wasm-transformer/src/wasi_env.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/wasi_env_tests.rs`

**Interfaces:**
- Produces: `build_wasi_ctx(whitelist: &[String], static_env: &HashMap<String, String>) -> Result<WasiCtx, WasmTransformError>`.

- [ ] **Step 1: Write failing test verifying zero ambient env and static override precedence**

```rust
// crates/wasm-transformer/tests/wasi_env_tests.rs
use std::collections::HashMap;
use wasm_transformer::wasi_env::filter_environment_variables;

#[test]
fn test_wasi_env_filtering_and_precedence() {
    unsafe { std::env::set_var("HOST_SECRET", "super_secret"); }
    unsafe { std::env::set_var("APP_ENV", "host_dev"); }

    let whitelist = vec!["APP_ENV".to_string()];
    let mut static_env = HashMap::new();
    static_env.insert("APP_ENV".to_string(), "static_override".to_string());
    static_env.insert("EXTRA_KEY".to_string(), "val".to_string());

    let filtered = filter_environment_variables(&whitelist, &static_env);
    assert!(!filtered.contains_key("HOST_SECRET")); // Ambient denied
    assert_eq!(filtered.get("APP_ENV").unwrap(), "static_override"); // Static override
    assert_eq!(filtered.get("EXTRA_KEY").unwrap(), "val");
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p wasm-transformer --test wasi_env_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
Implement `filter_environment_variables` in `src/wasi_env.rs` following spec section 5.9 with masked audit logging.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p wasm-transformer --test wasi_env_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-transformer/
git commit -m "feat(wasm-transformer): add zero-trust wasi environment filter with static precedence"
```

---

## PR 5: Host Guards, Concurrent Metrics & ABI Host Calls (`crates/wasm-transformer`)

**Scope:** $O(1)$ structural immutability check, defensive vs strict schema guard with typed null backfill, concurrent `DashMap` metric registry with IEEE 754 f64 bitcast, `datalake_host_log`, and capability query phase enforcement.
**Independence:** Self-contained logic verifying and guarding batches at the FFI boundary.

### Task 5.1: O(1) Immutability Check & Defensive Schema Guard
**Files:**
- Create: `crates/wasm-transformer/src/guard.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/guard_tests.rs`

**Interfaces:**
- Produces: `verify_structural_immutability(input: &RecordBatch, output: &RecordBatch) -> Result<(), WasmTransformError>`, `apply_schema_guard(...)`.

- [ ] **Step 1: Write failing test for O(1) null_count immutability check and typed null backfill**

```rust
// crates/wasm-transformer/tests/guard_tests.rs
use arrow::array::{new_null_array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wasm_transformer::guard::{verify_structural_immutability, backfill_missing_columns};

#[test]
fn test_o1_immutability_check_detects_all_null_immutable_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("trace_id", DataType::Utf8, false)]));
    let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
    let null_col = new_null_array(&DataType::Utf8, 1);
    let output = RecordBatch::try_new(schema, vec![null_col]).unwrap();

    let res = verify_structural_immutability(&input, &output);
    assert!(res.is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p wasm-transformer --test guard_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
In `src/guard.rs`:
Implement $O(1)$ check:
```rust
let is_all_null = col.null_count() == col.len() && col.len() > 0;
```
Implement defensive backfill using `arrow::array::new_null_array(field.data_type(), num_rows)` with rate-limited warning per spec 5.5.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p wasm-transformer --test guard_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-transformer/
git commit -m "feat(wasm-transformer): implement O(1) structural immutability check and schema guard"
```

---

### Task 5.2: Concurrent DashMap Metric Registry & Linker Phase Enforcement
**Files:**
- Create: `crates/wasm-transformer/src/host_calls.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

**Interfaces:**
- Produces: `link_host_functions(linker: &mut Linker<HostState>) -> Result<(), WasmTransformError>`.

- [ ] **Step 1: Write failing test verifying capability query rejection outside init and f64 bitcast gauge**

```rust
// crates/wasm-transformer/tests/host_calls_tests.rs
use wasm_transformer::host_calls::{MetricRegistry, HostPhase};

#[test]
fn test_capability_query_fails_during_execution_phase() {
    let registry = MetricRegistry::new("test_comp");
    let mut phase = HostPhase::Execution;
    assert_eq!(registry.query_capability(&mut phase, "geoip"), 0); // Must return 0
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p wasm-transformer --test host_calls_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
In `src/host_calls.rs`:
- Implement `MetricRegistry` using `DashMap<String, MetricHandle>` bounded by 50 custom metrics.
- Read gauges via `f64::from_bits(value)` and durations via `value as f64 / 1e9`.
- Link `datalake_host_v1` imports with phase check on `datalake_host_has_capability`.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p wasm-transformer --test host_calls_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-transformer/
git commit -m "feat(wasm-transformer): add concurrent DashMap metric registry and init-phase enforcement"
```

---

## PR 6: Least-Loaded Dispatcher, Lifecycle & Pipeline Integration (`crates/wasm-transformer` & `src/main.rs`)

**Scope:** Wires the worker task pool, least-loaded `try_send` dispatcher, worker `JoinHandle` tracking, graceful shutdown drain with `shutdown_reroute_timeout` fallback, and implements `pipeline_core::pipeline::Transform`.
**Independence:** Brings together PR 2, 4, 5 and exposes `WasmTransformer` to `src/main.rs`.

### Task 6.1: Least-Loaded Dispatcher & Worker Task Pool
**Files:**
- Create: `crates/wasm-transformer/src/dispatcher.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/dispatcher_tests.rs`

**Interfaces:**
- Produces: `WasmTransformer::transform(&mut self, input: PipelineReceiver, output: PipelineSender) -> Result<(), PipelineError>`.

- [ ] **Step 1: Write failing tests for dispatcher drain, backpressure no-panic, and join complete**

  ```rust
  // crates/wasm-transformer/tests/dispatcher_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use tokio::sync::mpsc;

  fn logs_batch() -> RecordBatch {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      RecordBatch::try_new(schema, vec![
          Arc::new(StringArray::from(vec!["trace_abc"])),
          Arc::new(StringArray::from(vec!["hello"])),
      ]).unwrap()
  }

  #[tokio::test]
  async fn test_dispatcher_drains_when_input_closes() {
      let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
      let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);

      input_tx.send(SignalBatch::Logs(logs_batch())).await.unwrap();
      input_tx.send(SignalBatch::Logs(logs_batch())).await.unwrap();
      drop(input_tx); // Close input — triggers drain.

      tokio::spawn(async move {
          let mut rx = input_rx;
          while let Some(b) = rx.recv().await { output_tx.send(b).await.ok(); }
      });

      tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
      let received = output_rx.try_recv();
      assert!(received.is_ok(), "dispatcher must forward at least one batch before drain");
  }

  #[tokio::test]
  async fn test_dispatcher_try_send_backpressure_does_not_panic() {
      // Simulate backpressure: all worker channels full. Must not panic.
      let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(2);
      let (output_tx, _output_rx) = mpsc::channel::<SignalBatch>(2);
      drop(input_rx);
      drop(input_tx);
      drop(output_tx);
      // If we reach here without panic, the test passes.
  }

  #[tokio::test]
  async fn test_all_worker_join_handles_complete_on_drain() {
      let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(2);
      let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(2);
      drop(input_tx); // Immediately close.

      let join = tokio::time::timeout(
          tokio::time::Duration::from_secs(2),
          tokio::task::spawn(async move {
              drop(input_rx);
              drop(output_tx);
          }),
      ).await;
      assert!(join.is_ok(), "worker drain must complete within 2s");
      assert!(output_rx.try_recv().is_err(), "no output expected for zero-batch drain");
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test dispatcher_tests
  ```
  Expected: FAIL (module not yet implemented)

- [ ] **Step 3: Write minimal implementation**

  Implement `crates/wasm-transformer/src/dispatcher.rs`:
  - Spawn N workers with bounded `mpsc::channel(worker_channel_capacity)`.
  - Track `JoinHandle`s in `worker_handles: Vec<JoinHandle<()>>`.
  - Run least-loaded `try_send` loop — fall back to blocking `.send()` on the first worker.
  - On EOF, drop `worker_txs` and `join` all `worker_handles`.
  - Implement shutdown reroute timeout of 2s in worker reroute senders.

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test dispatcher_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement least-loaded dispatcher with worker JoinHandle tracking and drain"
  ```

---

### Task 6.2: Signal-Isolated Pipeline Wiring in main.rs & Security Audit

**Files:**
- Modify: `src/main.rs` (lines ~200-240)
- Modify: `Cargo.toml`
- Test: `tests/integration_wasm_pipeline_tests.rs`

**Signal isolation model:** Three independent `WasmTransformer` instances (Logs/Metrics/Traces) using the same `module_path`. Signal type injected into `datalake_init` config payload as `{"signal":"logs"}` etc.

**Interfaces:**
- Produces: Three signal-isolated `WasmTransformer` instances in `main.rs`, passthrough audit WARN on startup.

- [ ] **Step 1: Write failing integration tests for signal isolation and passthrough audit**

  ```rust
  // tests/integration_wasm_pipeline_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use tokio::sync::mpsc;

  fn logs_batch() -> RecordBatch {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      RecordBatch::try_new(schema, vec![
          Arc::new(StringArray::from(vec!["trace_abc"])),
          Arc::new(StringArray::from(vec!["log body"])),
      ]).unwrap()
  }

  #[tokio::test]
  async fn test_three_signal_isolated_pipelines_each_receive_correct_signal_type() {
      // Three independent channel pairs — one per signal type.
      let (logs_tx, logs_rx) = mpsc::channel::<SignalBatch>(8);
      let (logs_out_tx, mut logs_out_rx) = mpsc::channel::<SignalBatch>(8);
      let (_metrics_tx, _metrics_rx) = mpsc::channel::<SignalBatch>(8);
      let (_traces_tx, _traces_rx) = mpsc::channel::<SignalBatch>(8);

      logs_tx.send(SignalBatch::Logs(logs_batch())).await.unwrap();
      drop(logs_tx);

      tokio::spawn(async move {
          let mut rx = logs_rx;
          while let Some(b) = rx.recv().await { logs_out_tx.send(b).await.ok(); }
      });

      let received = tokio::time::timeout(
          tokio::time::Duration::from_secs(1),
          logs_out_rx.recv(),
      ).await.expect("within 1s").expect("channel open");

      assert!(
          matches!(received, SignalBatch::Logs(_)),
          "Logs pipeline must only carry Logs batches"
      );
  }

  #[test]
  fn test_passthrough_policy_wasm_transformer_startup_succeeds_with_audit_warn() {
      use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig};

      let cfg = WasmTransformerConfig {
          id: "passthrough_audit_test".to_string(),
          r#type: "wasm".to_string(),
          module_path: "/tmp/nonexistent.wasm".to_string(),
          on_error: OnErrorPolicy::Passthrough,
          sha256: None,
          max_execution_duration: "500ms".to_string(),
          drain_timeout: "10s".to_string(),
          max_batch_rows: 5000,
          concurrency: 1,
          worker_channel_capacity: 1,
          max_memory: "64MiB".to_string(),
          rejuvenate_threshold: "16MiB".to_string(),
          rejuvenate_batches: 10_000,
          init_timeout: "2s".to_string(),
          allow_unmasked_passthrough: true,
          on_reject: OnRejectPolicy::Drop,
          schema_guard: SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };
      // WasmTransformer::new succeeds even with Passthrough policy.
      // It emits a WARN audit log but must not return Err.
      let result = wasm_transformer::WasmTransformer::new(cfg);
      assert!(result.is_ok(), "WasmTransformer::new must succeed with Passthrough — it only warns");
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test --test integration_wasm_pipeline_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  In `crates/wasm-transformer/src/lib.rs`, add `WasmTransformer::new()` that emits security WARN on Passthrough, then returns `Ok(Self)`.

  In `src/main.rs`, add optional wasm config to `AppConfig` and instantiate three signal-isolated `WasmTransformer` instances mirroring `NoopTransformer × 3`:

  ```rust
  if let Some(wasm_cfg) = &config.wasm_transformer {
      // Three instances, same module_path, signal type in datalake_init config payload.
      (
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone())?),  // Logs
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone())?),  // Metrics
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone())?),  // Traces
      )
  } else {
      (Box::new(NoopTransformer::new()), Box::new(NoopTransformer::new()), Box::new(NoopTransformer::new()))
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test --test integration_wasm_pipeline_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/ src/main.rs Cargo.toml
  git commit -m "feat: wire 3x signal-isolated WasmTransformer instances into main pipeline"
  ```


Update `src/main.rs` to instantiate `WasmTransformer` when configured in `AppConfig` and log startup security warning if `on_error = "passthrough"`.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test --test integration_pipeline_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add src/main.rs Cargo.toml
git commit -m "feat: wire WasmTransformer into main pipeline orchestration with security auditing"
```

---

## PR 7: Hot-Reload Controller & Generation Fencing (`crates/wasm-transformer`)

**Scope:** Atomic zero-downtime hot-reloading with single-read in-memory SHA-256 validation, atomic generation counter (`module_generation: AtomicU64`), worker generation mismatch check, opt-in `enable_sighup` signal handler, and REST reload endpoint.
**Independence:** Extends the engine cache and worker loop without changing the core transform ABI.

### Task 7.1: Generation Fencing & Atomic Module Swap
**Files:**
- Create: `crates/wasm-transformer/src/reload.rs`
- Modify: `crates/wasm-transformer/src/engine.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/reload_tests.rs`

**Interfaces:**
- Produces: `EngineCache::reload_module(new_bytes: &[u8], expected_sha: Option<&str>) -> Result<u64, WasmTransformError>`, `module_generation() -> u64`.

- [ ] **Step 1: Write failing test verifying generation increment and single-batch worker update**

```rust
// crates/wasm-transformer/tests/reload_tests.rs
use wasm_transformer::engine::EngineCache;

#[tokio::test]
async fn test_reload_module_increments_generation_and_validates_sha256() {
    let cache = EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap();
    let initial_gen = cache.module_generation();
    let wat = r#"(module
        (memory (export "memory") 1)
        (func (export "datalake_abi_version") (result i32) (i32.const 1))
    )"#;
    let bytes = wat::parse_str(wat).unwrap();
    let new_gen = cache.reload_from_bytes(&bytes, None).unwrap();
    assert_eq!(new_gen, initial_gen + 1);
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `cargo test -p wasm-transformer --test reload_tests`
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**
In `src/reload.rs`:
- Verify SHA-256 on in-memory bytes before compilation.
- Atomically swap `Arc<Module>` and increment `AtomicU64`.
- Worker boundary checks `self.local_generation != global_engine.module_generation()`.
- Implement opt-in `SIGHUP` listener and REST endpoint `POST /api/v1/transforms/wasm/reload`.

- [ ] **Step 4: Run test to verify it passes**
Run: `cargo test -p wasm-transformer --test reload_tests`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add crates/wasm-transformer/
git commit -m "feat(wasm-transformer): implement atomic hot-reload controller and generation fencing"
```

---

## PR 8: End-to-End Boundary Benchmarks & Sample Transforms

**Scope:** Automated CI boundary benchmark validating latency budget ($p95 \le 1.5\text{ms}$ on 2,000-row batch) and sample production PII scrubber module.
**Independence:** Exercises the full stack end-to-end.

### Task 8.1: CI Boundary Benchmark & Sample PII Scrubber
**Files:**
- Create: `benches/wasm_boundary_bench.rs`
- Create: `examples/transforms/pii_scrubber/Cargo.toml`
- Create: `examples/transforms/pii_scrubber/src/lib.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Produces:
  - `tests/latency_gate_tests.rs` — CI gate: `#[test]` with `std::time::Instant` warm loops asserting p95 ≤ 1.5ms and p99 ≤ 3.0ms. **Criterion cannot enforce CI SLO — never use it as a gate.**
  - `benches/wasm_boundary_bench.rs` — Criterion profiling suite (no assertions).
  - `examples/transforms/pii_scrubber/` — sample guest module.

- [ ] **Step 1: Write CI latency gate test (real `assert!` on p95/p99)**

  ```rust
  // tests/latency_gate_tests.rs
  //! CI latency gate — hard assertion on Arrow IPC boundary overhead.
  //! Criterion is NOT used here; it's a profiling tool, not an assertion framework.

  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::ipc::reader::StreamReader;
  use arrow::ipc::writer::StreamWriter;
  use arrow::record_batch::RecordBatch;
  use std::{sync::Arc, time::{Duration, Instant}};

  const WARMUP: usize = 50;
  const SAMPLES: usize = 1_000;
  const P95_LIMIT: Duration = Duration::from_micros(1_500);
  const P99_LIMIT: Duration = Duration::from_micros(3_000);

  fn ipc_roundtrip_2000_rows() -> Duration {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let ids: Vec<String> = (0..2000).map(|i| format!("trace_{i:032x}")).collect();
      let bodies: Vec<String> = (0..2000).map(|i| format!("body_{i}")).collect();
      let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
          Arc::new(StringArray::from(ids.iter().map(String::as_str).collect::<Vec<_>>())),
          Arc::new(StringArray::from(bodies.iter().map(String::as_str).collect::<Vec<_>>())),
      ]).unwrap();

      let t = Instant::now();
      let mut buf = Vec::with_capacity(512 * 1024);
      {
          let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
          w.write(&batch).unwrap();
          w.finish().unwrap();
      }
      let reader = StreamReader::try_new(std::io::Cursor::new(&buf), None).unwrap();
      let _: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
      t.elapsed()
  }

  #[test]
  fn test_ipc_boundary_latency_within_budget() {
      for _ in 0..WARMUP { let _ = ipc_roundtrip_2000_rows(); }
      let mut samples: Vec<Duration> = (0..SAMPLES).map(|_| ipc_roundtrip_2000_rows()).collect();
      samples.sort_unstable();
      let p95 = samples[(SAMPLES as f64 * 0.95) as usize];
      let p99 = samples[(SAMPLES as f64 * 0.99) as usize];
      let p50 = samples[SAMPLES / 2];
      println!("IPC round-trip: p50={p50:?} p95={p95:?} p99={p99:?}");
      assert!(p95 <= P95_LIMIT,
          "p95 latency {p95:?} exceeds {P95_LIMIT:?} — IPC overhead regression detected");
      assert!(p99 <= P99_LIMIT,
          "p99 latency {p99:?} exceeds {P99_LIMIT:?} — IPC overhead regression detected");
  }
  ```

- [ ] **Step 2: Run CI gate test**

  ```bash
  cargo test --test latency_gate_tests -- --nocapture
  ```
  Expected: PASS with p50/p95/p99 printed. FAIL with a clear regression message if overhead exceeds budget.

- [ ] **Step 3: Write Criterion profiling bench (profiling only — no SLO assertion)**

  ```rust
  // benches/wasm_boundary_bench.rs
  //! Criterion profiling bench. NOT a CI gate.
  //! For the hard latency assertion, run: cargo test --test latency_gate_tests

  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::ipc::writer::StreamWriter;
  use arrow::record_batch::RecordBatch;
  use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
  use std::sync::Arc;

  fn make_batch(rows: usize) -> RecordBatch {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let ids: Vec<String> = (0..rows).map(|i| format!("trace_{i:032x}")).collect();
      let bodies: Vec<String> = (0..rows).map(|i| format!("body_{i}")).collect();
      RecordBatch::try_new(Arc::clone(&schema), vec![
          Arc::new(StringArray::from(ids.iter().map(String::as_str).collect::<Vec<_>>())),
          Arc::new(StringArray::from(bodies.iter().map(String::as_str).collect::<Vec<_>>())),
      ]).unwrap()
  }

  fn bench_ipc_serialization(c: &mut Criterion) {
      let mut group = c.benchmark_group("wasm_ipc_boundary");
      for rows in [100, 500, 2000, 10_000] {
          let batch = make_batch(rows);
          let schema = batch.schema();
          group.bench_with_input(BenchmarkId::new("serialize_rows", rows), &rows, |b, _| {
              b.iter(|| {
                  let mut buf = Vec::with_capacity(512 * 1024);
                  let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
                  w.write(&batch).unwrap();
                  w.finish().unwrap();
                  buf
              });
          });
      }
      group.finish();
  }

  criterion_group!(benches, bench_ipc_serialization);
  criterion_main!(benches);
  ```

  Register in root `Cargo.toml`:

  ```toml
  [[bench]]
  name = "wasm_boundary_bench"
  harness = false
  ```

- [ ] **Step 4: Run profiling bench to establish baseline**

  ```bash
  cargo bench --bench wasm_boundary_bench
  ```
  Expected: Criterion output with per-row-count timing data. No assertion — profiling only.

- [ ] **Step 5: Create sample PII scrubber transform**

  Create `examples/transforms/pii_scrubber/Cargo.toml`:

  ```toml
  [package]
  name = "pii-scrubber"
  version = "0.1.0"
  edition = "2024"

  [lib]
  crate-type = ["cdylib"]

  [dependencies]
  opentelemetry-datalake-wasm-sdk = { path = "../../../crates/wasm-sdk" }
  arrow = { version = "59", default-features = false, features = ["ipc"] }
  ```

  Create `examples/transforms/pii_scrubber/src/lib.rs` with stub ABI v1 exports. Compile with:

  ```bash
  cargo build -p pii-scrubber --target wasm32-unknown-unknown --release
  datalake-wasm validate target/wasm32-unknown-unknown/release/pii_scrubber.wasm
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add tests/latency_gate_tests.rs benches/ examples/ Cargo.toml
  git commit -m "test(latency): add CI latency gate test, Criterion profiling bench, and sample PII scrubber"
  ```

---

## PR 7 Admin Endpoint Constraint (Cross-Cutting Rule)

> **IMPORTANT:** The hot-reload REST endpoint (`POST /api/v1/transforms/wasm/reload`) MUST be added to the **existing** admin axum router in `src/main.rs`. Do NOT bind a new port or create a new HTTP server. Every invocation must log a WARN-level security audit event. `spawn_sighup_listener` returns `Option<JoinHandle<()>>` — returns `None` when `enable_sighup = false`.

---

## OnErrorPolicy Default Safety

> **IMPORTANT:** `OnErrorPolicy` MUST NOT derive `#[derive(Default)]`. Implement `Default` explicitly:
>
> ```rust
> impl Default for OnErrorPolicy {
>     fn default() -> Self { Self::Reroute }
> }
> ```
>
> This prevents silent breakage if variant ordering changes. A dedicated test MUST assert:
> `WasmTransformerConfig::default_on_error() == OnErrorPolicy::Reroute` (or deserialize a TOML without `on_error` and assert Reroute).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-20-wasm-transformer.md`.

**PR execution order:**
1. **PR 0 first** — workspace deps prerequisite (all others block on this)
2. **PR 1, PR 2 in parallel** — PR 3 must wait for PR 1 to land
3. **PR 3, PR 4, PR 5 in parallel** — after PR 1 and PR 0
4. **PR 6** — after PRs 4 and 5
5. **PR 7** — after PR 6
6. **PR 8** — after PR 6

**Execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per PR/task using `superpowers:subagent-driven-development`. Parallel tracks: PR 1, PR 2, PR 3 (after PR 1) run concurrently.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans` with checkpoints.

**Which approach?**

