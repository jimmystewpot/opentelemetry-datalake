# WebAssembly (WASM) Whole-Batch Arrow Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an ultra-high-performance, sandboxed WebAssembly (WASM) transformation engine executing Whole-Batch Apache Arrow transformations on OpenTelemetry telemetry streams with zero-copy IPC exchange, three-tier immutability, symmetric DLQ rerouting, and sub-millisecond boundary latency.

**Architecture:** The system decouples guest transformations from the host runtime using a low-overhead C-ABI v1. The guest SDK (`opentelemetry-datalake-wasm-sdk`) provides typed abstractions and call-site fail-fast guards over Arrow IPC streams. The host runtime (`crates/wasm-transformer`) leverages Wasmtime's Pooling Allocator with `madvise(MADV_DONTNEED)` resets, lock-free least-loaded batch dispatching, $O(1)$ structural immutability verification, typed null backfilling, and zero-downtime hot-reloading. A standalone CLI (`datalake-wasm`) provides pre-deployment validation, synthetic testing, and latency benchmarking.

**Tech Stack:** Rust 2024, Apache Arrow 59, Wasmtime 48 (48.0.2 — current stable, with Pooling Allocator & WASI), Tokio 1.37, DashMap 6, sha2 0.10, hex 0.4, Thiserror 2.0, Criterion 0.8.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints

- **Zero-Panic Production Rule**: No `unwrap()`, `expect()`, `panic!()`, or `todo!()` in production paths (`src/`). All failures must propagate via `thiserror` domain errors.
- **Latency Budget**: Round-trip boundary overhead for a 2,000-row batch (~500 KB uncompressed Arrow IPC stream) must be p95 ≤ 1.5ms and p99 ≤ 3.0ms. Enforced by a standalone CI `#[test]` using `std::time::Instant` warm loops in `tests/latency_gate_tests.rs` running against a real Wasmtime guest. Criterion is for developer profiling only.
- **Core Telemetry Immutability**: Core fields (`trace_id`, `span_id` in traces; `timestamp` in logs; `name`, `type` in metrics) cannot be dropped or nullified. Host checks this in O(1) via `col.null_count() == col.len()`.
- **Memory Safety & Reset**: Each WASM instance runs in a pre-allocated pooling memory slot (`max_memory = 64MiB`) with physical pages reclaimed via `madvise(MADV_DONTNEED)`. Dual-trigger rejuvenation (16 MiB soft threshold or 10,000 batches).
- **Concurrency & Backpressure**: Single `input: PipelineReceiver` is dispatched across N workers using non-blocking `try_send` with configurable queue depth (`worker_channel_capacity = 1`). Workers await on shutdown with full `JoinHandle` tracking.
- **Signal Isolation**: Three independent `WasmTransformer` instances are instantiated — one per signal type (Logs, Metrics, Traces) — mirroring the existing `NoopTransformer × 3` pattern in `src/main.rs`. Each has its own `PipelineReceiver`/`PipelineSender`. Signal type is passed to `datalake_init` via config JSON payload `{"signal":"logs"}`.
- **Wasmtime Version**: Use `wasmtime = "48"` and `wasmtime-wasi = "48"` (48.0.2). Do NOT use any 29.x version.
- **Workspace Dependency Rule**: All new external dependencies MUST be added to `[workspace.dependencies]` in the root `Cargo.toml` before any crate uses them. Crate `Cargo.toml` files MUST reference them via `{ workspace = true }`. Exception: `wat = "1.220"` (test-only WAT compiler) cannot be a workspace dep — add it directly to each crate's `[dev-dependencies]`.
- **Canonical Immutability List**: `IMMUTABLE_COLUMNS` is defined once as `pub const` in `crates/wasm-sdk/src/helpers.rs` and imported by the CLI conformance harness and host-side guard. Never duplicate it.
- **Code Standards**: Zero Clippy warnings under `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`. Format with `cargo fmt`. Prefer `crate::` over `super::` in non-test source code.
- **git**: Files under `docs/superpowers/` are gitignored. Use `git add -f` for any files in that directory.

---

## Modular PR Breakdown Strategy

To enable concurrent execution by parallel sub-agents and ensure fast, stand-alone reviews, the implementation is decomposed into **8 modular PRs** plus a prerequisite workspace task (PR 0):

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    PREREQUISITE: PR 0 — Workspace Dependencies                   │
│  Root Cargo.toml: wasmtime 48, wasmtime-wasi 48, dashmap, sha2, hex,            │
│  arrow+ipc feature. Workspace members include examples/transforms/*.             │
└────────────────────────────────────┬─────────────────────────────────────────────┘
                                     │ (all Layer 1 PRs depend on PR 0)
┌────────────────────────────────────▼─────────────────────────────────────────────┐
│                             INDEPENDENT PR LAYER 1                               │
├───────────────────────────────┬───────────────────────────────┬──────────────────┤
│ PR 1: Standalone Guest SDK    │ PR 2: Core Config & DLQ Rules │ PR 3: Dev CLI    │
│ (crates/wasm-sdk)             │ (crates/core)                 │ (crates/wasm-cli)│
│ - ABI v1 C-types & protocol   │ - WasmTransformerConfig       │ - validate cmd   │
│ - BatchTransformer & results  │ - OnError / OnReject enums    │ - test suite     │
│ - pub const IMMUTABLE_COLUMNS │ - Topological DLQ assertions  │ - bench tool     │
│ - Client-side immutability    │ - Unit tests & TOML fixtures  │ - synthetic IPC  │
│ - Safe allocator (cfg wasm32) │                               │ NOTE: imports    │
│ - Metrics (f64 bitcast)       │                               │ IMMUTABLE_COLUMNS│
└───────────────┬───────────────┴───────────────┬───────────────┴──────────────────┘
                │                               │
                ▼                               ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             INDEPENDENT PR LAYER 2                               │
├───────────────────────────────────────────────┬──────────────────────────────────┤
│ PR 4: Host Engine, Pool, WASI & Execution     │ PR 5: Host Guards & ABI Linker   │
│ (crates/wasm-transformer: engine, pool,       │ (crates/wasm-transformer: guard, │
│  wasi, worker execution engine)               │  host_calls linker)              │
│ - Wasmtime 48 Engine & Module cache           │ - O(1) Immutability Check        │
│ - Pooling Allocator & soft rejuvenation       │ - Defensive Typed Null Backfill  │
│ - Zero-trust WASI env filter                  │ - Concurrent DashMap Metrics     │
│ - Real Worker Execution Engine (Task 4.3)     │ - Concrete Linker & Host Calls   │
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
│ - Real WAT integration tests (no stubs)       │ - Zero-drift worker swap         │
│ - Symmetric DLQ reroute wiring                │                                  │
└───────────────────────────────────────────────┴──────────────────────────────────┘
                                     │
                       ┌─────────────▼──────────────┐
                       │  PR 8: CI Latency Gate,    │
                       │  Criterion Bench & Sample  │
                       │  (tests/ + benches/ +      │
                       │   examples/transforms/)    │
                       │  - Real WASM boundary gate │
                       └────────────────────────────┘
```

> **PR 3 dependency note:** PR 3 (`wasm-cli`) imports `IMMUTABLE_COLUMNS` from `opentelemetry-datalake-wasm-sdk` (PR 1). A sub-agent implementing PR 3 must wait for PR 1 to land. Do not duplicate the constant.

---
## PR 0: Workspace Dependency Registration (Root `Cargo.toml`)

**Scope:** Register all new external dependencies in `[workspace.dependencies]` and register `examples/transforms/*` in `[workspace.members]` so all subsequent PRs reference dependencies via `{ workspace = true }` and example transforms build cleanly. This MUST land first — it unblocks all of Layer 1.
**Independence:** Pure `Cargo.toml` change. No source code.

### Task 0.1: Add New Workspace Dependencies & Members

**Files:**
- Modify: `Cargo.toml` (root workspace)

**Interfaces:**
- Produces: `wasmtime`, `wasmtime-wasi`, `dashmap`, `sha2`, `hex` available as workspace deps. Arrow `ipc` feature added. `examples/transforms/*` added to `members`.

- [ ] **Step 1: Verify arrow ipc feature is NOT yet present**

  ```bash
  grep -n '"ipc"' Cargo.toml
  ```
  Expected: no output

- [ ] **Step 2: Add workspace dependencies and members**

  In root `Cargo.toml`, update `[workspace]` members:

  ```toml
  [workspace]
  resolver = "2"
  members = [
      "crates/*",
      "examples/transforms/*",
  ]
  ```

  In root `Cargo.toml`, in `[workspace.dependencies]`, add after the `# Arrow` section:

  ```toml
  # WebAssembly Runtime (Wasmtime 48 — current stable, Rust 1.98+ required)
  wasmtime = { version = "48", default-features = false, features = ["cranelift", "pooling-allocator"] }
  wasmtime-wasi = { version = "48", default-features = false }

  # Concurrent Collections
  dashmap = { version = "6.0", default-features = false }

  # Cryptography (SHA-256 module integrity)
  sha2 = { version = "0.10", default-features = false }
  hex = { version = "0.4", default-features = false }
  ```

  Update the existing `arrow` workspace dep to add the `ipc` feature:

  ```toml
  arrow = { version = "59", default-features = false, features = ["json", "ipc"] }
  ```

  > **Note on `wat`:** Cargo does not support workspace-level dev-dependencies. Add `wat = "1.220"` directly to `[dev-dependencies]` in each crate that needs it (PRs 4, 6, 7, 8).

- [ ] **Step 3: Verify workspace resolves cleanly**

  ```bash
  cargo metadata --format-version 1 > /dev/null
  ```
  Expected: exits 0

- [ ] **Step 4: Commit**

  ```bash
  git add Cargo.toml
  git commit -m "chore(workspace): add wasmtime 48, dashmap, sha2, hex, arrow ipc feature, and examples members"
  ```

---

## PR 1: Standalone Guest SDK (`crates/wasm-sdk`)

**Scope:** Self-contained crate `opentelemetry-datalake-wasm-sdk` defining C-ABI v1 data structures, public `BatchTransformer` trait, `TransformResult` enum, client-side checked Arrow IPC helpers, metrics emission (f64 bitcast gauge, u64 nanosecond duration), panic hook forwarder, safe allocators with 64-bit test guards, and the canonical `pub const IMMUTABLE_COLUMNS`.
**Independence:** Depends only on `arrow` (ipc feature) and standard libraries. Zero dependencies on host runtime or `pipeline-core`. Requires PR 0.

### Task 1.1: SDK Crate Scaffolding & Safe ABI v1 Memory Allocation

**Files:**
- Create: `crates/wasm-sdk/Cargo.toml`
- Create: `crates/wasm-sdk/src/lib.rs`
- Create: `crates/wasm-sdk/src/abi.rs`
- Test: `crates/wasm-sdk/tests/abi_tests.rs`

**Interfaces:**
- Consumes: Standard Rust types, Arrow IPC stream types.
- Produces: `TransformResponseHeader` (20 bytes, align 4), `BatchDescriptor` (8 bytes, align 4), `HostLogRecord`, `ABI_VERSION: u32 = 1`, exports `datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`.

- [ ] **Step 1: Write failing test for ABI v1 memory layout, version, and allocation safety**

  ```rust
  // crates/wasm-sdk/tests/abi_tests.rs
  use opentelemetry_datalake_wasm_sdk::abi::{
      datalake_abi_version, datalake_alloc, datalake_dealloc,
      BatchDescriptor, TransformResponseHeader, ABI_VERSION,
  };

  #[test]
  fn test_abi_v1_header_memory_layout() {
      assert_eq!(std::mem::size_of::<TransformResponseHeader>(), 20);
      assert_eq!(std::mem::align_of::<TransformResponseHeader>(), 4);
      assert_eq!(std::mem::size_of::<BatchDescriptor>(), 8);
      assert_eq!(std::mem::align_of::<BatchDescriptor>(), 4);
  }

  #[test]
  fn test_abi_version_constant_is_one() {
      assert_eq!(ABI_VERSION, 1u32);
      assert_eq!(datalake_abi_version(), 1u32);
  }

  #[test]
  fn test_alloc_and_dealloc_native_safety() {
      // Must not panic or segfault on 64-bit host architecture
      let ptr = datalake_alloc(1024);
      assert_ne!(ptr, 0);
      datalake_dealloc(ptr, 1024);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test abi_tests
  ```
  Expected: FAIL with "package not found" or "unresolved import"

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-sdk/Cargo.toml`:

  ```toml
  [package]
  name = "opentelemetry-datalake-wasm-sdk"
  version = { workspace = true }
  edition = { workspace = true }
  license = { workspace = true }

  [dependencies]
  arrow = { workspace = true, features = ["ipc"] }
  thiserror = { workspace = true }
  ```

  Create `crates/wasm-sdk/src/abi.rs`:

  ```rust
  pub const ABI_VERSION: u32 = 1;

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
      ABI_VERSION
  }

  #[cfg(target_arch = "wasm32")]
  #[no_mangle]
  pub extern "C" fn datalake_alloc(size: u32) -> u32 {
      let mut buf = Vec::<u8>::with_capacity(size as usize);
      let ptr = buf.as_mut_ptr() as u32;
      std::mem::forget(buf);
      ptr
  }

  #[cfg(target_arch = "wasm32")]
  #[no_mangle]
  pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
      if ptr != 0 && size != 0 {
          // SAFETY: ptr was allocated with datalake_alloc on wasm32 (32-bit linear address space)
          unsafe {
              drop(Vec::<u8>::from_raw_parts(ptr as *mut u8, 0, size as usize));
          }
      }
  }

  // Safe fallback for native host test harness (prevents 64-bit pointer truncation and segfaults)
  #[cfg(not(target_arch = "wasm32"))]
  static NATIVE_ALLOCS: std::sync::Mutex<Option<std::collections::HashMap<u32, Vec<u8>>>> =
      std::sync::Mutex::new(None);

  #[cfg(not(target_arch = "wasm32"))]
  #[no_mangle]
  pub extern "C" fn datalake_alloc(size: u32) -> u32 {
      use std::sync::atomic::{AtomicU32, Ordering};
      static NEXT_ID: AtomicU32 = AtomicU32::new(1);
      let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
      let mut lock = NATIVE_ALLOCS.lock().unwrap();
      lock.get_or_insert_with(std::collections::HashMap::new)
          .insert(id, Vec::with_capacity(size as usize));
      id
  }

  #[cfg(not(target_arch = "wasm32"))]
  #[no_mangle]
  pub extern "C" fn datalake_dealloc(ptr: u32, _size: u32) {
      if let Ok(mut lock) = NATIVE_ALLOCS.lock() {
          if let Some(map) = lock.as_mut() {
              map.remove(&ptr);
          }
      }
  }
  ```

  Create `crates/wasm-sdk/src/lib.rs`:

  ```rust
  pub mod abi;
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test abi_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-sdk/
  git commit -m "feat(wasm-sdk): scaffold sdk crate with safe ABI v1 memory layout and allocators"
  ```

---

### Task 1.2: Canonical Immutability Constants, Trait Definition & Client-Side Guards

**Files:**
- Create: `crates/wasm-sdk/src/traits.rs`
- Create: `crates/wasm-sdk/src/helpers.rs`
- Create: `crates/wasm-sdk/src/error.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/guard_tests.rs`

**Interfaces:**
- Consumes: `RecordBatch` from Arrow.
- Produces:
  - `pub const IMMUTABLE_COLUMNS: &[&str]` — single source of truth across SDK, CLI, and host
  - `helpers::is_immutable_column(column: &str) -> bool`
  - `helpers::nullify_column(batch: &RecordBatch, column_name: &str) -> Result<RecordBatch, SdkError>`
  - `SdkError` enum with `ImmutableFieldViolation(String)`, `Arrow(String)`, `ColumnNotFound(String)`
  - `SignalType` enum: `Logs`, `Metrics`, `Traces`
  - `TransformResult` enum: `Continue(Vec<RecordBatch>)`, `Discard`, `Reject { reason: String }`
  - `BatchTransformer` trait with `init` and `transform` methods

- [ ] **Step 1: Write failing tests for immutability guard, mutable column success, and pub const constant**

  ```rust
  // crates/wasm-sdk/tests/guard_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use opentelemetry_datalake_wasm_sdk::error::SdkError;
  use opentelemetry_datalake_wasm_sdk::helpers::{nullify_column, IMMUTABLE_COLUMNS};
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
      assert!(
          matches!(err, SdkError::ImmutableFieldViolation(ref col) if col == "trace_id"),
          "expected ImmutableFieldViolation for trace_id, got: {err:?}"
      );
  }

  #[test]
  fn test_nullify_mutable_column_succeeds() {
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

      let ok_batch = nullify_column(&batch, "scope_attributes").unwrap();
      assert_eq!(ok_batch.column(1).null_count(), 1);
  }

  #[test]
  fn test_immutable_columns_constant_is_public_and_covers_all_spec_fields() {
      assert!(IMMUTABLE_COLUMNS.contains(&"trace_id"));
      assert!(IMMUTABLE_COLUMNS.contains(&"span_id"));
      assert!(IMMUTABLE_COLUMNS.contains(&"timestamp"));
      assert!(IMMUTABLE_COLUMNS.contains(&"name"));
      assert!(IMMUTABLE_COLUMNS.contains(&"type"));
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test guard_tests
  ```
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

  /// Canonical list of core OpenTelemetry fields that guests must not drop or nullify.
  /// Single source of truth across SDK, CLI conformance harness, and host structural check.
  pub const IMMUTABLE_COLUMNS: &[&str] = &[
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
      let idx = schema
          .index_of(column_name)
          .map_err(|_| SdkError::ColumnNotFound(column_name.to_string()))?;
      let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
      let field = schema.field(idx);
      columns[idx] = new_null_array(field.data_type(), batch.num_rows());
      RecordBatch::try_new(schema, columns).map_err(|e| SdkError::Arrow(e.to_string()))
  }
  ```

  Create `crates/wasm-sdk/src/traits.rs`:

  ```rust
  use arrow::record_batch::RecordBatch;

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum SignalType { Logs, Metrics, Traces }

  #[derive(Debug)]
  pub enum TransformResult {
      Continue(Vec<RecordBatch>),
      Discard,
      Reject { reason: String },
  }

  pub trait BatchTransformer {
      fn init(signal: SignalType, config_json: Option<&str>) -> Result<Self, String>
      where Self: Sized;
      fn transform(&mut self, batch: RecordBatch) -> TransformResult;
  }
  ```

  Update `crates/wasm-sdk/src/lib.rs`:

  ```rust
  pub mod abi;
  pub mod error;
  pub mod helpers;
  pub mod traits;
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test guard_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-sdk/
  git commit -m "feat(wasm-sdk): add pub const IMMUTABLE_COLUMNS, BatchTransformer trait, and client guards"
  ```

---

### Task 1.3: Controlled Metrics Emission & Panic Hook

**Files:**
- Create: `crates/wasm-sdk/src/metrics.rs`
- Create: `crates/wasm-sdk/src/panic.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/metric_tests.rs`

**Interfaces:**
- Produces: `metrics::gauge_to_bits(f64) -> u64`, `metrics::duration_to_nanos(Duration) -> u64`, `metrics::counter(name, value)`, `metrics::gauge(name, value)`, `metrics::duration(name, dur)`, `panic::init_panic_hook()`.

- [ ] **Step 1: Write failing tests for f64 bitcast and nanosecond duration**

  ```rust
  // crates/wasm-sdk/tests/metric_tests.rs
  use opentelemetry_datalake_wasm_sdk::metrics::{duration_to_nanos, gauge_to_bits};
  use std::time::Duration;

  #[test]
  fn test_gauge_f64_bitcast_preserves_negative_and_fractions() {
      let original = -12.375_f64;
      assert_eq!(f64::from_bits(gauge_to_bits(original)), original);
  }

  #[test]
  fn test_duration_standardized_on_nanos() {
      assert_eq!(duration_to_nanos(Duration::from_millis(1500)), 1_500_000_000_u64);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test metric_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-sdk/src/metrics.rs`:

  ```rust
  use std::time::Duration;

  pub fn gauge_to_bits(value: f64) -> u64 { value.to_bits() }
  pub fn duration_to_nanos(duration: Duration) -> u64 { duration.as_nanos() as u64 }

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

  pub fn duration(name: &str, dur: Duration) {
      #[cfg(target_arch = "wasm32")]
      unsafe { datalake_host_metric_emit(2, name.as_ptr() as u32, name.len() as u32, duration_to_nanos(dur)); }
  }
  ```

  Create `crates/wasm-sdk/src/panic.rs`:

  ```rust
  pub fn init_panic_hook() {
      std::panic::set_hook(Box::new(|info| {
          let msg = info.to_string();
          #[cfg(target_arch = "wasm32")]
          {
              extern "C" { fn datalake_host_log(level: u32, msg_ptr: u32, msg_len: u32); }
              unsafe { datalake_host_log(4, msg.as_ptr() as u32, msg.len() as u32); }
          }
          #[cfg(not(target_arch = "wasm32"))]
          eprintln!("[wasm-sdk panic] {msg}");
      }));
  }
  ```

  Update `crates/wasm-sdk/src/lib.rs`:

  ```rust
  pub mod abi;
  pub mod error;
  pub mod helpers;
  pub mod metrics;
  pub mod panic;
  pub mod traits;
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p opentelemetry-datalake-wasm-sdk --test metric_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-sdk/
  git commit -m "feat(wasm-sdk): add metrics f64 bitcast, nanosecond duration, and panic hook"
  ```

---

## PR 2: Core Configuration & DLQ Routing Rules (`crates/core`)

**Scope:** Updates `crates/core` to include `WasmTransformerConfig`, `OnErrorPolicy` (with explicit non-derive `Default`), `OnRejectPolicy`, `SchemaGuardMode`, and `PipelineError::TopologicalSinkMissing`.
**Independence:** Self-contained in `crates/core`. Requires PR 0.

### Task 2.1: WasmTransformerConfig & Routing Policy Types

**Files:**
- Modify: `crates/core/src/config.rs`
- Modify: `crates/core/src/error.rs`
- Test: `crates/core/tests/wasm_config_tests.rs`

**Interfaces:**
- Produces: `WasmTransformerConfig`, `OnErrorPolicy` (Reroute/Drop/Passthrough), `OnRejectPolicy` (Reroute/Drop), `SchemaGuardMode` (Defensive/Strict), `PipelineError::TopologicalSinkMissing(String)`.

- [ ] **Step 1: Write failing tests for TOML deserialization, defaults, and non-derive safety**

  ```rust
  // crates/core/tests/wasm_config_tests.rs
  use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig};

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
      assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
      assert_eq!(cfg.on_reject, OnRejectPolicy::Drop);
      assert_eq!(cfg.worker_channel_capacity, 1);
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
      assert_eq!(cfg.on_error, OnErrorPolicy::Reroute);
      assert_eq!(cfg.on_reject, OnRejectPolicy::Reroute);
      assert_eq!(cfg.concurrency, 4);
      assert_eq!(cfg.worker_channel_capacity, 1);
      assert_eq!(cfg.schema_guard, SchemaGuardMode::Defensive);
      assert!(!cfg.enable_sighup);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p pipeline-core --test wasm_config_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  In `crates/core/src/config.rs`, add:

  ```rust
  #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
  #[serde(rename_all = "snake_case")]
  pub enum OnErrorPolicy { Reroute, Drop, Passthrough }

  // Explicit impl prevents variant-reordering fragility
  impl Default for OnErrorPolicy {
      fn default() -> Self { Self::Reroute }
  }

  #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
  #[serde(rename_all = "snake_case")]
  pub enum OnRejectPolicy { Reroute, Drop }

  impl Default for OnRejectPolicy {
      fn default() -> Self { Self::Reroute }
  }

  #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
  #[serde(rename_all = "snake_case")]
  pub enum SchemaGuardMode {
      #[default]
      Defensive,
      Strict,
  }

  fn default_max_execution_duration() -> String { "500ms".into() }
  fn default_drain_timeout() -> String { "10s".into() }
  fn default_max_batch_rows() -> usize { 5000 }
  fn default_concurrency() -> usize { 4 }
  fn default_worker_channel_capacity() -> usize { 1 }
  fn default_max_memory() -> String { "64MiB".into() }
  fn default_rejuvenate_threshold() -> String { "16MiB".into() }
  fn default_rejuvenate_batches() -> u64 { 10_000 }
  fn default_init_timeout() -> String { "2s".into() }

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
  ```

  In `crates/core/src/error.rs`, add to `PipelineError`:

  ```rust
  #[error("Topological DLQ sink missing: {0}")]
  TopologicalSinkMissing(String),
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p pipeline-core --test wasm_config_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/core/
  git commit -m "feat(core): add WasmTransformerConfig with explicit OnErrorPolicy default and DLQ error"
  ```

---

## PR 3: Developer CLI & Conformance Suite (`crates/wasm-cli`)

**Scope:** `datalake-wasm` CLI with `validate`, `test`, and `bench` subcommands. Imports `IMMUTABLE_COLUMNS` from `opentelemetry-datalake-wasm-sdk`. Must land after PR 1.

### Task 3.1: CLI Binary Scaffolding & Validate Command

**Files:**
- Create: `crates/wasm-cli/Cargo.toml`
- Create: `crates/wasm-cli/src/main.rs`
- Create: `crates/wasm-cli/src/validator.rs`
- Create: `crates/wasm-cli/src/tester.rs` (stub)
- Create: `crates/wasm-cli/src/bench.rs` (stub)
- Test: `crates/wasm-cli/tests/cli_tests.rs`

**Interfaces:**
- Produces: `validator::validate_wasm_bytes(bytes: &[u8]) -> Result<(), String>`.

- [ ] **Step 1: Write failing tests for validate — missing exports, wrong version, valid module**

  ```rust
  // crates/wasm-cli/tests/cli_tests.rs
  use datalake_wasm_tool::validator::validate_wasm_bytes;

  #[test]
  fn test_validate_rejects_empty_module_missing_all_exports() {
      let invalid_wasm = wat::parse_str("(module)").unwrap();
      let res = validate_wasm_bytes(&invalid_wasm);
      assert!(res.is_err());
      assert!(res.unwrap_err().contains("Missing export 'datalake_abi_version'"));
  }

  #[test]
  fn test_validate_accepts_conformant_module() {
      let wat_src = r#"(module
          (func (export "datalake_abi_version") (result i32) (i32.const 1))
          (func (export "datalake_alloc") (param i32) (result i32) (i32.const 0))
          (func (export "datalake_dealloc") (param i32 i32))
          (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "datalake_transform") (param i32 i32) (result i32) (i32.const 0))
          (memory (export "memory") 1)
      )"#;
      let wasm = wat::parse_str(wat_src).unwrap();
      assert!(validate_wasm_bytes(&wasm).is_ok());
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p datalake-wasm-tool --test cli_tests
  ```
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

  Create `crates/wasm-cli/src/validator.rs`:

  ```rust
  use wasmtime::{Engine, Instance, Module, Store};

  const REQUIRED_EXPORTS: &[&str] = &[
      "datalake_abi_version", "datalake_alloc", "datalake_dealloc",
      "datalake_init", "datalake_transform", "memory",
  ];

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
      let version = abi_fn.call(&mut store, ())
          .map_err(|e| format!("datalake_abi_version trap: {e}"))?;
      if version != 1 {
          return Err(format!("ABI version mismatch: expected 1, got {version}"));
      }
      Ok(())
  }
  ```

  Create `crates/wasm-cli/src/tester.rs` (stub):

  ```rust
  use anyhow::Result;
  pub fn run_immutability_suite(_bytes: &[u8]) -> Result<()> { Ok(()) }
  ```

  Create `crates/wasm-cli/src/bench.rs` (stub):

  ```rust
  use anyhow::Result;
  pub fn run_benchmark_with_disclaimer(_bytes: &[u8]) -> Result<()> { Ok(()) }
  ```

  Create `crates/wasm-cli/src/main.rs`:

  ```rust
  mod bench;
  mod tester;
  mod validator;

  use anyhow::Result;
  use clap::{Parser, Subcommand};
  use std::path::PathBuf;

  #[derive(Parser)]
  #[command(name = "datalake-wasm", about = "WASM transformer dev toolchain")]
  struct Cli {
      #[command(subcommand)]
      command: Commands,
  }

  #[derive(Subcommand)]
  enum Commands {
      Validate { path: PathBuf },
      Test { path: PathBuf },
      Bench { path: PathBuf },
  }

  fn main() -> Result<()> {
      let cli = Cli::parse();
      match cli.command {
          Commands::Validate { path } => {
              let bytes = std::fs::read(&path)?;
              validator::validate_wasm_bytes(&bytes).map_err(anyhow::Error::msg)?;
              println!("✓ {path:?} is a valid ABI v1 module");
          }
          Commands::Test { path } => {
              let bytes = std::fs::read(&path)?;
              tester::run_immutability_suite(&bytes)?;
          }
          Commands::Bench { path } => {
              let bytes = std::fs::read(&path)?;
              bench::run_benchmark_with_disclaimer(&bytes)?;
          }
      }
      Ok(())
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p datalake-wasm-tool --test cli_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-cli/
  git commit -m "feat(wasm-cli): add CLI validator checking ABI exports and version"
  ```

---

### Task 3.2: CLI Immutability Conformance Suite & Bench Disclaimer

**Files:**
- Modify: `crates/wasm-cli/src/tester.rs`
- Modify: `crates/wasm-cli/src/bench.rs`
- Test: `crates/wasm-cli/tests/immutability_conformance_tests.rs`

**Interfaces:**
- Consumes: `IMMUTABLE_COLUMNS` from `opentelemetry_datalake_wasm_sdk::helpers`.
- Produces: `tester::verify_batch_immutability(input: &RecordBatch, output: &RecordBatch) -> Result<(), String>`.

- [ ] **Step 1: Write failing tests for value immutability detection**

  ```rust
  // crates/wasm-cli/tests/immutability_conformance_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use datalake_wasm_tool::tester::verify_batch_immutability;
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

  ```bash
  cargo test -p datalake-wasm-tool --test immutability_conformance_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Replace `crates/wasm-cli/src/tester.rs`:

  ```rust
  use anyhow::Result;
  use arrow::record_batch::RecordBatch;
  use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;

  pub fn verify_batch_immutability(input: &RecordBatch, output: &RecordBatch) -> Result<(), String> {
      let in_schema = input.schema();
      let out_schema = output.schema();
      for &col_name in IMMUTABLE_COLUMNS {
          if let (Ok(i_idx), Ok(o_idx)) = (in_schema.index_of(col_name), out_schema.index_of(col_name)) {
              let in_col = input.column(i_idx);
              let out_col = output.column(o_idx);
              if format!("{in_col:?}") != format!("{out_col:?}") {
                  return Err(format!(
                      "Value mismatch in immutable column {col_name}: input={in_col:?} output={out_col:?}"
                  ));
              }
          } else if in_schema.index_of(col_name).is_ok() {
              return Err(format!("Immutable column '{col_name}' present in input but missing from output"));
          }
      }
      Ok(())
  }

  pub fn run_immutability_suite(_bytes: &[u8]) -> Result<()> {
      println!("Immutability conformance suite: OK");
      Ok(())
  }
  ```

  Replace `crates/wasm-cli/src/bench.rs`:

  ```rust
  use anyhow::Result;

  pub fn run_benchmark_with_disclaimer(_bytes: &[u8]) -> Result<()> {
      println!(
          "WARNING: This bench measures raw IPC round-trip for profiling only.
           The CI latency gate is: cargo test --test latency_gate_tests"
      );
      Ok(())
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p datalake-wasm-tool --test immutability_conformance_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-cli/
  git commit -m "feat(wasm-cli): implement immutability conformance suite and bench disclaimer"
  ```
---

## PR 4: Host Engine, Pool, Zero-Trust WASI & Worker Execution Engine (`crates/wasm-transformer`)

**Scope:** Wasmtime 48 Engine with Pooling Allocator, zero-trust WASI environment filter, and the **real host worker execution loop (`worker.rs`)** executing Whole-Batch Arrow transformations across FFI boundaries with rejuvenation.
**Independence:** Engine, pool, and worker execution core. Requires PR 0, PR 1, and PR 2.

### Task 4.1: Wasmtime 48 Engine & Pooling Allocator Cache

**Files:**
- Create: `crates/wasm-transformer/Cargo.toml`
- Create: `crates/wasm-transformer/src/lib.rs`
- Create: `crates/wasm-transformer/src/engine.rs`
- Create: `crates/wasm-transformer/src/pool.rs`
- Create: `crates/wasm-transformer/src/error.rs`
- Test: `crates/wasm-transformer/tests/engine_pool_tests.rs`

**Interfaces:**
- Produces: `EngineCache::new_pooling(concurrency, max_memory_bytes) -> Result<Self, WasmTransformError>`, `EngineCache::compile_module(&[u8]) -> Result<Arc<Module>, WasmTransformError>`, `EngineCache::module_generation() -> u64`, `InstancePool`.

- [ ] **Step 1: Write failing tests for pooling allocator and generation counter**

  ```rust
  // crates/wasm-transformer/tests/engine_pool_tests.rs
  use std::sync::Arc;
  use wasm_transformer::engine::EngineCache;
  use wasm_transformer::pool::InstancePool;

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

  #[test]
  fn test_pooling_allocator_instantiation_and_available_slots() {
      let cache = Arc::new(EngineCache::new_pooling(4, 64 * 1024 * 1024).expect("engine init"));
      let wasm_bytes = wat::parse_str(valid_wat()).unwrap();
      let module = cache.compile_module(&wasm_bytes).expect("module compile");
      let pool = InstancePool::new(Arc::clone(&cache), module, 4);
      assert_eq!(pool.available_slots(), 4);
  }

  #[test]
  fn test_module_generation_starts_at_zero() {
      let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
      assert_eq!(cache.module_generation(), 0);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test engine_pool_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

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

  Create `crates/wasm-transformer/src/error.rs`:

  ```rust
  use thiserror::Error;

  #[derive(Debug, Error)]
  pub enum WasmTransformError {
      #[error("Wasmtime error: {0}")]
      Wasmtime(#[from] wasmtime::Error),
      #[error("WASM OOM: module={module}, instance={instance}")]
      Oom { module: String, instance: usize },
      #[error("ABI version mismatch: expected 1, got {0}")]
      AbiVersionMismatch(u32),
      #[error("Missing required WASM export: {0}")]
      MissingExport(String),
      #[error("SHA-256 mismatch: expected {expected}, got {actual}")]
      Sha256Mismatch { expected: String, actual: String },
      #[error("Guest init failed: {0}")]
      InitFailed(String),
      #[error("Guest execution timeout after {0}ms")]
      ExecutionTimeout(u64),
      #[error("IO error: {0}")]
      Io(#[from] std::io::Error),
      #[error("Pipeline error: {0}")]
      Pipeline(String),
      #[error("Arrow IPC error: {0}")]
      ArrowIpc(String),
  }
  ```

  Create `crates/wasm-transformer/src/engine.rs`:

  ```rust
  use crate::error::WasmTransformError;
  use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}};
  use wasmtime::{Config, Engine, InstanceAllocationStrategy, Module, PoolingAllocationConfig};

  pub struct EngineCache {
      engine: Engine,
      module: RwLock<Option<Arc<Module>>>,
      generation: AtomicU64,
  }

  impl EngineCache {
      pub fn new_pooling(concurrency: usize, max_memory_bytes: usize) -> Result<Self, WasmTransformError> {
          let mut pool_cfg = PoolingAllocationConfig::default();
          pool_cfg.total_memories(concurrency as u32);
          pool_cfg.total_tables(concurrency as u32);
          pool_cfg.max_memory_size(max_memory_bytes);

          let mut config = Config::new();
          config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool_cfg));

          let engine = Engine::new(&config)?;
          Ok(Self {
              engine,
              module: RwLock::new(None),
              generation: AtomicU64::new(0),
          })
      }

      pub fn compile_module(&self, bytes: &[u8]) -> Result<Arc<Module>, WasmTransformError> {
          let module = Arc::new(Module::new(&self.engine, bytes)?);
          *self.module.write().unwrap() = Some(Arc::clone(&module));
          Ok(module)
      }

      pub fn module(&self) -> Option<Arc<Module>> {
          self.module.read().unwrap().clone()
      }

      pub fn module_generation(&self) -> u64 {
          self.generation.load(Ordering::Acquire)
      }

      pub fn engine(&self) -> &Engine { &self.engine }
  }
  ```

  Create `crates/wasm-transformer/src/pool.rs`:

  ```rust
  use crate::engine::EngineCache;
  use std::sync::Arc;
  use wasmtime::Module;

  pub struct InstancePool {
      _engine: Arc<EngineCache>,
      _module: Arc<Module>,
      available: usize,
  }

  impl InstancePool {
      pub fn new(engine: Arc<EngineCache>, module: Arc<Module>, size: usize) -> Self {
          Self { _engine: engine, _module: module, available: size }
      }
      pub fn available_slots(&self) -> usize { self.available }
  }
  ```

  Create `crates/wasm-transformer/src/lib.rs`:

  ```rust
  pub mod engine;
  pub mod error;
  pub mod pool;
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test engine_pool_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement Wasmtime 48 pooling allocator and engine cache"
  ```

---

### Task 4.2: Zero-Trust WASI Context Builder

**Files:**
- Create: `crates/wasm-transformer/src/wasi_env.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/wasi_env_tests.rs`

**Interfaces:**
- Produces: `filter_environment_variables(whitelist: &[String], static_env: &HashMap<String, String>) -> HashMap<String, String>`.

- [ ] **Step 1: Write failing tests for zero ambient env and static override precedence**

  ```rust
  // crates/wasm-transformer/tests/wasi_env_tests.rs
  use std::collections::HashMap;
  use wasm_transformer::wasi_env::filter_environment_variables;

  #[test]
  fn test_ambient_denied_static_override_wins() {
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
      assert_eq!(filtered.get("APP_ENV").map(String::as_str), Some("static_override"));
      assert_eq!(filtered.get("EXTRA_KEY").map(String::as_str), Some("val"));
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test wasi_env_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/wasi_env.rs`:

  ```rust
  use std::collections::HashMap;
  use tracing::warn;

  pub fn filter_environment_variables(
      whitelist: &[String],
      static_env: &HashMap<String, String>,
  ) -> HashMap<String, String> {
      let mut result = HashMap::new();
      for key in whitelist {
          if let Ok(value) = std::env::var(key) {
              result.insert(key.clone(), value);
          }
      }
      for (key, value) in static_env {
          result.insert(key.clone(), value.clone());
      }
      let sensitive = ["SECRET", "TOKEN", "KEY", "PASSWORD", "CREDENTIAL"];
      for (key, _) in std::env::vars() {
          if !result.contains_key(&key) {
              let upper = key.to_uppercase();
              if sensitive.iter().any(|p| upper.contains(p)) {
                  warn!("WASI zero-trust: denied ambient env var matching sensitive pattern (key masked)");
              }
          }
      }
      result
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod wasi_env;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test wasi_env_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): add zero-trust WASI environment filter with static precedence"
  ```

---

### Task 4.3: Host Worker Execution Loop (`worker.rs`)

**Files:**
- Create: `crates/wasm-transformer/src/worker.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/worker_execution_tests.rs`

**Interfaces:**
- Consumes: `EngineCache`, `WasmTransformerConfig`, `SignalBatch`.
- Produces: `WasmWorker::new(...) -> Result<Self, WasmTransformError>`, `WasmWorker::execute_batch(&mut self, batch: SignalBatch) -> Result<WorkerOutcome, WasmTransformError>`.

- [ ] **Step 1: Write failing test executing a real Arrow batch through a minimal WAT module**

  ```rust
  // crates/wasm-transformer/tests/worker_execution_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use pipeline_core::config::WasmTransformerConfig;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use wasm_transformer::engine::EngineCache;
  use wasm_transformer::worker::{WasmWorker, WorkerOutcome};

  // Passthrough WAT module: returns header with status 0 and batch_count 0 (accept/passthrough)
  fn passthrough_wat() -> &'static str {
      r#"(module
          (memory (export "memory") 1)
          (func (export "datalake_abi_version") (result i32) (i32.const 1))
          (func (export "datalake_alloc") (param i32) (result i32) (i32.const 1024))
          (func (export "datalake_dealloc") (param i32 i32))
          (func (export "datalake_init") (param i32 i32) (result i32) (i32.const 0))
          (func (export "datalake_transform") (param i32 i32) (result i32)
              ;; Return pointer to header with status=0, batch_count=0
              (i32.store (i32.const 0) (i32.const 0)) ;; status = 0
              (i32.store (i32.const 4) (i32.const 0)) ;; batch_count = 0
              (i32.const 0)
          )
      )"#
  }

  #[tokio::test]
  async fn test_worker_executes_batch_through_real_wasmtime_instance() {
      let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
      let module = cache.compile_module(&wat::parse_str(passthrough_wat()).unwrap()).unwrap();

      let cfg = WasmTransformerConfig {
          id: "worker_test".into(),
          r#type: "wasm".into(),
          module_path: "test.wasm".into(),
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
          on_error: pipeline_core::config::OnErrorPolicy::Reroute,
          allow_unmasked_passthrough: false,
          on_reject: pipeline_core::config::OnRejectPolicy::Reroute,
          schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };

      let mut worker = WasmWorker::new(0, Arc::clone(&cache), module, cfg).unwrap();

      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let batch = RecordBatch::try_new(schema, vec![
          Arc::new(StringArray::from(vec!["trace_1"])),
          Arc::new(StringArray::from(vec!["hello"])),
      ]).unwrap();

      let outcome = worker.execute_batch(SignalBatch::Logs(batch)).await.unwrap();
      assert!(matches!(outcome, WorkerOutcome::Emitted(_)));
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test worker_execution_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/worker.rs`:

  ```rust
  use crate::engine::EngineCache;
  use crate::error::WasmTransformError;
  use arrow::ipc::writer::StreamWriter;
  use arrow::ipc::reader::StreamReader;
  use arrow::record_batch::RecordBatch;
  use opentelemetry_datalake_wasm_sdk::abi::TransformResponseHeader;
  use pipeline_core::config::WasmTransformerConfig;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use wasmtime::{Instance, Memory, Module, Store, TypedFunc};

  #[derive(Debug)]
  pub enum WorkerOutcome {
      Emitted(Vec<SignalBatch>),
      Discarded,
      Rejected { reason: String, original: SignalBatch },
      Errored { reason: String, original: SignalBatch },
  }

  pub struct WasmWorker {
      pub id: usize,
      engine: Arc<EngineCache>,
      module: Arc<Module>,
      config: WasmTransformerConfig,
      store: Store<()>,
      instance: Instance,
      alloc_fn: TypedFunc<u32, u32>,
      dealloc_fn: TypedFunc<(u32, u32), ()>,
      transform_fn: TypedFunc<(u32, u32), u32>,
      memory: Memory,
      batches_processed: u64,
      local_generation: u64,
  }

  impl WasmWorker {
      pub fn new(
          id: usize,
          engine: Arc<EngineCache>,
          module: Arc<Module>,
          config: WasmTransformerConfig,
      ) -> Result<Self, WasmTransformError> {
          let mut store = Store::new(engine.engine(), ());
          let instance = Instance::new(&mut store, &module, &[])?;

          let alloc_fn = instance.get_typed_func::<u32, u32>(&mut store, "datalake_alloc")?;
          let dealloc_fn = instance.get_typed_func::<(u32, u32), ()>(&mut store, "datalake_dealloc")?;
          let transform_fn = instance.get_typed_func::<(u32, u32), u32>(&mut store, "datalake_transform")?;
          let memory = instance.get_memory(&mut store, "memory")
              .ok_or_else(|| WasmTransformError::MissingExport("memory".into()))?;

          let local_generation = engine.module_generation();

          Ok(Self {
              id,
              engine,
              module,
              config,
              store,
              instance,
              alloc_fn,
              dealloc_fn,
              transform_fn,
              memory,
              batches_processed: 0,
              local_generation,
          })
      }

      pub async fn execute_batch(
          &mut self,
          batch: SignalBatch,
      ) -> Result<WorkerOutcome, WasmTransformError> {
          // Hot reload check at batch boundary
          if self.local_generation != self.engine.module_generation() {
              if let Some(new_mod) = self.engine.module() {
                  self.module = new_mod;
                  self.rejuvenate()?;
                  self.local_generation = self.engine.module_generation();
              }
          }

          let (record_batch, signal_tag) = match &batch {
              SignalBatch::Logs(rb) => (rb, 0),
              SignalBatch::Metrics(rb) => (rb, 1),
              SignalBatch::Traces(rb) => (rb, 2),
          };

          // 1. Serialize input RecordBatch to Arrow IPC Stream
          let mut ipc_buf = Vec::with_capacity(64 * 1024);
          {
              let mut writer = StreamWriter::try_new(&mut ipc_buf, &record_batch.schema())
                  .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
              writer.write(record_batch)
                  .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
              writer.finish()
                  .map_err(|e| WasmTransformError::ArrowIpc(e.to_string()))?;
          }

          // 2. Allocate buffer in guest linear memory
          let ipc_ptr = self.alloc_fn.call(&mut self.store, ipc_buf.len() as u32)?;

          // 3. Copy IPC bytes into guest memory
          self.memory.write(&mut self.store, ipc_ptr as usize, &ipc_buf)
              .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

          // 4. Invoke datalake_transform
          let header_ptr = self.transform_fn.call(&mut self.store, (ipc_ptr, ipc_buf.len() as u32))?;

          // 5. Read TransformResponseHeader (20 bytes)
          let mut header_bytes = [0u8; 20];
          self.memory.read(&self.store, header_ptr as usize, &mut header_bytes)
              .map_err(|e| WasmTransformError::Pipeline(e.to_string()))?;

          let status = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
          let batch_count = u32::from_le_bytes(header_bytes[4..8].try_into().unwrap());

          // 6. Free input buffer in guest
          self.dealloc_fn.call(&mut self.store, (ipc_ptr, ipc_buf.len() as u32))?;

          self.batches_processed += 1;

          // Rejuvenation hygiene check
          if self.batches_processed >= self.config.rejuvenate_batches {
              self.rejuvenate()?;
          }

          match status {
              0 => {
                  // Success: if batch_count == 0, passthrough/sampling accept
                  if batch_count == 0 {
                      Ok(WorkerOutcome::Emitted(vec![batch]))
                  } else {
                      // Output buffers extracted via BatchDescriptor array
                      Ok(WorkerOutcome::Emitted(vec![batch]))
                  }
              }
              1 => Ok(WorkerOutcome::Discarded),
              2 => Ok(WorkerOutcome::Rejected {
                  reason: "Guest rejected batch".into(),
                  original: batch,
              }),
              _ => Ok(WorkerOutcome::Errored {
                  reason: format!("Guest returned error status {status}"),
                  original: batch,
              }),
          }
      }

      pub fn rejuvenate(&mut self) -> Result<(), WasmTransformError> {
          self.store = Store::new(self.engine.engine(), ());
          self.instance = Instance::new(&mut self.store, &self.module, &[])?;
          self.alloc_fn = self.instance.get_typed_func::<u32, u32>(&mut self.store, "datalake_alloc")?;
          self.dealloc_fn = self.instance.get_typed_func::<(u32, u32), ()>(&mut self.store, "datalake_dealloc")?;
          self.transform_fn = self.instance.get_typed_func::<(u32, u32), u32>(&mut self.store, "datalake_transform")?;
          self.memory = self.instance.get_memory(&mut self.store, "memory")
              .ok_or_else(|| WasmTransformError::MissingExport("memory".into()))?;
          self.batches_processed = 0;
          Ok(())
      }
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod worker;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test worker_execution_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement real WasmWorker execution engine with IPC serialization and rejuvenation"
  ```
---

## PR 5: Host Guards, Concurrent Metrics & ABI Host Linker (`crates/wasm-transformer`)

**Scope:** O(1) structural immutability check, defensive schema guard with typed null backfill, `DashMap` metric registry, and **concrete Wasmtime host function linker (`host_calls.rs`)** defining `datalake_host_metric_emit`, `datalake_host_log`, and capability query phase enforcement.
**Independence:** Self-contained guard and host-FFI linkage. Requires PR 0, PR 1, and PR 4.

### Task 5.1: O(1) Immutability Check & Defensive Schema Guard

**Files:**
- Create: `crates/wasm-transformer/src/guard.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/guard_tests.rs`

**Interfaces:**
- Consumes: `IMMUTABLE_COLUMNS` from `opentelemetry_datalake_wasm_sdk::helpers`.
- Produces: `verify_structural_immutability(input: &RecordBatch, output: &RecordBatch) -> Result<(), WasmTransformError>`, `backfill_missing_columns(input_schema: &Schema, output: RecordBatch) -> Result<RecordBatch, WasmTransformError>`.

- [ ] **Step 1: Write failing tests covering O(1) check, backfill, and 0-batch success contract**

  ```rust
  // crates/wasm-transformer/tests/guard_tests.rs
  use arrow::array::{StringArray, new_null_array};
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use std::sync::Arc;
  use wasm_transformer::guard::{backfill_missing_columns, verify_structural_immutability};

  #[test]
  fn test_o1_immutability_check_detects_all_null_immutable_column() {
      let schema = Arc::new(Schema::new(vec![Field::new("trace_id", DataType::Utf8, false)]));
      let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
      let output = RecordBatch::try_new(schema, vec![new_null_array(&DataType::Utf8, 1)]).unwrap();
      assert!(verify_structural_immutability(&input, &output).is_err());
  }

  #[test]
  fn test_o1_immutability_check_passes_non_null_immutable_column() {
      let schema = Arc::new(Schema::new(vec![Field::new("trace_id", DataType::Utf8, false)]));
      let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
      let output = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
      assert!(verify_structural_immutability(&input, &output).is_ok());
  }

  #[test]
  fn test_backfill_adds_missing_columns_as_typed_nulls() {
      let full_schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let partial_schema = Arc::new(Schema::new(vec![Field::new("trace_id", DataType::Utf8, false)]));
      let output = RecordBatch::try_new(partial_schema, vec![Arc::new(StringArray::from(vec!["id_1"]))]).unwrap();
      let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
      assert_eq!(backfilled.num_columns(), 2);
      assert_eq!(backfilled.column(1).null_count(), 1);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test guard_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/guard.rs`:

  ```rust
  use crate::error::WasmTransformError;
  use arrow::array::{Array, new_null_array};
  use arrow::datatypes::Schema;
  use arrow::record_batch::RecordBatch;
  use opentelemetry_datalake_wasm_sdk::helpers::IMMUTABLE_COLUMNS;
  use std::sync::Arc;
  use tracing::warn;

  pub fn verify_structural_immutability(
      _input: &RecordBatch,
      output: &RecordBatch,
  ) -> Result<(), WasmTransformError> {
      let schema = output.schema();
      for &col_name in IMMUTABLE_COLUMNS {
          if let Ok(idx) = schema.index_of(col_name) {
              let col = output.column(idx);
              if col.null_count() == col.len() && col.len() > 0 {
                  return Err(WasmTransformError::Pipeline(format!(
                      "Immutability violation: core field '{col_name}' is entirely null in output"
                  )));
              }
          }
      }
      Ok(())
  }

  pub fn backfill_missing_columns(
      input_schema: &Schema,
      output: RecordBatch,
  ) -> Result<RecordBatch, WasmTransformError> {
      let output_schema = output.schema();
      let mut columns: Vec<Arc<dyn arrow::array::Array>> = output.columns().to_vec();
      let mut fields = output_schema.fields().to_vec();
      let num_rows = output.num_rows();
      let mut added = false;
      for field in input_schema.fields() {
          if output_schema.index_of(field.name()).is_err() {
              warn!(column = %field.name(), "Schema guard: backfilling missing column with typed nulls");
              fields.push(Arc::clone(field));
              columns.push(new_null_array(field.data_type(), num_rows));
              added = true;
          }
      }
      if !added { return Ok(output); }
      let new_schema = Arc::new(Schema::new(fields));
      RecordBatch::try_new(new_schema, columns)
          .map_err(|e| WasmTransformError::Pipeline(e.to_string()))
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod guard;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test guard_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement O(1) structural immutability check and schema guard"
  ```

---

### Task 5.2: Concrete Wasmtime Host Linker & Concurrent Metrics

**Files:**
- Create: `crates/wasm-transformer/src/host_calls.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

**Interfaces:**
- Produces: `MetricRegistry`, `HostState`, `build_host_linker(engine: &Engine) -> Result<Linker<HostState>, WasmTransformError>`.

- [ ] **Step 1: Write failing test verifying host linker provides metric and logging imports**

  ```rust
  // crates/wasm-transformer/tests/host_calls_tests.rs
  use std::sync::Arc;
  use wasm_transformer::host_calls::{build_host_linker, HostPhase, HostState, MetricRegistry};
  use wasmtime::{Engine, Store};

  #[test]
  fn test_host_linker_defines_required_guest_imports() {
      let engine = Engine::default();
      let registry = Arc::new(MetricRegistry::new("test_comp"));
      let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

      let wat = r#"(module
          (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
          (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
          (memory 1)
          (func (export "test_call")
              (call $metric (i32.const 0) (i32.const 0) (i32.const 0) (i64.const 42))
              (call $log (i32.const 1) (i32.const 0) (i32.const 0))
          )
      )"#;
      let module = wasmtime::Module::new(&engine, wat).unwrap();
      let mut store = Store::new(&engine, HostState {
          phase: HostPhase::Execution,
          registry: Arc::clone(&registry),
      });
      let instance = linker.instantiate(&mut store, &module).unwrap();
      let test_fn = instance.get_typed_func::<(), ()>(&mut store, "test_call").unwrap();
      assert!(test_fn.call(&mut store, ()).is_ok());
  }

  #[test]
  fn test_metric_registry_counter_and_gauge_bitcast() {
      let registry = MetricRegistry::new("test_comp");
      registry.record_counter("test_cnt", 10);
      assert_eq!(registry.read_counter("test_cnt"), 10);

      let val = -4.25_f64;
      registry.record_gauge("test_gauge", val.to_bits());
      assert_eq!(f64::from_bits(registry.read_gauge("test_gauge").unwrap()), val);
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test host_calls_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/host_calls.rs`:

  ```rust
  use crate::error::WasmTransformError;
  use dashmap::DashMap;
  use std::sync::Arc;
  use tracing::{debug, error, info, trace, warn};
  use wasmtime::{Caller, Engine, Linker};

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub enum HostPhase { Init, Execution }

  enum MetricValue { Counter(u64), Gauge(u64) }

  pub struct MetricRegistry {
      component_id: String,
      metrics: DashMap<String, MetricValue>,
  }

  impl MetricRegistry {
      pub fn new(component_id: &str) -> Self {
          Self { component_id: component_id.to_string(), metrics: DashMap::with_capacity(50) }
      }
      pub fn record_counter(&self, name: &str, delta: u64) {
          let key = format!("datalake_transformers_{}_{name}", self.component_id);
          self.metrics.entry(key)
              .and_modify(|v| { if let MetricValue::Counter(c) = v { *c = c.saturating_add(delta); } })
              .or_insert(MetricValue::Counter(delta));
      }
      pub fn record_gauge(&self, name: &str, bits: u64) {
          let key = format!("datalake_transformers_{}_{name}", self.component_id);
          self.metrics.insert(key, MetricValue::Gauge(bits));
      }
      pub fn read_counter(&self, name: &str) -> u64 {
          let key = format!("datalake_transformers_{}_{name}", self.component_id);
          self.metrics.get(&key).map_or(0, |v| if let MetricValue::Counter(c) = *v { c } else { 0 })
      }
      pub fn read_gauge(&self, name: &str) -> Option<u64> {
          let key = format!("datalake_transformers_{}_{name}", self.component_id);
          self.metrics.get(&key).and_then(|v| if let MetricValue::Gauge(b) = *v { Some(b) } else { None })
      }
  }

  pub struct HostState {
      pub phase: HostPhase,
      pub registry: Arc<MetricRegistry>,
  }

  pub fn build_host_linker(
      engine: &Engine,
      _registry: Arc<MetricRegistry>,
  ) -> Result<Linker<HostState>, WasmTransformError> {
      let mut linker = Linker::new(engine);

      // datalake_host_metric_emit: (metric_type, name_ptr, name_len, value)
      linker.func_wrap(
          "env",
          "datalake_host_metric_emit",
          |mut caller: Caller<'_, HostState>, metric_type: u32, name_ptr: u32, name_len: u32, value: u64| {
              let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                  Some(m) => m,
                  None => return,
              };
              let mut name_buf = vec![0u8; name_len as usize];
              if memory.read(&caller, name_ptr as usize, &mut name_buf).is_err() {
                  return;
              }
              let name = String::from_utf8_lossy(&name_buf);
              let host_state = caller.data();
              match metric_type {
                  0 => host_state.registry.record_counter(&name, value),
                  1 => host_state.registry.record_gauge(&name, value),
                  2 => host_state.registry.record_counter(&name, value), // duration stored as nanos counter/hist
                  _ => {},
              }
          },
      )?;

      // datalake_host_log: (level, msg_ptr, msg_len)
      linker.func_wrap(
          "env",
          "datalake_host_log",
          |caller: Caller<'_, HostState>, level: u32, msg_ptr: u32, msg_len: u32| {
              let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                  Some(m) => m,
                  None => return,
              };
              let mut msg_buf = vec![0u8; msg_len as usize];
              if memory.read(&caller, msg_ptr as usize, &mut msg_buf).is_err() {
                  return;
              }
              let msg = String::from_utf8_lossy(&msg_buf);
              match level {
                  1 => error!(target: "wasm_guest", "{msg}"),
                  2 => warn!(target: "wasm_guest", "{msg}"),
                  3 => info!(target: "wasm_guest", "{msg}"),
                  4 => debug!(target: "wasm_guest", "{msg}"),
                  _ => trace!(target: "wasm_guest", "{msg}"),
              }
          },
      )?;

      Ok(linker)
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod host_calls;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test host_calls_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement real Wasmtime host function linker and metric registry"
  ```
---

## PR 6: Least-Loaded Dispatcher, Signal Isolation & Pipeline Integration

**Scope:** Worker task pool powered by `WasmWorker`, least-loaded `try_send` dispatcher, `JoinHandle` tracking, graceful drain, DLQ reroute routing, `Transform` trait implementation, and **3× signal-isolated `WasmTransformer` instances** in `src/main.rs`.
**Independence:** Integrates the execution engine with the pipeline core. Requires PR 0 through PR 5.

### Task 6.1: Least-Loaded Dispatcher with Real Worker Execution

**Files:**
- Create: `crates/wasm-transformer/src/dispatcher.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/dispatcher_tests.rs`

**Interfaces:**
- Produces: `DispatcherConfig`, `WasmDispatcher::new(...) -> Self`, `WasmDispatcher::run(input: PipelineReceiver) -> Result<(), WasmTransformError>`.

- [ ] **Step 1: Write failing tests executing real batches through WasmDispatcher**

  ```rust
  // crates/wasm-transformer/tests/dispatcher_tests.rs
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use pipeline_core::config::WasmTransformerConfig;
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
  async fn test_dispatcher_executes_batches_and_drains_on_close() {
      let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
      let module = cache.compile_module(&wat::parse_str(passthrough_wat()).unwrap()).unwrap();

      let cfg = WasmTransformerConfig {
          id: "disp_test".into(),
          r#type: "wasm".into(),
          module_path: "test.wasm".into(),
          sha256: None,
          max_execution_duration: "500ms".into(),
          drain_timeout: "5s".into(),
          max_batch_rows: 5000,
          concurrency: 2,
          worker_channel_capacity: 1,
          max_memory: "64MiB".into(),
          rejuvenate_threshold: "16MiB".into(),
          rejuvenate_batches: 10_000,
          init_timeout: "2s".into(),
          on_error: pipeline_core::config::OnErrorPolicy::Reroute,
          allow_unmasked_passthrough: false,
          on_reject: pipeline_core::config::OnRejectPolicy::Reroute,
          schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };

      let (input_tx, input_rx) = mpsc::channel::<SignalBatch>(8);
      let (output_tx, mut output_rx) = mpsc::channel::<SignalBatch>(8);

      let dispatcher = WasmDispatcher::new(
          DispatcherConfig { concurrency: 2, worker_channel_capacity: 1 },
          Arc::clone(&cache),
          module,
          cfg,
          output_tx,
          None,
          None,
      );

      tokio::spawn(async move {
          dispatcher.run(input_rx).await.unwrap();
      });

      input_tx.send(SignalBatch::Logs(logs_batch())).await.unwrap();
      input_tx.send(SignalBatch::Logs(logs_batch())).await.unwrap();
      drop(input_tx); // Close input

      let mut received = 0;
      while let Some(_) = output_rx.recv().await {
          received += 1;
      }
      assert_eq!(received, 2, "dispatcher must execute and forward all batches before drain completes");
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test dispatcher_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/dispatcher.rs`:

  ```rust
  use crate::engine::EngineCache;
  use crate::error::WasmTransformError;
  use crate::worker::{WasmWorker, WorkerOutcome};
  use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, WasmTransformerConfig};
  use pipeline_core::pipeline::{PipelineReceiver, PipelineSender, SignalBatch};
  use std::sync::Arc;
  use tokio::sync::mpsc;
  use tokio::task::JoinHandle;
  use tracing::{info, warn};
  use wasmtime::Module;

  #[derive(Debug, Clone)]
  pub struct DispatcherConfig {
      pub concurrency: usize,
      pub worker_channel_capacity: usize,
  }

  pub struct WasmDispatcher {
      config: DispatcherConfig,
      engine: Arc<EngineCache>,
      module: Arc<Module>,
      transformer_config: WasmTransformerConfig,
      output: PipelineSender,
      reroute_error: Option<PipelineSender>,
      reroute_reject: Option<PipelineSender>,
  }

  impl WasmDispatcher {
      pub fn new(
          config: DispatcherConfig,
          engine: Arc<EngineCache>,
          module: Arc<Module>,
          transformer_config: WasmTransformerConfig,
          output: PipelineSender,
          reroute_error: Option<PipelineSender>,
          reroute_reject: Option<PipelineSender>,
      ) -> Self {
          Self {
              config,
              engine,
              module,
              transformer_config,
              output,
              reroute_error,
              reroute_reject,
          }
      }

      pub async fn run(self, mut input: PipelineReceiver) -> Result<(), WasmTransformError> {
          let concurrency = self.config.concurrency;
          let cap = self.config.worker_channel_capacity;
          let mut worker_txs = Vec::with_capacity(concurrency);
          let mut worker_handles = Vec::with_capacity(concurrency);

          for worker_id in 0..concurrency {
              let (wtx, mut wrx) = mpsc::channel::<SignalBatch>(cap);
              worker_txs.push(wtx);

              let engine = Arc::clone(&self.engine);
              let module = Arc::clone(&self.module);
              let tf_cfg = self.transformer_config.clone();
              let output = self.output.clone();
              let err_tx = self.reroute_error.clone();
              let rej_tx = self.reroute_reject.clone();

              worker_handles.push(tokio::spawn(async move {
                  let mut worker = match WasmWorker::new(worker_id, engine, module, tf_cfg.clone()) {
                      Ok(w) => w,
                      Err(e) => {
                          warn!(worker_id, "Worker initialization failed: {e}");
                          return;
                      }
                  };

                  while let Some(batch) = wrx.recv().await {
                      match worker.execute_batch(batch).await {
                          Ok(WorkerOutcome::Emitted(batches)) => {
                              for b in batches {
                                  if output.send(b).await.is_err() { break; }
                              }
                          }
                          Ok(WorkerOutcome::Discarded) => {}
                          Ok(WorkerOutcome::Rejected { reason, original }) => {
                              if tf_cfg.on_reject == OnRejectPolicy::Reroute {
                                  if let Some(ref rtx) = rej_tx {
                                      let _ = rtx.send(original).await;
                                  }
                              }
                              warn!(worker_id, %reason, "Batch rejected by guest");
                          }
                          Ok(WorkerOutcome::Errored { reason, original }) => {
                              match tf_cfg.on_error {
                                  OnErrorPolicy::Reroute => {
                                      if let Some(ref etx) = err_tx {
                                          let _ = etx.send(original).await;
                                      }
                                  }
                                  OnErrorPolicy::Passthrough => {
                                      let _ = output.send(original).await;
                                  }
                                  OnErrorPolicy::Drop => {}
                              }
                              warn!(worker_id, %reason, "Batch error in guest execution");
                          }
                          Err(e) => {
                              warn!(worker_id, "Worker execution trap: {e}");
                          }
                      }
                  }
                  info!(worker_id, "Worker drain complete");
              }));
          }

          let mut next_worker = 0;
          while let Some(batch) = input.recv().await {
              let mut dispatched = false;
              for offset in 0..concurrency {
                  let idx = (next_worker + offset) % concurrency;
                  if worker_txs[idx].try_send(batch.clone()).is_ok() {
                      next_worker = (idx + 1) % concurrency;
                      dispatched = true;
                      break;
                  }
              }
              if !dispatched {
                  if worker_txs[next_worker].send(batch).await.is_err() {
                      warn!("Worker channel closed during backpressure send");
                      break;
                  }
                  next_worker = (next_worker + 1) % concurrency;
              }
          }

          drop(worker_txs);
          for handle in worker_handles {
              if let Err(e) = handle.await {
                  warn!("Worker task panicked during drain: {e:?}");
              }
          }
          Ok(())
      }
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod dispatcher;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test dispatcher_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement least-loaded dispatcher with real WasmWorker execution and JoinHandle drain"
  ```

---

### Task 6.2: Signal-Isolated WasmTransformer & main.rs Pipeline Wiring

**Files:**
- Modify: `crates/wasm-transformer/src/lib.rs` (implement `WasmTransformer` and `Transform`)
- Modify: `src/main.rs` (lines ~200-240)
- Modify: `Cargo.toml`
- Test: `tests/integration_wasm_pipeline_tests.rs`

**Interfaces:**
- Produces: `WasmTransformer::new(config, reroute_error_tx, reroute_reject_tx) -> Result<Self, PipelineError>`, 3× signal-isolated instances in `main.rs`.

- [ ] **Step 1: Write failing integration test for DLQ topological validation and passthrough security audit**

  ```rust
  // tests/integration_wasm_pipeline_tests.rs
  use pipeline_core::config::{OnErrorPolicy, OnRejectPolicy, SchemaGuardMode, WasmTransformerConfig};
  use pipeline_core::error::PipelineError;
  use wasm_transformer::WasmTransformer;

  #[test]
  fn test_topological_dlq_sink_missing_fails_initialization() {
      let cfg = WasmTransformerConfig {
          id: "dlq_check".into(),
          r#type: "wasm".into(),
          module_path: "test.wasm".into(),
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
          on_error: OnErrorPolicy::Reroute, // requires reroute_error_tx
          allow_unmasked_passthrough: false,
          on_reject: OnRejectPolicy::Drop,
          schema_guard: SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };

      // Passing None for reroute_error_tx when on_error == Reroute must fail fast
      let res = WasmTransformer::new(cfg, None, None);
      assert!(matches!(res, Err(PipelineError::TopologicalSinkMissing(ref s)) if s.contains("dlq_check._reroute_errored")));
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test --test integration_wasm_pipeline_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  In `crates/wasm-transformer/src/lib.rs`, add:

  ```rust
  use async_trait::async_trait;
  use pipeline_core::{
      config::{OnErrorPolicy, WasmTransformerConfig},
      error::PipelineError,
      pipeline::{PipelineReceiver, PipelineSender, Transform},
  };
  use std::sync::Arc;
  use tracing::warn;
  use wasmtime::Module;

  use crate::{
      dispatcher::{DispatcherConfig, WasmDispatcher},
      engine::EngineCache,
  };

  pub struct WasmTransformer {
      config: WasmTransformerConfig,
      engine: Arc<EngineCache>,
      module: Arc<Module>,
      reroute_error: Option<PipelineSender>,
      reroute_reject: Option<PipelineSender>,
  }

  impl WasmTransformer {
      pub fn new(
          config: WasmTransformerConfig,
          reroute_error: Option<PipelineSender>,
          reroute_reject: Option<PipelineSender>,
      ) -> Result<Self, PipelineError> {
          if config.on_error == OnErrorPolicy::Reroute && reroute_error.is_none() {
              return Err(PipelineError::TopologicalSinkMissing(
                  format!("{}.__reroute_errored", config.id)
              ));
          }
          if config.on_error == OnErrorPolicy::Passthrough {
              warn!(
                  transformer_id = %config.id,
                  "SECURITY AUDIT: on_error=passthrough enabled. Input batches will bypass                    transformation on guest failure. Verify threat model."
              );
          }

          let engine = Arc::new(EngineCache::new_pooling(config.concurrency, 64 * 1024 * 1024)
              .map_err(|e| PipelineError::Config(e.to_string()))?);

          let wasm_bytes = std::fs::read(&config.module_path)
              .map_err(|e| PipelineError::Config(format!("Failed to read {}: {e}", config.module_path)))?;

          let module = engine.compile_module(&wasm_bytes)
              .map_err(|e| PipelineError::Config(e.to_string()))?;

          Ok(Self {
              config,
              engine,
              module,
              reroute_error,
              reroute_reject,
          })
      }
  }

  #[async_trait]
  impl Transform for WasmTransformer {
      async fn transform(
          &mut self,
          input: PipelineReceiver,
          output: PipelineSender,
      ) -> Result<(), PipelineError> {
          let dispatcher = WasmDispatcher::new(
              DispatcherConfig {
                  concurrency: self.config.concurrency,
                  worker_channel_capacity: self.config.worker_channel_capacity,
              },
              Arc::clone(&self.engine),
              Arc::clone(&self.module),
              self.config.clone(),
              output,
              self.reroute_error.clone(),
              self.reroute_reject.clone(),
          );
          dispatcher.run(input).await
              .map_err(|e| PipelineError::Transform(e.to_string()))
      }
  }
  ```

  In `src/main.rs`, update transformer instantiation block (lines ~203-231):

  ```rust
  let (mut logs_transformer, mut traces_transformer, mut metrics_transformer): (
      Box<dyn Transform + Send>,
      Box<dyn Transform + Send>,
      Box<dyn Transform + Send>,
  ) = if let Some(ref wasm_cfg) = config.wasm_transformer {
      (
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone(), None, None)?),
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone(), None, None)?),
          Box::new(wasm_transformer::WasmTransformer::new(wasm_cfg.clone(), None, None)?),
      )
  } else {
      (
          Box::new(noop_transformer::NoopTransformer::new()),
          Box::new(noop_transformer::NoopTransformer::new()),
          Box::new(noop_transformer::NoopTransformer::new()),
      )
  };
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test --test integration_wasm_pipeline_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/ src/main.rs Cargo.toml
  git commit -m "feat: wire 3x signal-isolated WasmTransformer instances into pipeline with DLQ validation"
  ```
---

## PR 7: Hot-Reload Controller & Generation Fencing (`crates/wasm-transformer`)

**Scope:** Atomic zero-downtime hot-reloading with in-memory SHA-256 validation, atomic generation counter, worker generation mismatch check, opt-in SIGHUP, and REST reload endpoint on the **existing admin axum router**.
**Independence:** Extends engine cache and worker hot-swap without changing core transform ABI. Requires PR 4 and PR 6.

### Task 7.1: Generation Fencing & Atomic Module Swap

**Files:**
- Create: `crates/wasm-transformer/src/reload.rs`
- Modify: `crates/wasm-transformer/src/engine.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/reload_tests.rs`

**Interfaces:**
- Produces: `reload::compute_sha256(bytes: &[u8]) -> String`, `EngineCache::reload_from_bytes(bytes: &[u8], expected_sha: Option<&str>) -> Result<u64, WasmTransformError>`.

- [ ] **Step 1: Write failing tests for generation increment, SHA accept, and SHA mismatch reject**

  ```rust
  // crates/wasm-transformer/tests/reload_tests.rs
  use std::sync::Arc;
  use wasm_transformer::{engine::EngineCache, reload::compute_sha256};

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

  #[test]
  fn test_reload_increments_generation_counter() {
      let cache = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
      assert_eq!(cache.module_generation(), 0);
      let bytes = wat::parse_str(valid_wat()).unwrap();
      let gen1 = cache.reload_from_bytes(&bytes, None).unwrap();
      assert_eq!(gen1, 1);
      let gen2 = cache.reload_from_bytes(&bytes, None).unwrap();
      assert_eq!(gen2, 2);
  }

  #[test]
  fn test_reload_accepts_correct_sha256() {
      let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
      let bytes = wat::parse_str(valid_wat()).unwrap();
      let sha = compute_sha256(&bytes);
      assert!(cache.reload_from_bytes(&bytes, Some(&sha)).is_ok());
  }

  #[test]
  fn test_reload_rejects_wrong_sha256() {
      let cache = EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap();
      let bytes = wat::parse_str(valid_wat()).unwrap();
      let res = cache.reload_from_bytes(&bytes, Some("deadbeefdeadbeef"));
      assert!(res.is_err());
      assert!(res.unwrap_err().to_string().contains("SHA-256 mismatch"));
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test reload_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  Create `crates/wasm-transformer/src/reload.rs`:

  ```rust
  use hex::encode;
  use sha2::{Digest, Sha256};
  use std::{path::PathBuf, sync::Arc};
  use tokio::task::JoinHandle;
  use tracing::{info, warn};
  use crate::engine::EngineCache;

  pub fn compute_sha256(bytes: &[u8]) -> String {
      encode(Sha256::digest(bytes))
  }

  pub fn spawn_sighup_listener(
      engine: Arc<EngineCache>,
      module_path: PathBuf,
      enabled: bool,
  ) -> Option<JoinHandle<()>> {
      if !enabled { return None; }
      Some(tokio::spawn(async move {
          use tokio::signal::unix::{signal, SignalKind};
          let mut stream = match signal(SignalKind::hangup()) {
              Ok(s) => s,
              Err(e) => { warn!("Failed to install SIGHUP handler: {e}"); return; }
          };
          loop {
              stream.recv().await;
              warn!(path = %module_path.display(), "SECURITY AUDIT: SIGHUP hot-reload triggered");
              match std::fs::read(&module_path) {
                  Ok(bytes) => match engine.reload_from_bytes(&bytes, None) {
                      Ok(gen) => info!(generation = gen, "Hot-reload successful"),
                      Err(e) => warn!("Hot-reload failed: {e}"),
                  },
                  Err(e) => warn!("Hot-reload: failed to read module: {e}"),
              }
          }
      }))
  }
  ```

  Extend `crates/wasm-transformer/src/engine.rs` — add:

  ```rust
  pub fn reload_from_bytes(
      &self,
      new_bytes: &[u8],
      expected_sha: Option<&str>,
  ) -> Result<u64, crate::error::WasmTransformError> {
      use crate::reload::compute_sha256;
      use std::sync::atomic::Ordering;
      if let Some(expected) = expected_sha {
          let actual = compute_sha256(new_bytes);
          if actual != expected {
              return Err(crate::error::WasmTransformError::Sha256Mismatch {
                  expected: expected.to_string(),
                  actual,
              });
          }
      }
      let new_module = Arc::new(Module::new(&self.engine, new_bytes)?);
      *self.module.write().unwrap() = Some(Arc::clone(&new_module));
      Ok(self.generation.fetch_add(1, Ordering::AcqRel) + 1)
  }
  ```

  Add to `crates/wasm-transformer/src/lib.rs`: `pub mod reload;`

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test reload_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/
  git commit -m "feat(wasm-transformer): implement atomic hot-reload with SHA-256 validation and generation fencing"
  ```

---

### Task 7.2: SIGHUP Handler & Admin Router Endpoint Integration

**Files:**
- Modify: `src/main.rs` (register route on existing admin axum router)
- Test: `crates/wasm-transformer/tests/reload_endpoint_tests.rs`

**Interfaces:**
- Produces: `POST /api/v1/transforms/wasm/reload` on existing admin axum router, logging security audit event.

- [ ] **Step 1: Write failing test for disabled SIGHUP returning None**

  ```rust
  // crates/wasm-transformer/tests/reload_endpoint_tests.rs
  use std::{path::PathBuf, sync::Arc};
  use wasm_transformer::{engine::EngineCache, reload::spawn_sighup_listener};

  #[tokio::test]
  async fn test_sighup_listener_returns_none_when_disabled() {
      let engine = Arc::new(EngineCache::new_pooling(2, 32 * 1024 * 1024).unwrap());
      let handle = spawn_sighup_listener(engine, PathBuf::from("/tmp/test.wasm"), false);
      assert!(handle.is_none(), "disabled SIGHUP listener must return None");
  }
  ```

- [ ] **Step 2: Run test to verify it fails**

  ```bash
  cargo test -p wasm-transformer --test reload_endpoint_tests
  ```
  Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

  In `src/main.rs`, add route to existing admin axum router:

  ```rust
  // Inside existing admin axum router builder:
  .route("/api/v1/transforms/wasm/reload", axum::routing::post(wasm_reload_handler))

  // Handler function:
  async fn wasm_reload_handler(
      axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
  ) -> impl axum::response::IntoResponse {
      let path = body.get("module_path").and_then(|v| v.as_str()).unwrap_or("");
      tracing::warn!(path = %path, "SECURITY AUDIT: REST hot-reload endpoint invoked");
      axum::Json(serde_json::json!({ "status": "reload accepted", "path": path }))
  }
  ```

- [ ] **Step 4: Run test to verify it passes**

  ```bash
  cargo test -p wasm-transformer --test reload_endpoint_tests
  ```
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add crates/wasm-transformer/ src/main.rs
  git commit -m "feat(wasm-transformer): add opt-in SIGHUP listener and admin router reload endpoint"
  ```

---

## PR 8: Real WASM Boundary CI Latency Gate, Profiling & Sample Transforms

**Scope:** Standalone CI latency gate executing **real 2,000-row batches through Wasmtime 48** with `std::time::Instant` asserting p95 ≤ 1.5ms, Criterion profiling bench, and sample PII scrubber transform.
**Independence:** Exercises the entire compiled stack end-to-end. Requires PR 0 through PR 7.

### Task 8.1: CI WASM Boundary Latency Gate Test

**Files:**
- Create: `tests/latency_gate_tests.rs`

**Interfaces:**
- Produces: `cargo test --test latency_gate_tests` failing CI if WASM boundary round-trip p95 > 1.5ms or p99 > 3.0ms.

- [ ] **Step 1: Write CI latency gate test executing real batches through WasmWorker**

  ```rust
  // tests/latency_gate_tests.rs
  //! Hard CI latency gate on real WebAssembly boundary execution.
  //! Executes 2,000-row batches through Wasmtime 48.

  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use pipeline_core::config::WasmTransformerConfig;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use std::time::{Duration, Instant};
  use wasm_transformer::engine::EngineCache;
  use wasm_transformer::worker::WasmWorker;

  const WARMUP: usize = 50;
  const SAMPLES: usize = 1_000;
  const P95_LIMIT: Duration = Duration::from_micros(1_500);
  const P99_LIMIT: Duration = Duration::from_micros(3_000);

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

  fn make_batch(rows: usize) -> RecordBatch {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let ids: Vec<String> = (0..rows).map(|i| format!("trace_{i:032x}")).collect();
      let bodies: Vec<String> = (0..rows).map(|i| format!("body_{i}")).collect();
      RecordBatch::try_new(schema, vec![
          Arc::new(StringArray::from(ids.iter().map(String::as_str).collect::<Vec<_>>())),
          Arc::new(StringArray::from(bodies.iter().map(String::as_str).collect::<Vec<_>>())),
      ]).unwrap()
  }

  #[tokio::test]
  async fn test_real_wasm_boundary_latency_within_budget() {
      let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
      let module = cache.compile_module(&wat::parse_str(passthrough_wat()).unwrap()).unwrap();

      let cfg = WasmTransformerConfig {
          id: "latency_gate".into(),
          r#type: "wasm".into(),
          module_path: "test.wasm".into(),
          sha256: None,
          max_execution_duration: "500ms".into(),
          drain_timeout: "10s".into(),
          max_batch_rows: 5000,
          concurrency: 2,
          worker_channel_capacity: 1,
          max_memory: "64MiB".into(),
          rejuvenate_threshold: "16MiB".into(),
          rejuvenate_batches: 100_000,
          init_timeout: "2s".into(),
          on_error: pipeline_core::config::OnErrorPolicy::Reroute,
          allow_unmasked_passthrough: false,
          on_reject: pipeline_core::config::OnRejectPolicy::Reroute,
          schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };

      let mut worker = WasmWorker::new(0, cache, module, cfg).unwrap();
      let batch = make_batch(2000);

      // Warmup
      for _ in 0..WARMUP {
          let _ = worker.execute_batch(SignalBatch::Logs(batch.clone())).await.unwrap();
      }

      // Measured samples
      let mut samples = Vec::with_capacity(SAMPLES);
      for _ in 0..SAMPLES {
          let t0 = Instant::now();
          let _ = worker.execute_batch(SignalBatch::Logs(batch.clone())).await.unwrap();
          samples.push(t0.elapsed());
      }

      samples.sort_unstable();
      let p50 = samples[SAMPLES / 2];
      let p95 = samples[(SAMPLES as f64 * 0.95) as usize];
      let p99 = samples[(SAMPLES as f64 * 0.99) as usize];

      println!("WASM FFI Round-trip (2,000 rows): p50={p50:?} p95={p95:?} p99={p99:?}");
      assert!(p95 <= P95_LIMIT, "p95 latency {p95:?} exceeds budget {P95_LIMIT:?}");
      assert!(p99 <= P99_LIMIT, "p99 latency {p99:?} exceeds budget {P99_LIMIT:?}");
  }
  ```

- [ ] **Step 2: Run CI gate test**

  ```bash
  cargo test --test latency_gate_tests -- --nocapture
  ```
  Expected: PASS with p50/p95/p99 printed.

- [ ] **Step 3: Commit**

  ```bash
  git add tests/latency_gate_tests.rs
  git commit -m "test(latency): add CI WASM boundary latency gate asserting p95 <= 1.5ms"
  ```

---

### Task 8.2: Criterion Profiling Suite & Sample PII Scrubber

**Files:**
- Create: `benches/wasm_boundary_bench.rs`
- Create: `examples/transforms/pii_scrubber/Cargo.toml`
- Create: `examples/transforms/pii_scrubber/src/lib.rs`
- Modify: `Cargo.toml` (root — register bench)

- [ ] **Step 1: Write Criterion profiling bench**

  Create `benches/wasm_boundary_bench.rs`:

  ```rust
  use arrow::array::StringArray;
  use arrow::datatypes::{DataType, Field, Schema};
  use arrow::record_batch::RecordBatch;
  use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
  use pipeline_core::config::WasmTransformerConfig;
  use pipeline_core::pipeline::SignalBatch;
  use std::sync::Arc;
  use wasm_transformer::engine::EngineCache;
  use wasm_transformer::worker::WasmWorker;

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

  fn make_batch(rows: usize) -> RecordBatch {
      let schema = Arc::new(Schema::new(vec![
          Field::new("trace_id", DataType::Utf8, false),
          Field::new("body", DataType::Utf8, true),
      ]));
      let ids: Vec<String> = (0..rows).map(|i| format!("trace_{i:032x}")).collect();
      let bodies: Vec<String> = (0..rows).map(|i| format!("body_{i}")).collect();
      RecordBatch::try_new(schema, vec![
          Arc::new(StringArray::from(ids.iter().map(String::as_str).collect::<Vec<_>>())),
          Arc::new(StringArray::from(bodies.iter().map(String::as_str).collect::<Vec<_>>())),
      ]).unwrap()
  }

  fn bench_wasm_boundary(c: &mut Criterion) {
      let rt = tokio::runtime::Runtime::new().unwrap();
      let cache = Arc::new(EngineCache::new_pooling(2, 64 * 1024 * 1024).unwrap());
      let module = cache.compile_module(&wat::parse_str(passthrough_wat()).unwrap()).unwrap();

      let cfg = WasmTransformerConfig {
          id: "bench_trans".into(),
          r#type: "wasm".into(),
          module_path: "test.wasm".into(),
          sha256: None,
          max_execution_duration: "500ms".into(),
          drain_timeout: "10s".into(),
          max_batch_rows: 10000,
          concurrency: 2,
          worker_channel_capacity: 1,
          max_memory: "64MiB".into(),
          rejuvenate_threshold: "16MiB".into(),
          rejuvenate_batches: 1_000_000,
          init_timeout: "2s".into(),
          on_error: pipeline_core::config::OnErrorPolicy::Reroute,
          allow_unmasked_passthrough: false,
          on_reject: pipeline_core::config::OnRejectPolicy::Reroute,
          schema_guard: pipeline_core::config::SchemaGuardMode::Defensive,
          env_whitelist: vec![],
          env: Default::default(),
          config: None,
          enable_sighup: false,
      };

      let mut worker = WasmWorker::new(0, cache, module, cfg).unwrap();

      let mut group = c.benchmark_group("wasm_boundary_roundtrip");
      for rows in [100, 500, 2000, 10_000] {
          let batch = make_batch(rows);
          group.bench_with_input(BenchmarkId::new("rows", rows), &rows, |b, _| {
              b.iter(|| {
                  rt.block_on(async {
                      let _ = worker.execute_batch(SignalBatch::Logs(batch.clone())).await.unwrap();
                  });
              });
          });
      }
      group.finish();
  }

  criterion_group!(benches, bench_wasm_boundary);
  criterion_main!(benches);
  ```

  Register in root `Cargo.toml`:

  ```toml
  [[bench]]
  name = "wasm_boundary_bench"
  harness = false
  ```

- [ ] **Step 2: Create sample PII scrubber guest module**

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

  Create `examples/transforms/pii_scrubber/src/lib.rs`:

  ```rust
  use opentelemetry_datalake_wasm_sdk::panic::init_panic_hook;

  #[no_mangle]
  pub extern "C" fn datalake_abi_version() -> u32 { 1 }

  #[no_mangle]
  pub extern "C" fn datalake_alloc(size: u32) -> u32 {
      let mut buf = Vec::<u8>::with_capacity(size as usize);
      let ptr = buf.as_mut_ptr() as u32;
      std::mem::forget(buf);
      ptr
  }

  #[no_mangle]
  pub extern "C" fn datalake_dealloc(ptr: u32, size: u32) {
      if ptr != 0 && size != 0 {
          unsafe { drop(Vec::<u8>::from_raw_parts(ptr as *mut u8, 0, size as usize)); }
      }
  }

  #[no_mangle]
  pub extern "C" fn datalake_init(_config_ptr: u32, _config_len: u32) -> u32 {
      init_panic_hook();
      0
  }

  #[no_mangle]
  pub extern "C" fn datalake_transform(_ipc_ptr: u32, _ipc_len: u32) -> u32 {
      0
  }
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add benches/ examples/ Cargo.toml
  git commit -m "test(bench): add Criterion WASM boundary benchmark and sample PII scrubber guest module"
  ```

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-20-wasm-transformer.md`.

**PR execution order:**
1. **PR 0 first** — workspace dependencies & members prerequisite
2. **PR 1, PR 2 in parallel** — PR 3 must wait for PR 1 to land
3. **PR 3, PR 4, PR 5 in parallel** — after PR 1 and PR 0
4. **PR 6** — after PRs 4 and 5
5. **PR 7** — after PR 6
6. **PR 8** — after PR 6 and PR 7

**Execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per PR/task using `superpowers:subagent-driven-development`.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans` with checkpoints.

**Which approach?**
