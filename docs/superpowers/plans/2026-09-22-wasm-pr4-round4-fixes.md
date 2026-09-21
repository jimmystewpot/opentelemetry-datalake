# Wasm Transformer PR 4 Round 4 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve compilation breakages from PR 62 merge (`dashmap` dependency and pattern match deref), support the documented 3-argument C-ABI v1 `datalake_transform(signal_type, ipc_ptr, ipc_len) -> u64`, and wire the host linker with `MetricRegistry` into `WasmWorker`.

**Architecture:**
1. Update `crates/wasm-transformer/Cargo.toml` to include `dashmap = { workspace = true }`.
2. Fix Rust 2024 pattern binding dereferences in `crates/wasm-transformer/src/host_calls.rs`.
3. In `crates/wasm-transformer/src/worker.rs`:
   - Introduce `TransformFunc` enum supporting `V1Standard(TypedFunc<(u32, u32, u32), u64>)` and `Legacy(TypedFunc<(u32, u32), u32>)`.
   - In `instantiate_guest`, probe for `(u32, u32, u32) -> u64` first; if signature mismatch, probe for `(u32, u32) -> u32`.
   - In `execute_batch`, pass signal type (`0` for Logs, `1` for Metrics, `2` for Traces), `ipc_ptr`, `ipc_len`. Unpack the packed 64-bit response header pointer `(res >> 32) as u32` (or `res as u32` if top 32 bits are 0).
   - In `WasmWorker`, store a `Arc<MetricRegistry>` and instantiate via `build_host_linker`.
4. Add comprehensive unit tests verifying both 3-argument and 2-argument transform guests, metric emissions, and host logging.

---

### Task 1: Fix `dashmap` Dependency & `host_calls.rs` Compilation

**Files:**
- Modify: `crates/wasm-transformer/Cargo.toml`
- Modify: `crates/wasm-transformer/src/host_calls.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

- [ ] **Step 1: Add `dashmap = { workspace = true }` to `crates/wasm-transformer/Cargo.toml`**
- [ ] **Step 2: Fix pattern dereference in `crates/wasm-transformer/src/host_calls.rs`**
- [ ] **Step 3: Run `cargo check -p wasm-transformer` and verify compilation**
- [ ] **Step 4: Run `cargo test -p wasm-transformer --test host_calls_tests` and verify all tests pass**

---

### Task 2: Implement C-ABI v1 Transform Signature `(u32, u32, u32) -> u64` with Dual-Mode Fallback

**Files:**
- Modify: `crates/wasm-transformer/src/worker.rs`
- Test: `crates/wasm-transformer/tests/worker_execution_tests.rs`

- [ ] **Step 1: Add a failing unit test for 3-argument C-ABI v1 `datalake_transform` in `tests/worker_execution_tests.rs`**
- [ ] **Step 2: Implement `TransformFunc` dual-resolution in `worker.rs`**
- [ ] **Step 3: Run tests and verify both C-ABI v1 and legacy WAT tests pass cleanly**

---

### Task 3: Wire Host Linker into `WasmWorker`

**Files:**
- Modify: `crates/wasm-transformer/src/worker.rs`
- Test: `crates/wasm-transformer/tests/worker_execution_tests.rs`

- [ ] **Step 1: Add a unit test verifying a guest module with `datalake_host_log` and `datalake_host_metric_emit` imports instantiates and executes cleanly**
- [ ] **Step 2: Update `WasmWorker` to use `build_host_linker` and store `HostState`**
- [ ] **Step 3: Run all unit and integration tests across `wasm-transformer` and verify green**

---

### Task 4: Full Workspace Verification & Linting

**Files:**
- Check: All crates

- [ ] **Step 1: Run `cargo fmt --all -- --check`**
- [ ] **Step 2: Run `cargo clippy --all-targets -p wasm-transformer -p datalake-wasm-tool -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`**
- [ ] **Step 3: Run `cargo test --all-targets`**
