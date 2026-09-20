# WASM Whole-Batch Arrow Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance WebAssembly (WASM) transformer for `opentelemetry-datalake` allowing users to manipulate, enrich, filter, or drop whole Arrow `RecordBatch`es, with a publishable guest SDK and testing/validation CLI.

**Architecture:** A three-crate workspace architecture: `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`) provides the guest development experience, panic hooks, init-time capability caching, and Arrow IPC bindings; `crates/wasm-transformer` provides the host-side `pipeline_core::pipeline::Transform` engine using `wasmtime` with pooling instance allocation, zero-trust environment variable whitelisting, typed null schema guards, in-memory single-read SHA-256 verification, and atomic zero-downtime hot-reloading; `crates/wasm-cli` (`datalake-wasm`) provides CLI verification, latency benchmarking, and testing tooling.

**Tech Stack:** Rust 2024 edition, Apache Arrow (v59), `wasmtime` (v31+), `wasmtime-wasi`, `arrow-ipc`, `tokio`, `tracing`, `clap`, `thiserror`, `sha2`, `hex`.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints
- Strictly adhere to the zero-panic policy in production code (`src/`): no `unwrap()`, `expect()`, or `panic!()`.
- All FFI exchanges across WASM linear memory use versioned C-ABI v1 (`datalake_abi_version() == 1`) with standard Apache Arrow IPC streaming format.
- Output batches must preserve canonical OpenTelemetry Arrow schemas; missing fields are backfilled using `arrow::array::new_null_array`.
- Batches exceeding `max_batch_rows` are rejected at the transformer boundary (delegating coalescing/splitting to upcoming `AccumulatorTransformer`).
- WASI environment isolation enforces zero-trust: no ambient host environment variables leak into the guest. Only keys in `env_whitelist` or explicit values in `env` are exposed.
- Memory management uses `wasmtime::PoolingAllocationConfig` with `madvise(MADV_DONTNEED)` resets and dual-trigger rejuvenation.
- Single-read in-memory compilation prevents TOCTOU vulnerabilities when verifying SHA-256 hashes.
- `on_error = "passthrough"` must be rejected at configuration time unless `allow_unmasked_passthrough = true` is explicitly enabled.
- Code must pass `cargo fmt` and `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`.

---

### Task 1: Scaffolding Workspace Crates & Dependencies

**Files:**
- Modify: `Cargo.toml:17-21, 55-65`
- Create: `crates/wasm-sdk/Cargo.toml`
- Create: `crates/wasm-sdk/src/lib.rs`
- Create: `crates/wasm-transformer/Cargo.toml`
- Create: `crates/wasm-transformer/src/lib.rs`
- Create: `crates/wasm-cli/Cargo.toml`
- Create: `crates/wasm-cli/src/main.rs`

**Interfaces:**
- Consumes: Workspace root dependencies (`arrow`, `tokio`, `tracing`, `thiserror`).
- Produces: Workspace crates recognized by `cargo check --workspace`.

- [ ] **Step 1: Write Cargo.toml files for crates**

Add `wasmtime = { version = "31", default-features = false, features = ["cranelift", "async", "pooling-allocator"] }`, `wasmtime-wasi = "31"`, `sha2 = "0.10"`, and `hex = "0.4"` to root `[workspace.dependencies]`.
Create:
- `crates/wasm-sdk/Cargo.toml` with package name `opentelemetry-datalake-wasm-sdk`.
- `crates/wasm-transformer/Cargo.toml` with `wasmtime`, `wasmtime-wasi`, `pipeline-core`, `arrow`, `arrow-ipc`, `sha2`, `hex`.
- `crates/wasm-cli/Cargo.toml` with `clap`, `wasmtime`, `arrow`, `arrow-ipc`.

- [ ] **Step 2: Add placeholder lib.rs and main.rs files**

Create empty modules and structs with documentation comments.

- [ ] **Step 3: Run `cargo check --workspace` to verify scaffolding**

Run: `cargo check --workspace`  
Expected: PASS with 0 errors.

- [ ] **Step 4: Commit**

```bash
git add -f Cargo.toml Cargo.lock crates/wasm-sdk crates/wasm-transformer crates/wasm-cli
git commit -m "chore: scaffold wasm-sdk, wasm-transformer, and wasm-cli crates"
```

---

### Task 2: Implement Guest SDK with Panic Hook & Init Capability Caching (`crates/wasm-sdk`)

**Files:**
- Create: `crates/wasm-sdk/src/abi.rs`
- Create: `crates/wasm-sdk/src/ipc.rs`
- Create: `crates/wasm-sdk/src/helpers.rs`
- Create: `crates/wasm-sdk/src/panic.rs`
- Create: `crates/wasm-sdk/src/logger.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/sdk_abi_tests.rs`

**Interfaces:**
- Consumes: `arrow` RecordBatch & Array types.
- Produces: `BatchTransformer` trait, `TransformResult`, `SignalType`, `export_transformer!` macro, `helpers::nullify_column`, panic hook.

- [ ] **Step 1: Write failing test for SDK Arrow IPC, ABI versioning, and panic handling**

Write unit test in `crates/wasm-sdk/tests/sdk_abi_tests.rs` testing IPC stream roundtripping, verifying `datalake_abi_version() == 1`, column nullification, and panic hook capture.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test sdk_abi_tests`  
Expected: FAIL with unresolved modules.

- [ ] **Step 3: Implement `ipc.rs`, `abi.rs`, `helpers.rs`, `panic.rs`, and `lib.rs`**

Implement:
- `abi::raw_alloc`, `abi::raw_dealloc`, `abi::dispatch_transform`
- `ipc::read_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, ArrowError>`
- `ipc::write_ipc_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError>`
- `helpers::nullify_column(batch: &RecordBatch, name: &str) -> Result<RecordBatch, ArrowError>`
- `helpers::filter_batch(batch: &RecordBatch, predicate: &arrow::array::BooleanArray) -> Result<RecordBatch, ArrowError>`
- `panic::set_wasm_panic_hook()` routing panic location/message to `datalake_host_log`.
- `export_transformer!` macro generating exports and auto-installing panic hook.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-sdk
git commit -m "feat(wasm-sdk): implement BatchTransformer, panic hook, and canonical schema helpers"
```

---

### Task 3: Guest SDK Native Testing Harness & Mock Generators

**Files:**
- Create: `crates/wasm-sdk/src/testing.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/native_test_harness.rs`

**Interfaces:**
- Consumes: `RecordBatch`, `SignalType`.
- Produces: `create_mock_logs_batch`, `create_mock_metrics_batch`, `create_mock_traces_batch`, `MockContext`.

- [ ] **Step 1: Write failing test verifying mock batch generators and trait testing**

Write `tests/native_test_harness.rs` verifying that a mock logs batch can be generated with specified columns and passed into a dummy `BatchTransformer`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test native_test_harness`  
Expected: FAIL with unresolved `testing` module.

- [ ] **Step 3: Implement `testing.rs`**

Implement:
- `create_mock_logs_batch(records: Vec<(&str, &str)>) -> RecordBatch`
- Helper utilities to inspect batches without boilerplate.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test native_test_harness`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-sdk
git commit -m "feat(wasm-sdk): add mock telemetry batch generators and testing utilities"
```

---

### Task 4: Wasmtime Engine, Pooling Allocator & Zero-Trust WASI Env (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/error.rs`
- Create: `crates/wasm-transformer/src/config.rs`
- Create: `crates/wasm-transformer/src/engine.rs`
- Create: `crates/wasm-transformer/src/pool.rs`
- Create: `crates/wasm-transformer/src/wasi_env.rs`
- Test: `crates/wasm-transformer/tests/engine_tests.rs`
- Test: `crates/wasm-transformer/tests/env_whitelist_tests.rs`

**Interfaces:**
- Consumes: `WasmTransformerConfig` (max_memory, rejuvenate_threshold, rejuvenate_batches, sha256, init_timeout, env_whitelist, env).
- Produces: `WasmEngine`, `InstancePool`, `WasiEnvBuilder`, `HostState`, `WasmTransformError`.

- [ ] **Step 1: Write failing test for pooling allocator, epoch timeout, and environment variable whitelisting**

Write `tests/engine_tests.rs` and `tests/env_whitelist_tests.rs` verifying:
- Wasmtime pooling allocator initialization.
- Ambient host environment variables (e.g. `SECRET_HOST_KEY`) are NOT visible to the guest.
- Explicitly whitelisted keys in `env_whitelist` and injected keys in `env` ARE visible to the guest.
- Single-read in-memory SHA-256 verification.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test engine_tests --test env_whitelist_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `error.rs`, `config.rs`, `engine.rs`, `pool.rs`, and `wasi_env.rs`**

Implement:
- `WasmTransformError` with `thiserror`.
- `WasmEngine` configuring `PoolingAllocationConfig` and epoch ticker thread (10ms).
- `InstancePool` managing stores and triggering `madvise(MADV_DONTNEED)` resets on threshold.
- `WasiEnvBuilder` constructing isolated WASI contexts passing only whitelisted and injected variables.
- In-memory single-read SHA-256 verification on module loading.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test engine_tests --test env_whitelist_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement wasmtime pooling allocator, epoch ticker, and zero-trust env whitelist"
```

---

### Task 5: Invariant Batch Guard & Type-Aware Schema Backfill (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/guard.rs`
- Test: `crates/wasm-transformer/tests/guard_tests.rs`

**Interfaces:**
- Consumes: `RecordBatch`, `SignalType`.
- Produces: `BatchGuard` (`max_batch_rows` invariant validation, typed null backfilling via `arrow::array::new_null_array`).

- [ ] **Step 1: Write failing test for max_batch_rows rejection and typed null backfilling**

Write tests asserting:
- Batches $> \text{max\_batch\_rows}$ return `WasmTransformError::BatchTooLarge`.
- Batches missing complex canonical columns (e.g. `attributes: Map<Utf8, Utf8>`) are backfilled with a typed null MapArray.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test guard_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `guard.rs`**

Implement:
- `BatchGuard::validate_batch_size(batch, max_rows) -> Result<(), WasmTransformError>`.
- `BatchGuard::enforce_canonical_schema(signal, batch) -> Result<RecordBatch, WasmTransformError>` utilizing `arrow::array::new_null_array`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test guard_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement invariant batch guard and typed null schema backfiller"
```

---

### Task 6: Host `WasmTransformer` Pipeline & Zero-Downtime Reloading (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/host_calls.rs`
- Create: `crates/wasm-transformer/src/reload.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/transformer_pipeline_tests.rs`
- Test: `crates/wasm-transformer/tests/hot_reload_tests.rs`

**Interfaces:**
- Consumes: `pipeline_core::pipeline::Transform`, `PipelineReceiver`, `PipelineSender`.
- Produces: `WasmTransformer` struct implementing `Transform`, `HotReloader`.

- [ ] **Step 1: Write failing test for concurrent transform execution and atomic hot reload**

Write tests running batches through `WasmTransformer` without ordering bottlenecks, asserting drops increment metrics and triggering hot reload swaps module without dropping batches.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test transformer_pipeline_tests --test hot_reload_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `host_calls.rs`, `reload.rs`, and `lib.rs`**

Implement:
- `datalake_host_log` parsing `HostLogRecord` and printing guest panics with line/file.
- `datalake_host_has_capability` logging warnings if invoked outside `datalake_init`.
- Worker pool reading from `input: PipelineReceiver` and sending directly to `output: PipelineSender`.
- `HotReloader` swapping `Arc<Module>` on reload signal.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test transformer_pipeline_tests --test hot_reload_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement Transform trait, worker pool, and zero-downtime hot reload"
```

---

### Task 7: Developer CLI & Validation Tool (`crates/wasm-cli`)

**Files:**
- Create: `crates/wasm-cli/src/validator.rs`
- Create: `crates/wasm-cli/src/runner.rs`
- Modify: `crates/wasm-cli/src/main.rs`
- Test: `crates/wasm-cli/tests/cli_tests.rs`

**Interfaces:**
- Consumes: Compiled `.wasm` files.
- Produces: CLI binary `datalake-wasm` with `validate`, `test`, `bench` subcommands.

- [ ] **Step 1: Write failing test for CLI validation subcommand**

Write `tests/cli_tests.rs` verifying `validate` checks `datalake_abi_version() == 1` and rejects invalid binaries.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-cli --test cli_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `validator.rs`, `runner.rs`, and `main.rs`**

Implement:
- `validate`: verifies ABI v1, exports, and memory boundaries.
- `test`: executes synthetic batches with DWARF debug info enabled for full stack traces; supports `--env KEY=VAL`.
- `bench`: measures p50/p95/p99 latency distribution.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-cli --test cli_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-cli
git commit -m "feat(wasm-cli): implement datalake-wasm validate, test, and bench subcommands"
```

---

### Task 8: Pipeline Core & `main.rs` Integration

**Files:**
- Modify: `crates/core/src/config.rs`
- Modify: `src/main.rs`
- Test: `tests/wasm_integration_tests.rs`

**Interfaces:**
- Consumes: TOML config file with `[pipeline.transforms.wasm]` options.
- Produces: Runtime pipeline selecting `WasmTransformer` when configured.

- [ ] **Step 1: Write integration test with TOML config containing wasm transformer**

Write test verifying that configuring `[pipeline.transform.wasm]` constructs `WasmTransformer` and validates security configuration (`on_error = "passthrough"` requires `allow_unmasked_passthrough = true`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test wasm_integration_tests`  
Expected: FAIL with unrecognized config field.

- [ ] **Step 3: Implement config parsing in `crates/core/src/config.rs` and pipeline wiring in `src/main.rs`**

Add `WasmTransformerConfig` to `PipelineConfig` and wire into `src/main.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test wasm_integration_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/core src/main.rs tests/wasm_integration_tests.rs
git commit -m "feat: integrate wasm transformer into datalake configuration and main pipeline"
```

---

### Task 9: End-to-End Example Module, Boundary Benchmarks & Quality Gates

**Files:**
- Create: `examples/wasm-pii-scrubber/Cargo.toml`
- Create: `examples/wasm-pii-scrubber/src/lib.rs`
- Create: `examples/wasm-pii-scrubber/tests/scrubber_test.rs`
- Create: `benches/wasm_boundary_bench.rs`

**Interfaces:**
- Consumes: `opentelemetry-datalake-wasm-sdk`, `wasm-transformer`.
- Produces: Working example WASM module and latency budget verification benchmark ($p95 \le 1.5\text{ms}$).

- [ ] **Step 1: Create example PII scrubber module**

Implement a `BatchTransformer` that detects credit cards in log messages, redacts them to `[REDACTED]`, and reads whitelisted environment variables.

- [ ] **Step 2: Run native tests for example module**

Run: `cargo test -p wasm-pii-scrubber`  
Expected: PASS.

- [ ] **Step 3: Add and run boundary latency benchmark**

Write `benches/wasm_boundary_bench.rs` and run criterion benchmark:
```bash
cargo bench --bench wasm_boundary_bench
```
Verify round-trip overhead satisfies $p95 \le 1.5\text{ms}$.

- [ ] **Step 4: Run full workspace quality gates**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo test --workspace
```
Expected: PASS with 0 warnings and 0 errors.

- [ ] **Step 5: Commit**

```bash
git add -f examples/wasm-pii-scrubber benches/wasm_boundary_bench.rs
git commit -m "docs(example): add wasm-pii-scrubber example, latency benchmark, and verify quality gates"
```
