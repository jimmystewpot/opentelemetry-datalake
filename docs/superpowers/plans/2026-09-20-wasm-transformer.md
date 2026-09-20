# WASM Whole-Batch Arrow Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance WebAssembly (WASM) transformer for `opentelemetry-datalake` allowing users to manipulate, enrich, filter, or drop whole Arrow `RecordBatch`es, with a publishable guest SDK and testing/validation CLI.

**Architecture:** A three-crate workspace architecture: `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`) provides the guest development experience and Arrow IPC bindings; `crates/wasm-transformer` provides the host-side `pipeline_core::pipeline::Transform` engine using `wasmtime` with epoch-based timeouts and instance pooling; `crates/wasm-cli` (`datalake-wasm`) provides CLI verification and testing tooling.

**Tech Stack:** Rust 2024 edition, Apache Arrow (v59), `wasmtime` (v31+), `arrow-ipc`, `tokio`, `tracing`, `clap`, `thiserror`.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints
- Strictly adhere to the zero-panic policy in production code (`src/`): no `unwrap()`, `expect()`, or `panic!()`.
- All FFI exchanges across WASM linear memory use standard Apache Arrow IPC streaming format.
- CPU timeouts must be enforced via wall-clock duration using Wasmtime epoch interruption (`max_execution_duration`).
- Linear memory caps must be enforced per instance via `wasmtime::ResourceLimiter` (`max_memory`).
- Host logger imports must enrich guest log events with `instance_id`, `signal_type`, and guest source location (`file`, `line`, `target`).
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

Add `wasmtime = { version = "31", default-features = false, features = ["cranelift", "async", "pooling-allocator"] }` to root `[workspace.dependencies]`.
Create:
- `crates/wasm-sdk/Cargo.toml` with package name `opentelemetry-datalake-wasm-sdk`.
- `crates/wasm-transformer/Cargo.toml` with `wasmtime`, `pipeline-core`, `arrow`, `arrow-ipc`.
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

### Task 2: Implement Guest SDK (`crates/wasm-sdk`)

**Files:**
- Create: `crates/wasm-sdk/src/abi.rs`
- Create: `crates/wasm-sdk/src/ipc.rs`
- Create: `crates/wasm-sdk/src/helpers.rs`
- Create: `crates/wasm-sdk/src/logger.rs`
- Modify: `crates/wasm-sdk/src/lib.rs`
- Test: `crates/wasm-sdk/tests/sdk_abi_tests.rs`

**Interfaces:**
- Consumes: `arrow` RecordBatch & Array types.
- Produces: `BatchTransformer` trait, `TransformResult`, `SignalType`, `export_transformer!` macro, `helpers` module.

- [ ] **Step 1: Write the failing test for SDK Arrow IPC serialization and helper methods**

Write unit test in `crates/wasm-sdk/tests/sdk_abi_tests.rs` creating a `RecordBatch`, writing to IPC stream using `crates/wasm-sdk/src/ipc.rs`, and verifying roundtrip reading.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test sdk_abi_tests`  
Expected: FAIL with unresolved modules.

- [ ] **Step 3: Implement `ipc.rs`, `abi.rs`, `helpers.rs`, and `lib.rs`**

Implement:
- `ipc::read_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, ArrowError>`
- `ipc::write_ipc_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError>`
- `helpers::drop_columns(batch: &RecordBatch, names: &[&str]) -> Result<RecordBatch, ArrowError>`
- `helpers::filter_batch(batch: &RecordBatch, predicate: &arrow::array::BooleanArray) -> Result<RecordBatch, ArrowError>`
- `export_transformer!` macro generating `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-sdk
git commit -m "feat(wasm-sdk): implement BatchTransformer trait, Arrow IPC codec, and export macro"
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

### Task 4: Host Wasmtime Engine & Resource Management (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/error.rs`
- Create: `crates/wasm-transformer/src/config.rs`
- Create: `crates/wasm-transformer/src/engine.rs`
- Create: `crates/wasm-transformer/src/instance.rs`
- Test: `crates/wasm-transformer/tests/engine_tests.rs`

**Interfaces:**
- Consumes: `WasmTransformerConfig` (module path, max_execution_duration, max_memory).
- Produces: `WasmEngine`, `WasmInstance`, `HostState`, `WasmTransformError`.

- [ ] **Step 1: Write failing test for engine compilation, epoch ticking, and memory limit**

Write `tests/engine_tests.rs` verifying:
- Compilation of a minimal WebAssembly module.
- Memory limiter rejects allocations exceeding `max_memory`.
- Epoch interruption triggers timeout when execution loop exceeds deadline.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test engine_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `error.rs`, `config.rs`, `engine.rs`, and `instance.rs`**

Implement:
- `WasmTransformError` with `thiserror` (Timeout, OutOfMemory, Trap, IpcError, InitializationFailed).
- `WasmEngine::new(config: &WasmTransformerConfig)` initializing `wasmtime::Engine` with `epoch_interruption(true)`.
- Background ticker thread calling `engine.increment_epoch()` every 10ms.
- `ResourceLimiter` implementation in `instance.rs` enforcing `max_memory` bytes.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test engine_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement wasmtime engine, epoch ticker, and memory limiter"
```

---

### Task 5: Host Imports & Guest Log Forwarding (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/host_calls.rs`
- Modify: `crates/wasm-transformer/src/instance.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

**Interfaces:**
- Consumes: Wasmtime `Linker<HostState>`.
- Produces: Imported host functions (`datalake_host_log`, `datalake_host_metric_inc`, `datalake_host_has_capability`).

- [ ] **Step 1: Write failing test for host log interception and capability probe**

Write `tests/host_calls_tests.rs` where a test WASM module invokes `datalake_host_log` and `datalake_host_has_capability`, asserting logs are emitted with instance ID and capability checks return 1.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test host_calls_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `host_calls.rs`**

Register imported functions in `wasmtime::Linker<HostState>`:
- `datalake_host_log`: reads guest string, emits `tracing::event!` with `instance_id`, `signal`, `file`, `line`.
- `datalake_host_metric_inc`: increments named metric counter.
- `datalake_host_has_capability`: checks supported capability strings.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test host_calls_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement host functions for logging, metrics, and capability probing"
```

---

### Task 6: Host `WasmTransformer` & Worker Pool Implementation

**Files:**
- Create: `crates/wasm-transformer/src/worker.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/transformer_pipeline_tests.rs`

**Interfaces:**
- Consumes: `pipeline_core::pipeline::Transform`, `PipelineReceiver`, `PipelineSender`.
- Produces: `WasmTransformer` struct implementing `Transform`.

- [ ] **Step 1: Write failing test for `Transform::transform` stream execution**

Write `tests/transformer_pipeline_tests.rs` running `SignalBatch::Logs` through `WasmTransformer`, asserting output batches are correctly received on `PipelineSender`, and aborts drop batches with incremented metrics.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test transformer_pipeline_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `worker.rs` and `lib.rs`**

Implement:
- Batch serialization to Arrow IPC.
- Calling `datalake_alloc`, writing buffer, invoking `datalake_transform`, parsing `TransformResponseHeader`.
- Handling `status == 1` (Drop/Abort), `status == 2` (Error), and `status == 0` (Success).
- Deallocating memory.
- Worker task pool reading from `input: PipelineReceiver` and sending to `output: PipelineSender`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test transformer_pipeline_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement Transform trait with worker pool and Arrow IPC batch exchange"
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

Write `tests/cli_tests.rs` calling validator on a valid vs invalid WASM binary (missing `datalake_transform` export), asserting `validate` returns Ok or Err.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-cli --test cli_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `validator.rs`, `runner.rs`, and `main.rs`**

Implement:
- `validate`: parses WASM exports, asserts presence of `datalake_alloc`, `datalake_dealloc`, `datalake_transform`, `datalake_init`.
- `test`: runs synthetic Arrow batch through module, validates output schema, verifies memory cleanup.
- `bench`: executes N batches and prints latency distribution.

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

Write test verifying that configuring `[pipeline.transform.wasm]` correctly constructs `WasmTransformer` instead of `NoopTransformer`.

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

### Task 9: End-to-End Example Module & Quality Gates

**Files:**
- Create: `examples/wasm-pii-scrubber/Cargo.toml`
- Create: `examples/wasm-pii-scrubber/src/lib.rs`
- Create: `examples/wasm-pii-scrubber/tests/scrubber_test.rs`
- Modify: `docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`

**Interfaces:**
- Consumes: `opentelemetry-datalake-wasm-sdk`.
- Produces: Working example WASM module demonstrating credit card masking and deliberate test batch dropping.

- [ ] **Step 1: Create example PII scrubber module**

Implement a `BatchTransformer` that detects credit cards in log messages, redacts them to `[REDACTED]`, and drops payloads matching test conditions.

- [ ] **Step 2: Run native tests for example module**

Run: `cargo test -p wasm-pii-scrubber`  
Expected: PASS.

- [ ] **Step 3: Run full workspace quality gates**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
cargo test --workspace
```
Expected: PASS with 0 warnings and 0 errors.

- [ ] **Step 4: Commit**

```bash
git add -f examples/wasm-pii-scrubber
git commit -m "docs(example): add wasm-pii-scrubber example module and verify quality gates"
```
