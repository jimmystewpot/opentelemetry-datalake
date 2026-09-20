# WASM Whole-Batch Arrow Transformer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance WebAssembly (WASM) transformer for `opentelemetry-datalake` allowing users to manipulate, enrich, filter, or drop whole Arrow `RecordBatch`es, with a publishable guest SDK and testing/validation CLI.

**Architecture:** A three-crate workspace architecture: `crates/wasm-sdk` (`opentelemetry-datalake-wasm-sdk`) provides the guest development experience, Arrow IPC bindings, and manual FFI escape hatch; `crates/wasm-transformer` provides the host-side `pipeline_core::pipeline::Transform` engine using `wasmtime` with epoch-based timeouts, instance pooling, and a bounded FIFO re-sequencing buffer; `crates/wasm-cli` (`datalake-wasm`) provides CLI verification, latency benchmarking, and testing tooling.

**Tech Stack:** Rust 2024 edition, Apache Arrow (v59), `wasmtime` (v31+), `arrow-ipc`, `tokio`, `tracing`, `clap`, `thiserror`.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints
- Strictly adhere to the zero-panic policy in production code (`src/`): no `unwrap()`, `expect()`, or `panic!()`.
- All FFI exchanges across WASM linear memory use versioned C-ABI v1 with standard Apache Arrow IPC streaming format.
- CPU timeouts must be enforced via wall-clock duration using Wasmtime epoch interruption (`max_execution_duration`).
- Linear memory caps must be enforced per instance via `wasmtime::ResourceLimiter` (`max_memory`).
- Host logger imports (`datalake_host_v1`) pass a structured `HostLogRecord` and enrich log events with `instance_id`, `signal_type`, and guest source location (`file`, `line`, `target`).
- Batch ordering must be preserved via a bounded FIFO re-sequencer when `ordered = true` (default).
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
- Produces: `BatchTransformer` trait, `TransformResult`, `SignalType`, `export_transformer!` macro, `abi::raw_alloc`, `abi::dispatch_transform` (manual escape hatch), `helpers` module.

- [ ] **Step 1: Write the failing test for SDK Arrow IPC serialization, ABI versioning, and manual escape hatch**

Write unit test in `crates/wasm-sdk/tests/sdk_abi_tests.rs` testing IPC stream roundtripping, verifying `datalake_abi_version() == 1`, and testing `dispatch_transform` without macros.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk --test sdk_abi_tests`  
Expected: FAIL with unresolved modules.

- [ ] **Step 3: Implement `ipc.rs`, `abi.rs`, `helpers.rs`, and `lib.rs`**

Implement:
- `abi::raw_alloc`, `abi::raw_dealloc`, `abi::dispatch_transform`
- `ipc::read_ipc_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, ArrowError>`
- `ipc::write_ipc_stream(batches: &[RecordBatch]) -> Result<Vec<u8>, ArrowError>`
- `helpers::drop_columns(batch: &RecordBatch, names: &[&str]) -> Result<RecordBatch, ArrowError>`
- `helpers::filter_batch(batch: &RecordBatch, predicate: &arrow::array::BooleanArray) -> Result<RecordBatch, ArrowError>`
- `export_transformer!` macro generating `datalake_abi_version`, `datalake_alloc`, `datalake_dealloc`, `datalake_init`, `datalake_transform`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p opentelemetry-datalake-wasm-sdk`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-sdk
git commit -m "feat(wasm-sdk): implement BatchTransformer trait, Arrow IPC codec, export macro, and manual FFI escape hatch"
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
- Consumes: `WasmTransformerConfig` (module path, max_execution_duration, max_memory, on_error, allow_unmasked_passthrough).
- Produces: `WasmEngine`, `WasmInstance`, `HostState`, `WasmTransformError`.

- [ ] **Step 1: Write failing test for engine compilation, epoch ticking, and memory limit**

Write `tests/engine_tests.rs` verifying:
- Compilation of a minimal WebAssembly module.
- Validation that `on_error = "passthrough"` without `allow_unmasked_passthrough = true` returns a configuration error.
- Memory limiter rejects allocations exceeding `max_memory`.
- Epoch interruption triggers timeout when execution loop exceeds deadline.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test engine_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `error.rs`, `config.rs`, `engine.rs`, and `instance.rs`**

Implement:
- `WasmTransformError` with `thiserror` (Timeout, OutOfMemory, Trap, IpcError, InitializationFailed, AbiVersionMismatch, SecurityConfigurationError).
- `WasmEngine::new(config: &WasmTransformerConfig)` initializing `wasmtime::Engine` with `epoch_interruption(true)`.
- Background ticker thread calling `engine.increment_epoch()` every 10ms.
- `ResourceLimiter` implementation in `instance.rs` enforcing `max_memory` bytes.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test engine_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement wasmtime engine, epoch ticker, and security config validation"
```

---

### Task 5: Host Imports & Structured Guest Log Forwarding (`crates/wasm-transformer`)

**Files:**
- Create: `crates/wasm-transformer/src/host_calls.rs`
- Modify: `crates/wasm-transformer/src/instance.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

**Interfaces:**
- Consumes: Wasmtime `Linker<HostState>`.
- Produces: Imported host functions under `datalake_host_v1` (`datalake_host_log` with `HostLogRecord`, `datalake_host_metric_inc`, `datalake_host_has_capability`).

- [ ] **Step 1: Write failing test for structured host log interception and capability probe**

Write `tests/host_calls_tests.rs` where a test WASM module invokes `datalake_host_log` with a `HostLogRecord` pointer and `datalake_host_has_capability`, asserting logs are emitted with instance ID and capability checks return 1.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test host_calls_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `host_calls.rs`**

Register imported functions in `wasmtime::Linker<HostState>` under `datalake_host_v1`:
- `datalake_host_log`: reads `HostLogRecord` struct, emits `tracing::event!` with `instance_id`, `signal`, `file`, `line`, `target`.
- `datalake_host_metric_inc`: increments named metric counter.
- `datalake_host_has_capability`: checks supported capability strings.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test host_calls_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement versioned datalake_host_v1 imports with structured HostLogRecord"
```

---

### Task 6: FIFO Re-Sequencing & Host `WasmTransformer` Implementation

**Files:**
- Create: `crates/wasm-transformer/src/reorder.rs`
- Create: `crates/wasm-transformer/src/worker.rs`
- Modify: `crates/wasm-transformer/src/lib.rs`
- Test: `crates/wasm-transformer/tests/reorder_tests.rs`
- Test: `crates/wasm-transformer/tests/transformer_pipeline_tests.rs`

**Interfaces:**
- Consumes: `pipeline_core::pipeline::Transform`, `PipelineReceiver`, `PipelineSender`.
- Produces: `ReorderBuffer`, `WasmTransformer` struct implementing `Transform`.

- [ ] **Step 1: Write failing test for out-of-order completion re-sequencing and pipeline execution**

Write `tests/reorder_tests.rs` verifying that out-of-order batches completed across workers are re-ordered into monotonic sequence order before emitting downstream.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test reorder_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `reorder.rs`, `worker.rs`, and `lib.rs`**

Implement:
- `ReorderBuffer`: bounded min-heap indexed by `seq_id: u64` with immediate contiguous drain.
- Batch serialization to Arrow IPC and version verification (`datalake_abi_version() == 1`).
- Calling `datalake_alloc`, writing buffer, invoking `datalake_transform`, parsing `TransformResponseHeader`.
- Handling `status == 1` (Drop/Abort), `status == 2` (Error), and `status == 0` (Success).
- Memory deallocation.
- Worker task pool with dispatcher and re-sequencer.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test reorder_tests --test transformer_pipeline_tests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -f crates/wasm-transformer
git commit -m "feat(wasm-transformer): implement FIFO re-sequencing buffer and Transform pipeline engine"
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

- [ ] **Step 1: Write failing test for CLI validation subcommand checking ABI version and exports**

Write `tests/cli_tests.rs` calling validator on a valid vs invalid WASM binary (missing `datalake_abi_version` export), asserting `validate` returns Ok or Err.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-cli --test cli_tests`  
Expected: FAIL.

- [ ] **Step 3: Implement `validator.rs`, `runner.rs`, and `main.rs`**

Implement:
- `validate`: verifies `datalake_abi_version() == 1`, asserts presence of exports, checks memory boundaries.
- `test`: runs synthetic Arrow batch through module, validates output schema, verifies memory cleanup.
- `bench`: executes N batches and prints latency distribution (p50, p95, p99) and rows/sec.

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

### Task 9: End-to-End Example Module, Boundary Benchmarks & Quality Gates

**Files:**
- Create: `examples/wasm-pii-scrubber/Cargo.toml`
- Create: `examples/wasm-pii-scrubber/src/lib.rs`
- Create: `examples/wasm-pii-scrubber/tests/scrubber_test.rs`
- Create: `benches/wasm_boundary_bench.rs`

**Interfaces:**
- Consumes: `opentelemetry-datalake-wasm-sdk`, `wasm-transformer`.
- Produces: Working example WASM module and latency budget verification benchmark.

- [ ] **Step 1: Create example PII scrubber module**

Implement a `BatchTransformer` that detects credit cards in log messages, redacts them to `[REDACTED]`, and drops payloads matching test conditions.

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
