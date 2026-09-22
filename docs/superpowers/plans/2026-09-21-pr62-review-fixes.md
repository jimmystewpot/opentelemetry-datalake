# PR 62 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all defects and edge cases discovered during Principal Code Review of PR #62, fix 32-bit address overflow validation, ensure schema nullability consistency on backfilled missing columns, and raise test coverage to 100% across `crates/wasm-transformer/src/guard.rs` and `crates/wasm-transformer/src/host_calls.rs`.

**Architecture:** Refactor `read_guest_string` in `host_calls.rs` to validate pointer arithmetic using 32-bit checked addition (`ptr.checked_add(len)`), eliminating unreachable 64-bit conversion branches. Update `backfill_missing_columns` in `guard.rs` to promote backfilled non-nullable fields to `with_nullable(true)` so downstream Arrow and Parquet writers remain structurally sound. Add targeted edge-case unit tests in `host_calls_tests.rs` and `guard_tests.rs`.

**Tech Stack:** Rust 2024, Apache Arrow 59, Wasmtime 48, Tokio 1.37, DashMap 6, tracing, wat 1.259.

**Spec:** [`docs/superpowers/specs/2026-09-20-wasm-transformer-design.md`](file:///home/jalamb/go/src/github.com/jimmystewpot/opentelemetry-datalake/docs/superpowers/specs/2026-09-20-wasm-transformer-design.md)

## Global Constraints

- **Zero-Panic Production Rule**: No `unwrap()`, `expect()`, `panic!()`, or `todo!()` in production paths (`src/`). All failures propagate via `thiserror` domain errors.
- **Code Standards**: Zero Clippy warnings under `cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`. Format with `cargo fmt`. Prefer `crate::` over `super::` in non-test code.
- **Memory & Latency**: Zero additional hot-path allocations. Bounded string reads capped at `MAX_METRIC_NAME_LEN` (256 B) and `MAX_LOG_MESSAGE_LEN` (64 KiB).
- **Test Coverage**: All branches in `guard.rs` and `host_calls.rs` must have active regression tests.

---

### Task 1: Defensive Nullability & Allocation Optimization in `guard.rs`

**Files:**
- Modify: `crates/wasm-transformer/src/guard.rs`
- Test: `crates/wasm-transformer/tests/guard_tests.rs`

**Interfaces:**
- Consumes: `input_schema: &Schema`, `output: RecordBatch`.
- Produces: `backfill_missing_columns(input_schema: &Schema, output: RecordBatch) -> Result<RecordBatch, WasmTransformError>`.

- [ ] **Step 1: Write failing test for backfilling non-nullable missing columns**

In `crates/wasm-transformer/tests/guard_tests.rs`, add:
```rust
#[test]
fn test_backfill_non_nullable_missing_column_sets_nullable_true() {
    let full_schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("non_nullable_counter", DataType::Int64, false), // Non-nullable in input!
    ]));
    let partial_schema = Arc::new(Schema::new(vec![Field::new(
        "trace_id",
        DataType::Utf8,
        false,
    )]));
    let output = RecordBatch::try_new(
        partial_schema,
        vec![Arc::new(StringArray::from(vec!["id_1", "id_2"]))],
    )
    .unwrap();

    let backfilled = backfill_missing_columns(&full_schema, output).unwrap();
    assert_eq!(backfilled.num_columns(), 2);
    assert_eq!(backfilled.num_rows(), 2);
    assert_eq!(backfilled.column(1).null_count(), 2);

    // The backfilled column MUST have is_nullable == true, even though input was false
    assert!(
        backfilled.schema().field(1).is_nullable(),
        "Backfilled null column must have is_nullable == true"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p wasm-transformer --test guard_tests test_backfill_non_nullable_missing_column_sets_nullable_true`
Expected: FAIL (assertion `backfilled.schema().field(1).is_nullable()` fails).

- [ ] **Step 3: Update `backfill_missing_columns` in `guard.rs`**

In `crates/wasm-transformer/src/guard.rs`:
1. Size vector capacities with `input_schema.fields().len().max(output_schema.fields().len())`.
2. When backfilling missing columns:
```rust
let field = if field.is_nullable() {
    Arc::clone(field)
} else {
    Arc::new(field.as_ref().clone().with_nullable(true))
};
fields.push(field);
columns.push(new_null_array(field.data_type(), num_rows));
added = true;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p wasm-transformer --test guard_tests`
Expected: PASS.

- [ ] **Step 5: Format and lint**

Run: `cargo fmt --check && cargo clippy -p wasm-transformer --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: PASS with 0 warnings.

---

### Task 2: 32-Bit Linear Memory Bounds & Dead-Branch Elimination in `host_calls.rs`

**Files:**
- Modify: `crates/wasm-transformer/src/host_calls.rs`
- Test: `crates/wasm-transformer/tests/host_calls_tests.rs`

**Interfaces:**
- Consumes: `Caller<'_, HostState>`, `ptr: u32`, `len: u32`, `max_len: usize`.
- Produces: `read_guest_string(...) -> Option<String>`.

- [ ] **Step 1: Write failing tests for gauge emission, non-memory export, and log message length capping**

In `crates/wasm-transformer/tests/host_calls_tests.rs`, add:
```rust
#[test]
fn test_edge_case_gauge_metric_emission_from_wasm() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("gauge_wasm"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (memory (export "memory") 1)
        (data (i32.const 0) "cpu_usage")
        (func (export "emit_gauge")
            ;; metric_type = 1 (GAUGE), name_ptr = 0, name_len = 9, value = 4607182418800017408 (1.0 f64 bits)
            (call $metric (i32.const 1) (i32.const 0) (i32.const 9) (i64.const 4607182418800017408))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_gauge")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    assert_eq!(
        registry.read_gauge("cpu_usage"),
        Some(4607182418800017408)
    );
}

#[test]
fn test_edge_case_memory_export_not_a_memory() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("not_mem"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

    // Module with export named "memory", but it is a function, not a linear memory
    let wat = r#"(module
        (import "env" "datalake_host_metric_emit" (func $metric (param i32 i32 i32 i64)))
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (func (export "memory"))
        (func (export "call_not_mem")
            (call $metric (i32.const 0) (i32.const 0) (i32.const 5) (i64.const 10))
            (call $log (i32.const 1) (i32.const 0) (i32.const 5))
        )
    )"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Init,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "call_not_mem")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
    assert_eq!(registry.read_counter("test"), 0);
}

#[test]
fn test_edge_case_log_message_allocation_capping() {
    let engine = Engine::default();
    let registry = Arc::new(MetricRegistry::new("log_cap"));
    let linker = build_host_linker(&engine, Arc::clone(&registry)).unwrap();

    // Module with 2 memory pages (131,072 bytes) and 70,000-byte log message
    let mut wat = String::from(
        r#"(module
        (import "env" "datalake_host_log" (func $log (param i32 i32 i32)))
        (memory (export "memory") 2)
        (func (export "emit_long_log")
            (call $log (i32.const 3) (i32.const 0) (i32.const 70000))
        )
    )"#,
    );

    let wasm_bytes = wat::parse_str(&wat).unwrap();
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();
    let mut store = Store::new(
        &engine,
        HostState {
            phase: HostPhase::Execution,
            registry: Arc::clone(&registry),
        },
    );
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let func = instance
        .get_typed_func::<(), ()>(&mut store, "emit_long_log")
        .unwrap();

    assert!(func.call(&mut store, ()).is_ok());
}
```

- [ ] **Step 2: Run tests to verify they compile and pass/fail**

Run: `cargo test -p wasm-transformer --test host_calls_tests`
Verify new tests execute.

- [ ] **Step 3: Refactor `read_guest_string` in `crates/wasm-transformer/src/host_calls.rs`**

Update `read_guest_string`:
```rust
fn read_guest_string(
    caller: &mut Caller<'_, HostState>,
    ptr: u32,
    len: u32,
    max_len: usize,
) -> Option<String> {
    let Some(export) = caller.get_export("memory") else {
        tracing::warn!("WASM guest invoked host function without exporting 'memory'");
        return None;
    };
    let Some(memory) = export.into_memory() else {
        tracing::warn!("WASM guest export 'memory' is not a linear memory");
        return None;
    };

    let Some(end_u32) = ptr.checked_add(len) else {
        tracing::warn!(
            ptr,
            len,
            "WASM guest memory address addition overflow"
        );
        return None;
    };

    let mem_data = memory.data(caller);
    let offset = ptr as usize;
    let total_end = end_u32 as usize;

    if total_end > mem_data.len() {
        tracing::warn!(
            offset,
            raw_len = len as usize,
            memory_len = mem_data.len(),
            "WASM guest memory read out of bounds"
        );
        return None;
    }

    let read_len = (len as usize).min(max_len);
    let end = offset.saturating_add(read_len);

    Some(String::from_utf8_lossy(&mem_data[offset..end]).into_owned())
}
```

- [ ] **Step 4: Run tests and verify code coverage**

Run: `cargo llvm-cov -p wasm-transformer --text`
Verify 100% line coverage in `read_guest_string` and `build_host_linker`.

- [ ] **Step 5: Format and lint**

Run: `cargo fmt --check && cargo clippy -p wasm-transformer --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc`
Expected: PASS with 0 warnings.
