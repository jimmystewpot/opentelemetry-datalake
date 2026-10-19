# AGENTS.md

## Overview

`opentelemetry-datalake` is an ultra-high-performance, horizontally scalable OpenTelemetry (OTLP) receiver written in Rust. It ingests OTLP metrics, traces, and logs, processes them in-memory using **Apache Arrow**, and streams them into modern open table formats (Delta Lake, Apache Iceberg, Apache Hudi, and Apache Paimon).

Because this system processes high-throughput telemetry, maintaining zero-cost abstractions, avoiding allocations in the hot path, ensuring strict memory safety, and preventing runtime panics are non-negotiable engineering requirements.

---

## Directory Structure Layout

All agents must strictly adhere to the following workspace structure. Do not create top-level files or arbitrary modules outside this layout.

```text
opentelemetry-datalake/
├── Cargo.toml                 # Workspace configuration
├── Makefile                   # Developer automation (build, test, lint)
├── LICENSE
├── README.md
├── AGENTS.md                  # This file
├── src/
│   └── main.rs                # Entry point and pipeline orchestration
├── crates/
│   ├── core/                  # Core pipeline traits, types, and telemetry
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, pipeline.rs, config.rs, error.rs, telemetry.rs}
│   ├── otlp-receiver/         # gRPC and HTTP/JSON OTLP ingestion endpoints
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── arrow-codec/           # Maps OTLP Protobuf to Arrow schemas & batching
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, common.rs, compliance.rs, logs.rs, metrics.rs, traces.rs}
│   ├── storage/               # Open Table Format sink implementations (Iceberg)
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, iceberg.rs}
│   ├── parquet-sink/          # Local & Object storage Parquet sink with variant encoding
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, sink.rs, router.rs, writer.rs, partition.rs, variant.rs}
│   ├── kafka-sink/            # Kafka sink implementation
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── starrocks-sink/        # StarRocks Stream Load sink implementation
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── elasticsearch-sink/    # Elasticsearch & OpenSearch data streams sink implementation
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, client.rs, config.rs, error.rs, serializer.rs, tls.rs}
│   ├── noop-transformer/      # Example/Pass-through transformer
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   ├── wasm-transformer/      # Sandboxed WebAssembly batch transformer runtime
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, dispatcher.rs, engine.rs, worker.rs, guard.rs, host_calls.rs}
│   ├── wasm-sdk/              # Guest SDK for authoring WebAssembly transformation plugins
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── wasm-cli/              # CLI tooling for testing, validating, and benchmarking WASM plugins
│       ├── Cargo.toml
│       └── src/{lib.rs, main.rs, bench.rs, tester.rs, validator.rs}
└── docs/                      # Documentation and specifications
    ├── ARCHITECTURE.md        # High-level architecture overview
    ├── administration.md      # Introspection & Admin API specification
    ├── buffer.md              # Buffer behavior and metrics specification
    ├── components.md          # Component behavior (sources, transforms, sinks)
    ├── configuration.md       # Configuration reference
    ├── elasticsearch.md       # Elasticsearch & OpenSearch data streams sink operator guide
    ├── instrumentation.md     # Pipeline telemetry & logging standards
    ├── lakehouse.md           # Lakehouse integration specification
    └── starrocks.md           # StarRocks Stream Load sink operator guide
```

---

## Tech Stack & Ecosystem Dependencies

Agents must strictly use the specific library ecosystem outlined below. Do not introduce competing dependencies (e.g., do not use `async-std` instead of `tokio`).

### 1. High-Performance Processing & Memory Layout

*   **In-Memory Format**: **Apache Arrow (Rust Native)** via the `arrow` crate. Data processing, transformations, and batching MUST happen natively inside Arrow Arrays and `RecordBatch` structures.
*   **Serialization/Deserialization**: `prost` for decoding OTLP Protobuf payloads.
*   **OTLP Definitions**: `opentelemetry-proto` for standard OTLP message definitions.

### 2. Async Runtime & Networking

*   **Async Engine**: `tokio` (full feature flag enabled).
*   **Network Protocol Engine**: `tonic` for gRPC OTLP ingestion, and `axum` for HTTP/JSON OTLP ingestion.

### 3. Storage Sinks

*   **Apache Iceberg**: `iceberg-rust` (the official Apache Iceberg Rust implementation).
*   **Kafka**: `rdkafka` (based on `librdkafka`).
*   **StarRocks**: `starrocks-stream-load` (HTTP Stream Load API). TLS backend is selectable at compile time via Cargo features (`tls-rustls` default, `tls-native-tls` opt-in). The two backends are mutually exclusive; enabling both produces a compile error.
*   **Elasticsearch & OpenSearch**: `elasticsearch-sink` (HTTP/2 NDJSON Bulk API streaming into Data Streams with automated failover, index template validation, and AWS SigV4 request signing).

### 4. Component Summary & Role Mapping

| Component Layer | Core Crate / Technology | Architecture Role |
| --- | --- | --- |
| **Ingestion Edge** | `otlp-receiver` (`tonic`/`axum`) | Non-blocking OTLP receivers accepting incoming telemetry streams. |
| **Codec** | `arrow-codec` (`prost` + `arrow`) | Translates raw proto structs into columnar `RecordBatch` layout on arrival. |
| **Transformation** | `noop-transformer` | Provides a hook for data transformation/enrichment (currently no-op). |
| **Core** | `pipeline-core` | Defines common traits (`Source`, `Transform`, `Sink`) and shared logic. |
| **Storage Sinks** | `storage` (Iceberg), `kafka-sink`, `starrocks-sink`, `elasticsearch-sink` | Translates Arrow record batches into physical storage, search engines, or message queues. |

---

## Technical Core Principles

### 1. Zero-Panic Policy (Strict Error Handling)

To ensure maximum availability under load, this codebase enforces a strict **no-panic policy** in production paths.

*   **Prohibited Macros**: Do not use `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` directories, especially on data originating from external sources (OTLP, Config, Catalog).
*   **Prohibited Panicking Arrow Methods**: Do NOT use panicking Arrow array downcasts like `.as_string::<i32>()` or `.as_primitive::<T>()`. Always downcast safely via `.as_any().downcast_ref::<T>()` and propagate `Result` on failure.
*   **Mandatory Arrow Null Checking**: Never call `.value(i)` on an Arrow array without first checking `.is_null(i)`. Unchecked `.value(i)` calls on null slots yield uninitialized or default zero values, which causes silent data corruption or panics (especially when defensive schema backfills are active).
*   **Safe Arithmetic**: Use `saturating_mul`, `checked_mul`, or `div_euclid` for timestamp or byte calculations. Avoid unchecked multiplication (e.g. `ts * 1000`) that overflows in strict/debug builds.
*   **Safe Data Conversion**: Handle malformed timestamps or protocol violations using `Result` propagation (e.g., use `Utc.timestamp_opt(...).single()` instead of `.unwrap()`).
*   **Allowed Exceptions**: `unwrap()` or `expect()` are permitted *only* in test suites (`#[cfg(test)]`) or setup code (e.g., parsing hardcoded configurations during initialization) where failure prevents startup entirely.
*   **Error Enforcement**: Use the `thiserror` crate for defining structured, domain-specific error types within libraries, and `anyhow` exclusively in binary targets or integration wrappers if required.

#### Incorrect (Will fail CI):

```rust
// Panic vulnerability under unexpected payloads
let batch_id = metadata.get("x-batch-id").unwrap(); 
let ts_array = col.as_primitive::<TimestampNanosecondType>(); // PANIC if type mismatch!
let ts_val = ts_array.value(i) * 1000; // Integer overflow & null-read vulnerability!
```

#### Correct:

```rust
// Explicit error propagation using structured domain errors and safe downcasts
let batch_id = metadata
    .get("x-batch-id")
    .ok_or_else(|| PipelineError::MissingMetadata("x-batch-id".to_string()))?;

let ts_array = col
    .as_any()
    .downcast_ref::<TimestampNanosecondArray>()
    .ok_or_else(|| PipelineError::Storage("Invalid TimestampNanosecondArray".into()))?;

let ts_nanos = if ts_array.is_null(i) {
    None
} else {
    Some(ts_array.value(i).saturating_mul(1000))
};
```

### 2. Memory & Layout Strategy (Apache Arrow)

*   Data must be transitioned to Apache Arrow `RecordBatch` format as close to the ingestion edge as possible.
*   **Zero-Alloc Hot Paths**:
    *   **No intermediate maps**: Never allocate a `HashMap<&str, String>` or per-attribute heap `String` when decoding OTLP KeyValue attributes. Stream JSON directly into a reusable scratch buffer or builder.
    *   **Stack-buffered hex encoding**: Do not allocate heap `String`s for hex IDs (e.g. `to_hex_string`). Use stack-allocated `[u8; 64]` buffers with `hex::encode_to_slice` and append `&str` directly to Arrow `StringBuilder`.
    *   **Builder pre-allocation**: Always call `StringBuilder::with_capacity(num_rows, estimated_bytes)` rather than `StringBuilder::new()`.
    *   **Schema caching**: Do not reconstruct `Arc<Schema>` on every batch decoding call. Cache schemas statically via `std::sync::LazyLock<SchemaRef>`.
*   **Bounded Byte Queueing**:
    *   Channels carrying `RecordBatch` instances (`tokio::sync::mpsc`) MUST NOT use oversized default capacities (e.g. `1000`). Bounded capacities MUST default to 32–64 batches, or enforce an atomic byte-budget semaphore to strictly bound total in-flight heap memory under downstream stalls.
    *   All gRPC and HTTP ingestion listeners MUST enforce explicit payload limits (`DefaultBodyLimit`) and active request concurrency limits (`ConcurrencyLimitLayer`).

### 3. Concurrency & Async

*   Use `tokio` for the asynchronous runtime.
*   **Strict Async/Blocking Isolation**:
    *   NEVER block the async executor loop. Synchronous, CPU-heavy compute operations MUST be isolated via `tokio::task::spawn_blocking` (or dedicated CPU thread pools).
    *   This includes: Protobuf/JSON payload decoding (> 64 KB), Arrow `compute::concat_batches`, Arrow sorting (`lexsort_to_indices` + `take`), row-by-row timestamp partitioning, full-batch serialization (NDJSON, Arrow IPC, CSV, Parquet), and WebAssembly module compilation, instantiation, or rejuvenation (`WasmWorker::rejuvenate`).
*   **Zero Process Environment Mutation**:
    *   NEVER call `std::env::set_var` or `std::env::remove_var` after the Tokio runtime or any background thread has been initialized. In multi-threaded programs, mutating the POSIX environment causes data races with concurrent `getenv` calls in libc, OpenSSL, or background worker threads, producing Undefined Behavior.
    *   Configure logging and tracing filters strictly through programmatic builders (`tracing_subscriber::EnvFilter::try_new`).
*   Favor lock-free structures or bounded `tokio::sync::mpsc` channels over heavy `Mutex` contention in the hot path.

### 4. Telemetry & Instrumentation

All pipeline components, buffers, and sinks MUST adhere to standard naming and telemetry emission patterns:
*   **Self-Referential Loop Safeguards**: The internal telemetry exporter MUST NOT default `otlp_endpoint` to the pipeline's own ingestion port (`127.0.0.1:4317` / `http://localhost:4317`) without loopback detection or disabled defaults. Exporting telemetry back into itself causes an infinite feedback storm.
*   **Graceful Shutdown Flush**: Any telemetry handle (`TelemetryGuard`) MUST implement `Drop` to explicitly invoke `.shutdown()` on `SdkTracerProvider` and `SdkMeterProvider`, ensuring pending diagnostic spans and metrics are flushed before process exit.
*   **Standard Buffer & Component Telemetry**: All storage sinks (Iceberg, Parquet, Kafka, StarRocks, Elasticsearch) and buffers MUST emit standard OpenTelemetry metrics for buffer events, buffer bytes, flush latencies, and dropped records conforming to [buffer.md](docs/buffer.md) and [instrumentation.md](docs/instrumentation.md).

### 5. Lifecycle, Supervision & Graceful Shutdown

*   **Comprehensive Task Supervision**: All background worker and sink tasks (`source_handle`, `transform_handles`, `sink_handles`) MUST be supervised within the main `tokio::select!` event loop and return `Result<(), PipelineError>`. Never detach background tasks or swallow sink errors (`if let Err(e) = ... { error!(...); }`).
*   **Topological Shutdown Drain**: Pipeline shutdown MUST drain in strict topological order from upstream to downstream:
    1. Stop ingestion listeners (gRPC/HTTP servers).
    2. Await `source_handle` so source channel senders are dropped.
    3. Await transformer handles so sink channel senders are dropped.
    4. Await sink handles so all remaining in-memory batches are flushed and committed to persistent storage.
    5. Flush telemetry providers and exit cleanly.
*   **Watch Channel Notification**: Never use bare `shutdown_rx.changed().await` without checking the current state. Always use `shutdown_rx.wait_for(|shutting_down| *shutting_down).await` to prevent missed-event race conditions when shutdown is pre-signaled.
*   **Producer Queue Flush**: Sinks wrapping background producer queues (e.g. `librdkafka::Producer`, StarRocks `StreamLoadManager`) MUST flush during shutdown before exiting.

### 6. Storage & Lakehouse Transaction Integrity

*   **Buffer Retention Across Flushes**: Sinks MUST NOT clear their in-memory batch buffers (`std::mem::take`) before persistent writes and ACID catalog commits succeed. If a commit or upload fails, retained batches must be preserved for retry or diverted to dead-letter storage to prevent silent data loss.
*   **Partition Specification Adherence**: Partitioned sinks (e.g. Iceberg, Parquet) must pass partition values and specifications to table data writers rather than writing unpartitioned files.
*   **Catalog Identifier Integrity**: Pass `catalog_name` to catalog loaders, not `table_identifier`.
*   **Optimistic Concurrency Retries**: ACID lakehouse commits must implement exponential backoff retries for transient catalog conflicts.

### 7. Rust Coding Anti-Patterns to Avoid

To maintain codebase cleanliness and compiler guarantees, agents MUST avoid the following patterns:
*   **Unnecessary `.clone()` Calls**: Avoid cloning large structures or buffers to satisfy the borrow checker. Inside loops or per-event decoders, use reference lifetimes or `.as_str()`. In `Fanout::send`, move the batch into the last channel without cloning.
*   **Zero-Clone Hot Paths**: In Arrow builders, prefer `.append_value(&slice)` over `.append_value(owned_string)`. Avoid `String` and `Vec` allocations for every record.
*   **At-Least-Once Delivery**: Never swallow `Err` from channel `.send().await`. Propagate backpressure to OTLP receiver handlers so they can return `503` or `UNAVAILABLE` to the producer.
*   **Premature `.collect()`**: Avoid collecting iterators early; leverage lazy evaluation.
*   **Path Resolution**: Prefer `crate::` over `super::` in production source code (`src/` directories). `super::` is acceptable only in test modules (`#[cfg(test)]`).
*   **Imports Visibility**: Do not use `pub use` on imports unless explicitly re-exposing a dependency for downstream library consumers.
*   **Shared State**: Avoid global mutable state (`lazy_static!`, `Once`, etc.). Pass explicit context structs for shared state.
*   **Domain Modeling**: Prefer strong types over raw strings. Use enums and newtypes for validation and closed domains.
*   **Unsafe Code**: Do not write `unsafe` blocks without explicit human sign-off and an accompanying `// SAFETY:` comment justifying the invariant validation. Never use `unsafe` to mutate global environment variables (`std::env::set_var`) in multi-threaded runtimes.

### 8. Semantic Convention Enforcement

The OpenTelemetry project maintains an official Rust crate containing the standard naming patterns for attributes, resources, metrics, and traces as constants: `opentelemetry-semantic-conventions`.

Instead of evaluating strings dynamically, leverage these constants inside your `arrow-codec` layer to drive structural mapping or validation masks on your `RecordBatch` or raw OTLP payloads.

```rust
// crates/arrow-codec/src/compliance.rs
use opentelemetry_semantic_conventions as semconv;

pub fn validate_http_attributes(key: &str) -> bool {
    match key {
        // Enforce compliance using upstream specification constants
        semconv::attribute::HTTP_REQUEST_METHOD |
        semconv::attribute::HTTP_RESPONSE_STATUS_CODE |
        semconv::attribute::URL_FULL => true,
        _ => false,
    }
}

```

### 9. DRY & Code Reuse Guidelines

To prevent code duplication, promote maintainability, and ensure consistency across signal types (Logs, Metrics, Traces):
*   **Centralize Codec Logic**: Common OTLP transformation patterns, such as timestamp conversion (`timestamp_to_i64`), hex formatting (`to_hex_string`), and attribute conversion (`convert_attributes`), must reside in the centralized `arrow-codec::common` module rather than being duplicated locally within signal-specific codecs.
*   **Unify Sink Pipelines**: Sinks must not triplicate pipeline processing loops across signal types. Instead, consolidate core processing logic (e.g. schema mode resolution, sorting, partitioning, and ACID transaction commits) into generic helper methods (such as `process_signal` inside `IcebergSink`) that parameterize behavior based on the signal type.
*   **Consolidate Configuration Models**: Ensure configurations avoid duplicate field setups and reuse centralized workspace structures where possible (e.g. flattening `PipelineConfig` inside `AppConfig`).

## 10. Define a Two-Tier Pipeline Remapping Strategy

To handle non-compliant versus compliant telemetry without destroying performance, use a **Dual-Path Remapper** design within your core engine.

### The Compliance Split Architecture

1. **Pass-Through Path (Compliant):** Telemetry that perfectly follows semantic naming passes directly to the Arrow allocator with zero alteration.
2. **Remap/Mutate Path (Non-Compliant):** Raw legacy or non-standard fields (e.g., `http.status` or `method`) are intercepted, renamed to their official equivalents (`http.response.status_code`, `http.request.method`), and standard defaults are injected.

#### High-Performance Compliance Filtering Pattern

Because you are using Apache Arrow, you can implement a compliance evaluator that appends a boolean metadata tag or dynamically segregates record chunks into a `compliant` table partition or a `quarantine/raw` table partition in your data lake.

```rust
// Conceptual implementation for dynamic validation masks
pub struct ComplianceEngine {
    // Tracks required keys for specific namespaces based on OTel specs
    required_http_keys: HashSet<String>,
}

impl ComplianceEngine {
    pub fn assess_and_remap(&self, batch: RecordBatch) -> Result<ComplianceOutput, PipelineError> {
        // 1. Vectorized evaluation of standard attributes via Arrow schemas
        let is_compliant = self.check_schema_compliance(batch.schema());
        
        if is_compliant {
            // Append an internal metadata flag specifying full OTel compliance
            Ok(ComplianceOutput::Compliant(batch))
        } else {
            // 2. Route to the remapper to resolve legacy or custom mutations
            let corrected_batch = self.execute_remap_rules(batch)?;
            Ok(ComplianceOutput::Remapped(corrected_batch))
        }
    }
}

```

## 11. How to Expose this to Users in Configuration

A standardized remapping process needs a clean configuration language so operators can bring their bespoke data definitions in line with OpenTelemetry specs.

You can use a YAML/TOML pipeline configuration that maps untrusted incoming keys directly to standard `semconv` identifiers:

```yaml
# pipeline.yaml
pipeline:
  name: otlp-http-ingress
  compliance:
    mode: strict # Options: drop_non_compliant, quarantine, or remap
    quarantine_sink: hudi_dead_letter_table
  
  # Standardize arbitrary telemetry before it reaches Apache Arrow memory layouts
  transforms:
    - type: remap_semantic_conventions
      namespace: http
      mappings:
        - src: "custom_fields.http_verb"
          target: "http.request.method" # Maps to semconv::attribute::HTTP_REQUEST_METHOD
        - src: "response.code"
          target: "http.response.status_code"

```

## 12. Compliance & Semantic Convention Enforcement

To guarantee downstream data lakes remain highly structured and query-optimized, `opentelemetry-datalake` implements a strict compliance verification layer using the `opentelemetry-semantic-conventions` crate.

### Compliance Rules for Agents:
1. **Zero Raw String Keys:** When checking for core OTel attributes (e.g., service names, host metrics, HTTP keys), do not hardcode strings like `"service.name"`. Always utilize constants exposed via `opentelemetry_semantic_conventions::resource::SERVICE_NAME` or equivalent namespaces.
2. **Metadata Tagging:** Telemetry batches that pass structural validation checks must have their Arrow schema metadata injected with an `otel::compliance::status = "verified"` flag.
3. **Quarantine Handling:** If the pipeline is configured in `strict` mode and an incoming data payload contains un-mappable, malformed, or missing required semantic fields, the batch must be diverted to a structured dead-letter path rather than risking data corruption or runtime panic in downstream ACID sinks (Delta/Iceberg).
4. **Vectorized Operations over Loops:** When mutating schemas or fields for compliance updates, use Arrow column-backed manipulations or `datafusion` relational transformations rather than iterating through records element-by-element.

### 13. Admin, Metrics & Profiling API Safeguards

All agents introducing or modifying diagnostic routes must follow these architectural boundaries:

1. **Port Isolation:** The introspection HTTP server (using `axum`) MUST bind to a separate configuration port from the primary data ingestion path. It MUST NOT process public traffic.
2. **Non-Blocking Allocator Lookups:** When capturing `jemalloc` statistics via `tikv-jemalloc-ctl`, agents must advance the allocator epoch via `tikv_jemalloc_ctl::epoch::advance()` before executing reads.
3. **Atomic Metrics Paths:** Counters evaluating incoming records per second must use `std::sync::atomic::AtomicU64` increments. Do not use blocking locks (`Mutex`) inside telemetry codecs or worker channels to update operational dashboard statistics.
4. **Conditional Compilation for Profiling:** Because `jemalloc` profiling relies on low-level OS capabilities, the allocator selection and `pprof/heap` hooks must use target environment gates (`#[cfg(not(target_env = "msvc"))]`). Do not break local test builds on unsupported target architectures.
5. **Zero User-Controlled Filesystem Paths (`rust/path-injection`):** HTTP, gRPC, and Admin API handlers MUST NEVER accept filesystem paths, filenames, or directory overrides from request bodies, query parameters, or headers (CWE-22, CWE-36, CWE-73). Endpoints that read or write local files (such as WASM module hot-reloading) MUST resolve paths exclusively from immutable, startup-validated operator configuration and read via bounded file descriptors (`AsyncReadExt::take`).
6. **Strict Control-Plane Request DTOs:** All Admin and control-plane request structs MUST annotate `#[serde(deny_unknown_fields)]` so unexpected or legacy client-supplied fields (e.g., `module_path`) fail closed with HTTP `400 Bad Request`.


## Coding Standards & Quality Gates

Before finalizing any code modification, agents must verify that the following constraints are satisfied:

### Linting & Formatting

*   Code must be perfectly formatted using `cargo fmt`.
*   Clippy check must return zero warnings or errors using the following configuration:
```bash
cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
```

### Explicit Code Generation Rules

*   **Safety**: Do not introduce `unsafe` blocks without explicit human sign-off and an accompanying `// SAFETY:` comment justifying the invariant validation.
*   **Defensive Programming**: Always use bounded channels for queues to prevent out-of-memory (OOM) failures under extreme backpressure conditions.
*   **Documentation**: Every public trait, struct, and function must feature brief documentation explaining its purpose and concurrency characteristics.

---

## Agent Instructions for Code Creation

1.  **Context Initialization**: When adding features to a specific sink (e.g., `iceberg.rs`), inspect `crates/core/src/pipeline.rs` first to ensure implementation alignment with existing streaming abstractions.
2.  **Schema Verification**: Ensure any schema mapping transformations between OTLP definitions and Arrow schemas are unit-tested for nullability constraints and structural changes.
3.  **No Placeholders**: Generate complete implementations. Do not leave `// TODO` or `...` comments in generated logic unless requested.
