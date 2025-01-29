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
├── crates/
│   ├── core/                  # Core pipeline orchestration, traits, and types
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, pipeline.rs, error.rs}
│   ├── otlp-receiver/         # gRPC and HTTP/JSON OTLP ingestion endpoints
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, grpc.rs, http.rs}
│   ├── arrow-codec/           # Maps OTLP Protobuf/Arrow schemas & batching
│   │   ├── Cargo.toml
│   │   └── src/{lib.rs, encoder.rs, schema.rs}
│   └── storage/               # Open Table Format sink implementations
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          # Sink registry and common writer traits
│           ├── delta.rs        # Delta Lake writer integration
│           ├── iceberg.rs      # Apache Iceberg writer integration
│           ├── hudi.rs         # Apache Hudi writer integration
│           └── paimon.rs       # Apache Paimon writer integration
├── benches/                   # Criterion-based microbenchmarks for hot paths
│   ├── ingestion_bench.rs
│   └── storage_bench.rs
└── docs/                      # Documentation and specifications
    ├── buffer.md              # Buffer behavior, events, and metrics specification
    └── instrumentation.md     # Pipeline telemetry & event naming standards
```

---

## Tech Stack & Ecosystem Dependencies

Agents must strictly use the specific library ecosystem outlined below. Do not introduce competing dependencies (e.g., do not use `async-std` instead of `tokio`).

### 1. High-Performance Processing & Memory Layout

*   **In-Memory Format**: **Apache Arrow (Rust Native)** via the `arrow` and `arrow-flight` crates. Data processing, transformations, and batching MUST happen natively inside Arrow Arrays (e.g., `PrimitiveArray`, `StringArray`) and `RecordBatch` structures to leverage vectorized CPU operations and SIMD optimization.
*   **Serialization/Deserialization**: `prost` for compiling and decoding high-throughput OTLP Protobuf payloads.
*   **JSON Handling**: Avoid using `serde_json` directly for high-throughput telemetry data processing where Arrow native methods or `arrow-json` exist. Standard `serde` is permitted and recommended for configuration parsing and startup/setup paths.

### 2. Async Runtime & Networking

*   **Async Engine**: `tokio` (multi-threaded feature flag enabled).
*   **Network Protocol Engine**: `tonic` for high-performance, low-latency gRPC OTLP ingestion, and `axum` for HTTP/JSON OTLP ingestion.
*   **Network Transport**: `hyper` handles underlying HTTP primitives.

### 3. Open Table Format Sinks (Storage Layer)

All table interactions must use the official, native Rust implementations where available, configuration mapping must be passed safely using native configuration maps:

*   **Delta Lake**: `delta-kernel-rs` or `deltalake` crate.
*   **Apache Iceberg**: `iceberg-rust` (the official Apache Iceberg Rust implementation).
*   **Apache Hudi / Apache Paimon**: Interfaced via native bindings or via optimized parquet streaming using `datafusion` / `object_store` matching the table specifications.

### 4. Component Summary & Role Mapping

| Component Layer | Core Crate / Technology | Architecture Role |
| --- | --- | --- |
| **Ingestion Edge** | `tonic` (gRPC) / `axum` (HTTP) | Non-blocking OTLP receivers accepting incoming telemetry streams. |
| **Buffer & Codec** | `prost` + `arrow` | Translates raw proto structs into columnar `RecordBatch` layout on arrival. |
| **Execution Engine** | `datafusion` (Optional) | Used inside `core` for memory-efficient querying, sorting, or local aggregations. |
| **I/O & Cloud Storage** | `object_store` | Standardized async interface for AWS S3, Azure Blob, Google Cloud Storage, and local disk. |
| **Format Writers** | `deltalake`, `iceberg`, hudi/paimon bindings | Translates standard Arrow record batches into metadata-rich ACID table commits. |

---

## Technical Core Principles

### 1. Zero-Panic Policy (Strict Error Handling)

To ensure maximum availability under load, this codebase enforces a strict **no-panic policy** in production paths.

*   **Prohibited Macros**: Do not use `unwrap()`, `expect()`, `panic!()`, or `todo!()` in `src/` directories, especially on data originating from external sources (OTLP, Config, Catalog).
*   **Safe Data Conversion**: Handle malformed timestamps or protocol violations using `Result` propagation (e.g., use `Utc.timestamp_opt(...).single()` instead of `.unwrap()`).
*   **Allowed Exceptions**: `unwrap()` or `expect()` are permitted *only* in test suites (`#[cfg(test)]`) or setup code (e.g., parsing hardcoded configurations during initialization) where failure prevents startup entirely.
*   **Error Enforcement**: Use the `thiserror` crate for defining structured, domain-specific error types within libraries, and `anyhow` exclusively in binary targets or integration wrappers if required.

#### Incorrect (Will fail CI):

```rust
// Panic vulnerability under unexpected payloads
let batch_id = metadata.get("x-batch-id").unwrap(); 
```

#### Correct:

```rust
// Explicit error propagation using structured domain errors
let batch_id = metadata
    .get("x-batch-id")
    .ok_or_else(|| PipelineError::MissingMetadata("x-batch-id".to_string()))?;
```

### 2. Memory & Layout Strategy (Apache Arrow)

*   Data must be transitioned to Apache Arrow `RecordBatch` format as close to the ingestion edge as possible.
*   Avoid intermediate allocations. Do not clone large structures, `String` types, or raw byte buffers unless absolutely required by downstream sink client boundaries.
*   Leverage Arrow's native vectorization capabilities. Avoid converting Arrow arrays back into standard library vectors (`Vec<T>`) for data transformations.

### 3. Concurrency & Async

*   Use `tokio` for the asynchronous runtime.
*   Do not block the async executor loop. Any blocking synchronous operations (e.g., historical file metadata resolution in sinks lacking fully async wrappers) must be isolated via `tokio::task::spawn_blocking`.
*   Favor lock-free structures or `tokio::sync::mpsc` channels over heavy `Mutex` contention in the hot path.

### 4. Telemetry & Instrumentation

All pipeline components, buffers, and sinks MUST adhere to standard naming and telemetry emission patterns:
*   Telemetry MUST be event-driven; events drive downstream metrics and logging.
*   Events and metrics naming MUST follow standard prefix, casing, and structures. Refer to the [Instrumentation Specification](docs/instrumentation.md) for naming guidelines, schemas, and metrics mapping.
*   Any buffer implementation MUST conform to the [Buffer Specification](docs/buffer.md) for state telemetry tracking (events, bytes, and startup recovery metrics).

### 5. Rust Coding Anti-Patterns to Avoid

To maintain codebase cleanlines and compiler guarantees, agents MUST avoid the following patterns:
*   **Unnecessary `.clone()` Calls**: Avoid cloning large structures or buffers to satisfy the borrow checker. Inside loops or per-event decoders, use reference lifetimes or `.as_str()`.
*   **Zero-Clone Hot Paths**: In Arrow builders, prefer `.append_value(&slice)` over `.append_value(owned_string)`. Avoid `String` and `Vec` allocations for every record.
*   **At-Least-Once Delivery**: Never swallow `Err` from channel `.send().await`. Propagate backpressure to OTLP receiver handlers so they can return `503` or `UNAVAILABLE` to the producer.
*   **Premature `.collect()`**: Avoid collecting iterators early; leverage lazy evaluation.
*   **Path Resolution**: Prefer `crate::` over `super::` in production source code (`src/` directories). `super::` is acceptable only in test modules (`#[cfg(test)]`).
*   **Imports Visibility**: Do not use `pub use` on imports unless explicitly re-exposing a dependency for downstream library consumers.
*   **Shared State**: Avoid global mutable state (`lazy_static!`, `Once`, etc.). Pass explicit context structs for shared state.
*   **Domain Modeling**: Prefer strong types over raw strings. Use enums and newtypes for validation and closed domains.
*   **Unsafe Code**: Do not write `unsafe` blocks without explicit human sign-off and an accompanying `// SAFETY:` comment justifying the invariant validation.

### 6. Semantic Convention Enforcement

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

### 7. DRY & Code Reuse Guidelines

To prevent code duplication, promote maintainability, and ensure consistency across signal types (Logs, Metrics, Traces):
*   **Centralize Codec Logic**: Common OTLP transformation patterns, such as timestamp conversion (`timestamp_to_i64`), hex formatting (`to_hex_string`), and attribute conversion (`convert_attributes`), must reside in the centralized `arrow-codec::common` module rather than being duplicated locally within signal-specific codecs.
*   **Unify Sink Pipelines**: Sinks must not triplicate pipeline processing loops across signal types. Instead, consolidate core processing logic (e.g. schema mode resolution, sorting, partitioning, and ACID transaction commits) into generic helper methods (such as `process_signal` inside `IcebergSink`) that parameterize behavior based on the signal type.
*   **Consolidate Configuration Models**: Ensure configurations avoid duplicate field setups and reuse centralized workspace structures where possible (e.g. flattening `PipelineConfig` inside `AppConfig`).

## 7. Define a Two-Tier Pipeline Remapping Strategy

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

## 8. How to Expose this to Users in Configuration

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

## 9. Compliance & Semantic Convention Enforcement

To guarantee downstream data lakes remain highly structured and query-optimized, `opentelemetry-datalake` implements a strict compliance verification layer using the `opentelemetry-semantic-conventions` crate.

### Compliance Rules for Agents:
1. **Zero Raw String Keys:** When checking for core OTel attributes (e.g., service names, host metrics, HTTP keys), do not hardcode strings like `"service.name"`. Always utilize constants exposed via `opentelemetry_semantic_conventions::resource::SERVICE_NAME` or equivalent namespaces.
2. **Metadata Tagging:** Telemetry batches that pass structural validation checks must have their Arrow schema metadata injected with an `otel::compliance::status = "verified"` flag.
3. **Quarantine Handling:** If the pipeline is configured in `strict` mode and an incoming data payload contains un-mappable, malformed, or missing required semantic fields, the batch must be diverted to a structured dead-letter path rather than risking data corruption or runtime panic in downstream ACID sinks (Delta/Iceberg).
4. **Vectorized Operations over Loops:** When mutating schemas or fields for compliance updates, use Arrow column-backed manipulations or `datafusion` relational transformations rather than iterating through records element-by-element.

## Coding Standards & Quality Gates

Before finalizing any code modification, agents must verify that the following constraints are satisfied:

### Linting & Formatting

*   Code must be perfectly formatted using `cargo fmt`.
*   Clippy check must return zero warnings or errors using the following configuration:
```bash
cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc
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
