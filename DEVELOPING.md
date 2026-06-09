# Developer Guide: `opentelemetry-datalake`

This document provides instructions for developers setting up, building, running, and testing `opentelemetry-datalake` locally.

---

## 1. Prerequisites

Before starting, ensure you have the following installed on your local machine:

- **Rust & Cargo**: Latest stable release (installed via [rustup](https://rustup.rs/)).
- **Docker & Docker Compose**: Needed for spinning up containerized local dependencies (MinIO, Apache Iceberg REST Catalog) during end-to-end (E2E) testing.
- **Python 3.8+**: Required for bootstrap and verification scripts in the integration test harness.
- **Make**: For automating development and quality gate tasks.
- **cargo-deny**: Optional but recommended to check dependency licenses and vulnerabilities.

---

## 2. Workspace Layout

The workspace is organized as a Cargo workspace with several crates:

- **`crates/core`**: Core pipeline orchestration, config loading, graceful shutdown traits, and general types.
- **`crates/otlp-receiver`**: Ingestion layer handling OTLP gRPC (`tonic`) and HTTP/JSON (`axum`) protocols.
- **`crates/arrow-codec`**: Translation layer compiling raw Protobuf/JSON payloads directly into Apache Arrow schemas and vectorized `RecordBatch` structures. Includes compliance checking and remapping logic.
- **`crates/storage`**: Writer layer integrating with Open Table Formats. Sub-modules exist for Delta Lake (`delta.rs`), Apache Iceberg (`iceberg.rs`), Apache Hudi (`hudi.rs`), and Apache Paimon (`paimon.rs`).
- **`crates/kafka-sink`**: Standard secondary Kafka integration sink.
- **`crates/noop-transformer`**: Transformer helper used for raw pass-through in streaming pipelines.
- **`src`**: Main application binary, routing receiver streams to sinks.

---

## 3. Basic Development Operations

The project includes a `Makefile` at the workspace root to simplify standard development commands.

### Building the Project
To compile the application in debug mode:
```bash
cargo build
```

To build a release-optimized binary:
```bash
cargo build --release
```

### Code Formatting
Ensure your code is formatted according to standard rules:
```bash
# Verify formatting
make fmt

# Apply formatting changes
cargo fmt --all
```

### Code Linting
Your code must pass all clippy checks with zero warnings or errors. We run clippy with strict pedantic rules:
```bash
make clippy
```

### Running Unit Tests
To run unit and doc tests:
```bash
make test
```
Or run individual tests using Cargo:
```bash
cargo test -p <crate-name> -- <test_name_pattern>
```

### Running Microbenchmarks
To execute Criterion benchmarks:
```bash
make bench
```

### Checking Dependencies
Verify that dependency licenses and security advisories are compliant:
```bash
cargo deny check
```

---

## 4. End-to-End (E2E) Testing

We provide a fully containerized end-to-end integration test harness in the `tests/e2e/` directory. This harness tests:
1. Spinning up local MinIO (acting as S3 storage) and an Apache Iceberg REST Catalog.
2. Creating Apache Iceberg tables in the catalog.
3. Starting the `opentelemetry-datalake` receiver.
4. Simulating live telemetry injection via the OpenTelemetry `telemetrygen` contributor utility.
5. Verifying that the telemetry is successfully received, translated to Arrow, written, and committed to Apache Iceberg tables.

### Running the E2E Suite Automatically
To run the complete suite automatically:
```bash
make e2e-test
```
This script automates building the binary, spinning up Docker containers, table bootstrapping, traffic generation, and data verification, before tearing everything down.

### Manually Running & Debugging the E2E Environment
If you are debugging a feature or want to inspect tables manually, you can execute the steps of the E2E harness incrementally:

#### 1. Spin up the Background Services
Start MinIO and the REST Catalog in the background:
```bash
docker compose -f tests/e2e/docker-compose.yml up -d
```
You can access the MinIO console at `http://localhost:9001` (Username: `admin`, Password: `password`).

#### 2. Bootstrap the Tables
Set up the Python virtual environment and run the bootstrap script to create namespaces and Iceberg tables:
```bash
python3 -m venv tests/e2e/.venv
source tests/e2e/.venv/bin/activate
pip install -r tests/e2e/requirements.txt
python tests/e2e/bootstrap.py
```
This creates the namespace `default` and three distinct destination tables: `default.logs`, `default.metrics`, and `default.traces`.

#### 3. Run the Receiver Local Instance
Run the compiled receiver binary pointing to the test configuration. You must export target storage environment variables so the Iceberg sink can write to MinIO:
```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000

# Build and run
cargo build --bin opentelemetry-datalake
./target/debug/opentelemetry-datalake --config tests/e2e/config.toml
```

#### 4. Inject Telemetry Traffic
From another terminal, you can send test metrics, logs, or traces using `telemetrygen`. On Linux, using host networking (`--network host`) is recommended:

```bash
# On Linux:
docker run --rm --network host \
  ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest logs \
  --otlp-endpoint=localhost:4317 \
  --otlp-insecure \
  --rate=10 \
  --duration=2s

# On macOS / Windows:
docker run --rm --add-host=host.docker.internal:host-gateway \
  ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest logs \
  --otlp-endpoint=host.docker.internal:4317 \
  --otlp-insecure \
  --rate=10 \
  --duration=2s
```

#### 5. Verify Committed Data
Run the verification script to query the tables via the REST catalog and inspect the committed Parquet metadata in MinIO:
```bash
source tests/e2e/.venv/bin/activate
python tests/e2e/verify.py
```

#### 6. Tear Down Environment
Stop and clean up containers:
```bash
docker compose -f tests/e2e/docker-compose.yml down -v
```

---

## 5. Development Guidelines & Best Practices

All additions to the codebase must conform to our core technical guidelines as outlined in `AGENTS.md`:

1. **Zero-Panic Policy**: Never use `unwrap()`, `expect()`, `panic!()`, or `todo!()` in the ingestion or storage paths. All errors must be propagated using structured `PipelineError` variants.
2. **Zero-Cost Allocations**: Avoid cloning large arrays, strings, or records in the hot path. Leverage Apache Arrow's columnar primitives and reference types (`&str` instead of `String`).
3. **Graceful Shutdown**: Always ensure your workers or sink tasks listen to the receiver shutdown channel and stop gracefully without leaving orphaned tasks.
