# OpenTelemetry Datalake

[![codecov](https://codecov.io/github/jimmystewpot/opentelemetry-datalake/graph/badge.svg?token=8WT93L06CN)](https://codecov.io/github/jimmystewpot/opentelemetry-datalake)
[![license](https://img.shields.io/badge/License-MPL--2.0-blue.svg)](https://opensource.org/licenses/MPL-2.0)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jimmystewpot_opentelemetry-datalake&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=jimmystewpot_opentelemetry-datalake)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=jimmystewpot_opentelemetry-datalake&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=jimmystewpot_opentelemetry-datalake)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=jimmystewpot_opentelemetry-datalake&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=jimmystewpot_opentelemetry-datalake)

`opentelemetry-datalake` is an ultra-high-performance, horizontally scalable OpenTelemetry (OTLP) receiver pipeline written in Rust. It ingests OTLP metrics, traces, and logs, decodes them into memory-efficient **Apache Arrow** formats, and channels them through to a downstream data lake sink (such as Kafka or ACID table formats like Delta Lake, Iceberg, and Hudi).

Designed with zero-cost abstractions, lock-free concurrency, and zero-panic error handling, this pipeline is engineered to ingest telemetry at maximum throughput.

---

## Architecture Overview

```text
               +--------------------------------------+
               |          OTLP Telemetry Ingress      |
               |       (gRPC: 4317 / HTTP: 4318)      |
               +------------------+-------------------+
                                  |
                                  | (Protobuf / JSON Payload)
                                  v
               +------------------+-------------------+
               |             Arrow Codec              |
               |  (Logs, Traces & Metrics Decoding)   |
               +------------------+-------------------+
                                  |
                                  | (Vectorized Arrow RecordBatches)
                                  v
               +------------------+-------------------+
               |             Signal Router            |
               |        (Dedicated MPSC Channels)     |
               +--------+---------+---------+---------+
                        |         |         |
          (Logs Channel) |         |         | (Metrics Channel)
                        |         | (Traces Channel)
                        v         v         v
                     +----+    +----+    +----+
                     |Log |    |Span|    |Met |  (No-Op Transformers)
                     +----+    +----+    +----+
                        |         |         |
                        v         v         v
                     +----+    +----+    +----+
                     |Sink|    |Sink|    |Sink|  (Kafka Producer Sinks)
                     +----+    +----+    +----+
                        |         |         |
                        +---------+---------+
                                  |
                                  v
               +------------------+-------------------+
               |         Apache Kafka / Data Lake     |
               +--------------------------------------+
```

---

## Workspace Layout

The project is structured as a Cargo virtual workspace consisting of the following crates:

*   **`src/main.rs`**: The main entry point. Bootstraps config parsing, configures pipeline instrumentation, schedules the DAG execution, and handles graceful shutdown.
*   **`crates/core`**: Core pipeline traits (`Source`, `Transform`, `Sink`), channel-based multiplexing routing (`Fanout`), and pipeline-wide observability/telemetry instrumentation.
*   **`crates/arrow-codec`**: Deserialization modules mapping OTLP Protobuf and JSON metrics, traces, and logs payloads directly to columnar Apache Arrow `RecordBatch`es.
*   **`crates/otlp-receiver`**: Ingest layer implementing a multi-protocol OTLP receiver with Tonic (gRPC) and Axum (HTTP/JSON).
*   **`crates/noop-transformer`**: Implementation of `Transform` that passes signal record batches directly through to the next phase of the pipeline.
*   **`crates/kafka-sink`**: High-performance sink implementing the `Sink` trait using `rdkafka` to stream Arrow IPC or JSON payloads to Kafka brokers.

---

## Configuration

Configuration is managed using `figment` and supports merging of file-based TOML configs and environment variable overrides.

### Example configuration (`config.toml`):

```toml
[server]
grpc_addr = "127.0.0.1:4317"
http_addr = "127.0.0.1:4318"

[kafka]
bootstrap_servers = "localhost:9092"
logs_topic = "otlp-logs"
traces_topic = "otlp-traces"
metrics_topic = "otlp-metrics"
serialization_format = "Ipc" # Options: "Ipc", "Json"

[kafka.options]
"queue.buffering.max.messages" = "100000"
"compression.codec" = "snappy"

[telemetry]
endpoint = "http://127.0.0.1:4317"
service_name = "opentelemetry-datalake"
cloud_region = "us-east-1"
```

### Environment Overrides:
Any configuration value can be overridden using the `OTEL_DATALAKE_` environment variable prefix. For example:
*   `OTEL_DATALAKE_KAFKA__BOOTSTRAP_SERVERS="kafka-broker:9092"`
*   `OTEL_DATALAKE_TELEMETRY__CLOUD_REGION="us-west-2"`

---

## Development & Operations

Developer tasks are automated via the root `Makefile`.

### Quality & Testing Gates

Run all quality checks (formatting, pedantic clippy linting, testing, and benchmarking):
```bash
make all
```

Individually execute development tasks:

*   **Format code**:
    ```bash
    cargo fmt
    ```
*   **Run lints (strict pedantic rules)**:
    ```bash
    make clippy
    ```
*   **Run test suite**:
    ```bash
    make test
    ```
*   **Run micro-benchmarks**:
    ```bash
    make bench
    ```

### Running the Application

Build the production release binary:
```bash
cargo build --release
```

Run the pipeline:
```bash
cargo run --bin opentelemetry-datalake -- --config config.toml
```
