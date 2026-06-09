# RFC Specifications: Admin, Introspection & Profiling API

---

## Status of this Memo

This document specifies the architectural and protocol constraints for the Administrative, Introspection, and Profiling API within the `opentelemetry-datalake` engine. It adheres strictly to the normative terminology defined in **RFC 2119**.

## 1. Introduction & Core Boundaries

The keywords **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

### 1.1 Network & Port Isolation

To protect internal node state and safeguard memory allocation maps from external interference, network isolation is paramount:

* The Administrative API **MUST** bind to a dedicated, independent network socket configuration (e.g., port `:9090` or `:8081`) completely separate from the primary OpenTelemetry (OTLP) data ingestion ports (see the [Configuration Reference](configuration.md)).
* The Administrative server **MUST NOT** share HTTP routing tables, worker pools, or runtime tasks directly with the public-facing ingestion endpoints.

---

## 2. Probe Specifications (`/healthz` and `/readyz`)

The runtime health system handles Kubernetes-style lifecycle events through two distinct endpoint states:

```text
               ┌───────────────────────┐
               │  Tokio Runtime Loops  │
               └───────────┬───────────┘
                           │
                 Is Core Initialized?
                 /               \
              YES                 NO
              /                     \
    ┌─────────────────┐     ┌─────────────────┐
    │ /healthz: 200   │     │ /healthz: 500   │
    └────────┬────────┘     └─────────────────┘
             │
     Are Sinks Up & 
   Queues Unblocked?
     /               \
  YES                 NO
  /                     \
┌─────────────────┐     ┌─────────────────┐
│  /readyz: 200   │     │  /readyz: 503   │
└─────────────────┘     └─────────────────┘

```

### 2.1 `/healthz` (Liveness)

* The liveness endpoint **MUST** return HTTP status code `200 OK` as soon as the background Tokio async runtime executors have successfully initialized and started running.
* If the engine's scheduler or core task orchestrator experiences an unrecoverable deadlock or goes out of memory before bootstrap completion, the endpoint **MUST NOT** return `200 OK`. It **SHALL** either timeout or emit an HTTP status code `500 Internal Server Error`.

### 2.2 `/readyz` (Readiness)

The readiness probe establishes if the pipeline is capable of actively handling data batches.

* The readiness evaluator **MUST** execute asynchronous state checks across downstream dependencies before returning an HTTP `200 OK`.
* The system **MUST** evaluate the current storage catalog connection health (e.g., checking responsiveness of the Iceberg REST Catalog or Delta meta-store locks).
* If the internal bounded memory buffers (see [Buffer Specification](buffer.md)) have hit maximum capacity under backpressure, or if cloud bucket endpoints are throwing unrecoverable writes, the endpoint **MUST** return an HTTP status code `503 Service Unavailable`.

---

## 3. Node Telemetry & Processing Metrics

Operational introspection requires non-blocking metric gathering (for self-monitoring configuration, see [Instrumentation Specification](instrumentation.md)).

### 3.1 Metric Extraction Paths (`/metrics` & `/debug/vars`)

* Operational statistics gathered under the `/metrics` path **MUST** conform strictly to the Prometheus text-based exposition format.
* The processing tracking counters used to measure internal state **MUST NOT** introduce atomic lock contention on the hot ingestion paths. Workers **MUST** mutate metrics exclusively utilizing atomic primitive updates (`std::sync::atomic::AtomicU64`).
* Calculations evaluating events processed per second inside the JSON `/debug/vars` response **SHOULD** be calculated using a sliding window or exponentially weighted moving average (EWMA) over a rolling 10-second interval to avoid instantaneous sampling skew.

---

## 4. Run-Time Profiling Protocol (`pprof`)

Profiling active infrastructure can create significant performance hits. The implementation of deep profiling endpoints **MUST** adhere to strict resource constraints.

### 4.1 CPU Profiling (`/debug/pprof/profile`)

* The CPU profiling interface **MUST** accept an optional `seconds` query string parameter (e.g., `?seconds=30`). If omitted, the profile duration **SHALL** default to exactly `30` seconds.
* During active sampling routines, the profiling loop **SHOULD NOT** sleep or stall execution blocks on other unrelated worker OS threads. It **MUST** utilize low-overhead hardware timers to record active call-stacks.
* The HTTP response payload **MUST** be returned as a compressed, symbolized gzipped Protobuf stream matching the standard Google `pprof` layout specification.

### 4.2 Heap Profiling (`/debug/pprof/heap`)

Because `opentelemetry-datalake` operates on vectorized Apache Arrow memory fragments, keeping allocations predictable is critical (for memory management and allocator safeguards, refer to the [AGENTS.md](../AGENTS.md) standards):

* The application layer **MUST** utilize `tikv-jemallocator` as the global system memory allocator, configured compiled-in with the profiling execution flags (`--enable-prof`).
* When the `/debug/pprof/heap` route is requested, the service **MUST** perform an explicit epoch advancement using the underlying allocator management controls before sampling internal state.
* The allocator metadata memory map **SHALL** be converted into a compatible `pprof` profile block file format and returned with appropriate HTTP attachment content headers (`Content-Type: application/octet-stream`).
* If the host platform architecture does not support raw memory profiling hooks (such as specific native MSVC compiler backends), the endpoint **MUST** gracefully return an HTTP status code `501 Not Implemented`.