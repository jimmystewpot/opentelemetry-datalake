# Instrumentation Specification

This document specifies the instrumentation standards for the development of `otel-datalake`.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Table of Contents
- [Introduction](#introduction)
- [Naming Standards](#naming-standards)
  - [Namespaces](#namespaces)
  - [Event Naming](#event-naming)
  - [Metric Naming](#metric-naming)
- [Emission Strategy](#emission-strategy)
  - [Batching](#batching)
  - [Event-Driven Telemetry](#event-driven-telemetry)
  - [Standard Events Schema](#standard-events-schema)
    - [Error](#1-error)
    - [EventsDropped](#2-eventsdropped)

---

## Introduction

Otel-datalake's telemetry drives critical interfaces for monitoring and operations. Telemetry is a first-class feature; this document guides developers to ensure consistent, high-quality instrumentation.

---

## Naming Standards

### Namespaces

All events and metrics MUST be namespaced under one of the following domains depending on where they are emitted:

- `Component`: Pipeline sources, transforms, and sinks.
- `Buffer`: Buffer implementations (in-memory or disk).
- `Topology`: Pipeline orchestration and structural logic.

### Event Naming

Otel-datalake implements an event-driven instrumentation pattern. Event names MUST adhere to the following rules:

- MUST only contain ASCII alphanumeric characters.
- MUST be in PascalCase (CamelCase starting with a capital letter) format.
- MUST follow the template: `<Namespace><Noun><Verb>[Error]`
  - `Namespace`: One of the namespaces listed above (e.g., `Buffer`).
  - `Noun`: The subject of the event (e.g., `Events`, `Bytes`).
  - `Verb`: The past-tense verb describing the event (e.g., `Received`, `Sent`, `Processed`).
  - `[Error]`: MUST append `Error` if the event represents an error.

*Examples*: `BufferEventsReceived`, `ComponentBytesSent`, `TopologyStartError`.

### Metric Naming

Metric names broadly follow Prometheus naming standards:

- MUST only contain ASCII lowercase alphanumeric characters and underscores (`_`).
- MUST be in snake_case format.
- MUST follow the template: `<namespace>_<name>_<unit>_[total]`
  - `namespace`: Lowercase representation of the namespace (e.g., `buffer`, `component`, `topology`).
  - `name`: Words describing the measurement (e.g., `received_events`, `memory_rss`).
  - `unit`: Plural base unit of the measurement, if applicable (e.g., `bytes`, `seconds`).
  - `total`: Counters MUST append `_total` (e.g., `buffer_received_bytes_total`).
- SHOULD be broad in scope and use labels/tags to differentiate specific dimensions (e.g., `buffer_size_bytes{buffer_type="memory"}`).

---

## Emission Strategy

### Batching

For optimal performance, instrumentation SHOULD be batched:
- Emit telemetry for entire event batches rather than individual records.
- Batching metrics and logs reduces overhead under high-throughput workloads.

### Event-Driven Telemetry

Telemetry MUST be event-driven. Events serve as the primary source of truth, and they drive the downstream emission of metrics and logs. Do not log or emit metrics directly unless you are writing integration code where importing `otel-datalake` events is not possible.

---

### Standard Events Schema

#### 1. Error
Emitted when a non-fatal operational error occurs in a component. 

> **Note**: If an error prevents a component from starting up, log the error but do not emit an event since telemetry collectors may not be running.

##### Event Schema (JSON)
```json
{
  "event_name": "ComponentError",
  "error_code": "connection_refused",
  "error_type": "network_failure",
  "stage": "sending"
}
```

##### Field Specification
*   `error_code` (`Option<String>`): A bounded, low-cardinality error code used as a metric tag (e.g., `invalid_json`, `disk_full`). Avoid raw, unparsed system error strings.
*   `error_type` (`String`): The category of the error. MUST be one of:
    - `"configuration_error"`
    - `"network_failure"`
    - `"storage_failure"`
    - `"format_error"`
    - `"resource_exhaustion"`
    - `"internal_error"`
*   `stage` (`String`): The pipeline stage where the failure occurred. MUST be one of:
    - `"receiving"`
    - `"processing"`
    - `"sending"`

##### Telemetry Mapping
*   **Metrics**: MUST increment `<namespace>_errors_total` counter with `error_code`, `error_type`, and `stage` as labels.
*   **Logs**: MUST log at the `error` level. Log messages SHOULD be rate-limited (e.g., max once per 10 seconds per error source).

---

#### 2. EventsDropped
Emitted when events are dropped during processing.

##### Event Schema (JSON)
```json
{
  "event_name": "BufferEventsDropped",
  "count": 500,
  "intentional": false,
  "reason": "buffer_overflow"
}
```

##### Field Specification
*   `count` (`u64`): The number of events dropped.
*   `intentional` (`bool`): `true` if dropped intentionally (e.g., filtered out), `false` if dropped due to an error/overflow.
*   `reason` (`String`): A short description of the reason (e.g., `filter_applied`, `buffer_overflow`).

##### Telemetry Mapping
*   **Metrics**: MUST increment `<namespace>_discarded_events_total` by `count`.
*   **Logs**: MUST log a `"Events dropped"` message with properties:
    - If `intentional == true`: log at `debug` level.
    - If `intentional == false`: log at `error` level, rate-limited to 10 seconds.

---

[base unit]: https://en.wikipedia.org/wiki/SI_base_unit
[camelcase]: https://en.wikipedia.org/wiki/Camel_case
[Prometheus metric naming standards]: https://prometheus.io/docs/practices/naming/
