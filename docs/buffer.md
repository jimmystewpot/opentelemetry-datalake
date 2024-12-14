# Buffer Specification

This document specifies otel-datalake's buffer behavior, events, and metrics required for all buffer implementations (e.g., in-memory and disk-backed buffers) to ensure correct development and integration.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Table of Contents
- [Scope](#scope)
- [Terms And Definitions](#terms-and-definitions)
- [Instrumentation Requirements](#instrumentation-requirements)
  - [Common Labels/Tags](#common-labelstags)
  - [Events and Metrics Schema](#events-and-metrics-schema)
    - [BufferCreated](#1-buffercreated)
    - [BufferEventsReceived](#2-buffereventsreceived)
    - [BufferEventsSent](#3-buffereventssent)
    - [BufferError](#4-buffererror)
    - [BufferEventsDropped](#5-buffereventsdropped)

---

## Scope

This specification addresses direct buffer development and telemetry. It does not cover global context metadata (e.g., `component_id`, `component_type`) that all buffers inherit globally when registered within the otel-datalake pipeline framework.

---

## Terms And Definitions

*   `byte_size` (Type: `u64`): The byte representation of events:
    *   **Memory Buffer**: MUST represent the exact in-memory layout size of the events (e.g., the total memory allocated for the Apache Arrow `RecordBatch` representation).
    *   **Disk Buffer**: MUST represent the exact serialized size of the event payload on disk.
*   `buffer_type` (Type: `String`): The storage medium of the buffer. MUST be one of:
    *   `"memory"`
    *   `"disk"`

---

## Buffering and Flushing Defaults

To ensure optimal performance and alignment with downstream sinks (such as Transactional Lakehouses), all buffer implementations **SHOULD** adhere to the following default flushing triggers:

*   **Default Max Batch Size**: `134217728` bytes (128 MB).
*   **Default Max Batch Interval**: `60` seconds.

The buffer **MUST** trigger a flush event when *either* of these thresholds is reached. These defaults are designed to balance data latency with write efficiency, particularly for Parquet-based storage formats.

---

## Instrumentation Requirements

This section extends the [Instrumentation Specification](instrumentation.md), which SHOULD be read first. All buffers MUST emit telemetry via structured events and corresponding metrics.

### Common Labels/Tags

All metrics emitted by a buffer MUST include the following tags/labels:
1.  `buffer_type` (e.g., `"memory"` or `"disk"`)
2.  `component_id` (inherited from the pipeline configuration)

---

### Events and Metrics Schema

#### 1. BufferCreated
Emitted once upon buffer initialization, and regularly at a periodic interval (e.g., every 15 seconds) to prevent stale metrics.

##### Event Schema (JSON)
```json
{
  "event_name": "BufferCreated",
  "max_size_bytes": 1073741824, 
  "max_size_events": null
}
```

##### Field Specification
*   `max_size_bytes` (`Option<u64>`): The maximum allowed size of the buffer in bytes. Set to `null` (None) if unlimited.
*   `max_size_events` (`Option<u64>`): The maximum allowed size of the buffer in number of events. Set to `null` (None) if unlimited.

##### Associated Metrics
*   `buffer_max_size_events` (Gauge): Emitted if `max_size_events` is present.
*   `buffer_max_event_size` (Gauge): Emitted for backward compatibility when `max_size_events` is present.
*   `buffer_max_size_bytes` (Gauge): Emitted if `max_size_bytes` is present.
*   `buffer_max_byte_size` (Gauge): Emitted for backward compatibility when `max_size_bytes` is present.

---

#### 2. BufferEventsReceived
Emitted under two distinct conditions:
1.  **Startup**: Upon initialization, if the buffer has existing persisted/restored events.
2.  **Runtime**: Immediately after receiving one or more events.

##### Event Schema (JSON)
```json
{
  "event_name": "BufferEventsReceived",
  "count": 100,
  "byte_size": 409600,
  "is_startup": false
}
```

##### Field Specification
*   `count` (`u64`): The number of events received/restored.
*   `byte_size` (`u64`): The byte size of the received/restored events.
*   `is_startup` (`bool`): `true` if emitting restored events during startup, `false` otherwise.

##### Metric Update Logic
*   **Always** (Startup and Runtime):
    *   Increment the `buffer_size_events` Gauge by `count` (backward compatibility: `buffer_events`).
    *   Increment the `buffer_size_bytes` Gauge by `byte_size` (backward compatibility: `buffer_byte_size`).
*   **Runtime Only** (`is_startup == false`):
    *   Increment the `buffer_received_events_total` Counter by `count`.
    *   Increment the `buffer_received_bytes_total` Counter by `byte_size`.
    > **LLM Implementation Warning**: To prevent double-counting across restarts, you MUST NOT increment `buffer_received_events_total` or `buffer_received_bytes_total` if `is_startup` is `true`.

---

#### 3. BufferEventsSent
Emitted immediately after successfully sending/flushing one or more events from the buffer.

##### Event Schema (JSON)
```json
{
  "event_name": "BufferEventsSent",
  "count": 50,
  "byte_size": 204800
}
```

##### Field Specification
*   `count` (`u64`): The number of events sent.
*   `byte_size` (`u64`): The byte size of the sent events.

##### Associated Metrics
*   Increment the `buffer_sent_events_total` Counter by `count`.
*   Increment the `buffer_sent_bytes_total` Counter by `byte_size`.
*   Decrement the `buffer_size_events` Gauge by `count` (backward compatibility: `buffer_events`).
*   Decrement the `buffer_size_bytes` Gauge by `byte_size` (backward compatibility: `buffer_byte_size`).

---

#### 4. BufferError
Extends the [Error Event](instrumentation.md#error). Emitted when an operational failure occurs within the buffer.

*   **Requirements**: Buffer errors are specific to the buffer type and operation (e.g., disk full, disk read failure). Emitted events MUST conform to the base instrumentation specification for error formats.

---

#### 5. BufferEventsDropped
Extends the [EventsDropped Event](instrumentation.md#eventsdropped). Emitted when events are dropped by the buffer (e.g., due to overflow policy under backpressure).

*   **Requirements**: Emitted events MUST conform to the base instrumentation specification for dropped events.
