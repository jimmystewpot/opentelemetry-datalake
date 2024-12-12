# Architecture

This document describes the high-level architecture of `otel-datalake`. It assumes some familiarity with the user-facing concepts and focuses instead on how they are wired together internally. The goal is to provide a starting point for navigating the codebase and to assist in understanding `otel-datalake`'s behavior and constraints.

For general codebase rules (such as the zero-panic policy, memory layouts, and async rules), refer to the [AGENTS.md](../AGENTS.md) standards.

---

## Overview

From a user's perspective, `otel-datalake` runs a configuration consisting of a directed, acyclic graph (DAG) of sources, transforms, and sinks. This logical representation of the configuration maps directly to the way it is laid out and executed internally.

### Logical Topology (ASCII Diagram)
```text
  [ Source: stdin ]
          |
          v
  [ Transform: filter ]
     /          \
    v            v
[ Sink: S3 ]  [ Sink: Delta Lake ]
```

A reasonably accurate mental model of running the above configuration is that `otel-datalake` spins up each component as an asynchronous `tokio` task and wires them together using channels. Below, we'll go through each type of component in more detail, discussing how it is translated into a running task and connected to the rest of the topology.

---

## Component Construction

After parsing and validating a user's configuration, we are left with a `Config` struct containing (among other things) the collections of `SourceConfig`s, `TransformConfig`s, and `SinkConfig`s corresponding to each of the configured components. Each of those traits has their own `build` method that constructs the component. This building occurs largely in the `crates/core/src/pipeline.rs` file (or `src/topology/builder.rs` in legacy layouts), along with some setup for the initial wiring into the topology.

### Sources

When a source config is built, the result is mainly two tasks: the "server" task of the source itself (which listens for incoming OTLP connections), and a "pump" task that forwards its output on to the rest of the system.

Construction begins by setting up a `SourceSender` for the configured source, which handles sending events to each of the outputs defined by the `SourceConfig::outputs` method. The outputs are built into the sender in such a way that attempting to send to an unknown output will result in an error.

Along with each of these outputs added to the sender, a corresponding `Fanout` instance is created which lives in a pump task and handles "fanning out" events to every downstream component that lists this source as an input. Each of these individual pump tasks (one per output) consists of forwarding events into the `Fanout`. The final pump task (one per source) spawns each of the output-specific pump tasks and drives them to completion.

Finally, the server task itself is built. `SourceConfig::build` takes a `SourceContext` as an argument, one field of which is the `SourceSender` built above. The result of the build function is wrapped in shutdown handling before being inserted into the topology.

#### Source Architecture (ASCII Diagram)
```text
+---------------------------------------------+
| Source Task (Server)                        |
|  - Receives external events (OTLP)          |
|  - Writes to SourceSender                   |
+----------------------|----------------------+
                       | (Channel)
                       v
+---------------------------------------------+
| SourceSender                                |
|  - Directs events to appropriate outputs    |
+----------------------|----------------------+
                       |
              +--------+--------+
              |                 |
              v (Output channel)v (Output channel)
+-------------------------+ +-------------------------+
| Fanout Task (Pump)      | | Fanout Task (Pump)      |
|  - Live Fanout instance | |  - Live Fanout instance |
|  - Write to Downstreams | |  - Write to Downstreams |
+------------|------------+ +------------|------------+
     +-------+-------+           +-------+-------+
     |               |           |               |
     v               v           v               v
(Downstream 1) (Downstream 2) (Downstream 3) (Downstream 4)
```

---

### Transforms

After sources, transforms are built. They are simpler than sources and mostly translate directly to `TransformConfig::build`. They can define `outputs` that are translated into `Fanout` instances in much the same way as sources, and also have an option to `enable_concurrency`. How that works depends on the type of transform.

#### 1. Synchronous Transforms

The simplest type of transform is a `Function` transform. It runs synchronously on a single event and writes to a single, unnamed output. One step above function transforms is the `Synchronous` transform. These are inclusive of function transforms, with the additional ability to write to multiple outputs if desired. From the perspective of the topology, both of these are run in exactly the same way.

In the simplest case, where the `enable_concurrency` flag is disabled, the resulting transform task is built to:
1. Pull a chunk of events from its input channel.
2. Process those events via the `transform` method into a `TransformOutputsBuf` (essentially a container of `Vec<Event>` for each of the transform's defined outputs).
3. Drain those outputs into the respective `Fanout` instances once the whole chunk of events has been processed.

If the `enable_concurrency` flag is enabled, the process is slightly more complex:
* For each chunk of input events, instead of being processed inline, a new task is spawned that does the work of processing the events into outputs. 
* Since there is some overhead to spawning tasks, `otel-datalake` attempts to pull larger chunks of events from the input for transforms running in this mode.
* The main transform task tracks the completion of those work tasks in the same order that they were spawned (ensuring that we do not reorder the resulting outputs).
* When a task completes, the main task receives the resulting `TransformOutputsBuf` and drains it into the respective fanouts.
* To prevent infinite buffering within those work tasks, the main task limits the maximum number of tasks in flight simultaneously. Spawning new tasks allows Tokio's work-stealing scheduler to spread the CPU work across multiple threads when capacity is available.

#### Synchronous Concurrency Options (ASCII Diagrams)

**Option A: Concurrency Disabled (Sequential Inline)**
```text
[Input Channel] 
       |
       v
+---------------------------------------+
| Main Transform Task                   |
|  1. Pull chunk of events from input   |
|  2. Process inline via `transform()`  |
|  3. Drain outputs to Fanout           |
+------------------|--------------------+
                   v
             [Fanout Tasks]
```

**Option B: Concurrency Enabled (Spawn Work Tasks)**
```text
[Input Channel]
       |
       v
+-----------------------------------------------------------+
| Main Transform Task                                       |
|                                                           |
|  1. Pulls large chunks of events from input               |
|  2. Spawns work tasks sequentially to process chunks      |
|  3. Tracks spawned tasks in-order (limits max in-flight)  |
+------|---------------------|---------------------|--------+
       |                     |                     |
       v (Spawn)             v (Spawn)             v (Spawn)
+--------------+      +--------------+      +--------------+
| Work Task 1  |      | Work Task 2  |      | Work Task 3  |
|  transform() |      |  transform() |      |  transform() |
+------|-------+      +------|-------+      +------|-------+
       |                     |                     |
       v (Results returned sequentially to Main Task)
+------|---------------------|---------------------|--------+
|  4. Receives in-order TransformOutputsBuf                 |
|  5. Drains outputs to Fanout                              |
+----------------------------|------------------------------+
                             v
                       [Fanout Tasks]
```

#### 2. Task-Style Transforms

A task-style transform differs from the synchronous variants in that it has the ability to do arbitrary, asynchronous stream-based transformations of its input. This includes emitting outputs after some timeout, independent of incoming events. From the topology's perspective, they are simple because they define most of their structure internally and are applied by passing the input channel into the `transform` method.

To build the full task, the transform itself is built, common filtering and telemetry are added by wrapping the input stream, and then the input stream is passed to the `transform` method. This results in an output stream, which is then forwarded to the transform's `Fanout` instance (task transforms do not support multiple outputs).

#### Task-Style Transform Architecture (ASCII Diagram)
```text
[Input Channel]
       |
       v
+---------------------------------------------+
| Task-Style Transform                        |
|                                             |
|  [Input Channel Stream]                     |
|         |                                   |
|         v                                   |
|  [Filter & Telemetry Stream Wrapper]        |
|         |                                   |
|         v                                   |
|  [transform(stream) -> Output Stream]       |
+-----------------|---------------------------+
                  |
                  v (Forward events)
            [Fanout Task]
```

---

### Sinks

Sinks have two components that make building them somewhat more complex than sources or transforms: healthchecks and buffers.

#### Healthchecks
Healthchecks are one-off tasks that run at startup to discover any issues that may prevent the sink from running properly (e.g., permissions or connectivity issues) and notify the user. They can be enabled or disabled both individually and at a global level, and the user can choose whether a failing healthcheck should prevent `otel-datalake` from starting.

#### Buffers
Buffers are a configurable mechanism for dealing with backpressure. By default, sinks will buffer some small number of events in memory before propagating backpressure upstream. Buffer configuration allows individual sinks to change that behavior:
* Choose between memory and disk for storing the buffered events.
* Set a maximum buffer size.
* Decide what should happen when the buffer is full (backpressure or load shedding).

Disk buffers add complexity in topology construction because they are persistent across config reloads and process restarts. They are built normally with their corresponding sink, but they are also stashed to the side if topology construction fails after a buffer has been built. This allows a subsequent build (likely of the previous configuration during a rollback) to pull from the already-built buffer without losing the persisted contents.

Once the healthcheck and buffer are built, the sink itself is constructed via `SinkConfig::build`. The surrounding task is defined to finalize its use of the buffer (removing it from the fallback stash), filter and wrap the input stream with telemetry, and then pass it to `SinkWriter::run` (or the corresponding writer trait).

#### Sink Architecture (ASCII Diagram)
```text
At Startup:
+----------------------------+
| Healthcheck Task           | ---> Performs validation / connectivity check
+----------------------------+

During Runtime:
[Upstream Output / Fanout]
             |
             v
+----------------------------+
| Buffer (Memory or Disk)    | ---> Handles backpressure / queuing
+----------------------------+
             |
             v (Input Stream)
+----------------------------+
| Telemetry & Filter Wrapper |
+----------------------------+
             |
             v
+----------------------------+
| SinkWriter::run()          | ---> Writes to modern open table format (e.g. Iceberg)
+----------------------------+
```

---

## Connecting Components

Once component construction is complete, we are left with a collection of yet-to-be-spawned tasks, as well as handles to their inputs and outputs. Specifically:
* A component input is the sender side of a channel (or buffer) that acts as the component's input stream.
* A component output is a `fanout::ControlChannel`, via which `otel-datalake` can send control messages that add or remove destinations for the component's actual output stream.

Given those definitions, wiring up an `otel-datalake` topology is a process of adding the appropriate inputs to the appropriate outputs. Consider the following example configuration:

```toml
[sources.foo]
type = "otlp"

[sinks.bar]
type = "iceberg"
inputs = ["foo"]
```

After construction, we'll have tasks for each component as well as a collection of inputs and outputs. In this case, each collection holds one item:
1. An output corresponding to the source `foo`, consisting of a control channel connected to the `Fanout` instance within the source's pump task.
2. An input corresponding to the sink `bar`, consisting of the sender side of the sink's input channel/buffer.

To wire up this topology, the `connect_diff` function on `RunningTopology` sees that `bar` specifies `foo` as an input, takes a clone of `bar`'s input, and sends that to `foo`'s output control channel. This results in the `Fanout` associated with the source `foo` adding a new sender to its list of destinations. The sink now receives events from the source on its input stream.

The actual code for this logic (found in `crates/core/src/pipeline.rs` or the running topology coordinator) is designed to apply modifications to an existing running topology dynamically during configuration reloads. This allows us to have a single unified execution path for starting, stopping, reloading, and rollback actions.

---

## Spawning the Topology

Once everything is properly connected, the final step is to spawn the actual tasks for each component. The supervisor tracks the handles of all running tasks, constructs tracing spans, sets up error handlers, and tracks shutdown processes to ensure clean exits without data loss.
