# Elasticsearch & OpenSearch Data Streams Sink

The `elasticsearch-sink` crate ingests OpenTelemetry signal batches (Logs, Metrics, Traces) from the internal pipeline and writes them to [Elasticsearch](https://www.elastic.co/elasticsearch/) or [OpenSearch](https://opensearch.org/) clusters using the HTTP/2 NDJSON Bulk API (`/_bulk`) targeting data streams.

---

## Requirements & Compatibility

| Requirement | Details |
|---|---|
| **Elasticsearch Version** | ≥ 7.9+ (Data streams support recommended; 8.x+ fully supported) |
| **OpenSearch Version** | ≥ 1.0+ (including Amazon OpenSearch Service & OpenSearch Serverless) |
| **Wire Protocol** | HTTP/1.1 or HTTP/2 with persistent connection pooling and gzip compression |
| **Network Access** | OTLP receiver → Elasticsearch/OpenSearch HTTP/HTTPS port (default `9200` or `443`) |

---

## Architecture & Data Streams Convention

Elasticsearch and OpenSearch data streams provide an append-only, time-series storage abstraction backed by multiple auto-generated backing indices. `opentelemetry-datalake` streams Arrow `RecordBatch`es directly into data streams using the NDJSON bulk `create` action:

```json
{"create": {}}
{"@timestamp": "2026-09-17T06:00:00.000000000Z", "service.name": "frontend", ...}
```

### Standard Data Streams Naming

Following the OpenTelemetry and Elastic convention `<type>-<dataset>-<namespace>`:

* **Logs**: `logs-otel-default`
* **Metrics**: `metrics-otel-default`
* **Traces**: `traces-otel-default`

### Index Template Requirements

Before sending data, ensure an index template exists matching your data stream pattern with `data_stream: {}` enabled and `@timestamp` configured as a `date` or `date_nanos` field.

```json
PUT _index_template/otel_data_streams
{
  "index_patterns": ["logs-otel-*", "metrics-otel-*", "traces-otel-*"],
  "data_stream": {},
  "template": {
    "settings": {
      "index.mode": "time_series",
      "index.routing.allocation.total_shards_per_node": 3
    },
    "mappings": {
      "properties": {
        "@timestamp": { "type": "date" },
        "service.name": { "type": "keyword" }
      }
    }
  },
  "priority": 200
}
```

When `validate_on_startup = true` (the default), the sink executes a 3-tier startup verification sequence before accepting traffic:
1. **Cluster Health (`GET /`)**: Verifies cluster connectivity and detects the engine version.
2. **Tier 1 Composable Template Validation (`GET /_index_template`)**: Evaluates all composable index templates, matches index patterns, resolves the winning template by highest priority, and asserts that `data_stream: {}` is declared.
3. **Tier 2 Scoped Template Fallback (`GET /_index_template/{data_stream}`)**: If Tier 1 is forbidden (HTTP 403) by restricted credentials or blocked (HTTP 404/405) by a reverse proxy, falls back to querying the exact data stream template, verifying that its `index_patterns` match the target stream and `data_stream: {}` is declared.
4. **Tier 3 Active Data Stream Fallback (`GET /_data_stream/{data_stream}`)**: If Tier 2 returns 404, 403 (credentials lacking template privileges), or pattern mismatch, verifies whether the data stream is already established and active in the cluster.

If all endpoints and validation tiers fail, startup aborts immediately before accepting OTLP traffic.

---

## Configuration Reference

Add an `[elasticsearch]` section to your `config.toml`. All fields with defaults are optional.

```toml
[elasticsearch]
# One or more cluster node HTTP/HTTPS URLs. Atomic round-robin load distribution
# and automated node-cooldown failover are applied across all endpoints.
endpoints = ["https://es01.internal:9200", "https://es02.internal:9200"]

# Unpack stringified JSON map/struct attributes into native JSON objects. Default: true.
unpack_attributes = true

# Enable HTTP gzip Content-Encoding for bulk payloads. Default: true.
gzip_compression = true

# Maximum concurrent outbound bulk requests (throttled by semaphore). Default: 8.
max_concurrent_requests = 8

# Maximum serialized bulk payload size in bytes. Default: 20 MiB (20971520).
max_payload_bytes = 20971520

# TCP connection timeout in seconds. Default: 10.
connect_timeout_secs = 10

# HTTP request timeout in seconds. Default: 30.
request_timeout_secs = 30

# Maximum SDK-level retries for transient HTTP errors. Default: 3.
max_retries = 3

# Delay between retries in seconds (overridden by Retry-After when present). Default: 1.
retry_interval_secs = 1

# Perform startup cluster health and index template verification. Default: true.
validate_on_startup = true

# Target data streams per signal type.
[elasticsearch.data_streams]
logs    = "logs-otel-default"
metrics = "metrics-otel-default"
traces  = "traces-otel-default"

# Authentication configuration. Options: "none", "basic", "api_key", "bearer", or "aws_sigv4".
[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="

# TLS configuration.
[elasticsearch.tls]
ca_cert_path = "/etc/ssl/certs/es-ca.crt"
insecure_skip_verify = false
```

---

## Authentication Schemes

### 1. No Authentication (`none`)
Used for local testing or secure private VPCs:
```toml
[elasticsearch.auth]
type = "none"
```

### 2. Basic Authentication (`basic`)
Username and password credentials:
```toml
[elasticsearch.auth]
type = "basic"
username = "elastic"
# Passwords should be provided via environment variables, not in plain text:
# export OTEL_DATALAKE_ELASTICSEARCH__AUTH__PASSWORD="secret_password"
```

### 3. API Key Authentication (`api_key`)
Elasticsearch and OpenSearch API keys (encoded `id:api_key` string):
```toml
[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="
```

### 4. Bearer Token Authentication (`bearer`)
OAuth2 or OIDC bearer token:
```toml
[elasticsearch.auth]
type = "bearer"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

### 5. AWS SigV4 Request Signing (`aws_sigv4`)
For Amazon OpenSearch Service and Amazon OpenSearch Serverless. Requires the binary to be compiled with the `aws` Cargo feature (`--features aws`).

```toml
[elasticsearch.auth]
type = "aws_sigv4"
region = "us-east-1"
service = "es" # Use "es" for OpenSearch Service, or "aoss" for OpenSearch Serverless
```

AWS credentials (access key, secret key, and optional session token) are automatically resolved from standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) or the AWS EC2/ECS/EKS instance metadata profile.

---

## Micro-Batching Buffer (`[elasticsearch.batching]`)

When configured, records are accumulated in memory across incoming OTLP batches until any threshold (bytes, records, or time) is met:

```toml
[elasticsearch.batching]
# Maximum accumulated Arrow byte size before flushing. Default: 10 MiB (10485760).
max_batch_size_bytes = 10485760

# Maximum record count before flushing. Default: 50000.
max_batch_records = 50000

# Maximum interval in seconds between flushes. Default: 10.
max_batch_interval_sec = 10
```

Flushing is handled on dedicated blocking threads via `tokio::task::spawn_blocking` to prevent CPU-intensive NDJSON serialization from starving Tokio async workers.

---

## Chronological Pre-Sorting (`[elasticsearch.order_by]`)

Pre-sorting records chronologically before emitting them to Elasticsearch or OpenSearch delivers major performance advantages:
1. **Lucene Segment Compaction**: Ingesting records ordered by `@timestamp` and high-cardinality terms (e.g. `service_name`) allows Lucene to build highly compressed, sequential index postings lists, reducing background merge I/O.
2. **Reduced Query Latency**: Time-series search queries can skip unindexed or out-of-range blocks efficiently.

```toml
[elasticsearch.order_by]
logs    = ["timestamp ASC", "service_name ASC NULLS LAST"]
metrics = ["timestamp ASC", "metric_name ASC"]
traces  = ["timestamp ASC"]
on_missing_column = "skip" # "skip" or "error"
```

Both SQL-like shorthand strings and structured syntax tables are supported.

---

## Resilience & Production Hardening

### Multi-Node Failover & Endpoint Cooldowns
The sink uses an atomic round-robin index over all configured `endpoints`. If a node experiences a network failure or returns an HTTP 5xx error:
1. The failing endpoint enters a 10-second cooldown period.
2. Subsequent bulk requests skip the cooled-down node and route to healthy siblings.
3. If all nodes are in cooldown (e.g. network partition), the sink attempts dispatch anyway rather than failing preemptively.

### Dynamic `Retry-After` Parsing
When Elasticsearch or OpenSearch nodes encounter high CPU or indexing pressure, they return HTTP `429 Too Many Requests` or `503 Service Unavailable`. The sink parses standard `Retry-After` headers (supporting both integer seconds and HTTP-date formats via `httpdate`) and sleeps dynamically up to a maximum cap of 60 seconds before retrying.

### Partial Bulk Item-Level Retries
Bulk responses returning HTTP 200 OK may still contain individual record rejections (`"errors": true`). The sink inspects the response `items` array:
* **Transient Item Errors (429 / 503)**: Sliced into a minimal retry NDJSON payload containing only the rejected records and retried across subsequent rounds.
* **Fatal Item Errors (400 Bad Request / Schema Mismatch)**: Logged with the exact rejection reason and dropped, preventing head-of-line blocking.
* **Successful Items**: Committed without re-transmission.

### Clean In-Flight Task Drainage
During shutdown or upon encountering a non-recoverable error, the sink awaits all in-flight asynchronous sibling tasks in its `JoinSet` before terminating, guaranteeing at-least-once delivery semantics without dropped batches.

---

## Security, RBAC & Network Architecture

In production and multi-tenant enterprise deployments, the Elasticsearch or OpenSearch cluster is frequently fronted by ingress reverse proxies (such as NGINX or Envoy) and secured with fine-grained Role-Based Access Control (RBAC).

### HTTP Endpoint Routing Matrix

The sink strictly interacts with the following minimal set of HTTP endpoints. Network firewalls, API gateways, and reverse proxies can restrict traffic exclusively to these URIs:

| Endpoint | Method | Purpose | Trigger Phase | Reverse Proxy Rule |
|---|---|---|---|---|
| `/` | `GET` | Cluster health & engine version detection | Boot time | Allow `GET /` |
| `/_index_template` | `GET` | Tier 1: Cluster-wide template priority resolution | Boot time | Allow `GET /_index_template` |
| `/_index_template/_simulate_index/{data_stream}` | `POST` | Tier 2: Scoped template simulation fallback | Boot time (fallback) | Allow `POST /_index_template/_simulate_index/*` |
| `/_data_stream/{data_stream}` | `GET` | Tier 3: Data stream liveness check fallback | Boot time (fallback) | Allow `GET /_data_stream/*` |
| `/{data_stream}/_bulk` | `POST` | Micro-batched NDJSON telemetry streaming | Ingestion runtime | Allow `POST /*/_bulk` |

All other Elasticsearch endpoints (such as `_search`, `_delete_by_query`, `_cluster/settings`, `_cat/*`, or document retrieval) are never called by the sink and should be blocked.

### Ingress Reverse Proxy Configuration

#### NGINX Reverse Proxy Allowlist

The following NGINX configuration protects the cluster by restricting incoming requests exclusively to the paths and HTTP methods required by `opentelemetry-datalake`:

```nginx
upstream elasticsearch_backend {
    server es-node-01.internal:9200;
    server es-node-02.internal:9200;
    server es-node-03.internal:9200;
    keepalive 64;
}

server {
    listen 9200 ssl http2;
    server_name es-ingress.internal;

    ssl_certificate /etc/ssl/certs/es-ingress.crt;
    ssl_certificate_key /etc/ssl/certs/es-ingress.key;

    # 1. Boot-time cluster health & version detection (exact match)
    location = / {
        limit_except GET { deny all; }
        proxy_pass https://elasticsearch_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }

    # 2. Boot-time index template inspection & simulation (Tier 1 & Tier 2)
    location ~ ^/_index_template(/.*)?$ {
        limit_except GET POST { deny all; }
        proxy_pass https://elasticsearch_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }

    # 3. Boot-time data stream inspection (Tier 3 fallback)
    location ~ ^/_data_stream(/.*)?$ {
        limit_except GET { deny all; }
        proxy_pass https://elasticsearch_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }

    # 4. Runtime NDJSON bulk ingestion
    location ~ ^/([^/]+)/_bulk$ {
        limit_except POST { deny all; }
        proxy_pass https://elasticsearch_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        client_max_body_size 25m;
        proxy_read_timeout 60s;
    }

    # Block all other endpoints by default
    location / {
        return 403 "Forbidden: Endpoint not permitted by OTLP data lake sink proxy policy\n";
    }
}
```

#### Envoy Proxy Allowlist

The following Envoy `route_config` applies the equivalent path and method allowlist filtering:

```yaml
static_resources:
  listeners:
    - name: elasticsearch_ingress
      address:
        socket_address:
          address: 0.0.0.0
          port_value: 9200
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: es_ingress
                route_config:
                  name: es_route_allowlist
                  virtual_hosts:
                    - name: es_backend
                      domains: ["*"]
                      routes:
                        # 1. Cluster health & version detection
                        - match:
                            path: "/"
                            headers:
                              - name: ":method"
                                exact_match: "GET"
                          route:
                            cluster: elasticsearch_cluster

                        # 2. Composable index templates & simulation (Tier 1 & Tier 2)
                        - match:
                            prefix: "/_index_template"
                            headers:
                              - name: ":method"
                                safe_regex:
                                  google_re2: {}
                                  regex: "^(GET|POST)$"
                          route:
                            cluster: elasticsearch_cluster

                        # 3. Data stream status (Tier 3)
                        - match:
                            prefix: "/_data_stream"
                            headers:
                              - name: ":method"
                                exact_match: "GET"
                          route:
                            cluster: elasticsearch_cluster

                        # 4. Runtime NDJSON bulk ingestion
                        - match:
                            safe_regex:
                              google_re2: {}
                              regex: "^/[^/]+/_bulk$"
                            headers:
                              - name: ":method"
                                exact_match: "POST"
                          route:
                            cluster: elasticsearch_cluster
                            timeout: 60s

                        # Default: deny any unmatched routes
                        - match:
                            prefix: "/"
                          direct_response:
                            status: 403
                            body:
                              inline_string: "Forbidden: Endpoint not permitted for OTLP data lake sink"
                http_filters:
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.http.router.v3.Router
```

### Role-Based Access Control (RBAC)

#### Elasticsearch RBAC Role Definitions

##### Standard Role (Recommended)
Grants permissions to query cluster templates during Tier 1 validation and ingest telemetry into data streams:

```json
{
  "cluster": ["monitor", "manage_index_templates"],
  "indices": [
    {
      "names": ["logs-otel-*", "metrics-otel-*", "traces-otel-*"],
      "privileges": ["create_index", "write", "auto_configure", "view_index_metadata"]
    }
  ]
}
```

##### Restricted / Least-Privilege Role (Shared / Multi-Tenant Clusters with Pre-Provisioned Streams)
In Elasticsearch, composable index template inspection (`GET /_index_template`) and index template simulation (`POST /_index_template/_simulate_index/{name}`) strictly require the cluster privilege `manage_index_templates`; index-level privileges such as `view_index_metadata` do not authorize template inspection or simulation.

When cluster security policies forbid granting cluster-level privileges to application credentials:
- Both Tier 1 (`GET /_index_template`) and Tier 2 (`POST /_index_template/_simulate_index/{name}`) return HTTP 403 Forbidden.
- The sink seamlessly falls back to **Tier 3 (`GET /_data_stream/{data_stream}`)** using the index-level `manage_data_stream` (or `view_index_metadata`) privilege.
- **Requirement**: Target data streams must be **pre-provisioned** by an administrator or infrastructure automation before sink startup. Once a data stream already exists, runtime ingestion writes append directly to the active stream without evaluating templates, preventing priority shadowing.

```json
{
  "cluster": ["monitor"],
  "indices": [
    {
      "names": ["logs-otel-*", "metrics-otel-*", "traces-otel-*"],
      "privileges": ["write", "create_index", "view_index_metadata", "manage_data_stream"]
    }
  ]
}
```

> [!NOTE]
> **Reverse Proxy Multi-Tenancy**: When using reverse proxies (e.g., NGINX/Envoy) to restrict API surfaces, the upstream Elasticsearch service account typically *does* retain `manage_index_templates`, but the reverse proxy blocks `GET /_index_template` (to prevent cross-tenant enumeration) while allowlisting `POST /_index_template/_simulate_index/{data_stream}`. In that topology, Tier 2 template simulation evaluates template priority across the cluster for the target stream and detects if conventional templates shadow the stream without leaking the full cluster template catalogue.

#### OpenSearch Security Action Groups Mapping

OpenSearch Security uses action groups instead of Elasticsearch privileges. The following role definitions correspond to the Standard and Least-Privilege tiers:

##### Standard Role
```json
{
  "description": "Standard OpenSearch role for OTLP data lake sink",
  "cluster_permissions": [
    "cluster_monitor",
    "cluster:admin/indices/template/get"
  ],
  "index_permissions": [
    {
      "index_patterns": [
        "logs-otel-*",
        "metrics-otel-*",
        "traces-otel-*"
      ],
      "allowed_actions": [
        "write",
        "create_index",
        "indices_monitor",
        "manage_data_stream"
      ]
    }
  ]
}
```

##### Restricted / Least-Privilege Role
```json
{
  "description": "Least-privilege OpenSearch role for OTLP data lake sink (multi-tenant/restricted)",
  "cluster_permissions": [
    "cluster_monitor"
  ],
  "index_permissions": [
    {
      "index_patterns": [
        "logs-otel-*",
        "metrics-otel-*",
        "traces-otel-*"
      ],
      "allowed_actions": [
        "write",
        "create_index",
        "indices_monitor",
        "manage_data_stream"
      ]
    }
  ]
}
```

##### Action Groups & Privileges Equivalence Reference

| Operational Capability | Elasticsearch Privilege | OpenSearch Action Group / Permission | Description |
|---|---|---|---|
| Cluster Health & Version Probe | `monitor` | `cluster_monitor` | Required for boot-time `GET /` connectivity and engine version check. |
| Cluster-Wide Template Inspection | `manage_index_templates` | `cluster:admin/indices/template/get` | Required for boot-time Tier 1 template priority resolution via `GET /_index_template`. |
| Scoped Template Simulation | `manage_index_templates` | Unsupported (falls to Tier 3) | Used during Tier 2 fallback via `POST /_index_template/_simulate_index/{data_stream}`. Requires `manage_index_templates` cluster privilege in Elasticsearch; unsupported in OpenSearch (which falls through to Tier 3 pre-provisioned stream validation). |
| Data Stream Liveness | `manage_data_stream` (or `view_index_metadata`) | `manage_data_stream` (or `indices_monitor`) | Used during Tier 3 fallback via `GET /_data_stream/{data_stream}` for pre-created streams. |
| Bulk Data Ingestion | `write` | `write` (or `crud`) | Ingests micro-batches via `POST /{data_stream}/_bulk`. |
| Dynamic Index Creation | `create_index`, `auto_configure` | `create_index` | Allows creation of backing indices when data streams rollover. |

---

## Full Example Configuration

```toml
[server]
grpc_addr = "0.0.0.0:4317"
http_addr = "0.0.0.0:4318"

[telemetry]
otlp_endpoint = "http://localhost:4317"
service_name = "opentelemetry-datalake"

[elasticsearch]
endpoints = [
  "https://es-node-01.internal:9200",
  "https://es-node-02.internal:9200",
  "https://es-node-03.internal:9200"
]
gzip_compression = true
unpack_attributes = true
max_concurrent_requests = 16
max_payload_bytes = 20971520 # 20 MiB
connect_timeout_secs = 5
request_timeout_secs = 30
max_retries = 5
retry_interval_secs = 2
validate_on_startup = true

[elasticsearch.data_streams]
logs    = "logs-otel-prod"
metrics = "metrics-otel-prod"
traces  = "traces-otel-prod"

[elasticsearch.auth]
type = "api_key"
api_key = "VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeW5rNVliY1RtZw=="

[elasticsearch.tls]
ca_cert_path = "/etc/ssl/certs/corporate-root-ca.pem"
insecure_skip_verify = false

[elasticsearch.batching]
max_batch_size_bytes = 15728640 # 15 MiB
max_batch_records = 50000
max_batch_interval_sec = 5

[elasticsearch.order_by]
logs    = ["timestamp ASC", "service_name ASC"]
metrics = ["timestamp ASC", "metric_name ASC"]
traces  = ["timestamp ASC"]
```
