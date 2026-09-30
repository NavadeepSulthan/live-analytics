# 2. Tech stack: only what's used, and why

## Backend (Go)

| Tech | Used for | Why this and not something else |
| --- | --- | --- |
| **Go 1.26** (`net/http` 1.22+ routing) | ingest, sink, query, loadgen | Cheap goroutines for thousands of concurrent requests, low GC pauses, a single static binary. The standard library router is enough; no web framework. Toolchain pinned to 1.26.8 in `go.mod` (security fixes). |
| **franz-go** (`kgo`, `kadm`, `kotel`) | Kafka producer, consumer, topic admin, trace propagation | Fastest pure-Go Kafka client, no cgo. Gives fine control needed for exactly-once: manual commits, `BlockRebalanceOnPoll`, partition assign/revoke callbacks. |
| **clickhouse-go v2** | Native-protocol batch inserts and queries | The native protocol sends columnar blocks, far faster than HTTP/JSON inserts. Supports bound parameters (no SQL injection). |
| **oapi-codegen** | Go types from `openapi.yaml` | Contract-first: the API spec is the source of truth. Run as a `go tool` so the version is pinned in `go.mod`. |
| **Prometheus client** | `/metrics` on every service | De-facto standard metrics format; histograms for latencies. |
| **OpenTelemetry** (OTLP/HTTP) | Distributed traces | Vendor-neutral. Trace context flows HTTP → Kafka record headers → sink. |
| **log/slog** | JSON structured logs with `trace_id` | Standard library; logs join to traces. |
| **x/time/rate** | Per-tenant token bucket | Correct, well-tested limiter; `ReserveN` gives an exact `Retry-After`. |
| **x/sync/errgroup** | Run sink loop and HTTP server together, fail together | Clean structured concurrency and shutdown. |

## Data infrastructure

| Tech | Used for | Why |
| --- | --- | --- |
| **Redpanda** | Durable, partitioned event log (Kafka API) | Kafka-compatible but a single binary with no JVM or ZooKeeper, so it fits on a laptop. Swapping in Apache Kafka / MSK / Confluent needs no code change. |
| **ClickHouse** | Event storage, rollups, analytical queries | Columnar OLAP database built for exactly this: billions of rows, aggregations in milliseconds, materialized views, TTL, t-digest quantiles, projections. |
| **Jaeger v2** | Trace viewer | Free, runs locally, accepts OTLP directly. |

## Frontend (dashboard)

| Tech | Used for | Why |
| --- | --- | --- |
| **React 19 + TypeScript (strict)** | UI | Components and hooks; strict typing including `noUncheckedIndexedAccess`. |
| **Vite** | Dev server and build | Fast, native ES module workers (`new Worker(new URL(...), { type: 'module' })`). |
| **openapi-typescript** | TS types from `openapi.yaml` | Same contract as the Go side; drift fails CI. |
| **Web Worker** | Stream parsing, merging, downsampling | Keeps the main thread free for painting and input. |
| **Canvas 2D + d3-scale / d3-shape / d3-array** | Charts | d3 only for the math (scales, ticks, line paths). Drawing to canvas avoids thousands of SVG DOM nodes. |
| **@tanstack/react-virtual** | Event log virtualization | Renders only visible rows. |
| **Vitest** | Unit tests (parser, LTTB, series store, buffer) | Vite-native, fast. |
| **Playwright** | E2E tests against the real stack | Real browser, real SSE. |
| **oxlint + Prettier** | Lint and formatting | Very fast lint, including React Compiler rules. |

## Delivery

| Tech | Used for |
| --- | --- |
| **Docker** (multi-stage, distroless `nonroot` runtime) | Go services image; nginx image for the dashboard |
| **Docker Compose** | Local infra, and the full stack with `--profile app` |
| **nginx** | Serves the static dashboard with CSP and caching headers |
| **GitHub Actions** | CI: lint, generate-drift, tests (with real Redpanda + ClickHouse), E2E, vulnerability scans, image publish |
| **Dependabot** | Dependency update PRs: Go and npm weekly, Actions and Docker base images monthly |
| **Make** | One entry point for every developer command |
| **Colima** (macOS) | Free Docker runtime (no Docker Desktop licence needed) |
