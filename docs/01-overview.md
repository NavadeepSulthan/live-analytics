# 1. Overview: what it does

## The problem

Every production system emits a constant stream of small facts: *"request `GET /orders/:id` on
service `checkout` took 42 ms and returned 500"*. Engineers need to ask questions of that stream:

- **Right now:** is the error rate spiking? Which service is slow?
- **Historically:** what did p99 latency look like over the last 24 hours or 7 days?
- **In detail:** show me the actual failing requests.

Answering those at **hundreds of thousands of events per second**, without losing or
double-counting events, and with a dashboard that updates every second without melting the
browser, is the problem this project solves.

## What a user sees

1. A service (or the included load generator) `POST`s batches of events to the ingest API.
2. About a quarter of a second later, the events are queryable.
3. The dashboard shows, per tenant:
   - **KPI cards:** requests/s, error rate, p50/p95/p99 latency, events stored.
   - **Live charts** (1, 5 or 15 min windows) that scroll with the clock, updated every second.
   - **Historical charts** (1 h, 24 h, 7 d) from pre-aggregated rollups.
   - A **services table** with per-service rate, errors and latency.
   - A **live event log** that you can pause, filter by service, and scroll back through
     thousands of events.

## Core concepts (read these before the code)

### Event
One request observation: `tenant`, `service`, `route`, `method`, `status`, `durationMs`,
timestamp and a unique `eventId`. Routes are **normalized** (`/users/123` → `/users/:id`) so they
can be grouped. The tenant is **never** taken from the payload; it comes from the API key.

### Durable log (Redpanda / Kafka)
The ingest API doesn't write to the database. It appends to a **partitioned, replicated log** and
returns `202 Accepted` once the log has durably stored the batch (`acks=all`). This decouples
accepting data (must be fast and always available) from storing it (can be slower, batched,
retried). If the database is down, events pile up safely in the log and drain later.

### Exactly-once storage (the key idea)
Kafka consumers are naturally **at-least-once**: if the sink writes a batch and crashes before
committing its offset, the batch is redelivered and would be stored twice. Two-phase commit
between Kafka and ClickHouse isn't possible, so this project makes the write **idempotent**:

- Every stored row carries the Kafka `partition` and `offset` it came from.
- A ClickHouse **projection** keeps `max(offset)` per partition. It's updated **in the same
  atomic insert** as the rows, so rows and "how far we got" can never disagree.
- When the sink is assigned a partition, it reads that watermark and **skips every record with
  `offset ≤ watermark`**.

Result: accepted events are stored exactly once, even after `kill -9`. (Client retries of a
request whose response was lost are still at-least-once; the `eventId` is stored so they can be
detected.)

### Columnar storage and rollups (ClickHouse)
ClickHouse stores each column separately and compresses it, so scanning millions of rows for
two columns is cheap. Two **materialized views** pre-aggregate every insert into per-second and
per-minute buckets. Latency percentiles are stored as mergeable **t-digest sketches**, so p99
over 7 days is computed by merging about 10k small sketches, not by sorting billions of numbers.

### Backpressure
A system that accepts more than it can process eventually falls over. The ingest API pushes back
early and explicitly:

| Condition | Response |
| --- | --- |
| Tenant over its rate (token bucket) | `429` + `Retry-After` |
| Too many requests in flight | `503` (shed load before queues grow) |
| Body or decompressed body too big | `413` |
| Log unavailable / produce timeout | `503` |

The load generator honors `Retry-After`, like a well-behaved client SDK.

### Live updates: SSE + Web Worker
The dashboard subscribes with **Server-Sent Events** (one long HTTP response, simpler than
WebSockets for server → client streams). The server runs **one poller per tenant** no matter how
many tabs are open. In the browser, a **Web Worker** does all the parsing, merging,
deduplication and **LTTB downsampling** (keeps the visual shape of a series with far fewer
points). It transfers typed arrays to the main thread zero-copy. The main thread only paints a
`<canvas>`.

### Keyset pagination
The event log pages backwards with a cursor encoding `(ts, eventId)` of the last row, not
`OFFSET`. Each page is an index seek, so page 1,000 is as fast as page 1.

## What this project deliberately does not do
- User accounts and login UI: API keys are enough to demonstrate tenancy and RBAC.
- A multi-node cluster: everything runs single-node locally. [03-architecture.md](03-architecture.md)
  describes how each part scales out.
- Arbitrary query language: queries are fixed, parameterized endpoints. That's safer and fast.
