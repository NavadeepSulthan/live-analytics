# 3. Architecture

## Components

| Component | Code | Responsibility | Stateful? |
| --- | --- | --- | --- |
| **ingest** (:8080) | [cmd/ingest](../cmd/ingest), [internal/ingest](../internal/ingest) | Authenticate, limit, validate, produce to Kafka, reply `202` | No |
| **Redpanda** | docker-compose | Durable, ordered, partitioned log `events.v1` (12 partitions) + `events.dlq` | Yes |
| **sink** (:8082 metrics) | [cmd/sink](../cmd/sink), [internal/sink](../internal/sink) | Consume, dedup, batch-insert into ClickHouse, commit offsets; runs migrations and ensures topics on start | No (state lives in ClickHouse) |
| **ClickHouse** | [internal/store/migrations](../internal/store/migrations) | `events` + `metrics_1s` / `metrics_1m` rollups | Yes |
| **query** (:8081) | [cmd/query](../cmd/query), [internal/query](../internal/query) | REST queries + SSE live stream | No |
| **dashboard** | [dashboard/](../dashboard) | React UI, Web Worker, Canvas charts | No |

## Write path

```mermaid
sequenceDiagram
  participant C as Client
  participant I as ingest
  participant K as Redpanda
  participant S as sink
  participant CH as ClickHouse
  C->>I: POST /v1/events (≤5000 events, gzip)
  I->>I: auth → shed load → size limits → rate limit → validate
  I->>K: produce (key = tenant/service, acks=all, zstd)
  K-->>I: ack
  I-->>C: 202 {accepted, rejected[]}
  loop every ≤500 ms or 50k records
    S->>K: poll (rebalance blocked)
    S->>S: Prepare(): decode, drop offset ≤ watermark, collect DLQ rejects
    S->>CH: INSERT one block (rows + partition/offset)
    CH->>CH: MVs update metrics_1s / metrics_1m; projection updates watermarks
    S->>K: commit offsets
    S->>K: allow rebalance
  end
```

**Partial acceptance:** in a batch, invalid events are rejected individually (returned with an
index and reason). Valid ones are still accepted. One bad event doesn't fail 4,999 good ones.

**Partition key = `tenant/service`:** events from one service stay ordered in one partition, and
load spreads across partitions as the number of services grows.

## Exactly-once storage: how and why it holds

The pure decision logic is [`Prepare()`](../internal/sink/plan.go): records + watermarks in, rows
to write + new watermarks + rejects out. It has no I/O and is fully unit-tested.

1. **Rows and watermark are one atomic write.** The `kafka_watermarks` projection (`max(offset)
   GROUP BY partition`) is materialized inside the same data part as the rows. ClickHouse inserts
   of one block are atomic, so the stored watermark can never be ahead of or behind the stored rows.
   That's why `SINK_MAX_BATCH` is capped (≤ 1M): each insert must stay a single block.
2. **No two writers per partition.** `BlockRebalanceOnPoll` means a partition can't move to
   another consumer between poll and commit.
3. **Reload on assignment.** On every partition assignment, the sink re-reads watermarks before
   processing its next batch.
4. **Commit is an optimization.** A lost commit only causes a replay. Replayed records are
   `≤ watermark` and are skipped (counted in `sink_duplicates_skipped_total`).

Proven by [`TestCrashBetweenWriteAndCommitIsDeduplicated`](../internal/sink/sink_integration_test.go)
(write 3,000, "crash" before commit, restart: 3,000 skipped, 3,000 stored) and by `kill -9` of the
sink under load.

**Boundary:** guarantees hold **after `202`**. If a client retries because it never saw the
`202`, the event can arrive twice. `eventId` is stored so such retries can be found (e.g.
`SELECT event_id, count() … HAVING count() > 1`).

## Read path

- **Summary / timeseries / services** read the rollups. `Rollup` picks `metrics_1s` for windows
  up to 48 h (its TTL), otherwise `metrics_1m`. `ChooseStep` picks a "nice" bucket size (1 s, 5 s,
  1 m, 5 m, 1 h, …) giving about 300 points per chart; requests that would produce too many buckets
  are rejected with `400`. Missing buckets are zero-filled so charts show gaps as zero traffic.
- **Events log** reads `events` with a keyset cursor `(ts, eventId)`, newest first.
- **Live stream (SSE):** the [`Hub`](../internal/query/hub.go) runs **one poller per active
  tenant**, once per `LIVE_INTERVAL` (1 s). Each poll is a single `GROUPING SETS` query (overall +
  per service) over the last 10 s, plus recent events. It fans out to all subscribers through a
  size-1 channel: a slow client gets the **latest** snapshot, never a growing backlog. A comment
  ping every 15 s keeps proxies from closing idle connections.
- **Browser:** the worker merges ticks into a `SeriesStore` (upsert by timestamp, so the
  overlapping 10 s windows don't double-count). It dedups events by id, downsamples with LTTB to
  the chart's pixel width, and posts transferable `Float64Array`s. Charts redraw on
  `requestAnimationFrame` so the time axis scrolls smoothly between updates.

## ClickHouse schema (why it's shaped like this)

| Table | Engine / key | TTL | Notes |
| --- | --- | --- | --- |
| `events` | MergeTree, `ORDER BY (tenant, service, ts)`, `PARTITION BY day` | 30 d | Sort key matches every filter; per-column codecs; `kafka_watermarks` projection |
| `metrics_1s` | AggregatingMergeTree | 2 d | `count`, errors, sum, `quantilesTDigestState(0.5, 0.95, 0.99)` |
| `metrics_1m` | AggregatingMergeTree | 90 d | Same states at minute grain |

Migrations are embedded SQL files applied in order and recorded in `schema_migrations`.

## Failure modes

| Failure | What happens | Recovery |
| --- | --- | --- |
| ClickHouse down | Sink retries the same batch with backoff (200 ms → 10 s), stops polling. Ingest keeps accepting into Redpanda. `sink_consumer_lag_records` grows. | Automatic when it's back. No loss, no duplicates. |
| Sink crash mid-batch | Uncommitted records are redelivered to a new or restarted consumer. | Watermarks skip what was already written. |
| Redpanda down | Ingest produce times out, returns `503`, `/readyz` fails. | Clients retry. The producer reconnects. |
| Undecodable record | Sent to `events.dlq` with `error` and `source` headers. The pipeline continues. | Inspect with `rpk topic consume events.dlq`. |
| Traffic spike | Token bucket → `429 Retry-After`. In-flight cap → `503`. Producer buffer bounded. | Clients back off. Memory stays bounded. |
| Slow dashboard client | Drops intermediate snapshots, always gets the latest. | Nothing to recover. |
| Query too slow | `TimeoutHandler` returns `503` (stream excluded). | — |

## Security

- **AuthN/AuthZ:** `Authorization: Bearer <key>`. Keys are configured as `tenant:role:secret`
  and held only as SHA-256 digests, compared in constant time. The `ingest` role can only write;
  the `read` role can only query. A read key on ingest returns `401`.
- **Tenant isolation:** the tenant comes from the key and is bound as a query parameter in every
  statement. Clients can't choose it.
- **Injection:** all SQL uses bound parameters; `service` and `route` values are validated against
  strict patterns.
- **Resource abuse:** request body and decompressed-size limits (gzip bomb), batch ≤ 5,000,
  rate limits, in-flight cap, server timeouts.
- **Browser:** strict CSP (`default-src 'self'`, `worker-src 'self'`, `connect-src` limited to the
  API), `nosniff`, `frame-ancestors 'none'`. The read key is kept in `sessionStorage` (cleared when
  the tab closes). The SSE connection uses `fetch` so the key goes in a header, not the URL (URLs end up in logs).
- **Supply chain:** distroless nonroot images, pinned toolchain, `govulncheck`, `npm audit`,
  Dependabot.

## Architecture decisions (ADRs, short form)

1. **Log in front of the database**, not direct inserts: absorbs spikes and outages, enables
   replay. Cost: one more system to run.
2. **Idempotent writes via offset watermarks**, not Kafka transactions: Kafka transactions only
   cover Kafka-to-Kafka. Exactly-once *into ClickHouse* needs the state stored with the data.
3. **Rollups via materialized views**, not a stream processor (Flink etc.): same result with one
   less system. The trade-off is less flexibility for complex windowing.
4. **t-digest sketches** instead of exact percentiles: mergeable across time buckets. Error is
   small at the tails.
5. **SSE**, not WebSockets: the flow is one-way, and SSE works with plain HTTP infrastructure.
   `fetch` streaming is used so an `Authorization` header can be sent.
6. **Canvas + worker**, not SVG charting libraries: thousands of points per second without DOM churn.
7. **Contract-first OpenAPI** with generated types on both sides.
8. **Negative result kept:** specialized codecs (`DoubleDelta`, `Gorilla`, `T64`) were
   benchmarked on real data. They came out *worse* (27.2 vs 26.6 bytes/row), because `event_id`
   (random UUID, 16 bytes) dominates. The simpler schema stayed.

## Scaling out (beyond one laptop)

- **ingest / query:** stateless. Run N replicas behind a load balancer. Per-tenant rate limits
  would move to a shared store (Redis) or be divided by replica count.
- **Redpanda:** 3+ brokers, replication factor 3 (`STREAM_REPLICATION=3`). More partitions for
  more sink parallelism.
- **sink:** run up to one instance per partition. The consumer group spreads partitions and the
  watermark logic already handles rebalances.
- **ClickHouse:** `ReplicatedMergeTree` for HA, then sharding by tenant with a `Distributed`
  table. Watermarks then become per shard.
- **SSE fan-out:** the per-tenant poller becomes one poller per tenant across the cluster
  (leader election, or push over Redis Pub/Sub).
