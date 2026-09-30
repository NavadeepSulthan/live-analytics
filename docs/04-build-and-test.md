# 4. Build & test: step by step

Everything here is free and runs on one machine. Commands are for macOS (Apple Silicon). On
Linux, install Docker Engine instead of Colima and skip step 1's Colima lines.

## Step 1: Prerequisites (one time)

```bash
brew install go node colima docker docker-compose docker-buildx

# Let the docker CLI find the brew-installed compose and buildx plugins
mkdir -p ~/.docker
echo '{ "cliPluginsExtraDirs": ["/opt/homebrew/lib/docker/cli-plugins"] }' > ~/.docker/config.json

# Start a Linux VM for containers (free alternative to Docker Desktop)
colima start --vm-type vz --cpu 4 --memory 6 --disk 20

go version && node -v && docker compose version && docker buildx version
```

> `go.mod` pins `toolchain go1.26.8`. If your installed Go is older, the `go` command downloads
> and verifies that exact version automatically on first use.

## Step 2: Configure

```bash
cd live-analytics
cp .env.example .env      # demo keys, local passwords; .env is git-ignored
```

The key settings in `.env`:

| Variable | Meaning |
| --- | --- |
| `API_KEYS` | `tenant:role:secret,...` with role `ingest` or `read`, secrets ≥ 16 chars (`openssl rand -hex 24`) |
| `LOADGEN_INGEST_KEY` / `LOADGEN_READ_KEY` | Keys the load generator uses |
| `VITE_READ_KEY` | Pre-fills the dashboard connect screen in **dev only** |
| `KAFKA_BROKERS`, `CLICKHOUSE_*` | Infrastructure addresses and credentials |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Uncomment to send traces to Jaeger |

Optional tuning (defaults shown): `TENANT_EVENTS_PER_SECOND=200000`, `TENANT_BURST=400000`,
`MAX_IN_FLIGHT=512`, `MAX_BODY_BYTES=5MiB`, `MAX_DECODED_BYTES=20MiB`, `PRODUCE_TIMEOUT=5s`,
`SINK_MAX_BATCH=50000`, `SINK_FLUSH_INTERVAL=500ms`, `STREAM_PARTITIONS=12`,
`STREAM_REPLICATION=1`, `LIVE_INTERVAL=1s`.

## Step 3: Run it

### Option A: everything in Docker (closest to production)

```bash
make up          # builds images, starts 7 containers, waits for health checks
docker compose --profile app ps
```

### Option B: services on the host (for development)

```bash
make infra-up    # Redpanda, ClickHouse, Jaeger
make run-sink    # terminal 1: migrates the DB, creates topics, starts consuming
make run-ingest  # terminal 2: :8080
make run-query   # terminal 3: :8081
make dashboard-install
make dashboard-dev   # terminal 4: http://localhost:5174
```

## Step 4: Send data and look at it

```bash
make load RATE=3000 DURATION=10m    # realistic traffic: 8 services, periodic incidents
```

Open http://localhost:8088 (Docker) or http://localhost:5174 (dev) and connect with
`http://localhost:8081` + `demo-read-key-change-me-0`. Things to try:

- The live charts scroll every second. Watch an **incident** appear as an error and latency spike.
- Hover a chart: all charts show the same instant.
- Switch to **1h / 24h / 7d**: historical rollups load in milliseconds (query time is shown).
- Click a service in the table to filter everything, including the log.
- Scroll down the event log: older pages load via the keyset cursor. Pause it to inspect.

Send one event by hand:

```bash
curl -i -X POST localhost:8080/v1/events \
  -H 'Authorization: Bearer demo-ingest-key-change-me' -H 'Content-Type: application/json' \
  -d "{\"events\":[{\"eventId\":\"$(uuidgen)\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
       \"service\":\"curl-demo\",\"route\":\"/orders/42\",\"method\":\"GET\",\"status\":500,\"durationMs\":123.4}]}"
# → 202 {"accepted":1,"errors":[],"rejected":0}
# Invalid events are reported per index, e.g. {"index":0,"message":"eventId: required"}

curl -s 'localhost:8081/v1/events?limit=1&service=curl-demo' \
  -H 'Authorization: Bearer demo-read-key-change-me-0'
# route comes back normalized: "/orders/:id"
```

## Step 5: Automated tests

| Command | What runs | Needs infra? |
| --- | --- | --- |
| `make lint` | `gofmt` + `go vet` | No |
| `make test` | Go unit tests with the race detector. Integration tests **skip** themselves. | No |
| `make infra-up && make test-integration` | Also runs ClickHouse integration tests and the Redpanda **crash-dedup test** | Yes |
| `make dashboard-test` | TypeScript typecheck, oxlint, Vitest (SSE parser, LTTB, series store, event buffer) | No |
| `cd dashboard && npm run format:check` | Prettier | No |
| `cd dashboard && DASHBOARD_URL=http://localhost:8088 npm run test:e2e` | Playwright: connect, live stream, virtualized log paging, filters and historical ranges | Full stack + some load |
| `make generate && git diff --exit-code` | Generated Go/TS types match `api/openapi.yaml` | No |
| `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` / `cd dashboard && npm audit` | Known-vulnerability scans | No |

What the important tests prove:

- `internal/sink/plan_test.go`: records at or below the watermark are skipped, replaying a batch
  writes nothing, undecodable records go to the DLQ but still advance the watermark, and inputs
  are never mutated.
- `internal/sink/sink_integration_test.go`: writes 3,000 events, simulates a crash **after the
  ClickHouse write and before the Kafka commit**, restarts. All 3,000 are redelivered and
  skipped, and exactly 3,000 are stored.
- `internal/ingest/handler_test.go`: auth, content type, size limits, gzip bombs, partial
  acceptance, `429` + `Retry-After`, load shedding, and a stream failure is never acknowledged.
- `internal/query/server_test.go`: tenant scoping, validation and auth, cursor paging, SSE
  ticks/events, and one shared poller per tenant that stops when the last client leaves.

## Step 6: Manual scenarios (the interesting ones)

### Exactly-once under load
```bash
make verify RATE=20000 DURATION=20s
# accepted 399500, stored 399500 (difference +0)
# verification passed: every accepted event was stored exactly once
```
`-verify` compares events stored (from the query API) before and after the run with the number
of events the ingest API accepted.

### Crash the sink mid-load (`kill -9`)
```bash
./bin/loadgen -rate 20000 -duration 25s -verify &     # host mode (Option B)
sleep 8 && pkill -9 -f bin/sink                       # hard kill, no shutdown hooks
make run-sink                                         # restart; it reloads watermarks
wait                                                  # verification must still pass
```
Docker variant: `docker compose kill -s SIGKILL sink`, then `docker compose --profile app up -d sink`.
Watch `curl -s localhost:8082/metrics | grep sink_duplicates_skipped_total` rise after restart.

### ClickHouse outage
```bash
docker compose stop clickhouse    # ingest still returns 202; sink logs retries; lag grows
curl -s localhost:8082/metrics | grep sink_consumer_lag_records
docker compose start clickhouse   # sink drains the backlog; verify still passes
```

### Rate limiting and backpressure
```bash
TENANT_EVENTS_PER_SECOND=5000 TENANT_BURST=5000 make run-ingest    # host mode
./bin/loadgen -rate 20000 -duration 10s    # many 429s; loadgen honours Retry-After
```

### Security checks
```bash
curl -i -X POST localhost:8080/v1/events -d '{}'                                         # 401
curl -i localhost:8081/v1/metrics/summary -H 'Authorization: Bearer demo-ingest-key-change-me'   # 401: wrong role
curl -I localhost:8088/ | grep -i content-security-policy                                # CSP present
```

### Traces
Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` (already set in Docker), send load, then
open http://localhost:16686. Sampling is 10%.

### Poke at ClickHouse directly
http://localhost:8123/play (user `analytics`, password `analytics-local-only`):
```sql
SELECT count(), formatReadableSize(sum(data_compressed_bytes)) FROM system.parts
WHERE table = 'events' AND active;
SELECT kafka_partition, max(kafka_offset) FROM analytics.events GROUP BY kafka_partition;  -- served by the projection
```

## Load test results (Apple Silicon laptop, 4-CPU / 6 GB Colima VM, all components local)

| Run | Command | Result |
| --- | --- | --- |
| Baseline | `make verify RATE=20000 DURATION=20s` | 399,500 accepted = 399,500 stored, p99 about 8 ms |
| Sink `kill -9` mid-run | see above | 499,500 = 499,500, duplicates skipped on restart |
| 100k/s | `./bin/loadgen -rate 100000 -duration 20s -batch 1000 -workers 64 -verify` | 1,998,000 stored exactly once, p99 8.6 ms |
| 200k/s | same with `-rate 200000` | 3,996,000 stored exactly once, p99 11.3 ms, ingest → queryable about 0.27 s |
| Overload 400k/s | `-rate 400000 -batch 2000 -workers 128 -verify` | Sustains about 210k/s. Backpressure (`429`/`503`), 2,426 retries, 42,000 rejected after retries. **Accepted 4,888,000 = stored.** |
| Dockerized stack 50k/s | `-rate 50000 -batch 1000 -workers 32 -verify` | 748,000 = 748,000 |
| Queries (12M+ rows) | dashboard / curl | summary, timeseries, services 15–27 ms; watermark lookup 3 ms (reads 19 rows) |
| Storage | `system.parts` | 26.9 bytes/event, 2.8× compression |
| Dashboard | Chrome, live 3k events/s | worker about 100 µs/frame, 0 CSP violations, 59 DOM rows for 3,600 log entries |

The 210k/s ceiling is the laptop, not the design: at that point ingest, broker, sink and database
all compete for the same 4 VM cores.

## Step 7: Stop and clean up

```bash
make down              # stop the Docker stack (volumes keep the data)
docker compose --profile app down -v    # also delete data
colima stop            # stop the VM and free its RAM
make clean             # remove ./bin and dashboard/dist
```

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `docker: 'compose' is not a docker command` / `buildx` missing | The `~/.docker/config.json` `cliPluginsExtraDirs` step above |
| `Cannot connect to the Docker daemon` | `colima start` |
| Port 5174/8080/8081 in use | Stop the other instance (`lsof -i :8080`), or stop Docker services before running on the host |
| Dashboard shows "unauthorized" | Use the **read** key, not the ingest key |
| Dashboard CORS error | Add your origin to `ALLOWED_ORIGINS` |
| Jaeger API returns 404 on `/api/services` | Jaeger v2 serves its API under `/api/v3/...` |
| In zsh, `command not found` for everything after a loop | You used `path` as a variable name. zsh ties it to `$PATH`. Rename it, then `export PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin` |
| Disk full during `docker build` | `docker builder prune -f` and `docker image prune -f` |
