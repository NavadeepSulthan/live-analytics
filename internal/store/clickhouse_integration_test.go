package store

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"live-analytics/internal/event"
)

// testConfig returns a config for a fresh, uniquely named database, or skips when
// CLICKHOUSE_ADDR isn't set (e.g. `go test ./...` without `docker compose up`).
func testConfig(t *testing.T) Config {
	t.Helper()
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set; skipping ClickHouse integration test")
	}
	return Config{
		Addr:     addr,
		Database: fmt.Sprintf("test_%d", time.Now().UnixNano()),
		User:     envOr("CLICKHOUSE_USER", "analytics"),
		Password: envOr("CLICKHOUSE_PASSWORD", "analytics-local-only"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func setup(t *testing.T) (*EventWriter, *Queries) {
	t.Helper()
	ctx := context.Background()
	cfg := testConfig(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := Migrate(ctx, cfg, log); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Migrations must be idempotent: running twice is a no-op.
	if err := Migrate(ctx, cfg, log); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	conn, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+cfg.Database+"`")
		_ = conn.Close()
	})
	return NewEventWriter(conn), NewQueries(conn)
}

func row(tenant, service string, ts time.Time, status uint16, ms float32, partition int32, offset int64) Row {
	return Row{
		Event: event.Event{
			Tenant: tenant, ID: uuid.New(), Timestamp: ts, ReceivedAt: ts.Add(50 * time.Millisecond),
			Service: service, Route: "/orders/:id", Method: "GET", Status: status, DurationMs: ms,
		},
		Partition: partition, Offset: offset,
	}
}

func TestClickHouseIntegration(t *testing.T) {
	writer, queries := setup(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Second)

	var rows []Row
	for i := 0; i < 100; i++ {
		status := uint16(200)
		if i%10 == 0 {
			status = 503
		}
		rows = append(rows, row("acme", "checkout", now, status, float32(i+1), 0, int64(i)))
	}
	rows = append(rows, row("acme", "search", now, 200, 5, 1, 41))
	rows = append(rows, row("globex", "checkout", now, 200, 5, 1, 42))
	if err := writer.Write(ctx, rows); err != nil {
		t.Fatalf("write: %v", err)
	}

	t.Run("watermarks come from the projection", func(t *testing.T) {
		w, err := writer.Watermarks(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if w[0] != 99 || w[1] != 42 {
			t.Fatalf("watermarks = %v, want {0:99, 1:42}", w)
		}
	})

	t.Run("summary is tenant-scoped with percentiles", func(t *testing.T) {
		s, err := queries.Summary(ctx, "acme", time.Minute, "checkout")
		if err != nil {
			t.Fatal(err)
		}
		if s.Requests != 100 || s.Errors != 10 {
			t.Fatalf("summary = %+v, want 100 requests / 10 errors", s)
		}
		if s.Latency.P50 < 45 || s.Latency.P50 > 56 || s.Latency.P99 < 95 {
			t.Fatalf("unexpected percentiles %+v", s.Latency)
		}
		other, err := queries.Summary(ctx, "globex", time.Minute, "")
		if err != nil || other.Requests != 1 {
			t.Fatalf("globex summary = %+v, %v; want 1 request", other, err)
		}
	})

	t.Run("time series is zero-filled and totals match", func(t *testing.T) {
		points, err := queries.TimeSeries(ctx, "acme", time.Minute, "", 10)
		if err != nil {
			t.Fatal(err)
		}
		var total uint64
		for _, p := range points {
			total += p.Requests
		}
		if len(points) < 6 || total != 101 {
			t.Fatalf("got %d points totalling %d, want >= 6 points totalling 101", len(points), total)
		}
	})

	t.Run("services breakdown", func(t *testing.T) {
		services, err := queries.Services(ctx, "acme", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(services) != 2 || services[0].Service != "checkout" || services[0].Requests != 100 {
			t.Fatalf("services = %+v", services)
		}
	})

	t.Run("live buckets split overall and per service", func(t *testing.T) {
		live, err := queries.Live(ctx, "acme")
		if err != nil {
			t.Fatal(err)
		}
		if len(live) != 1 || live[0].Totals.Requests != 101 || len(live[0].Services) != 2 {
			t.Fatalf("live = %+v", live)
		}
	})

	t.Run("keyset pagination visits every event exactly once", func(t *testing.T) {
		seen := map[uuid.UUID]bool{}
		cursor := ""
		for page := 0; page < 20; page++ {
			events, next, err := queries.ListEvents(ctx, "acme", EventFilter{Cursor: cursor, Limit: 30})
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range events {
				if seen[e.ID] {
					t.Fatalf("event %s returned twice", e.ID)
				}
				seen[e.ID] = true
			}
			if next == "" {
				break
			}
			cursor = next
		}
		if len(seen) != 101 {
			t.Fatalf("paged %d events, want 101", len(seen))
		}
	})

	t.Run("filters by status class", func(t *testing.T) {
		events, _, err := queries.ListEvents(ctx, "acme", EventFilter{StatusClass: "5xx", Limit: 100})
		if err != nil || len(events) != 10 {
			t.Fatalf("got %d 5xx events (%v), want 10", len(events), err)
		}
		if _, _, err := queries.ListEvents(ctx, "acme", EventFilter{StatusClass: "9xx", Limit: 10}); err == nil {
			t.Fatal("expected error for invalid status class")
		}
	})
}
