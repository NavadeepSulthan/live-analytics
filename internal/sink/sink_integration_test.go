package sink

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"live-analytics/internal/event"
	"live-analytics/internal/store"
	"live-analytics/internal/stream"
)

// TestCrashBetweenWriteAndCommitIsDeduplicated proves the effectively-once guarantee with
// real Redpanda and ClickHouse: sink A writes everything but never commits offsets (as if it
// crashed right after each insert). Sink B, in the same group, gets every record redelivered
// and must skip all of them using the ClickHouse watermarks.
func TestCrashBetweenWriteAndCommitIsDeduplicated(t *testing.T) {
	brokers, chAddr := os.Getenv("KAFKA_BROKERS"), os.Getenv("CLICKHOUSE_ADDR")
	if brokers == "" || chAddr == "" {
		t.Skip("KAFKA_BROKERS and CLICKHOUSE_ADDR required; skipping stream integration test")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	suffix := fmt.Sprint(time.Now().UnixNano())
	topic, group := "test-events-"+suffix, "test-sink-"+suffix
	cfg := store.Config{
		Addr: chAddr, Database: "test_sink_" + suffix,
		User: envOr("CLICKHOUSE_USER", "analytics"), Password: envOr("CLICKHOUSE_PASSWORD", "analytics-local-only"),
	}
	if err := store.Migrate(ctx, cfg, log); err != nil {
		t.Fatal(err)
	}
	conn, err := store.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+cfg.Database+"`")
		_ = conn.Close()
	})
	writer := store.NewEventWriter(conn)
	count := func() uint64 {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM events").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if err := stream.EnsureTopics(ctx, []string{brokers}, 3, 1, topic); err != nil {
		t.Fatal(err)
	}
	producer, err := stream.NewProducer([]string{brokers}, topic, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	const total = 3000
	events := make([]event.Event, total)
	for i := range events {
		events[i] = event.Event{
			Tenant: "acme", ID: uuid.New(), Timestamp: time.Now(), ReceivedAt: time.Now(),
			Service: fmt.Sprintf("svc-%d", i%7), Route: "/", Method: "GET", Status: 200, DurationMs: 1,
		}
	}
	if err := producer.Produce(ctx, events); err != nil {
		t.Fatal(err)
	}
	producer.Close(ctx)

	newSink := func() (*Sink, *prometheus.Registry) {
		reg := prometheus.NewRegistry()
		s, err := New(Config{Brokers: []string{brokers}, Topic: topic, Group: group, MaxBatch: 500, FlushInterval: 200 * time.Millisecond}, writer, reg, log)
		if err != nil {
			t.Fatal(err)
		}
		return s, reg
	}
	runUntil := func(s *Sink, done func() bool) {
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- s.Run(runCtx) }()
		for !done() && runCtx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
		}
		cancel()
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	crashing, _ := newSink()
	crashing.commit = func(context.Context, ...*kgo.Record) error { return fmt.Errorf("simulated crash before commit") }
	runUntil(crashing, func() bool { return count() == total })
	if got := count(); got != total {
		t.Fatalf("after first sink: %d rows, want %d", got, total)
	}

	recovering, reg := newSink()
	runUntil(recovering, func() bool { return testutil.ToFloat64(recovering.metrics.duplicates) == total })

	if dup := testutil.ToFloat64(recovering.metrics.duplicates); dup != total {
		t.Fatalf("recovering sink skipped %v duplicates, want %d", dup, total)
	}
	if got := count(); got != total {
		t.Fatalf("after redelivery: %d rows, want exactly %d", got, total)
	}
	_ = reg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
