// Command sink consumes the event stream and writes micro-batches into ClickHouse.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"live-analytics/internal/event"
	"live-analytics/internal/platform"
	"live-analytics/internal/sink"
	"live-analytics/internal/store"
	"live-analytics/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sink:", err)
		os.Exit(1)
	}
}

func run() error {
	env := &platform.Env{}
	addr := env.String("SINK_ADDR", ":8082")
	brokers := env.List("KAFKA_BROKERS", "localhost:19092")
	partitions := env.Int("STREAM_PARTITIONS", 12)
	replication := env.Int("STREAM_REPLICATION", 1)
	chCfg := store.Config{
		Addr:     env.String("CLICKHOUSE_ADDR", "localhost:9000"),
		Database: env.String("CLICKHOUSE_DB", "analytics"),
		User:     env.String("CLICKHOUSE_USER", "analytics"),
		Password: env.String("CLICKHOUSE_PASSWORD", "analytics-local-only"),
	}
	sinkCfg := sink.Config{
		Brokers:       brokers,
		Topic:         event.Topic,
		Group:         env.String("SINK_GROUP", "clickhouse-sink"),
		MaxBatch:      env.Int("SINK_MAX_BATCH", 50_000),
		FlushInterval: env.Duration("SINK_FLUSH_INTERVAL", 500*time.Millisecond),
	}
	logLevel := env.String("LOG_LEVEL", "info")
	if err := env.Err(); err != nil {
		return err
	}
	// One insert must stay one ClickHouse block (default max_insert_block_size is 1,048,576)
	// so a batch is all-or-nothing, which the watermark dedup relies on.
	if sinkCfg.MaxBatch > 1_000_000 {
		return errors.New("SINK_MAX_BATCH must be <= 1000000")
	}

	ctx, stop := platform.SignalContext()
	defer stop()
	log := platform.NewLogger("sink", logLevel)
	shutdownTracing, err := platform.StartTracing(ctx, "sink")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := store.Migrate(setupCtx, chCfg, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := stream.EnsureTopics(setupCtx, brokers, int32(partitions), int16(replication), event.Topic, event.DLQTopic); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}
	conn, err := store.Open(ctx, chCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	reg := platform.NewRegistry()
	writer := store.NewEventWriter(conn)
	s, err := sink.New(sinkCfg, writer, reg, log)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := errors.Join(writer.Ping(ctx), s.Ping(ctx)); err != nil {
			platform.WriteError(w, http.StatusServiceUnavailable, "not_ready", err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", platform.MetricsHandler(reg))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return s.Run(ctx) })
	group.Go(func() error { return platform.Serve(ctx, srv, 5*time.Second, log) })
	log.Info("sink started", "group", sinkCfg.Group, "max_batch", sinkCfg.MaxBatch, "flush_interval", sinkCfg.FlushInterval)
	return group.Wait()
}
