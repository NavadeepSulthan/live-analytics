// Command ingest runs the HTTP ingestion gateway.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"live-analytics/internal/event"
	"live-analytics/internal/ingest"
	"live-analytics/internal/platform"
	"live-analytics/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ingest:", err)
		os.Exit(1)
	}
}

func run() error {
	env := &platform.Env{}
	addr := env.String("INGEST_ADDR", ":8080")
	brokers := env.List("KAFKA_BROKERS", "localhost:19092")
	keyEntries := env.List("API_KEYS", "")
	origins := env.List("ALLOWED_ORIGINS", "http://localhost:5174")
	partitions := env.Int("STREAM_PARTITIONS", 12)
	replication := env.Int("STREAM_REPLICATION", 1)
	eventsPerSecond := env.Int("TENANT_EVENTS_PER_SECOND", 200_000)
	burst := env.Int("TENANT_BURST", 400_000)
	cfg := ingest.Config{
		MaxBodyBytes:    int64(env.Int("MAX_BODY_BYTES", 5<<20)),
		MaxDecodedBytes: int64(env.Int("MAX_DECODED_BYTES", 20<<20)),
		MaxInFlight:     env.Int("MAX_IN_FLIGHT", 512),
		ProduceTimeout:  env.Duration("PRODUCE_TIMEOUT", 5*time.Second),
	}
	logLevel := env.String("LOG_LEVEL", "info")
	if err := env.Err(); err != nil {
		return err
	}
	keys, err := platform.ParseKeys(keyEntries)
	if err != nil {
		return err
	}

	ctx, stop := platform.SignalContext()
	defer stop()
	log := platform.NewLogger("ingest", logLevel)
	shutdownTracing, err := platform.StartTracing(ctx, "ingest")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	setupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := stream.EnsureTopics(setupCtx, brokers, int32(partitions), int16(replication), event.Topic, event.DLQTopic); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}
	producer, err := stream.NewProducer(brokers, event.Topic, 1_000_000)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		producer.Close(flushCtx)
	}()

	reg := platform.NewRegistry()
	handler := ingest.NewHandler(cfg, keys, producer, ingest.NewTenantLimiter(float64(eventsPerSecond), burst), reg, log)

	mux := http.NewServeMux()
	mux.Handle("POST /v1/events", handler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := producer.Ping(ctx); err != nil {
			platform.WriteError(w, http.StatusServiceUnavailable, "not_ready", "stream unreachable")
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", platform.MetricsHandler(reg))

	srv := &http.Server{
		Addr:              addr,
		Handler:           platform.Middleware(mux, "ingest", origins, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	return platform.Serve(ctx, srv, 15*time.Second, log)
}
