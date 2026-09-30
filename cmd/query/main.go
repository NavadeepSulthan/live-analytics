// Command query serves the analytics REST API and the live SSE stream.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"live-analytics/internal/platform"
	"live-analytics/internal/query"
	"live-analytics/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "query:", err)
		os.Exit(1)
	}
}

func run() error {
	env := &platform.Env{}
	addr := env.String("QUERY_ADDR", ":8081")
	keyEntries := env.List("API_KEYS", "")
	origins := env.List("ALLOWED_ORIGINS", "http://localhost:5174")
	liveInterval := env.Duration("LIVE_INTERVAL", time.Second)
	chCfg := store.Config{
		Addr:     env.String("CLICKHOUSE_ADDR", "localhost:9000"),
		Database: env.String("CLICKHOUSE_DB", "analytics"),
		User:     env.String("CLICKHOUSE_USER", "analytics"),
		Password: env.String("CLICKHOUSE_PASSWORD", "analytics-local-only"),
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
	log := platform.NewLogger("query", logLevel)
	shutdownTracing, err := platform.StartTracing(ctx, "query")
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	conn, err := store.Open(ctx, chCfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	repo := store.NewQueries(conn)
	reg := platform.NewRegistry()

	mux := http.NewServeMux()
	query.NewServer(repo, keys, query.NewHub(repo, liveInterval, reg, log), reg, log).Routes(mux)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := repo.Ping(ctx); err != nil {
			platform.WriteError(w, http.StatusServiceUnavailable, "not_ready", "clickhouse unreachable")
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", platform.MetricsHandler(reg))

	srv := &http.Server{
		Addr:              addr,
		Handler:           platform.Middleware(mux, "query", origins, log),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
		// No WriteTimeout: SSE responses are long-lived; per-route timeouts cover REST.
	}
	return platform.Serve(ctx, srv, 10*time.Second, log)
}
