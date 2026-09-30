// Package query is the read side: REST endpoints over pre-aggregated metrics and raw events,
// plus a Server-Sent Events live stream.
package query

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"live-analytics/internal/api"
	"live-analytics/internal/platform"
	"live-analytics/internal/store"
)

const (
	defaultWindow = 300
	maxWindow     = 7 * 24 * 3600
	pingInterval  = 15 * time.Second
)

var servicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// Repository is the query side's view of storage.
type Repository interface {
	LiveSource
	Summary(ctx context.Context, tenant string, window time.Duration, service string) (store.Totals, error)
	TimeSeries(ctx context.Context, tenant string, window time.Duration, service string, step int) ([]store.Point, error)
	Services(ctx context.Context, tenant string, window time.Duration) ([]store.ServiceTotals, error)
	ListEvents(ctx context.Context, tenant string, f store.EventFilter) ([]store.StoredEvent, string, error)
}

type Server struct {
	repo    Repository
	keys    *platform.KeyStore
	hub     *Hub
	log     *slog.Logger
	latency *prometheus.HistogramVec
}

func NewServer(repo Repository, keys *platform.KeyStore, hub *Hub, reg prometheus.Registerer, log *slog.Logger) *Server {
	s := &Server{
		repo: repo, keys: keys, hub: hub, log: log,
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "query_duration_seconds", Help: "ClickHouse time per API query.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"endpoint"}),
	}
	reg.MustRegister(s.latency)
	return s
}

// Routes registers the API. Timeouts apply to request/response endpoints; the stream is
// long-lived and ends when the client disconnects.
func (s *Server) Routes(mux *http.ServeMux) {
	timed := func(h http.HandlerFunc) http.Handler {
		return http.TimeoutHandler(s.authenticated(h), 10*time.Second, `{"error":{"code":"timeout","message":"query timed out"}}`)
	}
	mux.Handle("GET /v1/metrics/summary", timed(s.summary))
	mux.Handle("GET /v1/metrics/timeseries", timed(s.timeSeries))
	mux.Handle("GET /v1/metrics/services", timed(s.services))
	mux.Handle("GET /v1/events", timed(s.events))
	mux.Handle("GET /v1/stream", s.authenticated(s.stream))
}

type tenantKey struct{}

func (s *Server) authenticated(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, err := s.keys.Authenticate(r, platform.RoleRead)
		if err != nil {
			platform.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid read API key")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), tenantKey{}, tenant)))
	})
}

func tenantOf(r *http.Request) string {
	return r.Context().Value(tenantKey{}).(string)
}

func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	window, service, ok := windowAndService(w, r)
	if !ok {
		return
	}
	started := time.Now()
	t, err := s.repo.Summary(r.Context(), tenantOf(r), window, service)
	if !s.observe(w, r, "summary", started, err) {
		return
	}
	platform.WriteJSON(w, http.StatusOK, api.Summary{
		WindowSeconds:     int(window.Seconds()),
		Requests:          int64(t.Requests),
		Errors:            int64(t.Errors),
		RequestsPerSecond: float64(t.Requests) / window.Seconds(),
		ErrorRate:         errorRate(t.Requests, t.Errors),
		LatencyMs:         toLatency(t.Latency),
		QueryMs:           millis(started),
	})
}

func (s *Server) timeSeries(w http.ResponseWriter, r *http.Request) {
	window, service, ok := windowAndService(w, r)
	if !ok {
		return
	}
	requested, ok := intParam(w, r, "stepSeconds", 0, 1, 86400)
	if !ok {
		return
	}
	step := store.ChooseStep(window, requested)
	if int(window.Seconds())/step > 10_000 {
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", "stepSeconds: too many buckets for this window")
		return
	}
	started := time.Now()
	points, err := s.repo.TimeSeries(r.Context(), tenantOf(r), window, service, step)
	if !s.observe(w, r, "timeseries", started, err) {
		return
	}
	out := api.TimeSeries{StepSeconds: step, Points: make([]api.TimeSeriesPoint, len(points))}
	for i, p := range points {
		out.Points[i] = api.TimeSeriesPoint{
			Ts: p.Bucket, Requests: int64(p.Requests), Errors: int64(p.Errors), LatencyMs: toLatency(p.Latency),
		}
	}
	out.QueryMs = millis(started)
	platform.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) services(w http.ResponseWriter, r *http.Request) {
	window, _, ok := windowAndService(w, r)
	if !ok {
		return
	}
	started := time.Now()
	services, err := s.repo.Services(r.Context(), tenantOf(r), window)
	if !s.observe(w, r, "services", started, err) {
		return
	}
	out := make([]api.ServiceStats, len(services))
	for i, svc := range services {
		out[i] = toServiceStats(svc)
	}
	platform.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	service := q.Get("service")
	if service != "" && !servicePattern.MatchString(service) {
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", "service: must be a lowercase slug")
		return
	}
	statusClass := q.Get("statusClass")
	switch statusClass {
	case "", "2xx", "3xx", "4xx", "5xx":
	default:
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", "statusClass: one of 2xx, 3xx, 4xx, 5xx")
		return
	}
	limit, ok := intParam(w, r, "limit", 100, 1, 500)
	if !ok {
		return
	}
	started := time.Now()
	events, next, err := s.repo.ListEvents(r.Context(), tenantOf(r), store.EventFilter{
		Service: service, StatusClass: statusClass, Cursor: q.Get("cursor"), Limit: limit,
	})
	if errors.Is(err, store.ErrBadCursor) {
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", "cursor: invalid")
		return
	}
	if !s.observe(w, r, "events", started, err) {
		return
	}
	page := api.EventPage{Events: make([]api.StoredEvent, len(events))}
	for i, e := range events {
		page.Events[i] = toStoredEvent(e)
	}
	if next != "" {
		page.NextCursor = &next
	}
	platform.WriteJSON(w, http.StatusOK, page)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprint(w, "retry: 3000\n\n"); err != nil || rc.Flush() != nil {
		return
	}

	updates, unsubscribe := s.hub.Subscribe(tenantOf(r))
	defer unsubscribe()
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case snap := <-updates:
			if _, err := fmt.Fprintf(w, "event: ticks\ndata: %s\n\nevent: events\ndata: %s\n\n", snap.Ticks, snap.Events); err != nil {
				return
			}
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

// observe records query latency and writes a 500 on failure. Returns false if it wrote an error.
func (s *Server) observe(w http.ResponseWriter, r *http.Request, endpoint string, started time.Time, err error) bool {
	s.latency.WithLabelValues(endpoint).Observe(time.Since(started).Seconds())
	if err != nil {
		s.log.ErrorContext(r.Context(), "query failed", "endpoint", endpoint, "err", err)
		platform.WriteError(w, http.StatusInternalServerError, "query_failed", "query failed")
		return false
	}
	return true
}

func windowAndService(w http.ResponseWriter, r *http.Request) (time.Duration, string, bool) {
	seconds, ok := intParam(w, r, "windowSeconds", defaultWindow, 1, maxWindow)
	if !ok {
		return 0, "", false
	}
	service := r.URL.Query().Get("service")
	if service != "" && !servicePattern.MatchString(service) {
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", "service: must be a lowercase slug")
		return 0, "", false
	}
	return time.Duration(seconds) * time.Second, service, true
}

func intParam(w http.ResponseWriter, r *http.Request, name string, fallback, lo, hi int) (int, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		platform.WriteError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("%s: must be an integer between %d and %d", name, lo, hi))
		return 0, false
	}
	return n, true
}

func millis(since time.Time) float64 {
	return float64(time.Since(since).Microseconds()) / 1000
}
