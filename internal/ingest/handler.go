// Package ingest is the HTTP ingestion gateway: authenticate, validate, rate-limit and durably
// hand events to the stream before acknowledging.
package ingest

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"live-analytics/internal/api"
	"live-analytics/internal/event"
	"live-analytics/internal/platform"
)

const maxReportedErrors = 20

// Producer durably writes events to the stream.
type Producer interface {
	Produce(ctx context.Context, events []event.Event) error
}

type Config struct {
	MaxBodyBytes    int64 // as sent (possibly gzip-compressed)
	MaxDecodedBytes int64 // after decompression: guards against gzip bombs
	MaxInFlight     int
	ProduceTimeout  time.Duration
}

type Handler struct {
	cfg      Config
	keys     *platform.KeyStore
	producer Producer
	limiter  *TenantLimiter
	inflight chan struct{}
	metrics  *metrics
	log      *slog.Logger
	now      func() time.Time
}

func NewHandler(cfg Config, keys *platform.KeyStore, producer Producer, limiter *TenantLimiter,
	reg prometheus.Registerer, log *slog.Logger) *Handler {
	return &Handler{
		cfg: cfg, keys: keys, producer: producer, limiter: limiter,
		inflight: make(chan struct{}, cfg.MaxInFlight),
		metrics:  newMetrics(reg),
		log:      log,
		now:      time.Now,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenant, err := h.keys.Authenticate(r, platform.RoleIngest)
	if err != nil {
		h.reject(w, http.StatusUnauthorized, "unauthorized", "missing or invalid ingest API key")
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		h.reject(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "expected application/json")
		return
	}

	// Shed load early instead of queueing unboundedly: a fast 503 lets clients back off.
	select {
	case h.inflight <- struct{}{}:
		defer func() { <-h.inflight }()
	default:
		h.metrics.requests.WithLabelValues("503").Inc()
		platform.WriteThrottled(w, http.StatusServiceUnavailable, time.Second, "server busy")
		return
	}

	batch, status, err := h.decode(w, r)
	if err != nil {
		h.reject(w, status, "invalid_request", err.Error())
		return
	}
	n := len(batch.Events)
	if n == 0 {
		h.reject(w, http.StatusBadRequest, "invalid_request", "events: at least one event is required")
		return
	}
	if n > event.MaxBatchSize {
		h.reject(w, http.StatusRequestEntityTooLarge, "batch_too_large", "events: at most 5000 per request")
		return
	}

	now := h.now()
	if wait, ok := h.limiter.Take(tenant, n, now); !ok {
		h.metrics.requests.WithLabelValues("429").Inc()
		platform.WriteThrottled(w, http.StatusTooManyRequests, wait, "tenant rate limit exceeded")
		return
	}

	result := api.IngestResult{Errors: []api.IngestError{}}
	valid := make([]event.Event, 0, n)
	for i, in := range batch.Events {
		e, err := event.FromAPI(in, tenant, now)
		if err != nil {
			result.Rejected++
			if len(result.Errors) < maxReportedErrors {
				result.Errors = append(result.Errors, api.IngestError{Index: i, Message: err.Error()})
			}
			continue
		}
		valid = append(valid, e)
	}

	if len(valid) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), h.cfg.ProduceTimeout)
		started := time.Now()
		err := h.producer.Produce(ctx, valid)
		cancel()
		h.metrics.produceSeconds.Observe(time.Since(started).Seconds())
		if err != nil {
			h.log.ErrorContext(r.Context(), "produce failed", "tenant", tenant, "events", len(valid), "err", err)
			h.metrics.requests.WithLabelValues("503").Inc()
			platform.WriteThrottled(w, http.StatusServiceUnavailable, 2*time.Second, "stream unavailable; retry")
			return
		}
	}

	result.Accepted = len(valid)
	h.metrics.events.WithLabelValues(tenant, "accepted").Add(float64(result.Accepted))
	h.metrics.events.WithLabelValues(tenant, "rejected").Add(float64(result.Rejected))
	h.metrics.batchSize.Observe(float64(n))
	h.metrics.requests.WithLabelValues("202").Inc()
	platform.WriteJSON(w, http.StatusAccepted, result)
}

func (h *Handler) decode(w http.ResponseWriter, r *http.Request) (api.IngestBatch, int, error) {
	var body io.Reader = http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes)
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return api.IngestBatch{}, http.StatusBadRequest, errors.New("invalid gzip body")
		}
		defer gz.Close()
		body = gz
	default:
		return api.IngestBatch{}, http.StatusUnsupportedMediaType, errors.New("unsupported Content-Encoding")
	}
	limited := &io.LimitedReader{R: body, N: h.cfg.MaxDecodedBytes + 1}

	var batch api.IngestBatch
	err := json.NewDecoder(limited).Decode(&batch)
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig) || limited.N <= 0:
		return batch, http.StatusRequestEntityTooLarge, errors.New("request body too large")
	case err != nil:
		return batch, http.StatusBadRequest, errors.New("malformed JSON: " + err.Error())
	}
	return batch, 0, nil
}

func (h *Handler) reject(w http.ResponseWriter, status int, code, message string) {
	h.metrics.requests.WithLabelValues(http.StatusText(status)).Inc()
	platform.WriteError(w, status, code, message)
}
