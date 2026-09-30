package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"live-analytics/internal/api"
	"live-analytics/internal/event"
	"live-analytics/internal/platform"
)

const ingestKey = "acme-ingest-secret-1"

type fakeProducer struct {
	mu     sync.Mutex
	events []event.Event
	err    error
	block  chan struct{}
}

func (f *fakeProducer) Produce(ctx context.Context, events []event.Event) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, events...)
	return nil
}

func newTestHandler(t *testing.T, producer Producer, limiter *TenantLimiter, cfg Config) *Handler {
	t.Helper()
	keys, err := platform.ParseKeys([]string{"acme:ingest:" + ingestKey, "acme:read:acme-read-secret-00001"})
	if err != nil {
		t.Fatal(err)
	}
	if limiter == nil {
		limiter = NewTenantLimiter(1e6, 1e6)
	}
	if cfg.MaxBodyBytes == 0 {
		cfg = Config{MaxBodyBytes: 1 << 20, MaxDecodedBytes: 4 << 20, MaxInFlight: 8, ProduceTimeout: time.Second}
	}
	return NewHandler(cfg, keys, producer, limiter, prometheus.NewRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func validEvent() map[string]any {
	return map[string]any{
		"eventId": uuid.NewString(), "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"service": "checkout", "route": "/orders/42", "method": "GET", "status": 200, "durationMs": 12.5,
	}
}

func post(h http.Handler, body []byte, mutate func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-API-Key", ingestKey)
	if mutate != nil {
		mutate(r)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func batchJSON(events ...map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{"events": events})
	return b
}

func TestAcceptsValidBatch(t *testing.T) {
	producer := &fakeProducer{}
	w := post(newTestHandler(t, producer, nil, Config{}), batchJSON(validEvent(), validEvent()), nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var res api.IngestResult
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res.Accepted != 2 || res.Rejected != 0 || len(producer.events) != 2 || producer.events[0].Tenant != "acme" {
		t.Fatalf("result %+v, produced %d", res, len(producer.events))
	}
}

func TestPartialAcceptanceReportsRejectedIndexes(t *testing.T) {
	bad := validEvent()
	bad["status"] = 999
	producer := &fakeProducer{}
	w := post(newTestHandler(t, producer, nil, Config{}), batchJSON(validEvent(), bad, validEvent()), nil)
	var res api.IngestResult
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if w.Code != http.StatusAccepted || res.Accepted != 2 || res.Rejected != 1 || res.Errors[0].Index != 1 {
		t.Fatalf("status %d result %+v", w.Code, res)
	}
}

func TestAcceptsGzip(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(batchJSON(validEvent()))
	_ = gz.Close()
	w := post(newTestHandler(t, &fakeProducer{}, nil, Config{}), buf.Bytes(), func(r *http.Request) {
		r.Header.Set("Content-Encoding", "gzip")
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
}

func TestRejectsGzipBomb(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(`{"events":[{"route":"` + strings.Repeat("a", 8<<20) + `"}]}`))
	_ = gz.Close()
	if buf.Len() > 1<<20 {
		t.Fatalf("test payload should compress below the body limit, got %d bytes", buf.Len())
	}
	w := post(newTestHandler(t, &fakeProducer{}, nil, Config{}), buf.Bytes(), func(r *http.Request) {
		r.Header.Set("Content-Encoding", "gzip")
	})
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", w.Code)
	}
}

func TestClientErrors(t *testing.T) {
	h := newTestHandler(t, &fakeProducer{}, nil, Config{})
	cases := []struct {
		name   string
		body   []byte
		mutate func(*http.Request)
		want   int
	}{
		{"no key", batchJSON(validEvent()), func(r *http.Request) { r.Header.Del("X-API-Key") }, 401},
		{"read key", batchJSON(validEvent()), func(r *http.Request) { r.Header.Set("X-API-Key", "acme-read-secret-00001") }, 401},
		{"content type", batchJSON(validEvent()), func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 415},
		{"encoding", batchJSON(validEvent()), func(r *http.Request) { r.Header.Set("Content-Encoding", "br") }, 415},
		{"malformed", []byte(`{"events":[`), nil, 400},
		{"empty", batchJSON(), nil, 400},
		{"oversized body", []byte(`{"events":[` + strings.Repeat(" ", 2<<20) + `]}`), nil, 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if w := post(h, tc.body, tc.mutate); w.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body)
			}
		})
	}
}

func TestTenantRateLimitReturnsRetryAfter(t *testing.T) {
	h := newTestHandler(t, &fakeProducer{}, NewTenantLimiter(1, 2), Config{})
	if w := post(h, batchJSON(validEvent(), validEvent()), nil); w.Code != http.StatusAccepted {
		t.Fatalf("first batch: %d", w.Code)
	}
	w := post(h, batchJSON(validEvent()), nil)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
	}
}

func TestStreamFailureIsNotAcknowledged(t *testing.T) {
	producer := &fakeProducer{err: errors.New("broker down")}
	w := post(newTestHandler(t, producer, nil, Config{}), batchJSON(validEvent()), nil)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d, want 503 with Retry-After", w.Code)
	}
}

func TestShedsLoadWhenSaturated(t *testing.T) {
	producer := &fakeProducer{block: make(chan struct{})}
	h := newTestHandler(t, producer, nil, Config{MaxBodyBytes: 1 << 20, MaxDecodedBytes: 1 << 20, MaxInFlight: 1, ProduceTimeout: 5 * time.Second})

	done := make(chan int)
	go func() { done <- post(h, batchJSON(validEvent()), nil).Code }()
	deadline := time.Now().Add(2 * time.Second)
	for len(h.inflight) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if w := post(h, batchJSON(validEvent()), nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("second request status %d, want 503", w.Code)
	}
	close(producer.block)
	if code := <-done; code != http.StatusAccepted {
		t.Fatalf("first request status %d, want 202", code)
	}
}

func TestTenantLimiterRejectsBatchLargerThanBurst(t *testing.T) {
	l := NewTenantLimiter(100, 10)
	if _, ok := l.Take("acme", 11, time.Now()); ok {
		t.Fatal("a batch larger than the burst can never be admitted")
	}
	if _, ok := l.Take("acme", 10, time.Now()); !ok {
		t.Fatal("a full-burst batch should be admitted")
	}
	if _, ok := l.Take("globex", 10, time.Now()); !ok {
		t.Fatal("tenants must have independent budgets")
	}
}
