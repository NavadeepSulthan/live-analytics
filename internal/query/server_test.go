package query

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"live-analytics/internal/api"
	"live-analytics/internal/platform"
	"live-analytics/internal/store"
)

const readKey = "acme-read-secret-00001"

type fakeRepo struct {
	livePolls  atomic.Int64
	lastTenant atomic.Value
}

func (f *fakeRepo) Live(_ context.Context, tenant string) ([]store.LiveBucket, error) {
	f.livePolls.Add(1)
	f.lastTenant.Store(tenant)
	return []store.LiveBucket{{
		Bucket:   time.Unix(1_790_000_000, 0).UTC(),
		Totals:   store.Totals{Requests: 10, Errors: 1, Latency: store.Latency{P50: 5, P95: 9, P99: 12}},
		Services: []store.ServiceTotals{{Service: "checkout", Totals: store.Totals{Requests: 10, Errors: 1}}},
	}}, nil
}

func (f *fakeRepo) RecentEvents(context.Context, string, time.Time, int) ([]store.StoredEvent, error) {
	return []store.StoredEvent{{ID: uuid.New(), Service: "checkout", Route: "/", Method: "GET", Status: 200}}, nil
}

func (f *fakeRepo) Summary(_ context.Context, tenant string, window time.Duration, _ string) (store.Totals, error) {
	f.lastTenant.Store(tenant)
	return store.Totals{Requests: 600, Errors: 30, Latency: store.Latency{P50: 10, P95: 50, P99: 90}}, nil
}

func (f *fakeRepo) TimeSeries(_ context.Context, _ string, _ time.Duration, _ string, step int) ([]store.Point, error) {
	return []store.Point{{Bucket: time.Unix(0, 0).UTC(), Totals: store.Totals{Requests: 3}}}, nil
}

func (f *fakeRepo) Services(context.Context, string, time.Duration) ([]store.ServiceTotals, error) {
	return []store.ServiceTotals{{Service: "checkout", Totals: store.Totals{Requests: 4, Errors: 1}}}, nil
}

func (f *fakeRepo) ListEvents(_ context.Context, _ string, filter store.EventFilter) ([]store.StoredEvent, string, error) {
	if filter.Cursor == "bad" {
		return nil, "", store.ErrBadCursor
	}
	return []store.StoredEvent{{ID: uuid.New(), Service: "checkout", Status: 500}}, "next-page", nil
}

func newTestServer(t *testing.T) (*httptest.Server, *fakeRepo, *Hub) {
	t.Helper()
	keys, err := platform.ParseKeys([]string{"acme:read:" + readKey, "acme:ingest:acme-ingest-secret-1"})
	if err != nil {
		t.Fatal(err)
	}
	repo := &fakeRepo{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := NewHub(repo, 20*time.Millisecond, prometheus.NewRegistry(), log)
	mux := http.NewServeMux()
	NewServer(repo, keys, hub, prometheus.NewRegistry(), log).Routes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, repo, hub
}

func get(t *testing.T, url, key string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func TestSummaryDerivesRatesAndIsTenantScoped(t *testing.T) {
	srv, repo, _ := newTestServer(t)
	res := get(t, srv.URL+"/v1/metrics/summary?windowSeconds=60", readKey)
	var s api.Summary
	_ = json.NewDecoder(res.Body).Decode(&s)
	if res.StatusCode != 200 || s.RequestsPerSecond != 10 || s.ErrorRate != 0.05 || s.LatencyMs.P99 != 90 {
		t.Fatalf("status %d summary %+v", res.StatusCode, s)
	}
	if repo.lastTenant.Load() != "acme" {
		t.Fatalf("tenant = %v, want acme (from the key, never the request)", repo.lastTenant.Load())
	}
}

func TestValidationAndAuth(t *testing.T) {
	srv, _, _ := newTestServer(t)
	cases := []struct {
		path, key string
		want      int
	}{
		{"/v1/metrics/summary", "", 401},
		{"/v1/metrics/summary", "acme-ingest-secret-1", 401},
		{"/v1/metrics/summary?windowSeconds=0", readKey, 400},
		{"/v1/metrics/summary?windowSeconds=abc", readKey, 400},
		{"/v1/metrics/summary?service=Bad%20Name", readKey, 400},
		{"/v1/metrics/timeseries?windowSeconds=604800&stepSeconds=1", readKey, 400},
		{"/v1/metrics/timeseries?windowSeconds=3600", readKey, 200},
		{"/v1/metrics/services", readKey, 200},
		{"/v1/events?statusClass=9xx", readKey, 400},
		{"/v1/events?limit=501", readKey, 400},
		{"/v1/events?cursor=bad", readKey, 400},
		{"/v1/events?statusClass=5xx", readKey, 200},
	}
	for _, tc := range cases {
		if res := get(t, srv.URL+tc.path, tc.key); res.StatusCode != tc.want {
			t.Errorf("GET %s: status %d, want %d", tc.path, res.StatusCode, tc.want)
		}
	}
}

func TestEventsPageIncludesCursor(t *testing.T) {
	srv, _, _ := newTestServer(t)
	var page api.EventPage
	_ = json.NewDecoder(get(t, srv.URL+"/v1/events", readKey).Body).Decode(&page)
	if len(page.Events) != 1 || page.NextCursor == nil || *page.NextCursor != "next-page" {
		t.Fatalf("page = %+v", page)
	}
}

func TestStreamSendsTicksAndEvents(t *testing.T) {
	srv, _, _ := newTestServer(t)
	res := get(t, srv.URL+"/v1/stream", readKey)
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	seen := map[string]string{}
	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var current string
	for scanner.Scan() && len(seen) < 2 {
		line := scanner.Text()
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			current = name
		} else if data, ok := strings.CutPrefix(line, "data: "); ok {
			seen[current] = data
		}
	}
	var ticks []api.LiveTick
	if err := json.Unmarshal([]byte(seen["ticks"]), &ticks); err != nil || len(ticks) != 1 || ticks[0].Requests != 10 {
		t.Fatalf("ticks = %q (%v)", seen["ticks"], err)
	}
	var events []api.StoredEvent
	if err := json.Unmarshal([]byte(seen["events"]), &events); err != nil || len(events) != 1 {
		t.Fatalf("events = %q (%v)", seen["events"], err)
	}
}

func TestHubSharesOnePollerPerTenantAndStopsWhenIdle(t *testing.T) {
	_, repo, hub := newTestServer(t)
	a, stopA := hub.Subscribe("acme")
	b, stopB := hub.Subscribe("acme")
	<-a
	<-b
	if hub.ActiveTenants() != 1 {
		t.Fatalf("active tenants = %d, want 1 shared poller", hub.ActiveTenants())
	}
	stopA()
	stopB()
	if hub.ActiveTenants() != 0 {
		t.Fatal("poller should stop when the last subscriber leaves")
	}
	polls := repo.livePolls.Load()
	time.Sleep(100 * time.Millisecond)
	if repo.livePolls.Load() > polls+1 {
		t.Fatalf("still polling after unsubscribe: %d -> %d", polls, repo.livePolls.Load())
	}
}
