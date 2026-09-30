package query

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"live-analytics/internal/api"
	"live-analytics/internal/store"
)

const (
	recentEventsLookback = 3 * time.Second
	recentEventsLimit    = 100
)

// LiveSource is the subset of the repository the live feed polls.
type LiveSource interface {
	Live(ctx context.Context, tenant string) ([]store.LiveBucket, error)
	RecentEvents(ctx context.Context, tenant string, since time.Time, limit int) ([]store.StoredEvent, error)
}

// Snapshot is one live update, JSON-encoded once and shared by every subscriber.
type Snapshot struct {
	Ticks  []byte
	Events []byte
}

// Hub fans live updates out to SSE subscribers. It runs one poller per tenant with at least
// one subscriber, so ClickHouse load scales with active tenants, not with open dashboards.
type Hub struct {
	src         LiveSource
	interval    time.Duration
	log         *slog.Logger
	subscribers prometheus.Gauge
	pollSeconds prometheus.Histogram

	mu    sync.Mutex
	feeds map[string]*feed
}

type feed struct {
	subs   map[chan Snapshot]struct{}
	cancel context.CancelFunc
}

func NewHub(src LiveSource, interval time.Duration, reg prometheus.Registerer, log *slog.Logger) *Hub {
	h := &Hub{
		src: src, interval: interval, log: log, feeds: map[string]*feed{},
		subscribers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "query_live_subscribers", Help: "Open live (SSE) streams.",
		}),
		pollSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "query_live_poll_duration_seconds", Help: "Time to build one live snapshot.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
	}
	reg.MustRegister(h.subscribers, h.pollSeconds)
	return h
}

// Subscribe returns a channel of snapshots for the tenant and a function to stop.
// The channel holds one snapshot: a slow reader always gets the newest, never a backlog.
func (h *Hub) Subscribe(tenant string) (<-chan Snapshot, func()) {
	ch := make(chan Snapshot, 1)
	h.mu.Lock()
	f, ok := h.feeds[tenant]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		f = &feed{subs: map[chan Snapshot]struct{}{}, cancel: cancel}
		h.feeds[tenant] = f
		go h.poll(ctx, tenant, f)
	}
	f.subs[ch] = struct{}{}
	h.mu.Unlock()
	h.subscribers.Inc()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(f.subs, ch)
			if len(f.subs) == 0 {
				f.cancel()
				delete(h.feeds, tenant)
			}
			h.mu.Unlock()
			h.subscribers.Dec()
		})
	}
}

// ActiveTenants is the number of tenants currently being polled.
func (h *Hub) ActiveTenants() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.feeds)
}

func (h *Hub) poll(ctx context.Context, tenant string, f *feed) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		snapshot, err := h.snapshot(ctx, tenant)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.log.Warn("live snapshot failed", "tenant", tenant, "err", err)
		} else {
			h.broadcast(f, snapshot)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Hub) snapshot(ctx context.Context, tenant string) (Snapshot, error) {
	started := time.Now()
	defer func() { h.pollSeconds.Observe(time.Since(started).Seconds()) }()

	buckets, err := h.src.Live(ctx, tenant)
	if err != nil {
		return Snapshot{}, err
	}
	// Events become queryable up to ~1 s after arrival, so re-read a short window;
	// clients deduplicate by eventId.
	events, err := h.src.RecentEvents(ctx, tenant, time.Now().Add(-recentEventsLookback), recentEventsLimit)
	if err != nil {
		return Snapshot{}, err
	}
	ticks := make([]api.LiveTick, len(buckets))
	for i, b := range buckets {
		ticks[i] = toLiveTick(b)
	}
	stored := make([]api.StoredEvent, len(events))
	for i, e := range events {
		stored[i] = toStoredEvent(e)
	}
	ticksJSON, err := json.Marshal(ticks)
	if err != nil {
		return Snapshot{}, err
	}
	eventsJSON, err := json.Marshal(stored)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Ticks: ticksJSON, Events: eventsJSON}, nil
}

func (h *Hub) broadcast(f *feed, s Snapshot) {
	h.mu.Lock()
	subs := make([]chan Snapshot, 0, len(f.subs))
	for ch := range f.subs {
		subs = append(subs, ch)
	}
	h.mu.Unlock()
	for _, ch := range subs {
		select {
		case <-ch: // drop the unread older snapshot
		default:
		}
		select {
		case ch <- s:
		default:
		}
	}
}
