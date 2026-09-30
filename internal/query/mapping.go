package query

import (
	openapi_types "github.com/oapi-codegen/runtime/types"

	"live-analytics/internal/api"
	"live-analytics/internal/store"
)

func toLatency(l store.Latency) api.LatencyPercentiles {
	return api.LatencyPercentiles{P50: l.P50, P95: l.P95, P99: l.P99}
}

func errorRate(requests, errors uint64) float64 {
	if requests == 0 {
		return 0
	}
	return float64(errors) / float64(requests)
}

func toLiveTick(b store.LiveBucket) api.LiveTick {
	tick := api.LiveTick{
		Ts:        b.Bucket,
		Requests:  int64(b.Totals.Requests),
		Errors:    int64(b.Totals.Errors),
		LatencyMs: toLatency(b.Totals.Latency),
		Services:  make([]api.ServiceTick, len(b.Services)),
	}
	for i, s := range b.Services {
		tick.Services[i] = api.ServiceTick{Service: s.Service, Requests: int64(s.Requests), Errors: int64(s.Errors)}
	}
	return tick
}

func toStoredEvent(e store.StoredEvent) api.StoredEvent {
	out := api.StoredEvent{
		EventId:    openapi_types.UUID(e.ID),
		Timestamp:  e.Timestamp,
		ReceivedAt: e.ReceivedAt,
		Service:    e.Service,
		Route:      e.Route,
		Method:     e.Method,
		Status:     int(e.Status),
		DurationMs: e.DurationMs,
	}
	if e.Region != "" {
		out.Region = &e.Region
	}
	if e.UserID != "" {
		out.UserId = &e.UserID
	}
	return out
}

func toServiceStats(s store.ServiceTotals) api.ServiceStats {
	return api.ServiceStats{
		Service:   s.Service,
		Requests:  int64(s.Requests),
		Errors:    int64(s.Errors),
		ErrorRate: errorRate(s.Requests, s.Errors),
		LatencyMs: toLatency(s.Latency),
	}
}
