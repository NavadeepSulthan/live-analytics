package ingest

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// TenantLimiter enforces a per-tenant events/second budget (token bucket). One noisy tenant
// gets 429s with a precise Retry-After instead of degrading everyone else.
type TenantLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	perSec   rate.Limit
	burst    int
}

func NewTenantLimiter(eventsPerSecond float64, burst int) *TenantLimiter {
	return &TenantLimiter{limiters: map[string]*rate.Limiter{}, perSec: rate.Limit(eventsPerSecond), burst: burst}
}

// Take consumes n tokens, or returns how long to wait before retrying.
func (l *TenantLimiter) Take(tenant string, n int, now time.Time) (retryAfter time.Duration, ok bool) {
	l.mu.Lock()
	lim, exists := l.limiters[tenant]
	if !exists {
		lim = rate.NewLimiter(l.perSec, l.burst)
		l.limiters[tenant] = lim
	}
	l.mu.Unlock()

	reservation := lim.ReserveN(now, n)
	if !reservation.OK() {
		// n exceeds the burst: this batch can never fit; ask the client to send smaller ones.
		return time.Second, false
	}
	if delay := reservation.DelayFrom(now); delay > 0 {
		reservation.CancelAt(now)
		return delay, false
	}
	return 0, true
}
