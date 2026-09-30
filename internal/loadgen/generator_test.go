package loadgen

import (
	"testing"
	"time"

	"live-analytics/internal/event"
)

func TestGeneratedEventsPassValidation(t *testing.T) {
	g := NewGenerator(12, 42)
	now := time.Now()
	for i, in := range g.Batch(5000, now) {
		if _, err := event.FromAPI(in, "acme", now); err != nil {
			t.Fatalf("event %d invalid: %v (%+v)", i, err, in)
		}
	}
}

func TestIncidentRaisesErrorRate(t *testing.T) {
	g := NewGenerator(1, 7)
	errorShare := func(at time.Time) float64 {
		errs := 0
		batch := g.Batch(20_000, at)
		for _, e := range batch {
			if e.Status >= 500 {
				errs++
			}
		}
		return float64(errs) / float64(len(batch))
	}
	during := errorShare(g.start.Add(5 * time.Second))
	after := errorShare(g.start.Add(40 * time.Second))
	if during < 0.12 || after > 0.02 {
		t.Fatalf("error share during incident %.3f, after %.3f", during, after)
	}
}
