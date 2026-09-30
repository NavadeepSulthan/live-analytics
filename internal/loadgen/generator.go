// Package loadgen produces realistic synthetic request telemetry for load tests and demos.
package loadgen

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"live-analytics/internal/api"
)

var serviceNames = []string{
	"gateway", "checkout", "search", "auth", "payments", "catalog",
	"cart", "inventory", "shipping", "reviews", "recommendations", "notifications",
}

var regions = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}

type route struct {
	method  api.IngestEventMethod
	pattern string
	weight  float64
}

type service struct {
	name       string
	medianMs   float64
	errorRate  float64
	routes     []route
	cumWeights []float64
}

// Generator builds events. Every `incidentEvery`, one service has an incident for
// `incidentFor`: 15% errors and 3x latency, so dashboards show realistic anomalies.
type Generator struct {
	services      []service
	rng           *rand.Rand
	start         time.Time
	incidentEvery time.Duration
	incidentFor   time.Duration
}

func NewGenerator(numServices int, seed uint64) *Generator {
	numServices = max(1, min(numServices, len(serviceNames)))
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	g := &Generator{rng: rng, start: time.Now(), incidentEvery: 60 * time.Second, incidentFor: 20 * time.Second}
	for i := 0; i < numServices; i++ {
		s := service{
			name:      serviceNames[i],
			medianMs:  8 + rng.Float64()*120,
			errorRate: 0.002 + rng.Float64()*0.01,
			routes: []route{
				{"GET", "/v1/" + serviceNames[i], 5},
				{"GET", "/v1/" + serviceNames[i] + "/{id}", 8},
				{"POST", "/v1/" + serviceNames[i], 2},
				{"PUT", "/v1/" + serviceNames[i] + "/{id}", 1},
				{"DELETE", "/v1/" + serviceNames[i] + "/{id}", 0.3},
			},
		}
		total := 0.0
		for _, r := range s.routes {
			total += r.weight
			s.cumWeights = append(s.cumWeights, total)
		}
		g.services = append(g.services, s)
	}
	return g
}

// Batch returns n events timestamped now.
func (g *Generator) Batch(n int, now time.Time) []api.IngestEvent {
	incident := g.incidentService(now)
	out := make([]api.IngestEvent, n)
	for i := range out {
		s := &g.services[g.rng.IntN(len(g.services))]
		r := s.routes[g.pickRoute(s)]
		errRate, latencyFactor := s.errorRate, 1.0
		if s.name == incident {
			errRate, latencyFactor = 0.15, 3
		}
		// Log-normal latency (sigma 0.6) is a good fit for real service latencies: long right tail.
		duration := s.medianMs * latencyFactor * math.Exp(g.rng.NormFloat64()*0.6)
		region := regions[g.rng.IntN(len(regions))]
		userID := fmt.Sprintf("u-%d", g.rng.IntN(50_000))
		out[i] = api.IngestEvent{
			EventId:    openapi_types.UUID(uuid.New()),
			Timestamp:  now.Add(-time.Duration(g.rng.IntN(500)) * time.Millisecond),
			Service:    s.name,
			Route:      concreteRoute(r.pattern, g.rng),
			Method:     r.method,
			Status:     g.status(r.method, errRate),
			DurationMs: float32(math.Round(duration*100) / 100),
			Region:     &region,
			UserId:     &userID,
		}
	}
	return out
}

func (g *Generator) incidentService(now time.Time) string {
	elapsed := now.Sub(g.start)
	cycle := int(elapsed / g.incidentEvery)
	if elapsed%g.incidentEvery >= g.incidentFor {
		return ""
	}
	return g.services[cycle%len(g.services)].name
}

func (g *Generator) pickRoute(s *service) int {
	x := g.rng.Float64() * s.cumWeights[len(s.cumWeights)-1]
	for i, w := range s.cumWeights {
		if x < w {
			return i
		}
	}
	return len(s.routes) - 1
}

func (g *Generator) status(method api.IngestEventMethod, errRate float64) int {
	switch x := g.rng.Float64(); {
	case x < errRate:
		return []int{500, 502, 503, 504}[g.rng.IntN(4)]
	case x < errRate+0.03:
		return []int{400, 401, 404, 409, 429}[g.rng.IntN(5)]
	case method == "POST":
		return 201
	case method == "DELETE":
		return 204
	default:
		return 200
	}
}

func concreteRoute(pattern string, rng *rand.Rand) string {
	if n := len(pattern); n > 4 && pattern[n-4:] == "{id}" {
		return fmt.Sprintf("%s%d", pattern[:n-4], rng.IntN(100_000))
	}
	return pattern
}
