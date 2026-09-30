// Package event is the telemetry domain: the canonical event, its validation rules and
// its wire encoding on the stream. It has no infrastructure dependencies.
package event

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"live-analytics/internal/api"
)

const (
	MaxBatchSize      = 5000
	maxPast           = 24 * time.Hour
	maxFuture         = 5 * time.Minute
	maxRouteLen       = 200
	maxRegionLen      = 32
	maxUserIDLen      = 128
	maxAttributes     = 20
	maxAttrKeyLen     = 64
	maxAttrValueLen   = 256
	maxDurationMillis = 600_000
)

var (
	servicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	validMethods   = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "HEAD": true, "OPTIONS": true}
)

// Event is one served HTTP request as stored in the pipeline.
type Event struct {
	Tenant     string            `json:"tn"`
	ID         uuid.UUID         `json:"id"`
	Timestamp  time.Time         `json:"ts"`
	ReceivedAt time.Time         `json:"rx"`
	Service    string            `json:"sv"`
	Route      string            `json:"rt"`
	Method     string            `json:"m"`
	Status     uint16            `json:"st"`
	DurationMs float32           `json:"d"`
	Region     string            `json:"rg,omitempty"`
	UserID     string            `json:"u,omitempty"`
	Attributes map[string]string `json:"a,omitempty"`
}

// FromAPI validates an incoming event and converts it to the domain model.
func FromAPI(in api.IngestEvent, tenant string, now time.Time) (Event, error) {
	// The contract limits the raw route; normalization could otherwise hide an oversized input.
	if len(in.Route) > maxRouteLen {
		return Event{}, fmt.Errorf("route: length must be 1-%d", maxRouteLen)
	}
	e := Event{
		Tenant:     tenant,
		ID:         uuid.UUID(in.EventId),
		Timestamp:  in.Timestamp.UTC(),
		ReceivedAt: now.UTC(),
		Service:    in.Service,
		Route:      NormalizeRoute(in.Route),
		Method:     string(in.Method),
		DurationMs: in.DurationMs,
		Region:     deref(in.Region),
		UserID:     deref(in.UserId),
	}
	if in.Attributes != nil {
		e.Attributes = *in.Attributes
	}
	if in.Status < 100 || in.Status > 599 {
		return Event{}, fmt.Errorf("status: must be 100-599, got %d", in.Status)
	}
	e.Status = uint16(in.Status)
	return e, e.Validate(now)
}

// Validate enforces the contract in api/openapi.yaml plus pipeline safety limits.
func (e Event) Validate(now time.Time) error {
	var errs []error
	if e.ID == uuid.Nil {
		errs = append(errs, errors.New("eventId: required"))
	}
	if e.Timestamp.Before(now.Add(-maxPast)) || e.Timestamp.After(now.Add(maxFuture)) {
		errs = append(errs, errors.New("timestamp: must be within the last 24h and at most 5m in the future"))
	}
	if !servicePattern.MatchString(e.Service) {
		errs = append(errs, errors.New("service: must be a lowercase slug"))
	}
	if e.Route == "" || len(e.Route) > maxRouteLen {
		errs = append(errs, fmt.Errorf("route: length must be 1-%d", maxRouteLen))
	}
	if !validMethods[e.Method] {
		errs = append(errs, fmt.Errorf("method: unsupported %q", e.Method))
	}
	if math.IsNaN(float64(e.DurationMs)) || e.DurationMs < 0 || e.DurationMs > maxDurationMillis {
		errs = append(errs, fmt.Errorf("durationMs: must be 0-%d", maxDurationMillis))
	}
	if len(e.Region) > maxRegionLen {
		errs = append(errs, fmt.Errorf("region: max %d chars", maxRegionLen))
	}
	if len(e.UserID) > maxUserIDLen {
		errs = append(errs, fmt.Errorf("userId: max %d chars", maxUserIDLen))
	}
	if len(e.Attributes) > maxAttributes {
		errs = append(errs, fmt.Errorf("attributes: max %d entries", maxAttributes))
	}
	for k, v := range e.Attributes {
		if k == "" || len(k) > maxAttrKeyLen || len(v) > maxAttrValueLen {
			errs = append(errs, fmt.Errorf("attributes: key 1-%d chars, value max %d chars", maxAttrKeyLen, maxAttrValueLen))
			break
		}
	}
	return errors.Join(errs...)
}

// NormalizeRoute replaces high-cardinality path segments (ids) with ":id" and drops the query
// string, so "/users/123?x=1" and "/users/456" aggregate as one route. Unbounded route
// cardinality is the classic way to blow up a metrics store.
func NormalizeRoute(route string) string {
	if i := strings.IndexAny(route, "?#"); i >= 0 {
		route = route[:i]
	}
	if route == "" {
		return route
	}
	segments := strings.Split(route, "/")
	for i, s := range segments {
		if isIdentifier(s) {
			segments[i] = ":id"
		}
	}
	return strings.Join(segments, "/")
}

func isIdentifier(segment string) bool {
	if segment == "" {
		return false
	}
	if _, err := uuid.Parse(segment); err == nil {
		return true
	}
	digits, hex := 0, 0
	for _, r := range segment {
		switch {
		case r >= '0' && r <= '9':
			digits++
			hex++
		case (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F'):
			hex++
		}
	}
	allDigits := digits == len(segment)
	longHex := len(segment) >= 16 && hex == len(segment)
	return allDigits || longHex
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
