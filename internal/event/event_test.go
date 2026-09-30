package event

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"live-analytics/internal/api"
)

var now = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func validInput() api.IngestEvent {
	region := "eu-west-1"
	return api.IngestEvent{
		EventId:    openapi_types.UUID(uuid.New()),
		Timestamp:  now.Add(-time.Second),
		Service:    "checkout",
		Route:      "/orders/42",
		Method:     api.IngestEventMethod("POST"),
		Status:     201,
		DurationMs: 12.5,
		Region:     &region,
	}
}

func TestFromAPIValid(t *testing.T) {
	e, err := FromAPI(validInput(), "acme", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Tenant != "acme" || e.Route != "/orders/:id" || e.Status != 201 || e.Region != "eu-west-1" {
		t.Fatalf("unexpected event: %+v", e)
	}
	if !e.ReceivedAt.Equal(now) {
		t.Fatalf("receivedAt = %v, want %v", e.ReceivedAt, now)
	}
}

func TestFromAPIRejects(t *testing.T) {
	cases := map[string]func(*api.IngestEvent){
		"nil id":           func(e *api.IngestEvent) { e.EventId = openapi_types.UUID(uuid.Nil) },
		"too old":          func(e *api.IngestEvent) { e.Timestamp = now.Add(-25 * time.Hour) },
		"future":           func(e *api.IngestEvent) { e.Timestamp = now.Add(10 * time.Minute) },
		"bad service":      func(e *api.IngestEvent) { e.Service = "Checkout Service" },
		"empty route":      func(e *api.IngestEvent) { e.Route = "" },
		"long route":       func(e *api.IngestEvent) { e.Route = "/" + strings.Repeat("a", 250) },
		"bad method":       func(e *api.IngestEvent) { e.Method = "BREW" },
		"bad status":       func(e *api.IngestEvent) { e.Status = 99 },
		"negative latency": func(e *api.IngestEvent) { e.DurationMs = -1 },
		"too many attributes": func(e *api.IngestEvent) {
			attrs := map[string]string{}
			for i := 0; i < 21; i++ {
				attrs[string(rune('a'+i))] = "v"
			}
			e.Attributes = &attrs
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			in := validInput()
			mutate(&in)
			if _, err := FromAPI(in, "acme", now); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestNormalizeRoute(t *testing.T) {
	cases := map[string]string{
		"/users/123":                                  "/users/:id",
		"/users/123/orders/9?page=2":                  "/users/:id/orders/:id",
		"/files/3f2b8c1d-9a7e-4c21-b5d4-0e6f7a8b9c0d": "/files/:id",
		"/blobs/deadbeefdeadbeefdeadbeef":             "/blobs/:id",
		"/v1/health":                                  "/v1/health",
		"/api/v2/cafe":                                "/api/v2/cafe",
		"/":                                           "/",
	}
	for in, want := range cases {
		if got := NormalizeRoute(in); got != want {
			t.Errorf("NormalizeRoute(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCodecRoundTrip(t *testing.T) {
	e, err := FromAPI(validInput(), "acme", now)
	if err != nil {
		t.Fatal(err)
	}
	data, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != e.ID || !got.Timestamp.Equal(e.Timestamp) || got.Route != e.Route || got.DurationMs != e.DurationMs {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, e)
	}
	if _, err := Decode([]byte(`{"id":"not-a-uuid"}`)); err == nil {
		t.Fatal("expected decode error for malformed record")
	}
	if _, err := Decode([]byte(`{}`)); err == nil {
		t.Fatal("expected decode error for missing tenant")
	}
}
