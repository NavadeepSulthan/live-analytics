package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRollupAndStep(t *testing.T) {
	cases := []struct {
		window    time.Duration
		requested int
		table     string
		step      int
	}{
		{5 * time.Minute, 0, "metrics_1s", 1},
		{time.Hour, 0, "metrics_1s", 15},
		{24 * time.Hour, 0, "metrics_1s", 300},
		{7 * 24 * time.Hour, 0, "metrics_1m", 3600},
		{7 * 24 * time.Hour, 5, "metrics_1m", 60},
		{time.Hour, 7, "metrics_1s", 7},
	}
	for _, tc := range cases {
		table, _ := Rollup(tc.window)
		if step := ChooseStep(tc.window, tc.requested); table != tc.table || step != tc.step {
			t.Errorf("window %v requested %d: got (%s, %d), want (%s, %d)", tc.window, tc.requested, table, step, tc.table, tc.step)
		}
	}
}

func TestFillBucketsZeroFillsGaps(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 30, 0, time.UTC)
	have := time.Date(2026, 9, 30, 12, 0, 10, 0, time.UTC)
	points := fillBuckets(map[int64]Point{have.Unix(): {Totals: Totals{Requests: 7}}}, now, 30*time.Second, 10)
	if len(points) != 4 {
		t.Fatalf("got %d points, want 4", len(points))
	}
	for i, p := range points {
		want := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC).Add(time.Duration(i*10) * time.Second)
		if !p.Bucket.Equal(want) {
			t.Fatalf("point %d at %v, want %v", i, p.Bucket, want)
		}
	}
	if points[1].Requests != 7 || points[0].Requests != 0 {
		t.Fatalf("unexpected fill: %+v", points)
	}
}

func TestCursorRoundTripAndRejection(t *testing.T) {
	ts := time.Date(2026, 9, 30, 12, 0, 0, 123_000_000, time.UTC)
	id := uuid.New()
	gotTs, gotID, err := decodeCursor(encodeCursor(ts, id))
	if err != nil || !gotTs.Equal(ts) || gotID != id {
		t.Fatalf("round trip failed: %v %v %v", gotTs, gotID, err)
	}
	for _, bad := range []string{"%%%", "bm90LWEtY3Vyc29y", encodeCursor(ts, id)[:5]} {
		if _, _, err := decodeCursor(bad); err == nil {
			t.Errorf("decodeCursor(%q) should fail", bad)
		}
	}
}
