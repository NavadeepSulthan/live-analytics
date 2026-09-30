package sink

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"live-analytics/internal/event"
)

func record(t *testing.T, partition int32, offset int64) Record {
	t.Helper()
	value, err := event.Encode(event.Event{
		Tenant: "acme", ID: uuid.New(), Timestamp: time.Now(), ReceivedAt: time.Now(),
		Service: "checkout", Route: "/", Method: "GET", Status: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	return Record{Partition: partition, Offset: offset, Value: value}
}

func TestPrepareSkipsRecordsAtOrBelowWatermark(t *testing.T) {
	records := []Record{record(t, 0, 8), record(t, 0, 9), record(t, 0, 10), record(t, 1, 0), record(t, 1, 1)}
	plan := Prepare(records, map[int32]int64{0: 9})

	if plan.Duplicates != 2 {
		t.Fatalf("duplicates = %d, want 2", plan.Duplicates)
	}
	if len(plan.Rows) != 3 || plan.Rows[0].Offset != 10 || plan.Rows[1].Partition != 1 {
		t.Fatalf("rows = %+v", plan.Rows)
	}
	if plan.Watermarks[0] != 10 || plan.Watermarks[1] != 1 {
		t.Fatalf("watermarks = %v", plan.Watermarks)
	}
}

func TestPrepareDoesNotMutateInputWatermarks(t *testing.T) {
	in := map[int32]int64{0: 1}
	Prepare([]Record{record(t, 0, 5)}, in)
	if in[0] != 1 {
		t.Fatal("Prepare must not mutate the committed watermarks before the write succeeds")
	}
}

func TestPrepareDeadLettersUndecodableRecordsButAdvancesWatermark(t *testing.T) {
	plan := Prepare([]Record{{Partition: 0, Offset: 3, Value: []byte("not json")}, record(t, 0, 4)}, nil)
	if len(plan.Rejected) != 1 || plan.Rejected[0].Record.Offset != 3 {
		t.Fatalf("rejected = %+v", plan.Rejected)
	}
	if len(plan.Rows) != 1 || plan.Watermarks[0] != 4 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestReplayingTheSameBatchWritesNothing(t *testing.T) {
	records := []Record{record(t, 0, 0), record(t, 0, 1), record(t, 2, 7)}
	first := Prepare(records, nil)
	second := Prepare(records, first.Watermarks)
	if len(first.Rows) != 3 || len(second.Rows) != 0 || second.Duplicates != 3 {
		t.Fatalf("first=%d rows, second=%d rows / %d duplicates", len(first.Rows), len(second.Rows), second.Duplicates)
	}
}
