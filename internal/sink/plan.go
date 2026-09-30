// Package sink moves events from the stream into ClickHouse in micro-batches with
// effectively-once semantics.
//
// Kafka consumption is at-least-once: a crash after a successful insert but before the offset
// commit redelivers records. Every row stores its (partition, offset), and ClickHouse keeps the
// max offset per partition in a projection written atomically with the insert. On every
// (re)assignment the sink reloads those watermarks and skips records at or below them, so
// redelivered records are never written twice.
package sink

import (
	"live-analytics/internal/event"
	"live-analytics/internal/store"
)

// Record is the minimal view of a stream record the batching logic needs.
type Record struct {
	Partition int32
	Offset    int64
	Value     []byte
}

// Rejected is a record that can't be decoded; it goes to the dead-letter topic.
type Rejected struct {
	Record Record
	Err    error
}

// Plan is what to do with one polled batch.
type Plan struct {
	Rows       []store.Row
	Rejected   []Rejected
	Duplicates int
	// Next watermark per partition after this batch is durably written.
	Watermarks map[int32]int64
}

// Prepare decodes a batch, drops records already stored (offset <= watermark) and splits
// out undecodable ones. It is pure, so it's tested without Kafka or ClickHouse.
func Prepare(records []Record, watermarks map[int32]int64) Plan {
	plan := Plan{Watermarks: map[int32]int64{}}
	for p, w := range watermarks {
		plan.Watermarks[p] = w
	}
	for _, r := range records {
		if w, ok := plan.Watermarks[r.Partition]; ok && r.Offset <= w {
			plan.Duplicates++
			continue
		}
		plan.Watermarks[r.Partition] = r.Offset
		e, err := event.Decode(r.Value)
		if err != nil {
			plan.Rejected = append(plan.Rejected, Rejected{Record: r, Err: err})
			continue
		}
		plan.Rows = append(plan.Rows, store.Row{Event: e, Partition: r.Partition, Offset: r.Offset})
	}
	return plan
}
