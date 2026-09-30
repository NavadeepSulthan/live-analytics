package store

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"live-analytics/internal/event"
)

// Row is an event plus the stream position it came from.
type Row struct {
	Event     event.Event
	Partition int32
	Offset    int64
}

// EventWriter inserts sink batches into the events table (which feeds the materialized views).
type EventWriter struct {
	conn driver.Conn
}

func NewEventWriter(conn driver.Conn) *EventWriter {
	return &EventWriter{conn: conn}
}

const insertEvents = `INSERT INTO events (tenant, event_id, ts, received_at, service, route, method,
	status, duration_ms, region, user_id, attributes, kafka_partition, kafka_offset)`

// Write inserts all rows as one block. ClickHouse inserts a block atomically, so the rows,
// the materialized-view aggregates and the offset watermark projection commit together.
func (w *EventWriter) Write(ctx context.Context, rows []Row) error {
	batch, err := w.conn.PrepareBatch(ctx, insertEvents)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer batch.Close()
	for _, r := range rows {
		e := r.Event
		attrs := e.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}
		if err := batch.Append(e.Tenant, e.ID, e.Timestamp, e.ReceivedAt, e.Service, e.Route, e.Method,
			e.Status, e.DurationMs, e.Region, e.UserID, attrs, uint16(r.Partition), uint64(r.Offset)); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// Watermarks returns the highest stored offset per stream partition (served by the
// kafka_watermarks projection, so it's cheap even with billions of rows).
func (w *EventWriter) Watermarks(ctx context.Context) (map[int32]int64, error) {
	rows, err := w.conn.Query(ctx, "SELECT kafka_partition, max(kafka_offset) FROM events GROUP BY kafka_partition")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int32]int64{}
	for rows.Next() {
		var partition uint16
		var offset uint64
		if err := rows.Scan(&partition, &offset); err != nil {
			return nil, err
		}
		out[int32(partition)] = int64(offset)
	}
	return out, rows.Err()
}

func (w *EventWriter) Ping(ctx context.Context) error {
	return w.conn.Ping(ctx)
}
