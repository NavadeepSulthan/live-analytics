package event

import (
	"encoding/json"
	"fmt"
)

// Topic names and schema version. Changing the encoding means a new topic (events.v2).
const (
	Topic    = "events.v1"
	DLQTopic = "events.dlq"
)

// Encode serializes an event for the stream. Compact JSON keys + zstd batch compression on
// the producer keep records small without a schema registry.
func Encode(e Event) ([]byte, error) {
	return json.Marshal(e)
}

// Decode parses and re-validates a stream record; records failing either go to the DLQ.
func Decode(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, fmt.Errorf("decode: %w", err)
	}
	if e.Tenant == "" {
		return Event{}, fmt.Errorf("decode: missing tenant")
	}
	return e, nil
}

// PartitionKey keeps one service's events ordered on one partition while spreading load.
func PartitionKey(e Event) []byte {
	return []byte(e.Tenant + "/" + e.Service)
}
