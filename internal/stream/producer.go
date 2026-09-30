// Package stream is the Kafka-protocol adapter (Redpanda locally, Kafka-compatible in prod).
package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"

	"live-analytics/internal/event"
)

// tracingHooks propagate W3C trace context through record headers, so a trace spans
// HTTP ingest -> stream -> sink.
func TracingHooks() kgo.Opt {
	tracer := kotel.NewTracer(
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
		kotel.LinkSpans(),
	)
	return kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()...)
}

// Producer writes validated events to the stream.
type Producer struct {
	client *kgo.Client
}

// NewProducer builds an idempotent producer (franz-go default) that waits for all in-sync
// replicas. Records from concurrent HTTP requests are batched per partition (5 ms linger)
// and zstd-compressed.
func NewProducer(brokers []string, topic string, maxBufferedRecords int) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxBufferedRecords(maxBufferedRecords),
		kgo.ProduceRequestTimeout(10*time.Second),
		TracingHooks(),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client}, nil
}

// Produce durably writes all events or returns an error. Callers must only acknowledge
// clients after this returns nil.
func (p *Producer) Produce(ctx context.Context, events []event.Event) error {
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		value, err := event.Encode(e)
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{Key: event.PartitionKey(e), Value: value})
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
}

// Close flushes buffered records (bounded by ctx) and closes the client.
func (p *Producer) Close(ctx context.Context) {
	_ = p.client.Flush(ctx)
	p.client.Close()
}

// EnsureTopics creates the given topics if they don't exist.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, replication int16, topics ...string) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	retention := fmt.Sprint((7 * 24 * time.Hour).Milliseconds())
	responses, err := admin.CreateTopics(ctx, partitions, replication,
		map[string]*string{"retention.ms": &retention, "compression.type": ptr("producer")},
		topics...)
	if err != nil {
		return err
	}
	for _, r := range responses.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

func ptr[T any](v T) *T { return &v }
