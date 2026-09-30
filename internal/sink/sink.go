package sink

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"live-analytics/internal/event"
	"live-analytics/internal/store"
	"live-analytics/internal/stream"
)

// Store is the sink's view of ClickHouse.
type Store interface {
	Write(ctx context.Context, rows []store.Row) error
	Watermarks(ctx context.Context) (map[int32]int64, error)
}

type Config struct {
	Brokers       []string
	Topic         string
	Group         string
	MaxBatch      int
	FlushInterval time.Duration
}

type Sink struct {
	cfg        Config
	client     *kgo.Client
	store      Store
	log        *slog.Logger
	metrics    *metrics
	watermarks map[int32]int64
	// Set by the rebalance callback; the poll loop reloads watermarks before the next batch.
	reload atomic.Bool
	// Swappable so tests can simulate a crash between write and commit.
	commit func(context.Context, ...*kgo.Record) error
}

func New(cfg Config, st Store, reg prometheus.Registerer, log *slog.Logger) (*Sink, error) {
	s := &Sink{cfg: cfg, store: st, log: log, metrics: newMetrics(reg)}
	s.reload.Store(true)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		// No rebalance can happen between polling a batch and committing it, so a partition is
		// never written by two consumers at once.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { s.reload.Store(true) }),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.FetchMaxBytes(64<<20),
		kgo.DefaultProduceTopic(event.DLQTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		stream.TracingHooks(),
	)
	if err != nil {
		return nil, err
	}
	s.client = client
	s.commit = client.CommitRecords
	return s, nil
}

// Run consumes until ctx is cancelled. Each iteration: poll up to MaxBatch records or until
// FlushInterval elapses, write them in one insert, commit, then allow rebalancing.
func (s *Sink) Run(ctx context.Context) error {
	defer s.client.Close()
	for ctx.Err() == nil {
		records := s.poll(ctx)
		if len(records) > 0 {
			if err := s.process(ctx, records); err != nil {
				if ctx.Err() != nil {
					break
				}
				return err
			}
		}
		s.client.AllowRebalance()
	}
	return nil
}

func (s *Sink) Ping(ctx context.Context) error {
	return s.client.Ping(ctx)
}

func (s *Sink) poll(ctx context.Context) []*kgo.Record {
	deadline := time.Now().Add(s.cfg.FlushInterval)
	var records []*kgo.Record
	for len(records) < s.cfg.MaxBatch {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		pollCtx, cancel := context.WithTimeout(ctx, remaining)
		fetches := s.client.PollRecords(pollCtx, s.cfg.MaxBatch-len(records))
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
				s.log.Warn("fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
			}
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if n := len(p.Records); n > 0 {
				lag := p.HighWatermark - p.Records[n-1].Offset - 1
				s.metrics.lag.WithLabelValues(strconv.Itoa(int(p.Partition))).Set(float64(lag))
			}
			records = append(records, p.Records...)
		})
	}
	return records
}

func (s *Sink) process(ctx context.Context, records []*kgo.Record) error {
	if s.reload.Load() {
		watermarks, err := retry(ctx, s.log, "load watermarks", func() (map[int32]int64, error) {
			return s.store.Watermarks(ctx)
		})
		if err != nil {
			return err
		}
		s.watermarks = watermarks
		s.reload.Store(false)
	}

	batch := make([]Record, len(records))
	for i, r := range records {
		batch[i] = Record{Partition: r.Partition, Offset: r.Offset, Value: r.Value}
	}
	plan := Prepare(batch, s.watermarks)

	if len(plan.Rejected) > 0 {
		if err := s.deadLetter(ctx, plan.Rejected); err != nil {
			return err
		}
	}
	if len(plan.Rows) > 0 {
		started := time.Now()
		if _, err := retry(ctx, s.log, "write batch", func() (struct{}, error) {
			return struct{}{}, s.store.Write(ctx, plan.Rows)
		}); err != nil {
			return err
		}
		s.metrics.writeSeconds.Observe(time.Since(started).Seconds())
		s.metrics.batchRows.Observe(float64(len(plan.Rows)))
		now := time.Now()
		for _, r := range plan.Rows {
			s.metrics.freshness.Observe(now.Sub(r.Event.ReceivedAt).Seconds())
		}
	}
	s.watermarks = plan.Watermarks
	s.metrics.rows.Add(float64(len(plan.Rows)))
	s.metrics.duplicates.Add(float64(plan.Duplicates))
	s.metrics.deadLettered.Add(float64(len(plan.Rejected)))

	// Committing is an optimization (less replay after restart); correctness rests on the
	// watermarks, so a failed commit is logged, not fatal.
	if err := s.commit(ctx, records...); err != nil && ctx.Err() == nil {
		s.log.Warn("offset commit failed; records will be replayed and skipped", "err", err)
	}
	return nil
}

func (s *Sink) deadLetter(ctx context.Context, rejected []Rejected) error {
	out := make([]*kgo.Record, len(rejected))
	for i, r := range rejected {
		s.log.Warn("dead-lettering record", "partition", r.Record.Partition, "offset", r.Record.Offset, "err", r.Err)
		out[i] = &kgo.Record{
			Value: r.Record.Value,
			Headers: []kgo.RecordHeader{
				{Key: "error", Value: []byte(r.Err.Error())},
				{Key: "source", Value: []byte(s.cfg.Topic + "/" + strconv.Itoa(int(r.Record.Partition)) + "/" + strconv.FormatInt(r.Record.Offset, 10))},
			},
		}
	}
	_, err := retry(ctx, s.log, "dead-letter", func() (struct{}, error) {
		return struct{}{}, s.client.ProduceSync(ctx, out...).FirstErr()
	})
	return err
}

// retry retries fn with exponential backoff (200 ms to 10 s) until it succeeds or ctx ends.
// While retrying the sink stops polling, so lag grows visibly instead of data being dropped.
func retry[T any](ctx context.Context, log *slog.Logger, op string, fn func() (T, error)) (T, error) {
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		log.Error("operation failed; retrying", "op", op, "attempt", attempt, "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}
