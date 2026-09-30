package sink

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	rows         prometheus.Counter
	duplicates   prometheus.Counter
	deadLettered prometheus.Counter
	writeSeconds prometheus.Histogram
	batchRows    prometheus.Histogram
	freshness    prometheus.Histogram
	lag          *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		rows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sink_rows_written_total", Help: "Events written to ClickHouse.",
		}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sink_duplicates_skipped_total", Help: "Redelivered records skipped by the offset watermark.",
		}),
		deadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sink_dead_lettered_total", Help: "Undecodable records sent to the DLQ topic.",
		}),
		writeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "sink_write_duration_seconds", Help: "ClickHouse batch insert latency.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
		}),
		batchRows: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "sink_batch_rows", Help: "Rows per insert.",
			Buckets: prometheus.ExponentialBuckets(10, 3, 10),
		}),
		freshness: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "sink_ingest_to_queryable_seconds", Help: "Time from ingest acceptance to being queryable.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sink_consumer_lag_records", Help: "Records behind the partition high watermark.",
		}, []string{"partition"}),
	}
	reg.MustRegister(m.rows, m.duplicates, m.deadLettered, m.writeSeconds, m.batchRows, m.freshness, m.lag)
	return m
}
