package ingest

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	requests       *prometheus.CounterVec
	events         *prometheus.CounterVec
	produceSeconds prometheus.Histogram
	batchSize      prometheus.Histogram
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_requests_total", Help: "Ingest requests by response code.",
		}, []string{"code"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_events_total", Help: "Events by tenant and outcome.",
		}, []string{"tenant", "result"}),
		produceSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ingest_produce_duration_seconds", Help: "Time for the stream to acknowledge a batch.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ingest_batch_events", Help: "Events per ingest request.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8),
		}),
	}
	reg.MustRegister(m.requests, m.events, m.produceSeconds, m.batchSize)
	return m
}
