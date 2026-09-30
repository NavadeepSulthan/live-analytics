// Command loadgen drives the ingest API at a target rate and optionally verifies that every
// accepted event became queryable exactly once.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"live-analytics/internal/api"
	"live-analytics/internal/loadgen"
	"live-analytics/internal/platform"
)

type options struct {
	ingestURL, queryURL, ingestKey, readKey string
	rate, batch, workers, services          int
	duration                                time.Duration
	gzip, verify                            bool
}

type stats struct {
	accepted, rejected, retries, missedTicks atomic.Int64
	mu                                       sync.Mutex
	latencies                                []time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.ingestURL, "url", "http://localhost:8080", "ingest base URL")
	flag.StringVar(&o.queryURL, "query-url", "http://localhost:8081", "query base URL (for -verify)")
	flag.StringVar(&o.ingestKey, "key", os.Getenv("LOADGEN_INGEST_KEY"), "ingest API key (or LOADGEN_INGEST_KEY)")
	flag.StringVar(&o.readKey, "read-key", os.Getenv("LOADGEN_READ_KEY"), "read API key for -verify (or LOADGEN_READ_KEY)")
	flag.IntVar(&o.rate, "rate", 20_000, "target events per second")
	flag.IntVar(&o.batch, "batch", 500, "events per request")
	flag.IntVar(&o.workers, "workers", 32, "concurrent HTTP senders")
	flag.IntVar(&o.services, "services", 8, "number of simulated services (1-12)")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "how long to send; 0 = until interrupted")
	flag.BoolVar(&o.gzip, "gzip", true, "gzip request bodies")
	flag.BoolVar(&o.verify, "verify", false, "after sending, check the query API stored every accepted event exactly once")
	flag.Parse()
	if o.ingestKey == "" || (o.verify && o.readKey == "") {
		fmt.Fprintln(os.Stderr, "loadgen: -key is required (and -read-key with -verify)")
		os.Exit(2)
	}
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(o options) error {
	ctx, stop := platform.SignalContext()
	defer stop()
	if o.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.duration)
		defer cancel()
	}
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: o.workers, MaxConnsPerHost: o.workers},
	}

	var before int64
	if o.verify {
		n, err := storedTotal(client, o)
		if err != nil {
			return fmt.Errorf("read baseline: %w", err)
		}
		before = n
	}

	st := &stats{}
	tickets := make(chan struct{}, o.workers)
	var wg sync.WaitGroup
	for w := 0; w < o.workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			gen := loadgen.NewGenerator(o.services, seed)
			for range tickets {
				send(context.WithoutCancel(ctx), client, o, gen, st)
			}
		}(uint64(w + 1))
	}

	started := time.Now()
	go report(ctx, st, started)
	// Open-loop pacing: tickets are issued on schedule whether or not responses are back, so a
	// slow server shows up as missed ticks instead of silently lowering the offered load.
	interval := time.Duration(float64(time.Second) * float64(o.batch) / float64(o.rate))
	ticker := time.NewTicker(interval)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			select {
			case tickets <- struct{}{}:
			default:
				st.missedTicks.Add(1)
			}
		}
	}
	ticker.Stop()
	close(tickets)
	wg.Wait()
	elapsed := time.Since(started)

	st.mu.Lock()
	slices.Sort(st.latencies)
	p50, p99 := percentile(st.latencies, 0.5), percentile(st.latencies, 0.99)
	st.mu.Unlock()
	accepted := st.accepted.Load()
	fmt.Printf("\nsent for %s: accepted %d events (%.0f/s), rejected %d, retries %d, missed ticks %d\n",
		elapsed.Round(time.Millisecond), accepted, float64(accepted)/elapsed.Seconds(), st.rejected.Load(), st.retries.Load(), st.missedTicks.Load())
	fmt.Printf("request latency p50 %s, p99 %s\n", p50.Round(100*time.Microsecond), p99.Round(100*time.Microsecond))

	if !o.verify {
		return nil
	}
	return verify(client, o, before, accepted)
}

// send posts one batch, retrying 429/503/network errors with the same event ids.
func send(ctx context.Context, client *http.Client, o options, gen *loadgen.Generator, st *stats) {
	body, err := encode(api.IngestBatch{Events: gen.Batch(o.batch, time.Now())}, o.gzip)
	if err != nil {
		return
	}
	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, o.ingestURL+"/v1/events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", o.ingestKey)
		if o.gzip {
			req.Header.Set("Content-Encoding", "gzip")
		}
		started := time.Now()
		res, err := client.Do(req)
		if err == nil {
			latency := time.Since(started)
			var result api.IngestResult
			_ = json.NewDecoder(res.Body).Decode(&result)
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			switch {
			case res.StatusCode == http.StatusAccepted:
				st.accepted.Add(int64(result.Accepted))
				st.rejected.Add(int64(result.Rejected))
				st.mu.Lock()
				st.latencies = append(st.latencies, latency)
				st.mu.Unlock()
				return
			case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
				if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
					backoff = time.Duration(s) * time.Second
				}
			default:
				st.rejected.Add(int64(o.batch))
				return
			}
		}
		st.retries.Add(1)
		time.Sleep(backoff)
		backoff = min(backoff*2, 5*time.Second)
	}
	st.rejected.Add(int64(o.batch))
}

func encode(batch api.IngestBatch, compress bool) ([]byte, error) {
	var buf bytes.Buffer
	var w io.Writer = &buf
	var gz *gzip.Writer
	if compress {
		gz = gzip.NewWriter(&buf)
		w = gz
	}
	if err := json.NewEncoder(w).Encode(batch); err != nil {
		return nil, err
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func report(ctx context.Context, st *stats, started time.Time) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := st.accepted.Load()
			fmt.Printf("\r%6.0fs  %8d events/s  total %10d  retries %d  missed %d   ",
				time.Since(started).Seconds(), now-last, now, st.retries.Load(), st.missedTicks.Load())
			last = now
		}
	}
}

// storedTotal reads the tenant's request count over the metrics_1s retention window.
func storedTotal(client *http.Client, o options) (int64, error) {
	req, _ := http.NewRequest(http.MethodGet, o.queryURL+"/v1/metrics/summary?windowSeconds=172800", nil)
	req.Header.Set("Authorization", "Bearer "+o.readKey)
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("summary: HTTP %d", res.StatusCode)
	}
	var s api.Summary
	if err := json.NewDecoder(res.Body).Decode(&s); err != nil {
		return 0, err
	}
	return s.Requests, nil
}

// verify waits for the pipeline to drain, then compares stored vs accepted. Assumes this
// run is the only traffic for the tenant.
func verify(client *http.Client, o options, before, accepted int64) error {
	fmt.Println("verifying: waiting for the pipeline to drain…")
	deadline := time.Now().Add(2 * time.Minute)
	var stored, stableFor int64
	for time.Now().Before(deadline) {
		total, err := storedTotal(client, o)
		if err != nil {
			return err
		}
		if total-before == stored {
			stableFor++
		} else {
			stableFor = 0
		}
		stored = total - before
		if stored >= accepted && stableFor >= 3 {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("accepted %d, stored %d (difference %+d)\n", accepted, stored, stored-accepted)
	if stored != accepted {
		return fmt.Errorf("verification failed: stored %d != accepted %d", stored, accepted)
	}
	fmt.Println("verification passed: every accepted event was stored exactly once")
	return nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(len(sorted)-1, int(p*float64(len(sorted))))]
}
