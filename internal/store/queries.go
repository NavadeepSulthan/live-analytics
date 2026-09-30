package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrBadCursor is returned for cursors that weren't produced by this API.
var ErrBadCursor = errors.New("invalid cursor")

const (
	// metrics_1s keeps 2 days; longer windows read the per-minute rollup.
	secondRollupMaxWindow = 48 * time.Hour
	targetBuckets         = 300
	liveWindowSeconds     = 10
	queryTimeout          = 5 * time.Second
)

var niceSteps = []int{1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400}

type Latency struct{ P50, P95, P99 float64 }

type Totals struct {
	Requests, Errors uint64
	Latency          Latency
}

type Point struct {
	Bucket time.Time
	Totals
}

type ServiceTotals struct {
	Service string
	Totals
}

type LiveBucket struct {
	Bucket   time.Time
	Totals   Totals
	Services []ServiceTotals
}

type StoredEvent struct {
	ID         uuid.UUID
	Timestamp  time.Time
	ReceivedAt time.Time
	Service    string
	Route      string
	Method     string
	Status     uint16
	DurationMs float32
	Region     string
	UserID     string
}

type EventFilter struct {
	Service     string
	StatusClass string // "2xx".."5xx" or ""
	Cursor      string
	Limit       int
}

// Queries reads pre-aggregated metrics and raw events. Every query is scoped by tenant and
// uses server-side bound parameters ({name:Type}); user input never becomes SQL text.
type Queries struct {
	conn driver.Conn
	now  func() time.Time
}

func NewQueries(conn driver.Conn) *Queries {
	return &Queries{conn: conn, now: time.Now}
}

// Rollup picks the pre-aggregated table and minimum bucket width for a window.
func Rollup(window time.Duration) (table string, minStep int) {
	if window <= secondRollupMaxWindow {
		return "metrics_1s", 1
	}
	return "metrics_1m", 60
}

// ChooseStep returns the requested step, or a "nice" step giving about 300 buckets.
func ChooseStep(window time.Duration, requested int) int {
	_, minStep := Rollup(window)
	step := requested
	if step <= 0 {
		ideal := int(window.Seconds()) / targetBuckets
		step = niceSteps[len(niceSteps)-1]
		for _, s := range niceSteps {
			if s >= ideal {
				step = s
				break
			}
		}
	}
	return max(step, minStep)
}

func (q *Queries) Summary(ctx context.Context, tenant string, window time.Duration, service string) (Totals, error) {
	table, _ := Rollup(window)
	sql := `SELECT sum(requests), sum(errors), quantilesTDigestMerge(0.5, 0.95, 0.99)(latency)
		FROM ` + table + ` WHERE tenant = {tenant:String}
		AND bucket >= now() - toIntervalSecond({window:UInt32})` + serviceClause(service)
	var t Totals
	err := q.queryRow(ctx, sql, params(tenant, window, service), func(row driver.Row) error {
		var quantiles []float32
		if err := row.Scan(&t.Requests, &t.Errors, &quantiles); err != nil {
			return err
		}
		t.Latency = toLatency(quantiles)
		return nil
	})
	return t, err
}

// TimeSeries returns one point per step for the whole window, zero-filled where no data exists.
func (q *Queries) TimeSeries(ctx context.Context, tenant string, window time.Duration, service string, step int) ([]Point, error) {
	table, _ := Rollup(window)
	sql := `SELECT toStartOfInterval(bucket, toIntervalSecond({step:UInt32})) AS t,
		sum(requests), sum(errors), quantilesTDigestMerge(0.5, 0.95, 0.99)(latency)
		FROM ` + table + ` WHERE tenant = {tenant:String}
		AND bucket >= now() - toIntervalSecond({window:UInt32})` + serviceClause(service) + `
		GROUP BY t ORDER BY t`
	p := params(tenant, window, service)
	p["step"] = strconv.Itoa(step)

	byBucket := map[int64]Point{}
	err := q.query(ctx, sql, p, func(rows driver.Rows) error {
		var pt Point
		var quantiles []float32
		if err := rows.Scan(&pt.Bucket, &pt.Requests, &pt.Errors, &quantiles); err != nil {
			return err
		}
		pt.Latency = toLatency(quantiles)
		byBucket[pt.Bucket.Unix()] = pt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fillBuckets(byBucket, q.now(), window, step), nil
}

func (q *Queries) Services(ctx context.Context, tenant string, window time.Duration) ([]ServiceTotals, error) {
	table, _ := Rollup(window)
	sql := `SELECT service, sum(requests) AS r, sum(errors), quantilesTDigestMerge(0.5, 0.95, 0.99)(latency)
		FROM ` + table + ` WHERE tenant = {tenant:String}
		AND bucket >= now() - toIntervalSecond({window:UInt32})
		GROUP BY service ORDER BY r DESC LIMIT 200`
	var out []ServiceTotals
	err := q.query(ctx, sql, params(tenant, window, ""), func(rows driver.Rows) error {
		var s ServiceTotals
		var quantiles []float32
		if err := rows.Scan(&s.Service, &s.Requests, &s.Errors, &quantiles); err != nil {
			return err
		}
		s.Latency = toLatency(quantiles)
		out = append(out, s)
		return nil
	})
	return out, err
}

// Live returns the last 10 one-second buckets, overall and per service, in one scan.
func (q *Queries) Live(ctx context.Context, tenant string) ([]LiveBucket, error) {
	sql := `SELECT bucket, grouping(service) AS overall, service,
		sum(requests), sum(errors), quantilesTDigestMerge(0.5, 0.95, 0.99)(latency)
		FROM metrics_1s WHERE tenant = {tenant:String}
		AND bucket >= toStartOfSecond(now64()) - toIntervalSecond({window:UInt32})
		GROUP BY GROUPING SETS ((bucket), (bucket, service))
		ORDER BY bucket`
	buckets := map[int64]*LiveBucket{}
	var order []int64
	err := q.query(ctx, sql, params(tenant, liveWindowSeconds*time.Second, ""), func(rows driver.Rows) error {
		var (
			bucket    time.Time
			overall   uint64
			service   string
			totals    Totals
			quantiles []float32
		)
		if err := rows.Scan(&bucket, &overall, &service, &totals.Requests, &totals.Errors, &quantiles); err != nil {
			return err
		}
		totals.Latency = toLatency(quantiles)
		key := bucket.Unix()
		b, ok := buckets[key]
		if !ok {
			b = &LiveBucket{Bucket: bucket.UTC()}
			buckets[key] = b
			order = append(order, key)
		}
		// grouping() = 1 means `service` was aggregated away: the all-services row.
		if overall == 1 {
			b.Totals = totals
		} else {
			b.Services = append(b.Services, ServiceTotals{Service: service, Totals: totals})
		}
		return nil
	})
	out := make([]LiveBucket, 0, len(order))
	for _, key := range order {
		out = append(out, *buckets[key])
	}
	return out, err
}

// RecentEvents returns the newest events received after `since`, newest first.
func (q *Queries) RecentEvents(ctx context.Context, tenant string, since time.Time, limit int) ([]StoredEvent, error) {
	sql := `SELECT event_id, ts, received_at, service, route, method, status, duration_ms, region, user_id
		FROM events WHERE tenant = {tenant:String}
		AND ts >= now() - INTERVAL 5 MINUTE AND received_at > {since:DateTime64(3, 'UTC')}
		ORDER BY received_at DESC LIMIT {limit:UInt32}`
	p := clickhouse.Parameters{
		"tenant": tenant,
		"since":  since.UTC().Format("2006-01-02 15:04:05.000"),
		"limit":  strconv.Itoa(limit),
	}
	return q.scanEvents(ctx, sql, p)
}

// ListEvents pages through raw events newest-first using a (ts, event_id) keyset cursor,
// which stays fast at any depth (unlike OFFSET, which rescans skipped rows).
func (q *Queries) ListEvents(ctx context.Context, tenant string, f EventFilter) ([]StoredEvent, string, error) {
	var where []string
	p := clickhouse.Parameters{"tenant": tenant, "limit": strconv.Itoa(f.Limit + 1)}
	where = append(where, "tenant = {tenant:String}", "ts >= now() - INTERVAL 24 HOUR")
	if f.Service != "" {
		where = append(where, "service = {service:String}")
		p["service"] = f.Service
	}
	if f.StatusClass != "" {
		lower, err := strconv.Atoi(f.StatusClass[:1])
		if err != nil || len(f.StatusClass) != 3 || lower < 2 || lower > 5 {
			return nil, "", fmt.Errorf("invalid status class %q", f.StatusClass)
		}
		where = append(where, "intDiv(status, 100) = {statusClass:UInt8}")
		p["statusClass"] = strconv.Itoa(lower)
	}
	if f.Cursor != "" {
		ts, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, "(ts, event_id) < ({cursorTs:DateTime64(3, 'UTC')}, {cursorId:UUID})")
		p["cursorTs"] = ts.UTC().Format("2006-01-02 15:04:05.000")
		p["cursorId"] = id.String()
	}
	sql := `SELECT event_id, ts, received_at, service, route, method, status, duration_ms, region, user_id
		FROM events WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY ts DESC, event_id DESC LIMIT {limit:UInt32}`

	events, err := q.scanEvents(ctx, sql, p)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(events) > f.Limit {
		events = events[:f.Limit]
		last := events[len(events)-1]
		next = encodeCursor(last.Timestamp, last.ID)
	}
	return events, next, nil
}

func (q *Queries) Ping(ctx context.Context) error {
	return q.conn.Ping(ctx)
}

func (q *Queries) scanEvents(ctx context.Context, sql string, p clickhouse.Parameters) ([]StoredEvent, error) {
	var out []StoredEvent
	err := q.query(ctx, sql, p, func(rows driver.Rows) error {
		var e StoredEvent
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.ReceivedAt, &e.Service, &e.Route, &e.Method,
			&e.Status, &e.DurationMs, &e.Region, &e.UserID); err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	return out, err
}

func (q *Queries) query(ctx context.Context, sql string, p clickhouse.Parameters, scan func(driver.Rows) error) error {
	ctx, cancel := context.WithTimeout(clickhouse.Context(ctx, clickhouse.WithParameters(p)), queryTimeout)
	defer cancel()
	rows, err := q.conn.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (q *Queries) queryRow(ctx context.Context, sql string, p clickhouse.Parameters, scan func(driver.Row) error) error {
	ctx, cancel := context.WithTimeout(clickhouse.Context(ctx, clickhouse.WithParameters(p)), queryTimeout)
	defer cancel()
	return scan(q.conn.QueryRow(ctx, sql))
}

func params(tenant string, window time.Duration, service string) clickhouse.Parameters {
	p := clickhouse.Parameters{"tenant": tenant, "window": strconv.Itoa(int(window.Seconds()))}
	if service != "" {
		p["service"] = service
	}
	return p
}

func serviceClause(service string) string {
	if service == "" {
		return ""
	}
	return " AND service = {service:String}"
}

func toLatency(q []float32) Latency {
	if len(q) != 3 {
		return Latency{}
	}
	return Latency{P50: sanitize(q[0]), P95: sanitize(q[1]), P99: sanitize(q[2])}
}

// Empty groups yield NaN quantiles, which JSON can't encode.
func sanitize(v float32) float64 {
	f := float64(v)
	if f != f {
		return 0
	}
	return f
}

func fillBuckets(byBucket map[int64]Point, now time.Time, window time.Duration, step int) []Point {
	stepDur := time.Duration(step) * time.Second
	end := now.UTC().Truncate(stepDur)
	start := now.UTC().Add(-window).Truncate(stepDur)
	out := make([]Point, 0, int(end.Sub(start)/stepDur)+1)
	for t := start; !t.After(end); t = t.Add(stepDur) {
		if p, ok := byBucket[t.Unix()]; ok {
			p.Bucket = t
			out = append(out, p)
		} else {
			out = append(out, Point{Bucket: t})
		}
	}
	return out
}

func encodeCursor(ts time.Time, id uuid.UUID) string {
	raw := strconv.FormatInt(ts.UnixMilli(), 10) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	msPart, idPart, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	ms, err := strconv.ParseInt(msPart, 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	id, err := uuid.Parse(idPart)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	return time.UnixMilli(ms).UTC(), id, nil
}
