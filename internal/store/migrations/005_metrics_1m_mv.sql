CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_1m_mv TO metrics_1m AS
SELECT
    tenant,
    service,
    toStartOfMinute(toDateTime(ts, 'UTC'))              AS bucket,
    count()                                             AS requests,
    countIf(status >= 500)                              AS errors,
    sum(toFloat64(duration_ms))                         AS duration_sum,
    quantilesTDigestState(0.5, 0.95, 0.99)(duration_ms) AS latency
FROM events
GROUP BY tenant, service, bucket
