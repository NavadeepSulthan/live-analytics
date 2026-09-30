CREATE TABLE IF NOT EXISTS events
(
    tenant          LowCardinality(String),
    event_id        UUID,
    ts              DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    service         LowCardinality(String),
    route           LowCardinality(String),
    method          LowCardinality(String),
    status          UInt16,
    duration_ms     Float32,
    region          LowCardinality(String),
    user_id         String,
    attributes      Map(LowCardinality(String), String),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    -- Written atomically with every part: the sink reads it to skip records it already stored.
    PROJECTION kafka_watermarks
    (
        SELECT kafka_partition, max(kafka_offset) GROUP BY kafka_partition
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (tenant, service, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY
