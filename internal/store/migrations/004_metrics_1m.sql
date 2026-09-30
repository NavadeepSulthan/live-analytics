CREATE TABLE IF NOT EXISTS metrics_1m
(
    tenant       LowCardinality(String),
    service      LowCardinality(String),
    bucket       DateTime('UTC'),
    requests     SimpleAggregateFunction(sum, UInt64),
    errors       SimpleAggregateFunction(sum, UInt64),
    duration_sum SimpleAggregateFunction(sum, Float64),
    latency      AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), Float32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (tenant, service, bucket)
TTL bucket + INTERVAL 90 DAY
