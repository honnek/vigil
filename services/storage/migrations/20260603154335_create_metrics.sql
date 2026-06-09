-- +goose Up
SELECT 'up SQL query';
CREATE TABLE metrics (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    host TEXT NOT NULL,
    name TEXT NOT NULL,
    type SMALLINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    labels JSONB,
    ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

CREATE INDEX idx_metrics_query ON metrics (host, name, ts DESC);

CREATE TABLE metrics_2026_06 PARTITION OF metrics
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');



-- +goose Down
SELECT 'down SQL query';

DROP TABLE metrics;