-- +goose NO TRANSACTION
-- ClickHouse не поддерживает транзакционный DDL — goose не оборачивает в транзакцию.

-- +goose Up
CREATE TABLE IF NOT EXISTS logs
(
    timestamp DateTime64(3),
    host      LowCardinality(String),
    service   LowCardinality(String),
    level     Enum8('UNSPECIFIED' = 0, 'DEBUG' = 1, 'INFO' = 2, 'WARN' = 3, 'ERROR' = 4),
    message   String,
    trace_id  String,
    fields    Map(String, String),

    -- «дешёвый полнотекст»: bloom-фильтр по токенам message, пропускает блоки без искомого слова
    INDEX idx_msg message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (service, timestamp)
PARTITION BY toYYYYMMDD(timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- +goose Down
DROP TABLE IF EXISTS logs;
