-- price_integrity DB는 docker-compose CLICKHOUSE_DB 환경변수로 자동 생성됨.

CREATE TABLE IF NOT EXISTS price_integrity.raw_upbit__ticker
(
    ingested_at  DateTime64(3, 'UTC'),
    raw_payload  String                    -- JSON array[0] 그대로 보관
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY ingested_at;

CREATE TABLE IF NOT EXISTS price_integrity.raw_bithumb__ticker
(
    ingested_at  DateTime64(3, 'UTC'),
    raw_payload  String                    -- JSON data 객체 그대로 보관
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY ingested_at;
