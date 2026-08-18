-- v0.2 WebSocket 수집용 raw 테이블.
-- REST(raw_upbit__ticker / raw_bithumb__ticker)와 분리 (논점①): 스트림 틱 vs 스냅샷 성격이 다름.
-- Upbit·Bithumb 2.x WS는 응답 포맷이 동일(Upbit 표준)이라 소스별로 나누지 않고
-- 단일 테이블 + source_code 컬럼으로 구분한다 (REST 때 스키마가 달라 나눴던 것과 대비).
--
-- ⚠️ 이 파일은 볼륨 최초 생성 시에만 auto-run 됨. 기존 볼륨엔 수동 적용:
--   docker exec -i price-integrity-clickhouse clickhouse-client --multiquery < scripts/clickhouse-init/02_create_ws_raw_tables.sql

CREATE TABLE IF NOT EXISTS price_integrity.raw_ws__ticker
(
    ingested_at  DateTime64(3, 'UTC'),                 -- 적재 시각 (D2 fallback·clock skew 분석용, 항상 보존)
    source_code  LowCardinality(String),               -- 'upbit' | 'bithumb'
    raw_payload  String                                -- WS ticker 메시지 JSON 원본 그대로
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_code, ingested_at);
