-- Upbit 표준 포맷 WS ticker 파싱 (upbit · bithumb 2.x 공용).
-- v0.3 5단계: 기존 stg_ws__price_tick 의 파싱 로직을 그대로 옮겨온 모델 (로직 무변경).
-- dedup은 하지 않는다 → union 모델(stg_ws__price_tick)이 전 소스 공통으로 1회 수행.
-- source_code는 payload가 아니라 raw 컬럼에서 온다 (WS payload엔 거래소 구분 정보 없음).

with source as (
    select
        ingested_at,
        source_code,
        raw_payload
    from {{ source('raw', 'raw_ws__ticker') }}
    -- raw는 전 소스 공용 테이블 → 이 포맷을 쓰는 소스만 골라낸다.
    where source_code in ('upbit', 'bithumb')
)

select
    ingested_at,
    source_code,

    -- ① asset_code: v0.1 단일 자산이라 'BTCKRW' 하드코딩 유지 (stg_upbit 방식과 동일).
    --    다자산 확장 시 payload 'code'(KRW-BTC) split+swap 파생으로 교체.
    'BTCKRW'                                                            as asset_code,

    -- ② ts_utc / ts_minute_utc: D2 정책 = 체결시각(trade_timestamp, Unix ms) 우선.
    fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'trade_timestamp')
    )                                                                   as ts_utc,
    toStartOfMinute(fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'trade_timestamp')
    ))                                                                  as ts_minute_utc,

    -- ③ price / volume_24h: upbit=소수·bithumb=정수지만 JSONExtractFloat가 공통 처리.
    JSONExtractFloat(raw_payload, 'trade_price')                       as price,
    'KRW'                                                               as quote_ccy,
    JSONExtractFloat(raw_payload, 'acc_trade_volume_24h')              as volume_24h,

    toString(ingested_at)                                              as raw_payload_id
from source
