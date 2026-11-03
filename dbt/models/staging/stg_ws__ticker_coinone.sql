-- Coinone WS TICKER 파싱 → 공통 9컬럼.
-- 메시지: {"response_type":"DATA","channel":"TICKER","data":{...}} — 숫자 필드는 전부 STRING.
-- dedup은 union 모델(stg_ws__price_tick)이 담당.

with source as (
    select
        ingested_at,
        source_code,
        raw_payload
    from {{ source('raw', 'raw_ws__ticker') }}
    where source_code = 'coinone'
      -- raw에는 CONNECTED·SUBSCRIBED·PONG 같은 비데이터 메시지도 원문 그대로 들어 있다.
      -- 여기서 걸러내지 않으면 price=0 / ts=1970 유령 tick이 된다.
      and JSONExtractString(raw_payload, 'response_type') = 'DATA'
      and JSONExtractString(raw_payload, 'channel') = 'TICKER'
      and JSONExtractString(raw_payload, 'data', 'quote_currency') = 'KRW'
      and JSONExtractString(raw_payload, 'data', 'target_currency') = 'BTC'
)

select
    ingested_at,
    source_code,

    'BTCKRW'                                                            as asset_code,

    -- ts_utc: ticker 이벤트 시각(data.timestamp, Unix ms). ticker 메시지엔 별도 체결시각 필드가 없다.
    --   샘플 검증(2026-11-03, 366건): 91.5%는 체결 시각과 동일(나머지 상당수도 +1ms),
    --   그 외는 분 경계(:00)·호가 변화로 재발행된 ticker → 가격(last)은 항상 직전 체결가 유지.
    --   ⇒ D2(체결시각 우선)와의 차이: 체결 없는 분에도 직전가 carry-forward 행이 생길 수 있음.
    fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'data', 'timestamp')
    )                                                                   as ts_utc,
    toStartOfMinute(fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'data', 'timestamp')
    ))                                                                  as ts_minute_utc,

    -- price: data.last (STRING, 최근 체결가). JSONExtractFloat가 숫자형 문자열도 파싱(CH 24.6 확인).
    JSONExtractFloat(raw_payload, 'data', 'last')                      as price,
    'KRW'                                                               as quote_ccy,
    -- volume_24h: data.target_volume = 24h 거래량 BTC 수량.
    --   ⚠️ quote_volume 은 KRW 금액이라 쓰면 안 됨 (단위 불일치).
    JSONExtractFloat(raw_payload, 'data', 'target_volume')             as volume_24h,

    toString(ingested_at)                                              as raw_payload_id
from source
