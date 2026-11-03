-- Korbit(현 디지털엑스) WS ticker 파싱 → 공통 9컬럼.
-- 메시지: {"symbol":"btc_krw","timestamp":...,"type":"ticker","snapshot":bool?,"data":{...}} — 가격은 STRING.
-- dedup은 union 모델(stg_ws__price_tick)이 담당.

with source as (
    select
        ingested_at,
        source_code,
        raw_payload
    from {{ source('raw', 'raw_ws__ticker') }}
    where source_code = 'korbit'
      and JSONExtractString(raw_payload, 'type') = 'ticker'
      and JSONExtractString(raw_payload, 'symbol') = 'btc_krw'
)

select
    ingested_at,
    source_code,

    'BTCKRW'                                                            as asset_code,

    -- ts_utc: D2 정책 = 체결시각 → data.lastTradedAt (Unix ms).
    --   최상위 timestamp는 메시지 발행 시각(체결 없어도 ~2.5초마다 재발행)이라 쓰지 않는다.
    --   같은 lastTradedAt 반복 메시지는 union 모델의 grain dedup에서 1건으로 접힘.
    --   샘플 검증(2026-11-03): lastTradedAt == trade 채널 체결시각 79/79.
    fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'data', 'lastTradedAt')
    )                                                                   as ts_utc,
    toStartOfMinute(fromUnixTimestamp64Milli(
        JSONExtractUInt(raw_payload, 'data', 'lastTradedAt')
    ))                                                                  as ts_minute_utc,

    -- price: data.close (STRING). 문서상 "최근 24시간 종가" = 롤링 윈도 끝 = 최근 체결가.
    --   샘플 검증: close == 직전 trade 체결가 78/79.
    JSONExtractFloat(raw_payload, 'data', 'close')                     as price,
    'KRW'                                                               as quote_ccy,
    -- volume_24h: data.volume = base(BTC) 수량. quoteVolume(KRW 금액) 아님.
    JSONExtractFloat(raw_payload, 'data', 'volume')                    as volume_24h,

    toString(ingested_at)                                              as raw_payload_id
from source
