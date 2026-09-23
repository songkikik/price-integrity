-- v0.2 WebSocket raw(raw_ws__ticker) → 공통 9컬럼으로 정규화.
-- REST용 stg_upbit__/stg_bithumb__price_tick 을 대체 (int가 이 모델을 참조하도록 5단계에서 전환).
-- 두 소스가 Upbit 표준 포맷으로 동일 → 소스 분기 없이 한 모델로 파싱.
-- source_code는 payload가 아니라 raw 컬럼에서 온다 (WS payload엔 거래소 구분 정보 없음).

with source as (
    select
        ingested_at,
        source_code,
        raw_payload
    from {{ source('raw', 'raw_ws__ticker') }}
),

parsed as (
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
),

deduped as (
    select *
    from parsed
    qualify row_number() over (
        partition by asset_code, source_code, ts_utc
        order by ingested_at desc
    ) = 1
)

select * from deduped
