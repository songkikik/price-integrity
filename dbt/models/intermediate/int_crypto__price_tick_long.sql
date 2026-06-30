-- crypto 자산의 소스별 분 단위 시세를 long 포맷으로 통합.
-- grain: asset_code × source_code × ts_minute_utc (= 자산 × 소스 × 분, 1행)
-- D1 정책: 1분 내 다중 tick은 last tick(close) = ts_utc 최대값 1건만 채택.

with unioned as (
    select
        asset_code,
        source_code,
        ts_utc,
        ts_minute_utc,
        price,
        quote_ccy,
        volume_24h,
        ingested_at
    from {{ ref('stg_upbit__price_tick') }}

    union all

    select
        asset_code,
        source_code,
        ts_utc,
        ts_minute_utc,
        price,
        quote_ccy,
        volume_24h,
        ingested_at
    from {{ ref('stg_bithumb__price_tick') }}
),

minute_close as (
    select
        asset_code,
        source_code,
        ts_minute_utc,
        price,
        quote_ccy,
        volume_24h,
        ts_utc      as tick_ts_utc,   -- 채택된 tick의 원본 체결 시각 (추적·clock skew 분석용)
        ingested_at
    from unioned
    qualify row_number() over (
        partition by asset_code, source_code, ts_minute_utc
        order by ts_utc desc
    ) = 1
)

select * from minute_close
