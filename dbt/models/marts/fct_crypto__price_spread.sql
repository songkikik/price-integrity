-- 소스 간 가격 괴리(spread) 지표. cross-check 결과.
-- grain: asset_code × ts_minute_utc (= 자산 × 분, 1행)
-- 같은 분에 소스가 2개 이상 모일 때만 괴리가 의미 있음 → n_sources_active로 표시.
-- D3 정책: 환산 없이 동일 자산 코드(BTCKRW) 내에서만 비교.

with minute_long as (
    select
        asset_code,
        ts_minute_utc,
        source_code,
        price
    from {{ ref('int_crypto__price_tick_long') }}
),

agg as (
    select
        asset_code,
        ts_minute_utc,
        count(distinct source_code)     as n_sources_active,
        min(price)                      as price_min,
        max(price)                      as price_max,
        median(price)                   as price_median,
        max(price) - min(price)         as spread_abs
    from minute_long
    group by asset_code, ts_minute_utc
)

select
    asset_code,
    ts_minute_utc,
    n_sources_active,
    price_min,
    price_max,
    price_median,
    spread_abs,
    spread_abs / nullIf(price_median, 0)                                    as spread_pct,
    -- v0.1 단순 플래그: 소스 2개 이상 + 괴리율 0.5% 초과.
    -- N분 연속 판정·severity 구분·이벤트화는 fct_crypto__anomaly_event 레이어 책임(미구현).
    (n_sources_active >= 2
        and spread_abs / nullIf(price_median, 0) > 0.005)                   as is_anomaly
from agg
