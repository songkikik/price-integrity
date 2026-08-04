-- 소스 간 가격 괴리가 임계를 넘은 '이상 이벤트' 감지. 감지 = 결정론적/멱등 (D4).
-- grain: asset_code × ts_minute_utc × rule_code (한 분에 warn·critical 둘 다 발화 가능 → 각 행).
-- 임계는 dbt var로 파라미터화 (실데이터 분포 보고 조정 + 검증 시 임시 하향).
-- cooldown·중복 억제는 여기 없음 → 통지 정책은 fct_crypto__notification_dispatch 책임 (D4).

{% set warn_pct = var('anomaly_warn_pct', 0.005) %}
{% set critical_pct = var('anomaly_critical_pct', 0.02) %}

with spread as (
    select
        asset_code,
        ts_minute_utc,
        spread_pct
    from {{ ref('fct_crypto__price_spread') }}
    where n_sources_active >= 2          -- 소스 2개 이상에서만 괴리가 의미 있음
      and spread_pct is not null
),

-- 직전 '분'의 괴리율 (연속성 판정용). 데이터 공백 구분 위해 직전 분 '시각'도 함께.
with_prev as (
    select
        asset_code,
        ts_minute_utc,
        spread_pct,
        lagInFrame(spread_pct, 1)     over w  as prev_spread_pct,
        lagInFrame(ts_minute_utc, 1)  over w  as prev_ts_minute_utc
    from spread
    window w as (partition by asset_code order by ts_minute_utc)
),

-- critical: 즉시(1분) 임계 초과
critical_events as (
    select
        asset_code,
        ts_minute_utc,
        'critical'              as rule_code,
        'critical'              as severity,
        spread_pct,
        {{ critical_pct }}      as threshold_pct
    from with_prev
    where spread_pct > {{ critical_pct }}
),

-- warn: 임계 초과가 '연속된 2분' 지속.
-- TODO(나): 아래 where 의 `1 = 0` 을 조건 3개(AND)로 교체.
--   1) 이번 분 spread_pct 가 warn 임계 초과            → spread_pct > {{ warn_pct }}
--   2) 직전 분 prev_spread_pct 도 warn 임계 초과        → prev_spread_pct > {{ warn_pct }}
--   3) prev_ts_minute_utc 가 '진짜 직전 분'(1분 전)     → 데이터 공백이면 연속 아님이라 발화 X
--   힌트: ClickHouse에서 '1분 전' = ts_minute_utc - INTERVAL 1 MINUTE
--        (prev_spread_pct 는 데이터 없으면 NULL → NULL 비교는 자동으로 false 처리됨)
warn_events as (
    select
        asset_code,
        ts_minute_utc,
        'warn_2min'             as rule_code,
        'warn'                  as severity,
        spread_pct,
        {{ warn_pct }}          as threshold_pct
    from with_prev
    where spread_pct > {{ warn_pct }}                                  -- 이번 분 초과
      and prev_spread_pct > {{ warn_pct }}                            -- 직전 분도 초과
      and prev_ts_minute_utc = ts_minute_utc - INTERVAL 1 MINUTE      -- 그 직전이 정확히 1분 전(연속 보장)
)

select * from critical_events
union all
select * from warn_events
