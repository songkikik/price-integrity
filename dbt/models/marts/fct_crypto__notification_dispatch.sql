-- 감지된 이상 이벤트(anomaly_event) 중 '실제로 알림 보낼 것'만 선별. 통지 = 정책 의존/비멱등 (D4).
-- grain: dispatch_id (1 발송 결정 = 1행). append-only(incremental)로 발송 이력 누적 → cooldown 성립.
-- cooldown: 같은 (asset, severity)에서 직전 발송과의 간격이 임계 미만이면 억제.
--   severity별 파티션이라 warn cooldown 중에도 critical은 독립 발송(D4 'severity 상승 시 무시').
-- 실제 Slack 발송은 dbt 밖(Python)이 이 테이블을 읽어 수행. dbt는 '보낼 목록' 산출까지만.

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    order_by='dispatched_at'
) }}

{% set warn_cooldown = var('dispatch_warn_cooldown_min', 10) %}
{% set critical_cooldown = var('dispatch_critical_cooldown_min', 5) %}

with candidates as (
    select
        asset_code,
        ts_minute_utc,
        rule_code,
        severity,
        spread_pct
    from {{ ref('fct_crypto__anomaly_event') }}
    {% if is_incremental() %}
    -- 이미 처리한 분 이후의 이벤트만 (중복 발송 방지)
    where ts_minute_utc > (select max(source_ts_minute_utc) from {{ this }})
    {% endif %}
),

-- 같은 (asset, severity)에서 직전 이벤트 시각 → cooldown 판정용
with_gap as (
    select
        asset_code,
        ts_minute_utc,
        rule_code,
        severity,
        spread_pct,
        lagInFrame(ts_minute_utc, 1) over (
            partition by asset_code, severity
            order by ts_minute_utc
        ) as prev_ts_minute_utc
    from candidates
),

to_dispatch as (
    select *
    from with_gap
    where prev_ts_minute_utc is null    -- 직전 발송 없음 = 첫 알림
       or dateDiff('minute', prev_ts_minute_utc, ts_minute_utc)
            >= (case when severity = 'critical' then {{ critical_cooldown }} else {{ warn_cooldown }} end)
)

select
    generateUUIDv4()        as dispatch_id,        -- 발송 식별자 (비멱등: 발송마다 고유)
    now()                   as dispatched_at,      -- 발송 결정 시각 (UTC)
    asset_code              as source_asset_code,
    ts_minute_utc           as source_ts_minute_utc,
    rule_code,
    severity,
    spread_pct
from to_dispatch
