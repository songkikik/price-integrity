-- v0.3 5단계: 포맷별 WS staging 모델을 하나로 합치는 union 모델 (이름 유지 → int 이하 무수정).
-- 포맷 차이는 각 stg_ws__ticker_* 가 흡수하고, 이 모델은 union + grain dedup만 책임진다.
--   - stg_ws__ticker_upbit_fmt : upbit, bithumb (Upbit 표준 포맷)
--   - stg_ws__ticker_coinone   : coinone
--   - stg_ws__ticker_korbit    : korbit
-- 소스 추가 = 포맷 모델 1개 + 아래 union 1줄.

with unioned as (
    select ingested_at, source_code, asset_code, ts_utc, ts_minute_utc,
           price, quote_ccy, volume_24h, raw_payload_id
    from {{ ref('stg_ws__ticker_upbit_fmt') }}

    union all

    select ingested_at, source_code, asset_code, ts_utc, ts_minute_utc,
           price, quote_ccy, volume_24h, raw_payload_id
    from {{ ref('stg_ws__ticker_coinone') }}

    union all

    select ingested_at, source_code, asset_code, ts_utc, ts_minute_utc,
           price, quote_ccy, volume_24h, raw_payload_id
    from {{ ref('stg_ws__ticker_korbit') }}
),

-- grain: (asset_code, source_code, ts_utc). 동일 ts_utc 중복(재전송·replay·korbit 반복 ticker)은
-- 최신 ingested_at 1건만 남긴다. v0.2와 동일한 규칙을 전 소스에 1회 적용.
deduped as (
    select *
    from unioned
    qualify row_number() over (
        partition by asset_code, source_code, ts_utc
        order by ingested_at desc
    ) = 1
)

select * from deduped
