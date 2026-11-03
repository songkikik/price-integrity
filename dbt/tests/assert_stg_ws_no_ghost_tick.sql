-- 유령 tick 검출: 파싱 실패 시 JSONExtract* 는 에러 대신 0을 돌려준다.
-- → 비데이터 메시지가 필터를 통과했거나 필드명이 바뀌면 price=0 / ts=1970 행이 조용히 생긴다.
-- 이 테스트는 그런 행이 0건인지 검사한다 (반환 행 = 실패).
select
    source_code,
    ts_utc,
    price,
    raw_payload_id
from {{ ref('stg_ws__price_tick') }}
where price <= 0
   or ts_utc < toDateTime64('2020-01-01 00:00:00', 3, 'UTC')
