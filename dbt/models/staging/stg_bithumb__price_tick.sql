with source as (
    select
        ingested_at,
        raw_payload
    from {{ source('raw', 'raw_bithumb__ticker') }}
),

parsed as (
    select
        ingested_at,
        'BTCKRW'                                                            as asset_code,
        'bithumb'                                                           as source_code,
        fromUnixTimestamp64Milli(
            toUInt64(JSONExtractString(raw_payload, 'date'))
        )                                                                   as ts_utc,
        toStartOfMinute(fromUnixTimestamp64Milli(
            toUInt64(JSONExtractString(raw_payload, 'date'))
        ))                                                                  as ts_minute_utc,
        toFloat64(JSONExtractString(raw_payload, 'closing_price'))          as price,
        'KRW'                                                               as quote_ccy,
        toFloat64(JSONExtractString(raw_payload, 'units_traded_24H'))       as volume_24h,
        toString(ingested_at)                                               as raw_payload_id
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
