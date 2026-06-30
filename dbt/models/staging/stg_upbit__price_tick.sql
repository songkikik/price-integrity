with source as (
    select
        ingested_at,
        raw_payload
    from {{ source('raw', 'raw_upbit__ticker') }}
),

parsed as (
    select
        ingested_at,
        'BTCKRW'                                                            as asset_code,
        'upbit'                                                             as source_code,
        fromUnixTimestamp64Milli(
            JSONExtractUInt(raw_payload, 'trade_timestamp')
        )                                                                   as ts_utc,
        toStartOfMinute(fromUnixTimestamp64Milli(
            JSONExtractUInt(raw_payload, 'trade_timestamp')
        ))                                                                  as ts_minute_utc,
        JSONExtractFloat(raw_payload, 'trade_price')                       as price,
        'KRW'                                                               as quote_ccy,
        JSONExtractFloat(raw_payload, 'acc_trade_volume_24h')              as volume_24h,
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
