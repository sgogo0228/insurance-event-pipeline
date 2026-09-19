{{ config(tags=['policy'], unique_key='change_id', order_by='(policy_id, lsn)') }}

with source as (
    select message_value, ingested_at
    from {{ source('raw', 'kafka_messages') }}
    where topic = 'cdc.public.policies'
      and isValidJSON(message_value)
      {{ incremental_lookback() }}
),

parsed as (
    select
        -- Debezium envelope: op = c (insert), u (update), d (delete), r (initial snapshot read).
        JSONExtractString(message_value, 'op') as op,
        -- A delete only carries the row image before the change; every other operation carries "after".
        if(op = 'd', JSONExtractRaw(message_value, 'before'), JSONExtractRaw(message_value, 'after')) as row_image,
        JSONExtractString(row_image, 'policy_id') as policy_id,
        JSONExtractString(row_image, 'customer_id') as customer_id,
        JSONExtractString(row_image, 'product_code') as product_code,
        JSONExtractInt(row_image, 'sum_insured') as sum_insured,
        JSONExtractInt(row_image, 'annual_premium') as annual_premium,
        JSONExtractString(row_image, 'status') as status,
        JSONExtractString(message_value, 'before', 'status') as previous_status,
        parseDateTime64BestEffort(JSONExtractString(row_image, 'issued_at'), 3, 'UTC') as issued_at,
        parseDateTime64BestEffort(JSONExtractString(row_image, 'updated_at'), 3, 'UTC') as updated_at,
        JSONExtractUInt(message_value, 'source', 'lsn') as lsn,
        fromUnixTimestamp64Milli(JSONExtractInt(message_value, 'source', 'ts_ms'), 'UTC') as changed_at,
        concat(policy_id, ':', toString(lsn)) as change_id,
        ingested_at
    from source
),

deduplicated as (
    select
        *,
        -- Debezium may re-send changes after a restart: same policy + same log position = same change.
        row_number() over (partition by change_id order by ingested_at) as copy_number
    from parsed
)

select * except (row_image, copy_number)
from deduplicated
where copy_number = 1
