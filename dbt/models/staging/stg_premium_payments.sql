{{ config(unique_key='event_id', order_by='(paid_at, event_id)') }}

with source as (
    select message_value, ingested_at
    from {{ source('raw', 'kafka_messages') }}
    where topic = 'insurance_events'
      and isValidJSON(message_value)
      and JSONExtractString(message_value, 'event_type') = 'premium_paid'
      {{ incremental_lookback() }}
),

parsed as (
    select
        JSONExtractString(message_value, 'event_id') as event_id,
        JSONExtractString(message_value, 'payload', 'payment_id') as payment_id,
        JSONExtractString(message_value, 'payload', 'policy_id') as policy_id,
        JSONExtractInt(message_value, 'payload', 'amount') as amount,
        JSONExtractString(message_value, 'payload', 'payment_method') as payment_method,
        parseDateTime64BestEffort(JSONExtractString(message_value, 'event_ts'), 3, 'UTC') as paid_at,
        ingested_at,
        -- The same event can land more than once (producer retry or consumer re-read): keep the first copy.
        row_number() over (partition by JSONExtractString(message_value, 'event_id') order by ingested_at) as copy_number
    from source
)

select * except (copy_number)
from parsed
where copy_number = 1
