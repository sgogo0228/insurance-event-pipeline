{{ config(unique_key='event_id', order_by='(filed_at, event_id)') }}

with source as (
    select message_value, ingested_at
    from {{ source('raw', 'kafka_messages') }}
    where topic = 'insurance_events'
      and isValidJSON(message_value)
      and JSONExtractString(message_value, 'event_type') = 'claim_filed'
      {{ incremental_lookback() }}
),

parsed as (
    select
        JSONExtractString(message_value, 'event_id') as event_id,
        JSONExtractString(message_value, 'payload', 'claim_id') as claim_id,
        JSONExtractString(message_value, 'payload', 'policy_id') as policy_id,
        JSONExtractInt(message_value, 'payload', 'claim_amount') as claim_amount,
        -- Flatten the semi-structured claim_detail object and its documents array.
        JSONExtractString(message_value, 'payload', 'claim_detail', 'category') as claim_category,
        JSONExtractString(message_value, 'payload', 'claim_detail', 'description') as claim_description,
        JSONExtract(message_value, 'payload', 'claim_detail', 'documents', 'Array(String)') as documents,
        length(documents) as document_count,
        parseDateTime64BestEffort(JSONExtractString(message_value, 'event_ts'), 3, 'UTC') as filed_at,
        ingested_at,
        row_number() over (partition by JSONExtractString(message_value, 'event_id') order by ingested_at) as copy_number
    from source
)

select * except (copy_number)
from parsed
where copy_number = 1
