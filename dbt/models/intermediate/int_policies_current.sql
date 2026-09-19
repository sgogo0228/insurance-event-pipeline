{{ config(tags=['policy'], order_by='policy_id') }}

-- Replay the CDC history: the change with the highest log position is the current state of each policy.
with ranked as (
    select
        *,
        row_number() over (partition by policy_id order by lsn desc, changed_at desc) as recency
    from {{ ref('stg_policy_changes') }}
)

select
    policy_id,
    customer_id,
    product_code,
    sum_insured,
    annual_premium,
    status,
    issued_at,
    updated_at
from ranked
where recency = 1
  and op != 'd'
