{{ config(tags=['shared'], order_by='policy_id') }}

-- One row per policy combining its current state (A), payments (B) and claims (C).
with payments as (
    select
        policy_id,
        count() as payment_count,
        sum(amount) as premium_collected,
        max(paid_at) as last_paid_at
    from {{ ref('stg_premium_payments') }}
    group by policy_id
),

claims as (
    select
        policy_id,
        count() as claim_count,
        sum(claim_amount) as total_claim_amount
    from {{ ref('stg_claims') }}
    group by policy_id
)

select
    policies.policy_id as policy_id,
    policies.customer_id as customer_id,
    policies.product_code as product_code,
    policies.status as status,
    policies.sum_insured as sum_insured,
    policies.annual_premium as annual_premium,
    policies.issued_at as issued_at,
    -- ClickHouse fills unmatched LEFT JOIN columns with type defaults (0), not NULL.
    payments.payment_count as payment_count,
    payments.premium_collected as premium_collected,
    claims.claim_count as claim_count,
    claims.total_claim_amount as claim_amount
from {{ ref('int_policies_current') }} as policies
left join payments on payments.policy_id = policies.policy_id
left join claims on claims.policy_id = policies.policy_id
