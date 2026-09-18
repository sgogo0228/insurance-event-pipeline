{{ config(order_by='(payment_date, product_code)') }}

with payments as (
    select
        payments.policy_id as policy_id,
        payments.amount as amount,
        toDate(payments.paid_at) as payment_date,
        -- Late-arriving data: a payment whose policy has not arrived yet is kept as UNKNOWN
        -- and gets its real product on a later run, instead of being dropped or blocking the pipeline.
        if(empty(policies.policy_id), 'UNKNOWN', policies.product_code) as product_code
    from {{ ref('stg_premium_payments') }} as payments
    left join {{ ref('int_policies_current') }} as policies on policies.policy_id = payments.policy_id
)

select
    payment_date,
    product_code,
    count() as payment_count,
    uniqExact(policy_id) as paying_policies,
    sum(amount) as premium_collected
from payments
group by payment_date, product_code
