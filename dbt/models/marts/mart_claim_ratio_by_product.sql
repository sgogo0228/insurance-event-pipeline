{{ config(order_by='product_code') }}

select
    product_code,
    count() as policy_count,
    countIf(status = 'active') as active_policies,
    countIf(status != 'active') as terminated_policies,
    sum(premium_collected) as total_premium_collected,
    sum(claim_count) as total_claims,
    sum(claim_amount) as total_claim_amount,
    round(if(total_premium_collected = 0, 0, total_claim_amount / total_premium_collected), 4) as claim_ratio
from {{ ref('int_policy_activity') }}
group by product_code
