-- Blocking check: an invalid claim must stop the downstream models from being built.
select event_id, claim_id, claim_amount
from {{ ref('stg_claims') }}
where claim_amount <= 0
