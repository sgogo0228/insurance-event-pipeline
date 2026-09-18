-- Non-blocking check: payments whose policy has not arrived yet (late-arriving data) only raise a warning.
{{ config(severity='warn') }}

select payment_date, product_code, payment_count
from {{ ref('fct_premium_daily') }}
where product_code = 'UNKNOWN'
