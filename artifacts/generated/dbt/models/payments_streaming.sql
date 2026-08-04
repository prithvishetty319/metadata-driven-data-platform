{{ config(materialized='incremental', unique_key=['payment_id']) }}

select
    payment_id,
    claim_id,
    amount,
    currency,
    event_time
from {{ source('platform_raw', 'payments_streaming') }}
{% if is_incremental() %}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
{% endif %}
