{{ config(materialized='table', unique_key=['claim_id']) }}

select
    claim_id,
    member_id,
    service_date,
    paid_amount,
    diagnosis_group
from {{ source('platform_raw', 'claims_daily') }}
{% if is_incremental() %}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
{% endif %}
