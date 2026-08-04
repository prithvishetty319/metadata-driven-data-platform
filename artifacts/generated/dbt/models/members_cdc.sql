{{ config(materialized='incremental', unique_key=['member_id']) }}

select
    member_id,
    plan_id,
    state_code,
    effective_date,
    source_sequence
from {{ source('platform_raw', 'members_cdc') }}
{% if is_incremental() %}
where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
{% endif %}
