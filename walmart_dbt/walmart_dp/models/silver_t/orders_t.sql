{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

select
    *,
    current_timestamp() as processed_at
from {{ source('walmart_dp', 'orders') }}



{% if is_incremental() %}
where updated_timestamp > (select (coalesce(max(updated_timestamp), cast('1970-01-01' as timestamp))) from {{ this }})
{% endif %}