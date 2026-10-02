-- One row per event_id (replayed micro-batches removed; earliest batch wins).
select
    event_id,
    user_id,
    session_id,
    event_type,
    variant,
    product_id,
    cast(price as {{ dbt.type_numeric() }})  as price,
    cast(event_ts as timestamp)              as event_ts,
    _batch_id
from {{ source('raw', 'events') }}
where event_id is not null
qualify row_number() over (partition by event_id order by _batch_id) = 1
