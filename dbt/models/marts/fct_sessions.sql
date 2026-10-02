-- One row per session with funnel flags. A session belongs to one variant
-- (assignment is by user); test_session_single_variant enforces it.
select
    session_id,
    min(user_id)                                                          as user_id,
    min(variant)                                                          as variant,
    min(event_ts)                                                         as session_start,
    max(event_ts)                                                         as session_end,
    count(*)                                                              as events,
    max(case when event_type = 'add_to_cart' then 1 else 0 end)          as reached_cart,
    max(case when event_type = 'checkout' then 1 else 0 end)             as reached_checkout,
    max(case when event_type = 'purchase' then 1 else 0 end)             as converted,
    coalesce(sum(case when event_type = 'purchase' then price end), 0)   as revenue
from {{ ref('stg_events') }}
group by session_id
