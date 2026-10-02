-- One row per (minute, variant). A window is emitted once (append mode), so
-- duplicates can only come from a replayed batch; keep the earliest.
select
    cast(window_start as timestamp)  as window_start,
    cast(window_end as timestamp)    as window_end,
    variant,
    page_views,
    add_to_carts,
    checkouts,
    purchases,
    revenue,
    _batch_id
from {{ source('raw', 'minute_metrics') }}
where true
qualify row_number() over (partition by window_start, variant order by _batch_id) = 1
