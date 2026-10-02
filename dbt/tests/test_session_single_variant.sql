-- a session must never contain events from both variants
select session_id
from {{ ref('stg_events') }}
group by session_id
having count(distinct variant) > 1
