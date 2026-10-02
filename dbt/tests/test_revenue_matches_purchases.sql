-- revenue in the daily funnel must equal the sum of purchase prices
with funnel as (select sum(revenue) as r from {{ ref('fct_daily_funnel') }}),
events as (
    select coalesce(sum(price), 0) as r from {{ ref('stg_events') }} where event_type = 'purchase'
)
select * from funnel, events where abs(funnel.r - events.r) > 0.01
