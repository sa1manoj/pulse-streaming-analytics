select
    cast(session_start as date)          as session_date,
    variant,
    count(*)                             as sessions,
    sum(reached_cart)                    as cart_sessions,
    sum(reached_checkout)                as checkout_sessions,
    sum(converted)                       as converted_sessions,
    sum(revenue)                         as revenue,
    {{ dbt.safe_cast("sum(converted)", dbt.type_float()) }} / count(*) as conversion_rate
from {{ ref('fct_sessions') }}
group by 1, 2
