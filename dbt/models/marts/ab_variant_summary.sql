-- Inputs for the significance test in src/pulse/abtest.py (run by the Airflow DAG).
select
    variant,
    count(*)                         as sessions,
    sum(converted)                   as conversions,
    {{ dbt.safe_cast("sum(converted)", dbt.type_float()) }} / count(*) as conversion_rate,
    sum(revenue)                     as revenue,
    sum(revenue) / count(*)          as revenue_per_session
from {{ ref('fct_sessions') }}
group by variant
