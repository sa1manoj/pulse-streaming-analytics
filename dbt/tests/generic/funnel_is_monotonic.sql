{% test funnel_is_monotonic(model) %}
select * from {{ model }}
where not (sessions >= cart_sessions
       and cart_sessions >= checkout_sessions
       and checkout_sessions >= converted_sessions)
{% endtest %}
