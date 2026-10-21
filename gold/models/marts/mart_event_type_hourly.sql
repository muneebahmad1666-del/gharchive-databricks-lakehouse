-- grain: event type x hour x day
select
    f.date_key,
    d.full_date,
    d.day_name,
    f.hour_key,
    t.hour_label,
    t.day_part,
    f.event_type_key,
    e.event_type,
    e.event_category,
    count(*)                    as events,
    count_if(f.actor_is_bot)    as bot_events,
    count(distinct f.actor_id)  as distinct_actors,
    count(distinct f.repo_id)   as distinct_repos
from {{ ref('fact_events') }} f
join {{ ref('dim_date') }}       d on d.date_key = f.date_key
join {{ ref('dim_time') }}       t on t.hour_key = f.hour_key
join {{ ref('dim_event_type') }} e on e.event_type_key = f.event_type_key
group by f.date_key, d.full_date, d.day_name, f.hour_key, t.hour_label, t.day_part,
         f.event_type_key, e.event_type, e.event_category
