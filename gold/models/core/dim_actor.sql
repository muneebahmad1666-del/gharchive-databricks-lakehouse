-- one row per actor: latest login (no history kept)
with latest as (
    select actor_id, actor_login, is_bot
    from {{ source('silver', 'events') }}
    qualify row_number() over (partition by actor_id order by created_at desc) = 1
),
stats as (
    select
        actor_id,
        min(event_date)          as first_seen_date,
        max(event_date)          as last_seen_date,
        count(*)                 as total_events,
        count(distinct repo_id)  as repos_touched
    from {{ source('silver', 'events') }}
    group by actor_id
)
select
    l.actor_id,
    l.actor_login,
    l.is_bot,
    case when l.is_bot then 'bot' else 'human' end as actor_type,
    s.first_seen_date,
    s.last_seen_date,
    s.total_events,
    s.repos_touched
from latest l
join stats s on s.actor_id = l.actor_id
