-- grain: one row per star (a WatchEvent with action 'started')
select
    w.event_id,
    {{ date_key('w.event_date') }}        as date_key,
    cast(w.event_hour as int)             as hour_key,
    w.repo_id,
    w.actor_id,
    coalesce(a.is_bot, false)             as actor_is_bot,
    w.created_at
from {{ source('silver', 'watch_events') }} w
left join {{ ref('dim_actor') }} a on a.actor_id = w.actor_id
where w.is_star
