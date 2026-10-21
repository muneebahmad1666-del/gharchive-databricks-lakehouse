-- grain: one row per fork (repo_id is the repo that was forked)
select
    f.event_id,
    f.forkee_id,
    f.forkee_name,
    split(f.forkee_name, '/')[0]          as forkee_owner,
    {{ date_key('f.event_date') }}        as date_key,
    cast(f.event_hour as int)             as hour_key,
    f.repo_id,
    f.actor_id,
    coalesce(a.is_bot, false)             as actor_is_bot,
    f.created_at
from {{ source('silver', 'fork_events') }} f
left join {{ ref('dim_actor') }} a on a.actor_id = f.actor_id
