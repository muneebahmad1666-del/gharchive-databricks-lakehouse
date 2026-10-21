-- grain: one row per branch, tag or repository creation
select
    c.event_id,
    {{ date_key('c.event_date') }}        as date_key,
    cast(c.event_hour as int)             as hour_key,
    c.repo_id,
    c.actor_id,
    coalesce(a.is_bot, false)             as actor_is_bot,
    c.ref,
    c.ref_type,
    c.master_branch,
    c.pusher_type,
    c.created_at
from {{ source('silver', 'create_events') }} c
left join {{ ref('dim_actor') }} a on a.actor_id = c.actor_id
