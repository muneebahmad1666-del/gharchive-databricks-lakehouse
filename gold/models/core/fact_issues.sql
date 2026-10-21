-- grain: one row per issue event
select
    i.event_id,
    i.issue_id,
    i.issue_number,
    {{ date_key('i.event_date') }}        as date_key,
    cast(i.event_hour as int)             as hour_key,
    i.repo_id,
    i.actor_id,
    coalesce(a.is_bot, false)             as actor_is_bot,
    i.action,
    i.issue_state,
    i.comment_count,
    i.created_at
from {{ source('silver', 'issue_events') }} i
left join {{ ref('dim_actor') }} a on a.actor_id = i.actor_id
