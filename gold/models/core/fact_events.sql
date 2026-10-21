-- grain: one row per event
select
    e.event_id,
    {{ date_key('e.event_date') }}        as date_key,
    cast(e.event_hour as int)             as hour_key,
    coalesce(t.event_type_key, -1)        as event_type_key,
    e.repo_id,
    e.actor_id,
    e.org_login,
    e.is_public,
    e.is_bot                              as actor_is_bot,
    e.created_at
from {{ source('silver', 'events') }} e
left join {{ ref('dim_event_type') }} t on t.event_type = e.type
