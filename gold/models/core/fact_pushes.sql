-- grain: one row per push
select
    p.event_id,
    p.push_id,
    {{ date_key('p.event_date') }}        as date_key,
    cast(p.event_hour as int)             as hour_key,
    p.repo_id,
    p.actor_id,
    coalesce(a.is_bot, false)             as actor_is_bot,
    p.ref,
    case when p.ref like 'refs/heads/%' then substring(p.ref, 12) end as branch,
    p.ref like 'refs/tags/%'              as is_tag_push,
    p.head,
    p.commit_count,
    p.distinct_commit_count,
    p.created_at
from {{ source('silver', 'push_events') }} p
left join {{ ref('dim_actor') }} a on a.actor_id = p.actor_id
