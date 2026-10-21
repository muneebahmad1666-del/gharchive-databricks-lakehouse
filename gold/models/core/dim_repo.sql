-- one row per repo: latest name, most recent known language
with latest as (
    select repo_id, repo_name, org_login
    from {{ source('silver', 'events') }}
    qualify row_number() over (partition by repo_id order by created_at desc) = 1
),
stats as (
    select
        repo_id,
        min(event_date)            as first_seen_date,
        max(event_date)            as last_seen_date,
        count(*)                   as total_events,
        count(distinct actor_id)   as distinct_actors
    from {{ source('silver', 'events') }}
    group by repo_id
),
lang as (
    select repo_id, repo_language as primary_language
    from {{ source('silver', 'pull_request_events') }}
    where repo_language is not null and repo_language <> 'unknown'
    qualify row_number() over (partition by repo_id order by created_at desc) = 1
)
select
    l.repo_id,
    l.repo_name,
    split(l.repo_name, '/')[0]                      as repo_owner,
    l.org_login,
    coalesce(g.primary_language, 'unknown')         as primary_language,
    s.first_seen_date,
    s.last_seen_date,
    s.total_events,
    s.distinct_actors
from latest l
join stats s on s.repo_id = l.repo_id
left join lang g on g.repo_id = l.repo_id
