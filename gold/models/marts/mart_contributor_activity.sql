-- grain: one row per human actor (bots excluded)
with ev as (
    select actor_id,
           count(*)                    as total_events,
           count(distinct repo_id)     as repos_touched,
           count(distinct date_key)    as active_days
    from {{ ref('fact_events') }}
    group by actor_id
),
pushes as (
    select actor_id, count(*) as pushes, sum(commit_count) as commits_pushed
    from {{ ref('fact_pushes') }}
    group by actor_id
),
prs as (
    select actor_id,
           count_if(action = 'opened')                  as prs_opened,
           count_if(action = 'closed' and is_merged)    as prs_merged_by_actor
    from {{ ref('fact_pull_requests') }}
    group by actor_id
),
issues as (
    select actor_id, count_if(action = 'opened') as issues_opened
    from {{ ref('fact_issues') }}
    group by actor_id
)
select
    a.actor_id,
    a.actor_login,
    ev.total_events,
    ev.repos_touched,
    ev.active_days,
    coalesce(pushes.pushes, 0)               as pushes,
    coalesce(pushes.commits_pushed, 0)       as commits_pushed,
    coalesce(prs.prs_opened, 0)              as prs_opened,
    coalesce(prs.prs_merged_by_actor, 0)     as prs_merged_by_actor,
    coalesce(issues.issues_opened, 0)        as issues_opened,
    dense_rank() over (order by ev.total_events desc) as activity_rank
from {{ ref('dim_actor') }} a
join ev on ev.actor_id = a.actor_id
left join pushes on pushes.actor_id = a.actor_id
left join prs    on prs.actor_id    = a.actor_id
left join issues on issues.actor_id = a.actor_id
where not a.is_bot
