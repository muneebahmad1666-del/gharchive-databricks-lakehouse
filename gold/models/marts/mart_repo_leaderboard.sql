-- grain: one row per repo, all time in the data
with ev as (
    select repo_id,
           count(*)                         as total_events,
           count_if(not actor_is_bot)       as human_events,
           count(distinct actor_id)         as distinct_actors
    from {{ ref('fact_events') }}
    group by repo_id
),
prs as (
    select repo_id,
           count_if(action = 'opened')                  as prs_opened,
           count_if(action = 'closed' and is_merged)    as prs_merged,
           round(avg(case when action = 'closed' and is_merged then merge_time_hours end), 2) as avg_merge_time_hours
    from {{ ref('fact_pull_requests') }}
    group by repo_id
),
pushes as (
    select repo_id, count(*) as pushes, sum(commit_count) as commits_pushed
    from {{ ref('fact_pushes') }}
    group by repo_id
),
stars as (select repo_id, count(*) as stars from {{ ref('fact_stars') }} group by repo_id),
forks as (select repo_id, count(*) as forks from {{ ref('fact_forks') }} group by repo_id)
select
    r.repo_id,
    r.repo_name,
    r.repo_owner,
    r.primary_language,
    ev.total_events,
    ev.human_events,
    ev.distinct_actors,
    coalesce(stars.stars, 0)            as stars,
    coalesce(forks.forks, 0)            as forks,
    coalesce(pushes.pushes, 0)          as pushes,
    coalesce(pushes.commits_pushed, 0)  as commits_pushed,
    coalesce(prs.prs_opened, 0)         as prs_opened,
    coalesce(prs.prs_merged, 0)         as prs_merged,
    prs.avg_merge_time_hours,
    dense_rank() over (order by coalesce(stars.stars, 0) desc)  as star_rank,
    dense_rank() over (order by ev.total_events desc)           as activity_rank
from {{ ref('dim_repo') }} r
join ev on ev.repo_id = r.repo_id
left join prs    on prs.repo_id    = r.repo_id
left join pushes on pushes.repo_id = r.repo_id
left join stars  on stars.repo_id  = r.repo_id
left join forks  on forks.repo_id  = r.repo_id
