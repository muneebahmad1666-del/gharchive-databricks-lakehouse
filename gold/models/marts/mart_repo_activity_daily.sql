-- grain: repo x day. fact_events holds every event, so it carries every (repo, day) pair
with ev as (
    select repo_id, date_key,
           count(*)                       as total_events,
           count_if(actor_is_bot)         as bot_events,
           count(distinct actor_id)       as active_actors
    from {{ ref('fact_events') }}
    group by repo_id, date_key
),
pushes as (
    select repo_id, date_key, count(*) as pushes, sum(commit_count) as commits_pushed
    from {{ ref('fact_pushes') }}
    group by repo_id, date_key
),
prs as (
    select repo_id, date_key,
           count_if(action = 'opened')                      as prs_opened,
           count_if(action = 'closed' and is_merged)        as prs_merged
    from {{ ref('fact_pull_requests') }}
    group by repo_id, date_key
),
issues as (
    select repo_id, date_key,
           count_if(action = 'opened') as issues_opened,
           count_if(action = 'closed') as issues_closed
    from {{ ref('fact_issues') }}
    group by repo_id, date_key
),
stars as (
    select repo_id, date_key, count(*) as stars
    from {{ ref('fact_stars') }}
    group by repo_id, date_key
),
forks as (
    select repo_id, date_key, count(*) as forks
    from {{ ref('fact_forks') }}
    group by repo_id, date_key
),
creates as (
    select repo_id, date_key, count_if(ref_type = 'branch') as branches_created
    from {{ ref('fact_creates') }}
    group by repo_id, date_key
)
select
    ev.repo_id,
    r.repo_name,
    r.repo_owner,
    r.primary_language,
    ev.date_key,
    d.full_date,
    d.day_name,
    d.is_weekend,
    ev.total_events,
    ev.bot_events,
    ev.active_actors,
    coalesce(pushes.pushes, 0)              as pushes,
    coalesce(pushes.commits_pushed, 0)      as commits_pushed,
    coalesce(prs.prs_opened, 0)             as prs_opened,
    coalesce(prs.prs_merged, 0)             as prs_merged,
    coalesce(issues.issues_opened, 0)       as issues_opened,
    coalesce(issues.issues_closed, 0)       as issues_closed,
    coalesce(stars.stars, 0)                as stars,
    coalesce(forks.forks, 0)                as forks,
    coalesce(creates.branches_created, 0)   as branches_created
from ev
join {{ ref('dim_repo') }} r on r.repo_id = ev.repo_id
join {{ ref('dim_date') }} d on d.date_key = ev.date_key
left join pushes  on pushes.repo_id  = ev.repo_id and pushes.date_key  = ev.date_key
left join prs     on prs.repo_id     = ev.repo_id and prs.date_key     = ev.date_key
left join issues  on issues.repo_id  = ev.repo_id and issues.date_key  = ev.date_key
left join stars   on stars.repo_id   = ev.repo_id and stars.date_key   = ev.date_key
left join forks   on forks.repo_id   = ev.repo_id and forks.date_key   = ev.date_key
left join creates on creates.repo_id = ev.repo_id and creates.date_key = ev.date_key
