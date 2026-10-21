-- returns a row for every fact whose row count differs from its silver source
select * from (
    select 'fact_events' as fact,
           (select count(*) from {{ ref('fact_events') }}) as gold_rows,
           (select count(*) from {{ source('silver', 'events') }}) as silver_rows
    union all
    select 'fact_pull_requests',
           (select count(*) from {{ ref('fact_pull_requests') }}),
           (select count(*) from {{ source('silver', 'pull_request_events') }})
    union all
    select 'fact_pushes',
           (select count(*) from {{ ref('fact_pushes') }}),
           (select count(*) from {{ source('silver', 'push_events') }})
    union all
    select 'fact_issues',
           (select count(*) from {{ ref('fact_issues') }}),
           (select count(*) from {{ source('silver', 'issue_events') }})
    union all
    select 'fact_stars',
           (select count(*) from {{ ref('fact_stars') }}),
           (select count(*) from {{ source('silver', 'watch_events') }} where is_star)
    union all
    select 'fact_forks',
           (select count(*) from {{ ref('fact_forks') }}),
           (select count(*) from {{ source('silver', 'fork_events') }})
    union all
    select 'fact_creates',
           (select count(*) from {{ ref('fact_creates') }}),
           (select count(*) from {{ source('silver', 'create_events') }})
) as checks
where gold_rows <> silver_rows
