-- returns a row for every mart whose totals do not add up to fact_events
select * from (
    select 'mart_repo_activity_daily' as mart,
           (select sum(total_events) from {{ ref('mart_repo_activity_daily') }}) as mart_total,
           (select count(*) from {{ ref('fact_events') }}) as fact_total
    union all
    select 'mart_event_type_hourly',
           (select sum(events) from {{ ref('mart_event_type_hourly') }}),
           (select count(*) from {{ ref('fact_events') }})
) as checks
where mart_total <> fact_total
