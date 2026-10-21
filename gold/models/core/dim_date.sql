-- one row per calendar day, from Jan 1 of the earliest year any fact points at
-- to Dec 31 of the latest event year. PRs are often opened long before the events
-- window, so pr_created_at and merged_at must be inside the range.
with bounds as (
    select
        greatest(
            least(
                (select min(event_date) from {{ source('silver', 'events') }}),
                (select min(to_date(pr_created_at)) from {{ source('silver', 'pull_request_events') }}),
                (select min(to_date(merged_at)) from {{ source('silver', 'pull_request_events') }})
            ),
            date'2008-01-01'
        ) as lo,
        (select max(event_date) from {{ source('silver', 'events') }}) as hi
),
days as (
    select explode(sequence(
        make_date(year(lo), 1, 1),
        make_date(year(hi), 12, 31),
        interval 1 day
    )) as full_date
    from bounds
)
select
    {{ date_key('full_date') }}          as date_key,
    full_date,
    year(full_date)                      as year,
    quarter(full_date)                   as quarter,
    month(full_date)                     as month,
    date_format(full_date, 'MMMM')       as month_name,
    weekofyear(full_date)                as week_of_year,
    dayofmonth(full_date)                as day_of_month,
    dayofweek(full_date)                 as day_of_week,   -- 1 = Sunday ... 7 = Saturday
    date_format(full_date, 'EEEE')       as day_name,
    dayofweek(full_date) in (1, 7)       as is_weekend
from days