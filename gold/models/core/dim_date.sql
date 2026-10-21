-- one row per calendar day, for every year that appears in the data
with bounds as (
    select min(event_date) as lo, max(event_date) as hi
    from {{ source('silver', 'events') }}
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
