-- the 24 hours of the day
select
    cast(hour_key as int)                as hour_key,
    format_string('%02d:00', hour_key)   as hour_label,
    case
        when hour_key < 6  then 'night'
        when hour_key < 12 then 'morning'
        when hour_key < 18 then 'afternoon'
        else 'evening'
    end                                  as day_part
from (select explode(sequence(0, 23)) as hour_key) as h
