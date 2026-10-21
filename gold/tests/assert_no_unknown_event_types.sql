-- silver filters unknown types, so nothing should land on the -1 'Unknown' key
select event_id
from {{ ref('fact_events') }}
where event_type_key = -1
