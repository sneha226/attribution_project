select
    user_id,
    event_name as last_click_event,
    event_timestamp as last_click_timestamp
from {{ ref('int_user_journey') }}
qualify row_number() over(partition by user_id order by event_timestamp desc) = 1
