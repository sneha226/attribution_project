SELECT user_id, MIN(event_timestamp) AS first_click_time, traffic_source
FROM {{ ref('int_user_journey') }}
GROUP BY user_id, traffic_source
