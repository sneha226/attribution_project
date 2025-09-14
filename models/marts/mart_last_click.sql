SELECT user_id, MAX(event_timestamp) AS last_click_time, traffic_source
FROM {{ ref('int_user_journey') }}
GROUP BY user_id, traffic_source
