WITH ranked_events AS (
    SELECT
        user_id,
        event_name,
        event_timestamp,
        traffic_source,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp ASC) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp DESC) AS rn_last
    FROM {{ ref('int_user_journey') }}
)

SELECT
    user_id,
    event_name AS first_click_event,
    event_timestamp AS first_click_timestamp,
    traffic_source
FROM ranked_events
WHERE rn_first = 1
