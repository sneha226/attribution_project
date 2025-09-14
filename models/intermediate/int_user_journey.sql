WITH ordered_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_timestamp ASC) AS rn_asc,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_timestamp DESC) AS rn_desc
    FROM {{ ref('stg_events') }}
)
SELECT *
FROM ordered_events
