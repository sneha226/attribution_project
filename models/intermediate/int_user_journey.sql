WITH ordered_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_timestamp) AS event_order
    FROM {{ ref('stg_events') }}
)
SELECT *
FROM ordered_events