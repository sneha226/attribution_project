WITH base_events AS (
    SELECT
        user_id,
        event_name,
        event_timestamp,
        platform,
        traffic_source,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, event_name, event_timestamp
            ORDER BY event_timestamp
        ) AS dedupe_rank
    FROM {{ ref('stg_events') }}
),

deduped AS (
    SELECT
        user_id,
        event_name,
        event_timestamp,
        platform,
        traffic_source
    FROM base_events
    WHERE dedupe_rank = 1
),

ordered_events AS (
    SELECT
        user_id,
        event_name,
        event_timestamp,
        platform,
        traffic_source,
        ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY event_timestamp ASC
        ) AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY event_timestamp DESC
        ) AS rn_desc
    FROM deduped
)

SELECT
    user_id,
    event_name,
    event_timestamp,
    platform,
    traffic_source,
    rn_asc,
    rn_desc
FROM ordered_events
