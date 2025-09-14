SELECT
    user_id,
    event_name,
    event_timestamp,
    platform,
    traffic_source
FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE event_name IS NOT NULL
