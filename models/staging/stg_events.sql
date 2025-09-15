SELECT
  user_pseudo_id AS user_id,
  event_name,
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  device.category AS platform,
  traffic_source.source AS traffic_source
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE user_pseudo_id IS NOT NULL
  AND event_name IS NOT NULL
