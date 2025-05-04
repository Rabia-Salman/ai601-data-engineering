-- Transform raw logs into fact table with data type conversion
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT 
  user_id,
  content_id,
  action,
  CAST(timestamp AS TIMESTAMP) AS timestamp,
  device,
  region,
  session_id,
  year,
  month,
  day
FROM raw_logs
WHERE year=2025 AND month=02 AND day=01;

-- Transform raw metadata into dimension table
INSERT OVERWRITE TABLE dim_content
SELECT DISTINCT content_id, title, category, length, artist
FROM raw_metadata;
