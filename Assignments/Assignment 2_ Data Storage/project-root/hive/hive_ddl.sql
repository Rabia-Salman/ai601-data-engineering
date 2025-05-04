-- Create an external table for raw user activity logs, partitioned by year/month/day
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user_logs (
  user_id INT,
  content_id INT,
  action STRING,
  timestamp STRING,
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs';

-- Create an external table for content metadata
CREATE EXTERNAL TABLE IF NOT EXISTS raw_content_metadata (
  content_id INT,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/metadata';


-- Fact table for user actions (store as Parquet)
CREATE TABLE fact_user_actions (
  user_id INT,
  content_id INT,
  action STRING,
  event_time TIMESTAMP,
  device STRING,
  region STRING,
  session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET;

-- Dimension table for content metadata
CREATE TABLE dim_content (
  content_id INT,
  title STRING,
  category STRING,
  length INT,
  artist STRING
)
STORED AS PARQUET;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT 
  user_id,
  content_id,
  action,
  CAST(timestamp AS TIMESTAMP) AS event_time,
  device,
  region,
  session_id,
  year,
  month,
  day
FROM raw_user_logs;


INSERT OVERWRITE TABLE dim_content
SELECT * FROM raw_content_metadata;


-- Query 1: Monthly active users by region
SELECT region, year, month, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
GROUP BY region, year, month;

-- Query 2: Top content categories by play count
SELECT m.category, COUNT(*) AS play_count
FROM fact_user_actions f
JOIN dim_content m ON f.content_id = m.content_id
WHERE f.action = 'play'
GROUP BY m.category
ORDER BY play_count DESC;

-- Query 3: Average session length weekly
WITH session_durations AS (
  SELECT 
    session_id,
    weekofyear(event_time) AS week,
    MAX(event_time) AS end_time,
    MIN(event_time) AS start_time
  FROM fact_user_actions
  GROUP BY session_id, weekofyear(event_time)
)
SELECT 
  week,
  AVG(unix_timestamp(end_time) - unix_timestamp(start_time)) AS avg_session_length_seconds
FROM session_durations
GROUP BY week;
