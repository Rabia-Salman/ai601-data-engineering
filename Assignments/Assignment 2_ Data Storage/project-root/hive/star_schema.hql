-- File: project-root/hive/star_schema.hql

-- Enable dynamic partitioning for the fact table insert.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Create the fact table for user actions stored in Parquet.
CREATE TABLE IF NOT EXISTS fact_user_actions (
    user_id INT,
    content_id INT,
    action STRING,
    ts TIMESTAMP,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET;

-- Create the dimension table for content metadata stored in Parquet.
CREATE TABLE IF NOT EXISTS dim_content (
    content_id INT,
    title STRING,
    category STRING,
    length INT,
    artist STRING
)
STORED AS PARQUET;

-- Transform raw user logs into the fact table.
INSERT OVERWRITE TABLE fact_user_actions PARTITION (year, month, day)
SELECT
    user_id,
    content_id,
    action,
    CAST(log_timestamp AS TIMESTAMP) AS ts,
    device,
    region,
    session_id,
    year,
    month,
    day
FROM raw_user_logs;

-- Transform raw content metadata into the dimension table.
INSERT OVERWRITE TABLE dim_content
SELECT
    content_id,
    title,
    category,
    length,
    artist
FROM raw_content_metadata;
