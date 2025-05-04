-- File: project-root/hive/raw_tables.hql

-- Create external table for raw user activity logs.
-- The table is partitioned by year, month, and day.
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user_logs (
    user_id INT,
    content_id INT,
    action STRING,
    log_timestamp STRING,
    device STRING,
    region STRING,
    session_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/logs';

-- Create external table for content metadata.
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
