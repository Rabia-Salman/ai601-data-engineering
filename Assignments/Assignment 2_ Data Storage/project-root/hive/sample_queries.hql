-- File: project-root/hive/sample_queries.hql

-- Query 1: Monthly Active Users by Region
SELECT year, month, region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
GROUP BY year, month, region;

-- Query 2: Top Content by Play Count
SELECT a.content_id, b.title, COUNT(*) AS play_count
FROM fact_user_actions a
JOIN dim_content b ON a.content_id = b.content_id
WHERE a.action = 'play'
GROUP BY a.content_id, b.title
ORDER BY play_count DESC;

-- Query 3: Average Session Duration per Week
-- (Assumes each session_id groups multiple events and calculates the difference between the first and last timestamp)
WITH session_times AS (
    SELECT session_id,
           MIN(ts) AS session_start,
           MAX(ts) AS session_end
    FROM fact_user_actions
    GROUP BY session_id
),
session_durations AS (
    SELECT session_id,
           unix_timestamp(session_end) - unix_timestamp(session_start) AS duration,
           weekofyear(session_start) AS week
    FROM session_times
)
SELECT week, AVG(duration) AS avg_session_duration
FROM session_durations
GROUP BY week;
