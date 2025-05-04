-- Query 1: Monthly Active Users by Region
SELECT region, COUNT(DISTINCT user_id) AS active_users
FROM fact_user_actions
WHERE year=2023 AND month=09
GROUP BY region;

-- Query 2: Top Content by Play Count
SELECT d.title, COUNT(f.action) AS play_count
FROM fact_user_actions f
JOIN dim_content d ON f.content_id = d.content_id
WHERE f.action = 'play' AND f.year = 2023 AND f.month = 09
GROUP BY d.title
ORDER BY play_count DESC
LIMIT 5;

-- Query 3: Average Session Length Weekly (example approach)
SELECT WEEK(timestamp) AS week, AVG(session_length) AS avg_session_length
FROM (
  SELECT session_id, MAX(timestamp) - MIN(timestamp) AS session_length
  FROM fact_user_actions
  WHERE year = 2023
  GROUP BY session_id
) session_times
GROUP BY WEEK(timestamp);
