-- Login Anomaly Detection
-- Returns accounts with more than 3 failed login attempts in the last hour
-- Reads from mv_login_anomaly (SummingMergeTree) not the raw table

SELECT
    account_id,
    hour_bucket,
    sum(failed_attempts) AS total_failed,
    if(sum(failed_attempts) > 5, 'HIGH', 'MEDIUM') AS risk_level
FROM mv_login_anomaly
WHERE hour_bucket >= now() - INTERVAL 1 HOUR
GROUP BY account_id, hour_bucket
HAVING total_failed > 3
ORDER BY total_failed DESC
LIMIT 10;
