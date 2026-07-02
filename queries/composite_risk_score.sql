-- Composite Risk Score
-- Joins all four detection signals per account into a single ranked view
-- HIGH = 3 signals triggered, MEDIUM = 2, LOW = 1

SELECT
    account_id,
    countIf(source = 'velocity')   AS velocity_flag,
    countIf(source = 'login')      AS login_flag,
    countIf(source = 'high_value') AS high_value_flag,
    velocity_flag + login_flag + high_value_flag AS total_signals,
    if(total_signals >= 3, 'HIGH',
       if(total_signals = 2, 'MEDIUM', 'LOW')) AS risk_level
FROM (
    SELECT account_id, 'velocity' AS source
    FROM mv_velocity_check
    WHERE window_start >= now() - INTERVAL 30 MINUTE
    GROUP BY account_id
    HAVING sum(tx_count) > 5

    UNION ALL

    SELECT account_id, 'login' AS source
    FROM mv_login_anomaly
    WHERE hour_bucket >= now() - INTERVAL 1 HOUR
    GROUP BY account_id
    HAVING sum(failed_attempts) > 3

    UNION ALL

    SELECT account_id, 'high_value' AS source
    FROM mv_high_value_alerts
    WHERE event_time >= now() - INTERVAL 30 MINUTE
    GROUP BY account_id
)
GROUP BY account_id
HAVING total_signals >= 1
ORDER BY total_signals DESC, account_id
LIMIT 15;
